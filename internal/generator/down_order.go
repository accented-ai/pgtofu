package generator

import (
	"fmt"
	"slices"
	"strings"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/graph"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func orderChangesForDown(changes []differ.Change) ([]differ.Change, error) {
	reversed := make([]differ.Change, len(changes))
	for index := range changes {
		reversed[index] = changes[len(changes)-1-index]
	}

	dependencyGraph := graph.NewDirectedGraph[int]()
	providers := make(map[string][]int)

	for index, change := range reversed {
		dependencyGraph.AddNode(index)

		if !providesObjectOnRollback(change) {
			continue
		}

		for _, name := range dependencyLookupNames(change.ObjectName) {
			providers[name] = append(providers[name], index)
		}
	}

	for dependentIndex, change := range reversed {
		for _, dependency := range change.RollbackDependsOn {
			for _, name := range dependencyLookupNames(dependency) {
				for _, providerIndex := range providers[name] {
					if providerIndex == dependentIndex {
						continue
					}

					if err := dependencyGraph.AddEdge(dependentIndex, providerIndex); err != nil {
						return nil, fmt.Errorf("order rollback dependency: %w", err)
					}
				}
			}
		}
	}

	if err := addRollbackRecreationEdges(dependencyGraph, reversed); err != nil {
		return nil, err
	}

	if err := addRollbackEdgesForAddedObjects(dependencyGraph, reversed); err != nil {
		return nil, err
	}

	order, err := dependencyGraph.TopologicalSort()
	if err != nil {
		return nil, fmt.Errorf("order down migration: %w", err)
	}

	ordered := make([]differ.Change, 0, len(reversed))
	for _, index := range order {
		ordered = append(ordered, reversed[index])
	}

	return ordered, nil
}

func addRollbackRecreationEdges(
	dependencyGraph *graph.DirectedGraph[int],
	changes []differ.Change,
) error {
	for restoreIndex, restore := range changes {
		for removeIndex, remove := range changes {
			if !isRecreationPair(restore, remove) {
				continue
			}

			if err := dependencyGraph.AddEdge(restoreIndex, removeIndex); err != nil {
				return fmt.Errorf("order rollback object recreation: %w", err)
			}
		}
	}

	return nil
}

func isRecreationPair(drop, add differ.Change) bool {
	if drop.ObjectName != add.ObjectName || drop.ObjectType != add.ObjectType {
		return false
	}

	switch drop.Type {
	case differ.ChangeTypeDropView:
		return add.Type == differ.ChangeTypeAddView
	case differ.ChangeTypeDropMaterializedView:
		return add.Type == differ.ChangeTypeAddMaterializedView
	case differ.ChangeTypeDropTrigger:
		return add.Type == differ.ChangeTypeAddTrigger
	case differ.ChangeTypeDropPolicy:
		return add.Type == differ.ChangeTypeAddPolicy
	case differ.ChangeTypeDropConstraint:
		previous, _ := drop.Details["constraint"].(*schema.Constraint)
		next, _ := add.Details["constraint"].(*schema.Constraint)

		return add.Type == differ.ChangeTypeAddConstraint && previous != nil && next != nil &&
			previous.Name == next.Name
	default:
		return false
	}
}

func addRollbackEdgesForAddedObjects(
	dependencyGraph *graph.DirectedGraph[int],
	changes []differ.Change,
) error {
	for providerIndex, change := range changes {
		if change.Type != differ.ChangeTypeAddFunction &&
			change.Type != differ.ChangeTypeAddView &&
			change.Type != differ.ChangeTypeAddMaterializedView &&
			change.Type != differ.ChangeTypeAddColumn {
			continue
		}

		for viewIndex, viewChange := range changes {
			switch viewChange.Type {
			case differ.ChangeTypeAddView,
				differ.ChangeTypeModifyView,
				differ.ChangeTypeAddMaterializedView,
				differ.ChangeTypeModifyMaterializedView,
				differ.ChangeTypeAddIndex,
				differ.ChangeTypeModifyIndex,
				differ.ChangeTypeAddTrigger,
				differ.ChangeTypeModifyTrigger,
				differ.ChangeTypeAddPolicy,
				differ.ChangeTypeModifyPolicy:
				if slices.Contains(viewChange.DependsOn, change.ObjectName) {
					if err := dependencyGraph.AddEdge(providerIndex, viewIndex); err != nil {
						return fmt.Errorf("order rollback object drop: %w", err)
					}
				}
			}
		}
	}

	return nil
}

func providesObjectOnRollback(change differ.Change) bool {
	switch change.Type {
	case differ.ChangeTypeDropConstraint, differ.ChangeTypeModifyConstraint:
		constraint, _ := change.Details["constraint"].(*schema.Constraint)
		if constraint == nil {
			constraint, _ = change.Details["current"].(*schema.Constraint)
		}

		return constraint != nil && (constraint.IsPrimaryKey() || constraint.IsUnique())
	case differ.ChangeTypeDropIndex, differ.ChangeTypeModifyIndex:
		index, _ := change.Details["index"].(*schema.Index)
		if index == nil {
			index, _ = change.Details["current"].(*schema.Index)
		}

		return index != nil && index.IsUnique && !index.IsPartial() && !index.IsExpression()
	case differ.ChangeTypeDropTable,
		differ.ChangeTypeDropColumn,
		differ.ChangeTypeDropCustomType,
		differ.ChangeTypeDropView,
		differ.ChangeTypeDropMaterializedView,
		differ.ChangeTypeDropFunction:
		return true
	case differ.ChangeTypeModifyView,
		differ.ChangeTypeModifyMaterializedView,
		differ.ChangeTypeModifyFunction:
		_, hasCurrent := change.Details["current"]
		return hasCurrent
	default:
		return false
	}
}

func dependencyLookupNames(objectName string) []string {
	normalized := normalizeObjectName(objectName)
	names := []string{normalized}

	parts := strings.Split(normalized, ".")
	if len(parts) == 2 && parts[0] == schema.DefaultSchema {
		names = append(names, parts[1])
	}

	return names
}

func rollbackDependencyCrossesBoundary(changes []differ.Change, splitAfter int) bool {
	leftProviders := rollbackProviderNames(changes[:splitAfter+1])
	rightProviders := rollbackProviderNames(changes[splitAfter+1:])

	return changesDependOnRollbackProviders(changes[:splitAfter+1], rightProviders) ||
		changesDependOnRollbackProviders(changes[splitAfter+1:], leftProviders)
}

func hasOpenRecreation(changes []differ.Change) bool {
	open := make(map[string]bool)

	for _, change := range changes {
		key := change.ObjectType + "\x00" + change.ObjectName
		switch change.Type {
		case differ.ChangeTypeDropPolicy,
			differ.ChangeTypeDropConstraint,
			differ.ChangeTypeDropView,
			differ.ChangeTypeDropMaterializedView,
			differ.ChangeTypeDropTrigger:
			recreate, _ := change.Details["recreate"].(bool)

			willBeRecreated, _ := change.Details["will_be_recreated"].(bool)
			if recreate || willBeRecreated {
				open[key] = true
			}
		case differ.ChangeTypeAddPolicy,
			differ.ChangeTypeAddConstraint,
			differ.ChangeTypeAddView,
			differ.ChangeTypeAddMaterializedView,
			differ.ChangeTypeAddTrigger:
			delete(open, key)
		}
	}

	return len(open) > 0
}

func rollbackProviderNames(changes []differ.Change) map[string]struct{} {
	names := make(map[string]struct{})

	for _, change := range changes {
		if !providesObjectOnRollback(change) {
			continue
		}

		for _, name := range dependencyLookupNames(change.ObjectName) {
			names[name] = struct{}{}
		}
	}

	return names
}

func changesDependOnRollbackProviders(
	changes []differ.Change,
	providerNames map[string]struct{},
) bool {
	for _, change := range changes {
		for _, dependency := range change.RollbackDependsOn {
			for _, name := range dependencyLookupNames(dependency) {
				if _, exists := providerNames[name]; exists {
					return true
				}
			}
		}
	}

	return false
}
