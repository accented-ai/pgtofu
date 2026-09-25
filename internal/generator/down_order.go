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

	if err := addViewRollbackEdgesForAddedFunctions(dependencyGraph, reversed); err != nil {
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

func addViewRollbackEdgesForAddedFunctions(
	dependencyGraph *graph.DirectedGraph[int],
	changes []differ.Change,
) error {
	for functionIndex, change := range changes {
		if change.Type != differ.ChangeTypeAddFunction {
			continue
		}

		for viewIndex, viewChange := range changes {
			switch viewChange.Type {
			case differ.ChangeTypeAddView,
				differ.ChangeTypeModifyView,
				differ.ChangeTypeAddMaterializedView,
				differ.ChangeTypeModifyMaterializedView:
				if slices.Contains(viewChange.DependsOn, change.ObjectName) {
					if err := dependencyGraph.AddEdge(functionIndex, viewIndex); err != nil {
						return fmt.Errorf("order rollback function drop: %w", err)
					}
				}
			}
		}
	}

	return nil
}

func providesObjectOnRollback(change differ.Change) bool {
	switch change.Type {
	case differ.ChangeTypeDropTable,
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
