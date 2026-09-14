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
	modifiedViews := make(map[string]int)

	for index, change := range reversed {
		dependencyGraph.AddNode(index)

		if change.Type != differ.ChangeTypeModifyView {
			continue
		}

		for _, name := range viewDependencyLookupNames(change.ObjectName) {
			modifiedViews[name] = index
		}
	}

	for dependentIndex, change := range reversed {
		if change.Type != differ.ChangeTypeModifyView {
			continue
		}

		for _, dependency := range change.RollbackDependsOn {
			providerIndex, exists := modifiedViews[normalizeObjectName(dependency)]
			if !exists && !strings.Contains(dependency, ".") {
				providerIndex, exists = modifiedViews[strings.ToLower(dependency)]
			}

			if !exists || providerIndex == dependentIndex {
				continue
			}

			if err := dependencyGraph.AddEdge(dependentIndex, providerIndex); err != nil {
				return nil, fmt.Errorf("order view rollback dependency: %w", err)
			}
		}
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

func viewDependencyLookupNames(objectName string) []string {
	normalized := normalizeObjectName(objectName)
	names := []string{normalized}

	parts := strings.Split(normalized, ".")
	if len(parts) == 2 && parts[0] == schema.DefaultSchema {
		names = append(names, parts[1])
	}

	return names
}

func modifiedViewDependencyCrossesBoundary(changes []differ.Change, splitAfter int) bool {
	leftViews := modifiedViewNames(changes[:splitAfter+1])
	rightViews := modifiedViewNames(changes[splitAfter+1:])

	return changesDependOnViews(changes[:splitAfter+1], rightViews) ||
		changesDependOnViews(changes[splitAfter+1:], leftViews)
}

func modifiedViewNames(changes []differ.Change) map[string]struct{} {
	names := make(map[string]struct{})

	for _, change := range changes {
		if change.Type != differ.ChangeTypeModifyView {
			continue
		}

		for _, name := range viewDependencyLookupNames(change.ObjectName) {
			names[name] = struct{}{}
		}
	}

	return names
}

func changesDependOnViews(changes []differ.Change, viewNames map[string]struct{}) bool {
	for _, change := range changes {
		if change.Type != differ.ChangeTypeModifyView {
			continue
		}

		dependencies := append(
			slices.Clone(change.DependsOn),
			change.RollbackDependsOn...,
		)
		for _, dependency := range dependencies {
			for _, name := range viewDependencyLookupNames(dependency) {
				if _, exists := viewNames[name]; exists {
					return true
				}
			}
		}
	}

	return false
}
