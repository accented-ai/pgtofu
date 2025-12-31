package differ

import (
	"strings"
)

type CycleNode interface {
	comparable
}

type CycleGraph[T CycleNode] struct {
	GetEdges     func(T) []T
	FormatNode   func(T) string
	HasRemaining func(T) bool
}

func FindCycle[T CycleNode](
	remaining []T,
	graph CycleGraph[T],
) string {
	if len(remaining) == 0 {
		return "unknown cycle"
	}

	var cycle []string

	path := make([]T, 0)
	pathSet := make(map[T]bool)
	visited := make(map[T]bool)

	var dfs func(T) bool

	dfs = func(node T) bool {
		if pathSet[node] {
			startIdx := 0

			for i, n := range path {
				if n == node {
					startIdx = i
					break
				}
			}

			cyclePath := append(path[startIdx:], node) //nolint:gocritic

			cycle = make([]string, len(cyclePath))
			for i, n := range cyclePath {
				cycle[i] = graph.FormatNode(n)
			}

			return true
		}

		if visited[node] {
			return false
		}

		visited[node] = true
		pathSet[node] = true
		path = append(path, node)

		edges := graph.GetEdges(node)
		for _, dep := range edges {
			if graph.HasRemaining(dep) && dfs(dep) {
				return true
			}
		}

		path = path[:len(path)-1]
		pathSet[node] = false

		return false
	}

	for _, node := range remaining {
		if !visited[node] {
			if dfs(node) {
				break
			}
		}
	}

	if len(cycle) == 0 {
		cycle = make([]string, len(remaining))
		for i, node := range remaining {
			cycle[i] = graph.FormatNode(node)
		}
	}

	return strings.Join(cycle, " -> ")
}
