package graph

import (
	"fmt"
	"maps"
	"sort"
)

type DirectedGraph[T comparable] struct {
	nodes    map[T]bool
	edges    map[T]map[T]bool
	inDegree map[T]int
}

func NewDirectedGraph[T comparable]() *DirectedGraph[T] {
	return &DirectedGraph[T]{
		nodes:    make(map[T]bool),
		edges:    make(map[T]map[T]bool),
		inDegree: make(map[T]int),
	}
}

func (g *DirectedGraph[T]) AddNode(node T) {
	g.nodes[node] = true
	if _, exists := g.inDegree[node]; !exists {
		g.inDegree[node] = 0
	}

	if g.edges[node] == nil {
		g.edges[node] = make(map[T]bool)
	}
}

func (g *DirectedGraph[T]) HasNode(node T) bool {
	return g.nodes[node]
}

func (g *DirectedGraph[T]) AddEdge(from, to T) error {
	if !g.nodes[from] || !g.nodes[to] {
		return fmt.Errorf("both nodes must exist before adding edge: %v -> %v", from, to)
	}

	if g.edges[to] == nil {
		g.edges[to] = make(map[T]bool)
	}

	if !g.edges[to][from] {
		g.edges[to][from] = true
		g.inDegree[from]++
	}

	return nil
}

func (g *DirectedGraph[T]) GetRemainingNodes(inDegree map[T]int) []T {
	remaining := make([]T, 0)

	for node, degree := range inDegree {
		if degree > 0 {
			remaining = append(remaining, node)
		}
	}

	return remaining
}

type CycleError[T comparable] struct {
	Remaining []T
	Message   string
}

func (e *CycleError[T]) Error() string {
	if e.Message != "" {
		return e.Message
	}

	return fmt.Sprintf("circular dependency detected: %v", e.Remaining)
}

func (g *DirectedGraph[T]) TopologicalSort() ([]T, error) {
	inDegree := make(map[T]int)
	maps.Copy(inDegree, g.inDegree)

	var queue []T

	for node := range g.nodes {
		if inDegree[node] == 0 {
			queue = append(queue, node)
		}
	}

	sortQueue(queue)

	var result []T

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		result = append(result, node)

		if g.edges[node] != nil {
			for dependent := range g.edges[node] {
				inDegree[dependent]--
				if inDegree[dependent] == 0 {
					queue = append(queue, dependent)
					sortQueue(queue)
				}
			}
		}
	}

	if len(result) != len(g.nodes) {
		remaining := g.GetRemainingNodes(inDegree)
		return nil, &CycleError[T]{Remaining: remaining}
	}

	return result, nil
}

func sortQueue[T comparable](queue []T) {
	if len(queue) <= 1 {
		return
	}

	switch v := any(queue[0]).(type) {
	case string:
		sort.Slice(queue, func(i, j int) bool {
			return any(queue[i]).(string) < any(queue[j]).(string) //nolint:forcetypeassert
		})
	case int:
		sort.Slice(queue, func(i, j int) bool {
			return any(queue[i]).(int) < any(queue[j]).(int) //nolint:forcetypeassert
		})
	default:
		_ = v
	}
}
