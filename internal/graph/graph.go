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

func (g *DirectedGraph[T]) CondensationOrder() [][]T {
	components := g.stronglyConnectedComponents()
	if len(components) == 0 {
		return nil
	}

	componentOf := make(map[T]int, len(g.nodes))

	for i, component := range components {
		for _, node := range component {
			componentOf[node] = i
		}
	}

	componentEdges := make([]map[int]bool, len(components))
	componentInDegree := make([]int, len(components))

	for provider, dependents := range g.edges {
		for dependent := range dependents {
			providerComponent := componentOf[provider]

			dependentComponent := componentOf[dependent]
			if providerComponent == dependentComponent {
				continue
			}

			if componentEdges[providerComponent] == nil {
				componentEdges[providerComponent] = make(map[int]bool)
			}

			if !componentEdges[providerComponent][dependentComponent] {
				componentEdges[providerComponent][dependentComponent] = true
				componentInDegree[dependentComponent]++
			}
		}
	}

	var queue []int

	for i := range components {
		if componentInDegree[i] == 0 {
			queue = append(queue, i)
		}
	}

	sortComponentQueue(queue, components)

	result := make([][]T, 0, len(components))

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		result = append(result, components[current])

		for dependent := range componentEdges[current] {
			componentInDegree[dependent]--
			if componentInDegree[dependent] == 0 {
				queue = append(queue, dependent)
				sortComponentQueue(queue, components)
			}
		}
	}

	return result
}

// stronglyConnectedComponents computes SCCs with Tarjan's algorithm. Each
// component's members are sorted, and roots are visited in sorted order so
// the result is deterministic.
func (g *DirectedGraph[T]) stronglyConnectedComponents() [][]T {
	nodes := make([]T, 0, len(g.nodes))
	for node := range g.nodes {
		nodes = append(nodes, node)
	}

	sortQueue(nodes)

	var (
		stack      []T
		components [][]T
	)

	index := make(map[T]int, len(nodes))
	lowLink := make(map[T]int, len(nodes))
	onStack := make(map[T]bool, len(nodes))
	nextIndex := 0

	var strongConnect func(node T)

	strongConnect = func(node T) {
		index[node] = nextIndex
		lowLink[node] = nextIndex
		nextIndex++

		stack = append(stack, node)
		onStack[node] = true

		neighbors := make([]T, 0, len(g.edges[node]))
		for neighbor := range g.edges[node] {
			neighbors = append(neighbors, neighbor)
		}

		sortQueue(neighbors)

		for _, neighbor := range neighbors {
			if _, visited := index[neighbor]; !visited {
				strongConnect(neighbor)

				if lowLink[neighbor] < lowLink[node] {
					lowLink[node] = lowLink[neighbor]
				}
			} else if onStack[neighbor] && index[neighbor] < lowLink[node] {
				lowLink[node] = index[neighbor]
			}
		}

		if lowLink[node] == index[node] {
			var component []T

			for {
				top := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[top] = false

				component = append(component, top)
				if top == node {
					break
				}
			}

			sortQueue(component)
			components = append(components, component)
		}
	}

	for _, node := range nodes {
		if _, visited := index[node]; !visited {
			strongConnect(node)
		}
	}

	return components
}

func sortComponentQueue[T comparable](queue []int, components [][]T) {
	if len(queue) <= 1 {
		return
	}

	sort.Slice(queue, func(i, j int) bool {
		return lessNode(components[queue[i]][0], components[queue[j]][0])
	})
}

func lessNode[T comparable](a, b T) bool {
	switch av := any(a).(type) {
	case string:
		return av < any(b).(string) //nolint:forcetypeassert
	case int:
		return av < any(b).(int) //nolint:forcetypeassert
	default:
		return false
	}
}
