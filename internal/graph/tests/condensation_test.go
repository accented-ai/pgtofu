package graph_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/graph"
)

func buildGraph(nodes []string, deps map[string][]string) *graph.DirectedGraph[string] {
	dg := graph.NewDirectedGraph[string]()
	for _, node := range nodes {
		dg.AddNode(node)
	}

	for from, tos := range deps {
		for _, to := range tos {
			_ = dg.AddEdge(from, to)
		}
	}

	return dg
}

func TestCondensationOrderAcyclicMatchesTopologicalSort(t *testing.T) {
	t.Parallel()

	dg := buildGraph(
		[]string{"alpha", "beta", "gamma", "delta"},
		map[string][]string{
			"beta":  {"alpha"},
			"gamma": {"beta"},
			"delta": {"alpha"},
		},
	)

	ordered, err := dg.TopologicalSort()
	require.NoError(t, err)

	condensed := dg.CondensationOrder()
	require.Len(t, condensed, 4)

	flattened := make([]string, 0, len(condensed))
	for _, component := range condensed {
		require.Len(t, component, 1)
		flattened = append(flattened, component[0])
	}

	assert.Equal(t, ordered, flattened)
}

func TestCondensationOrderMergesCycle(t *testing.T) {
	t.Parallel()

	// beta and gamma depend on each other; alpha is a shared dependency and
	// delta depends on the cycle.
	dg := buildGraph(
		[]string{"alpha", "beta", "gamma", "delta"},
		map[string][]string{
			"beta":  {"alpha", "gamma"},
			"gamma": {"beta"},
			"delta": {"gamma"},
		},
	)

	_, err := dg.TopologicalSort()
	require.Error(t, err, "sanity check: plain topological sort rejects the cycle")

	condensed := dg.CondensationOrder()
	assert.Equal(t, [][]string{{"alpha"}, {"beta", "gamma"}, {"delta"}}, condensed)
}

func TestCondensationOrderMultipleCycles(t *testing.T) {
	t.Parallel()

	dg := buildGraph(
		[]string{"alpha", "beta", "gamma", "delta", "epsilon"},
		map[string][]string{
			"alpha":   {"beta"},
			"beta":    {"alpha"},
			"gamma":   {"delta", "alpha"},
			"delta":   {"gamma"},
			"epsilon": {"delta"},
		},
	)

	condensed := dg.CondensationOrder()
	assert.Equal(t, [][]string{{"alpha", "beta"}, {"delta", "gamma"}, {"epsilon"}}, condensed)
}

func TestCondensationOrderDeterministic(t *testing.T) {
	t.Parallel()

	nodes := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	deps := map[string][]string{
		"beta":  {"alpha"},
		"gamma": {"alpha"},
		"delta": {"epsilon"},
	}

	first := buildGraph(nodes, deps).CondensationOrder()
	for range 20 {
		assert.Equal(t, first, buildGraph(nodes, deps).CondensationOrder())
	}
}

func TestCondensationOrderEmptyGraph(t *testing.T) {
	t.Parallel()

	dg := graph.NewDirectedGraph[string]()
	assert.Nil(t, dg.CondensationOrder())
}
