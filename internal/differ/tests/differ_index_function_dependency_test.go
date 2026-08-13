package differ_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestIndexDependency_AddedFunctionPrecedesReferencingIndex(t *testing.T) {
	t.Parallel()

	table := schema.Table{
		Schema: "search",
		Name:   "documents",
		Columns: []schema.Column{
			{Name: "title", DataType: "TEXT"},
			{Name: "summary", DataType: "TEXT"},
			{Name: "keywords", DataType: "TEXT[]"},
		},
	}
	function := schema.Function{
		Schema:        "search",
		Name:          "build_search_document",
		ArgumentTypes: []string{"TEXT", "TEXT", "TEXT[]"},
		ReturnType:    "TEXT",
		Language:      "sql",
		Volatility:    schema.VolatilityImmutable,
		Body:          "SELECT COALESCE(title, '')",
	}
	desiredTable := table
	desiredTable.Indexes = []schema.Index{{
		Schema:    "search",
		TableName: table.Name,
		Name:      "idx_documents_search_trgm",
		Type:      schema.IndexTypeGIN,
		Columns: []string{
			"search.build_search_document(" +
				"title, summary, keywords) gin_trgm_ops",
		},
	}}

	current := &schema.Database{Tables: []schema.Table{table}}
	desired := &schema.Database{
		Tables:    []schema.Table{desiredTable},
		Functions: []schema.Function{function},
	}

	result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)

	functionKey := differ.FunctionKey(function.Schema, function.Name, function.ArgumentTypes)
	functionPosition := -1
	indexPosition := -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddFunction:
			functionPosition = i
		case differ.ChangeTypeAddIndex:
			indexPosition = i

			require.Contains(t, change.DependsOn, functionKey)
		}
	}

	require.NotEqual(t, -1, functionPosition)
	require.NotEqual(t, -1, indexPosition)
	require.Less(t, functionPosition, indexPosition,
		"a function must exist before an expression index that calls it")
}
