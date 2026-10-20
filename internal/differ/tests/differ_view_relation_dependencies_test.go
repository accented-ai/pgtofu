package differ_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestViewRelationDependenciesRespectSQLScopes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name         string
		definition   string
		dependencies []string
	}{
		{
			name: "parenthesized joins and comma sources",
			definition: `SELECT a.id FROM ((catalog.records a JOIN catalog.labels b ON a.id=b.id)
JOIN catalog.groups c ON a.id=c.id), catalog.extras d`,
			dependencies: []string{"catalog.records", "catalog.labels", "catalog.groups", "catalog.extras"},
		},
		{
			name:         "nonrecursive CTE reads its same-named base relation",
			definition:   `WITH records AS (SELECT id FROM records) SELECT id FROM records`,
			dependencies: []string{"records"},
		},
		{
			name: "recursive CTE references are local",
			definition: `WITH RECURSIVE records AS (
SELECT id FROM catalog.seeds UNION ALL SELECT id+1 FROM records WHERE id<3)
SELECT id FROM records`,
			dependencies: []string{"catalog.seeds"},
		},
		{
			name: "nested CTE does not escape its scope",
			definition: `SELECT id FROM records WHERE EXISTS (
WITH records AS (SELECT id FROM catalog.seeds) SELECT id FROM records)`,
			dependencies: []string{"records", "catalog.seeds"},
		},
		{
			name: "qualified relation is not a CTE reference",
			definition: `WITH records AS (SELECT 1 AS id)
SELECT id FROM public.records WHERE EXISTS (SELECT id FROM records)`,
			dependencies: []string{"public.records"},
		},
		{
			name: "literals and comments do not name relations",
			definition: `SELECT 'FROM catalog.fake' AS description FROM records
-- JOIN catalog.also_fake`,
			dependencies: []string{"records"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			desired := &schema.Database{Views: []schema.View{{
				Schema: "reporting", Name: "summary", Definition: test.definition,
			}}}
			result, err := differ.New(differ.DefaultOptions()).Compare(&schema.Database{}, desired)
			require.NoError(t, err)
			require.Len(t, result.Changes, 1)
			require.ElementsMatch(t, test.dependencies, result.Changes[0].DependsOn)
		})
	}
}
