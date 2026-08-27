package differ_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestNormalizeViewDefinitionHandlesPostgresOperatorRewrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sourceExpr string
		storedExpr string
	}{
		{
			name:       "IN becomes ANY array",
			sourceExpr: "r.status IN ('draft', 'ready')",
			storedExpr: "r.status = ANY (ARRAY['draft'::text, 'ready'::text])",
		},
		{
			name:       "NOT IN becomes ALL array",
			sourceExpr: "r.status NOT IN ('draft', 'ready')",
			storedExpr: "r.status <> ALL (ARRAY['draft'::text, 'ready'::text])",
		},
		{
			name:       "single value IN becomes equality",
			sourceExpr: "r.status IN ('draft')",
			storedExpr: "r.status = 'draft'::text",
		},
		{
			name:       "single value NOT IN becomes inequality",
			sourceExpr: "r.status NOT IN ('draft')",
			storedExpr: "r.status <> 'draft'::text",
		},
		{
			name:       "LIKE becomes PostgreSQL operator",
			sourceExpr: "r.label LIKE 'a%'",
			storedExpr: "r.label ~~ 'a%'::text",
		},
		{
			name:       "NOT LIKE becomes PostgreSQL operator",
			sourceExpr: "r.label NOT LIKE 'a%'",
			storedExpr: "r.label !~~ 'a%'::text",
		},
		{
			name:       "ILIKE becomes PostgreSQL operator",
			sourceExpr: "r.label ILIKE 'a%'",
			storedExpr: "r.label ~~* 'a%'::text",
		},
		{
			name:       "NOT ILIKE becomes PostgreSQL operator",
			sourceExpr: "r.label NOT ILIKE 'a%'",
			storedExpr: "r.label !~~* 'a%'::text",
		},
		{
			name:       "SIMILAR TO becomes regex function",
			sourceExpr: "r.label SIMILAR TO 'a%'",
			storedExpr: "r.label ~ similar_to_escape('a%'::text)",
		},
		{
			name:       "NOT SIMILAR TO becomes negated regex function",
			sourceExpr: "r.label NOT SIMILAR TO 'a%'",
			storedExpr: "r.label !~ similar_to_escape('a%'::text)",
		},
		{
			name:       "SIMILAR TO preserves explicit escape",
			sourceExpr: "r.label SIMILAR TO 'a#%' ESCAPE '#'",
			storedExpr: "r.label ~ similar_to_escape('a#%'::text, '#'::text)",
		},
		{
			name:       "BETWEEN becomes bounded comparisons",
			sourceExpr: "r.rank BETWEEN 1 AND 5",
			storedExpr: "r.rank >= 1 AND r.rank <= 5",
		},
		{
			name:       "NOT BETWEEN becomes outside comparisons",
			sourceExpr: "r.rank NOT BETWEEN 1 AND 5",
			storedExpr: "r.rank < 1 OR r.rank > 5",
		},
		{
			name:       "BETWEEN SYMMETRIC becomes both bound orders",
			sourceExpr: "r.rank BETWEEN SYMMETRIC 1 AND 5",
			storedExpr: "r.rank >= 1 AND r.rank <= 5 OR r.rank >= 5 AND r.rank <= 1",
		},
		{
			name:       "NOT BETWEEN SYMMETRIC becomes both outside ranges",
			sourceExpr: "r.rank NOT BETWEEN SYMMETRIC 1 AND 5",
			storedExpr: "(r.rank < 1 OR r.rank > 5) AND (r.rank < 5 OR r.rank > 1)",
		},
		{
			name:       "IS NOT DISTINCT FROM becomes negated predicate",
			sourceExpr: "r.rank IS NOT DISTINCT FROM 1",
			storedExpr: "NOT r.rank IS DISTINCT FROM 1",
		},
		{
			name:       "ISNULL becomes IS NULL",
			sourceExpr: "r.label ISNULL",
			storedExpr: "r.label IS NULL",
		},
		{
			name:       "NOTNULL becomes IS NOT NULL",
			sourceExpr: "r.label NOTNULL",
			storedExpr: "r.label IS NOT NULL",
		},
		{
			name:       "SOME becomes ANY",
			sourceExpr: "r.status = SOME (ARRAY['draft', 'ready'])",
			storedExpr: "r.status = ANY (ARRAY['draft'::text, 'ready'::text])",
		},
		{
			name:       "not equal operator spelling",
			sourceExpr: "r.status != 'draft'",
			storedExpr: "r.status <> 'draft'::text",
		},
		{
			name:       "AT TIME ZONE preserves implicit text cast",
			sourceExpr: "r.created_at AT TIME ZONE 'UTC'",
			storedExpr: "r.created_at AT TIME ZONE 'UTC'::text",
		},
		{
			name:       "starts-with operator preserves implicit text cast",
			sourceExpr: "r.label ^@ 'a'",
			storedExpr: "r.label ^@ 'a'::text",
		},
		{
			name:       "IS NOT NORMALIZED becomes negated predicate",
			sourceExpr: "r.label IS NOT NORMALIZED",
			storedExpr: "NOT (r.label IS NORMALIZED)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := "SELECT " + tt.sourceExpr +
				" AS matches FROM reporting.records AS r"
			stored := "SELECT " + tt.storedExpr +
				" AS matches FROM reporting.records r"

			comparator := differ.NewViewComparator(differ.DefaultOptions())
			assert.True(
				t,
				comparator.AreEqual(
					schema.View{Definition: source},
					schema.View{Definition: stored},
				),
			)
		})
	}
}

func TestNormalizeViewDefinitionKeepsDistinctOperatorsDifferent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		left  string
		right string
	}{
		{
			name:  "IN and NOT IN",
			left:  "r.status IN ('draft', 'ready')",
			right: "r.status NOT IN ('draft', 'ready')",
		},
		{
			name:  "LIKE and ILIKE",
			left:  "r.label LIKE 'a%'",
			right: "r.label ILIKE 'a%'",
		},
		{
			name:  "equality and inequality",
			left:  "r.rank = 1",
			right: "r.rank <> 1",
		},
		{
			name:  "ANY and ALL",
			left:  "r.rank > ANY (ARRAY[1, 2])",
			right: "r.rank > ALL (ARRAY[1, 2])",
		},
		{
			name:  "IS NULL and IS NOT NULL",
			left:  "r.label IS NULL",
			right: "r.label IS NOT NULL",
		},
		{
			name:  "IS DISTINCT and IS NOT DISTINCT",
			left:  "r.rank IS DISTINCT FROM 1",
			right: "r.rank IS NOT DISTINCT FROM 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			comparator := differ.NewViewComparator(differ.DefaultOptions())
			assert.False(t, comparator.AreEqual(
				schema.View{Definition: "SELECT " + tt.left +
					" AS matches FROM reporting.records AS r"},
				schema.View{Definition: "SELECT " + tt.right +
					" AS matches FROM reporting.records AS r"},
			))
		})
	}

	comparator := differ.NewViewComparator(differ.DefaultOptions())
	assert.False(t, comparator.AreEqual(
		schema.View{Definition: "SELECT left_record.minimum_score > right_record.maximum_score " +
			"FROM left_record, right_record"},
		schema.View{Definition: "SELECT right_record.maximum_score > left_record.minimum_score " +
			"FROM left_record, right_record"},
	))
}

func TestNormalizeViewDefinitionRecognizesReversedComparisons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		original string
		reversed string
	}{
		{
			name:     "equality",
			original: "SELECT left_record.id = right_record.left_id FROM left_record, right_record",
			reversed: "SELECT right_record.left_id = left_record.id FROM left_record, right_record",
		},
		{
			name: "inequality",
			original: "SELECT left_record.minimum_score > right_record.maximum_value " +
				"FROM left_record, right_record",
			reversed: "SELECT right_record.maximum_value < left_record.minimum_score " +
				"FROM left_record, right_record",
		},
		{
			name: "not distinct",
			original: "SELECT left_record.region IS NOT DISTINCT FROM right_record.region " +
				"FROM left_record, right_record",
			reversed: "SELECT right_record.region IS NOT DISTINCT FROM left_record.region " +
				"FROM left_record, right_record",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			comparator := differ.NewViewComparator(differ.DefaultOptions())
			assert.True(t, comparator.AreEqual(
				schema.View{Definition: tt.original},
				schema.View{Definition: tt.reversed},
			))
		})
	}
}

func TestPostgresOperatorRewritesApplyToAllStoredQueryObjects(t *testing.T) {
	t.Parallel()

	const source = `SELECT
    r.id
FROM reporting.records AS r
WHERE r.status NOT IN ('draft', 'ready')`

	const stored = ` SELECT r.id
   FROM reporting.records r
  WHERE (r.status <> ALL (ARRAY['draft'::text, 'ready'::text]))`

	tests := []struct {
		name    string
		current *schema.Database
		desired *schema.Database
	}{
		{
			name: "view",
			current: &schema.Database{Views: []schema.View{{
				Schema: "reporting", Name: "record_overview", Definition: stored,
			}}},
			desired: &schema.Database{Views: []schema.View{{
				Schema: "reporting", Name: "record_overview", Definition: source,
			}}},
		},
		{
			name: "materialized view",
			current: &schema.Database{MaterializedViews: []schema.MaterializedView{{
				Schema: "reporting", Name: "record_overview", Definition: stored,
			}}},
			desired: &schema.Database{MaterializedViews: []schema.MaterializedView{{
				Schema: "reporting", Name: "record_overview", Definition: source,
			}}},
		},
		{
			name: "continuous aggregate",
			current: &schema.Database{ContinuousAggregates: []schema.ContinuousAggregate{{
				Schema: "reporting", ViewName: "record_overview", Query: stored,
				HypertableSchema: "reporting", HypertableName: "records",
			}}},
			desired: &schema.Database{ContinuousAggregates: []schema.ContinuousAggregate{{
				Schema: "reporting", ViewName: "record_overview", Query: source,
				HypertableSchema: "reporting", HypertableName: "records",
			}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := differ.New(differ.DefaultOptions()).Compare(tt.current, tt.desired)
			require.NoError(t, err)
			assert.Empty(t, result.Changes)
		})
	}
}
