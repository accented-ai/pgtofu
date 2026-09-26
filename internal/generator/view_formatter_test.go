package generator //nolint:testpackage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatViewQueryPreservesHyphenatedAnyArrayLiteral(t *testing.T) {
	t.Parallel()

	formatted, err := formatViewQuery(`
		SELECT 'value' = ANY(ARRAY[
			'pipeline-primary-v12',
			'pipeline-fallback-v1'
		])
	`)
	require.NoError(t, err)
	assert.Contains(t, formatted, "'pipeline-primary-v12'")
	assert.Contains(t, formatted, "'pipeline-fallback-v1'")
	assert.NotContains(t, formatted, "'pipeline - primary - v12'")
}

func TestFormatViewQueryWrapsLongJSONAccess(t *testing.T) {
	t.Parallel()

	const longJSONPath = "{workflow,authority,initial_decision," +
		"faithfulness_self_check,localized_instruction_language_is_semantically_correct}"

	formatted, err := formatViewQuery(`
        SELECT
            CASE
                WHEN (
                    record_dependency.selection_evidence #>> '` + longJSONPath + `'
                ) = 'true' THEN 'ready'
                ELSE 'pending'
            END AS state
        FROM reporting.record_dependencies AS record_dependency
    `)
	require.NoError(t, err)

	assert.Contains(t, formatted,
		"record_dependency.selection_evidence\n"+
			strings.Repeat(" ", 16)+
			"#>> '"+longJSONPath+"'",
	)

	for line := range strings.SplitSeq(formatted, "\n") {
		assert.LessOrEqual(t, len(line), generatedSQLLineLength)
	}
}

func TestFormatViewQueryKeepsSelectModifiersWithSelect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name: "distinct in CTE with multiple targets",
			query: `WITH selected_items AS (
				SELECT DISTINCT item.id AS item_id, category.name AS category_name
				FROM example.items AS item
				INNER JOIN example.categories AS category ON item.category_id = category.id
			)
			SELECT * FROM selected_items`,
			want: "selected_items AS (\n" +
				"    SELECT DISTINCT\n" +
				"        item.id AS item_id,\n" +
				"        category.name AS category_name",
		},
		{
			name:  "distinct with one target",
			query: `SELECT DISTINCT item.id FROM example.items AS item`,
			want:  "SELECT DISTINCT item.id\nFROM example.items AS item",
		},
		{
			name: "distinct on with multiple targets",
			query: `SELECT DISTINCT ON (item.group_id) item.id, item.state
				FROM example.items AS item ORDER BY item.group_id, item.id`,
			want: "SELECT DISTINCT ON (item.group_id)\n" +
				"    item.id,\n" +
				"    item.state",
		},
		{
			name: "distinct on with wrapped expressions",
			query: `SELECT DISTINCT ON (
				item.very_long_column_name_that_ends_here,
				item.another_very_long_column_name_that_ends_here,
				item.third_very_long_column_name_that_ends_here
			) item.id, item.name
			FROM example.items AS item`,
			want: "SELECT DISTINCT ON (\n" +
				"    item.very_long_column_name_that_ends_here,\n" +
				"    item.another_very_long_column_name_that_ends_here,\n" +
				"    item.third_very_long_column_name_that_ends_here\n" +
				")\n" +
				"    item.id,\n" +
				"    item.name",
		},
		{
			name:  "distinct inside aggregate",
			query: `SELECT COUNT(DISTINCT item.state) AS state_count FROM example.items AS item`,
			want:  "SELECT COUNT(DISTINCT item.state) AS state_count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			formatted, err := formatViewQuery(tt.query)
			require.NoError(t, err)
			assert.Contains(t, formatted, tt.want)
			assert.NotContains(t, formatted, "SELECT\n    DISTINCT")
		})
	}
}
