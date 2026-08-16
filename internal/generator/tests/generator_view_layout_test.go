package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestViewFormattingUsesLintCompatibleCTELayout(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema: "reporting",
		Name:   "current_records",
		Definition: `WITH direct_records AS (
    SELECT record.id, record.status
    FROM reporting.records AS record
    INNER JOIN reporting.record_links AS link
        ON record.id = link.record_id
    INNER JOIN reporting.record_windows AS record_window
        ON record.rank BETWEEN record_window.minimum_rank AND record_window.maximum_rank
),
scoped_records AS (
    SELECT record.id, record.status
    FROM reporting.records AS record
    INNER JOIN reporting.record_scopes AS scope
        ON record.id = scope.record_id
        AND record.scope = scope.scope
        AND record.owner_id IS NOT DISTINCT FROM scope.owner_id
    WHERE record.is_visible = false
        AND record.status IS DISTINCT FROM 'archived'
)
SELECT * FROM direct_records
UNION ALL
SELECT * FROM scoped_records`,
	}
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{Views: []schema.View{view}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddView,
			ObjectName: differ.ViewKey(view.Schema, view.Name),
		}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	assert.Contains(t, statement.SQL, `WITH
direct_records AS (
    SELECT
        record.id,`)
	assert.Contains(t, statement.SQL, `scoped_records AS (
    SELECT
        record.id,`)
	assert.Contains(t, statement.SQL, `INNER JOIN
        reporting.record_links AS link
        ON record.id = link.record_id`)
	assert.Contains(t, statement.SQL, `INNER JOIN
        reporting.record_windows AS record_window
        ON
            record.rank BETWEEN record_window.minimum_rank
            AND record_window.maximum_rank`)
	assert.Contains(t, statement.SQL, `INNER JOIN
        reporting.record_scopes AS scope
        ON
            record.id = scope.record_id
            AND record.scope = scope.scope
            AND record.owner_id IS NOT DISTINCT FROM scope.owner_id`)
	assert.Contains(t, statement.SQL, `SELECT *
FROM
    direct_records
UNION ALL
SELECT *
FROM
    scoped_records`)
	assert.Contains(t, statement.SQL, `WHERE
        record.is_visible = FALSE
        AND record.status IS DISTINCT FROM 'archived'`)
	assert.NotContains(t, statement.SQL, "WITH\n    direct_records")
}
