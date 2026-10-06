package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestViewFormattingCompactsValuesSourceInLateralQuery(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema: "reporting", Name: "visible_records",
		Definition: `SELECT record.id, visible.semantics
FROM reporting.records AS record
INNER JOIN LATERAL (
    SELECT reporting.visible_semantics(record.id) AS semantics
    FROM (VALUES (TRUE)) AS request_scope(is_bounded)
    OFFSET 0
) AS visible ON visible.semantics IS NOT NULL`,
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
	assert.Contains(
		t,
		statement.SQL,
		"        FROM (VALUES (TRUE)) AS request_scope(is_bounded)\n        OFFSET 0",
	)
}
