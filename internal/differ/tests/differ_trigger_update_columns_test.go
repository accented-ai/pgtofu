package differ_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func triggerUpdateColumnsDB(updateColumns []string) *schema.Database {
	return &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "status", DataType: "text", IsNullable: false, Position: 2},
					{Name: "shipped_at", DataType: "timestamptz", IsNullable: true, Position: 3},
				},
			},
		},
		Triggers: []schema.Trigger{
			{
				Schema:         schema.DefaultSchema,
				Name:           "notify_changes",
				TableName:      "items",
				Timing:         "AFTER",
				Events:         []string{"INSERT", "UPDATE"},
				UpdateColumns:  updateColumns,
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "notify",
			},
		},
	}
}

func TestTriggerComparator_UpdateOfColumnsRoundTrip(t *testing.T) {
	t.Parallel()

	// The extractor reads columns from tgattr; the parser keeps the written
	// order. The sorted comparison must treat the two orderings as equal.
	current := triggerUpdateColumnsDB([]string{"shipped_at", "status"})
	desired := triggerUpdateColumnsDB([]string{"status", "shipped_at"})

	d := differ.New(differ.DefaultOptions())
	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	require.Empty(t, result.GetChangesByType(differ.ChangeTypeModifyTrigger),
		"identical column-scoped triggers should not drift")
	require.Empty(t, result.GetChangesByType(differ.ChangeTypeAddTrigger))
	require.Empty(t, result.GetChangesByType(differ.ChangeTypeDropTrigger))
}

func TestTriggerComparator_DetectsUpdateColumnChange(t *testing.T) {
	t.Parallel()

	current := triggerUpdateColumnsDB([]string{"status"})
	desired := triggerUpdateColumnsDB([]string{"status", "shipped_at"})

	d := differ.New(differ.DefaultOptions())
	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	require.Len(t, result.GetChangesByType(differ.ChangeTypeModifyTrigger), 1,
		"changing the UPDATE OF column set should modify the trigger")
}
