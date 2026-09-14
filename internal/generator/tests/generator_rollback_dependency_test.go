package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDroppedTableRollbackRestoresTableBeforeDependents(t *testing.T) {
	t.Parallel()

	triggerFunction := schema.Function{
		Schema:        schema.DefaultSchema,
		Name:          "validate_record",
		ArgumentTypes: []string{},
		ReturnType:    "trigger",
		Language:      "plpgsql",
		Body:          "BEGIN RETURN NEW; END;",
		Volatility:    schema.VolatilityVolatile,
	}
	table := schema.Table{
		Schema: schema.DefaultSchema,
		Name:   "records",
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
			{Name: "state", DataType: "text", IsNullable: false, Position: 2},
		},
		Indexes: []schema.Index{{
			Schema:    schema.DefaultSchema,
			Name:      "idx_records_state",
			TableName: "records",
			Columns:   []string{"state"},
		}},
	}
	trigger := schema.Trigger{
		Schema:         schema.DefaultSchema,
		Name:           "validate_record",
		TableName:      "records",
		Timing:         "BEFORE",
		Events:         []string{"INSERT"},
		ForEachRow:     true,
		FunctionSchema: schema.DefaultSchema,
		FunctionName:   triggerFunction.Name,
	}

	current := &schema.Database{
		Tables:    []schema.Table{table},
		Functions: []schema.Function{triggerFunction},
		Triggers:  []schema.Trigger{trigger},
	}
	desired := &schema.Database{Functions: []schema.Function{triggerFunction}}

	diffResult, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.MaxOperationsPerFile = 1
	generated, err := generator.New(opts).Generate(diffResult)
	require.NoError(t, err)
	require.Len(t, generated.Migrations, 1,
		"a dropped table and its dependents must share a reversible migration")

	down := generated.Migrations[0].DownFile.Content
	tablePosition := strings.Index(down, "CREATE TABLE public.records")
	indexPosition := strings.Index(down, "CREATE INDEX idx_records_state")
	triggerPosition := strings.Index(down, "CREATE TRIGGER validate_record")

	require.GreaterOrEqual(t, tablePosition, 0)
	require.GreaterOrEqual(t, indexPosition, 0)
	require.GreaterOrEqual(t, triggerPosition, 0)
	require.Less(t, tablePosition, indexPosition)
	require.Less(t, tablePosition, triggerPosition)
}
