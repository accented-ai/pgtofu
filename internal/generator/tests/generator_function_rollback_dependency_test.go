package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDroppedSQLFunctionRollbackFollowsReferencedView(t *testing.T) {
	t.Parallel()

	table := schema.Table{
		Schema: "source_data",
		Name:   "records",
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
			{Name: "state", DataType: "text", IsNullable: false, Position: 2},
		},
	}
	currentView := schema.View{
		Schema:     "source_data",
		Name:       "record_states",
		Definition: "SELECT id, state AS legacy_state FROM source_data.records",
	}
	desiredView := currentView
	desiredView.Definition = "SELECT id, state AS current_state FROM source_data.records"
	legacyFunction := schema.Function{
		Schema:        "reporting",
		Name:          "record_has_legacy_state",
		ArgumentTypes: []string{"bigint"},
		ArgumentNames: []string{"target_id"},
		ReturnType:    "boolean",
		Language:      "sql",
		Body: "SELECT legacy_state IS NOT NULL " +
			"FROM source_data.record_states WHERE id = target_id",
		Volatility: schema.VolatilityStable,
	}
	currentFunction := legacyFunction
	currentFunction.Name = "record_has_current_state"
	currentFunction.Body = "SELECT current_state IS NOT NULL " +
		"FROM source_data.record_states WHERE id = target_id"

	current := &schema.Database{
		Tables:    []schema.Table{table},
		Views:     []schema.View{currentView},
		Functions: []schema.Function{legacyFunction},
	}
	desired := &schema.Database{
		Tables:    []schema.Table{table},
		Views:     []schema.View{desiredView},
		Functions: []schema.Function{currentFunction},
	}

	diffResult, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.MaxOperationsPerFile = 1
	generated, err := generator.New(opts).Generate(diffResult)
	require.NoError(t, err)
	require.Len(t, generated.Migrations, 1,
		"a restored function must share a migration with its restored view")

	down := generated.Migrations[0].DownFile.Content
	viewPosition := strings.Index(down, "CREATE OR REPLACE VIEW source_data.record_states")
	functionPosition := strings.Index(
		down,
		"CREATE OR REPLACE FUNCTION reporting.RECORD_HAS_LEGACY_STATE",
	)

	require.GreaterOrEqual(t, viewPosition, 0)
	require.GreaterOrEqual(t, functionPosition, 0)
	require.Less(t, viewPosition, functionPosition)
}
