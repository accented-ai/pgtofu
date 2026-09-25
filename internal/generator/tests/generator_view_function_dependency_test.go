package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestViewReplacementsWaitForAddedFunctionAndReleaseItOnRollback(t *testing.T) {
	t.Parallel()

	baseViews, dependentViews, function := viewFunctionFixtures()
	generated := generateViewFunctionMigration(t,
		&schema.Database{Views: baseViews},
		&schema.Database{Views: dependentViews, Functions: []schema.Function{function}},
	)

	up := strings.ToLower(generated.UpFile.Content)
	down := strings.ToLower(generated.DownFile.Content)
	functionCreate := requireSQLPosition(t, up, "create or replace function reporting.row_allowed")
	functionDrop := requireSQLPosition(t, down, "drop function if exists reporting.row_allowed")

	for _, viewName := range []string{"eligible_records", "visible_records"} {
		viewSQL := "create or replace view reporting." + viewName
		require.Less(t, functionCreate, requireSQLPosition(t, up, viewSQL),
			"the function must exist before the view calls it")
		require.Less(t, requireSQLPosition(t, down, viewSQL), functionDrop,
			"the view must release the function before rollback drops it")
	}
}

func TestDroppedFunctionWaitsForViewReplacementsAndReturnsBeforeRollback(t *testing.T) {
	t.Parallel()

	baseViews, dependentViews, function := viewFunctionFixtures()
	generated := generateViewFunctionMigration(t,
		&schema.Database{Views: dependentViews, Functions: []schema.Function{function}},
		&schema.Database{Views: baseViews},
	)

	up := strings.ToLower(generated.UpFile.Content)
	down := strings.ToLower(generated.DownFile.Content)
	functionDrop := requireSQLPosition(t, up, "drop function if exists reporting.row_allowed")
	functionCreate := requireSQLPosition(
		t, down, "create or replace function reporting.row_allowed",
	)

	for _, viewName := range []string{"eligible_records", "visible_records"} {
		viewSQL := "create or replace view reporting." + viewName
		require.Less(t, requireSQLPosition(t, up, viewSQL), functionDrop,
			"the view must release the function before it is dropped")
		require.Less(t, functionCreate, requireSQLPosition(t, down, viewSQL),
			"the function must exist before rollback restores the view")
	}
}

func TestCrossSchemaViewFunctionDependenciesOrderMigrations(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema:     "reporting",
		Name:       "visible_records",
		Definition: "SELECT 1 AS record_id WHERE utilities.row_allowed(1)",
	}
	function := schema.Function{
		Schema:        "utilities",
		Name:          "row_allowed",
		ArgumentTypes: []string{"integer"},
		ArgumentNames: []string{"record_id"},
		ReturnType:    "boolean",
		Language:      "sql",
		Body:          "SELECT record_id > 0",
	}
	populated := &schema.Database{
		Views:     []schema.View{view},
		Functions: []schema.Function{function},
	}
	empty := &schema.Database{}

	for _, test := range []struct {
		name      string
		current   *schema.Database
		desired   *schema.Database
		firstSQL  string
		secondSQL string
	}{
		{
			name:      "create",
			current:   empty,
			desired:   populated,
			firstSQL:  "create or replace function utilities.row_allowed",
			secondSQL: "create view reporting.visible_records",
		},
		{
			name:      "drop",
			current:   populated,
			desired:   empty,
			firstSQL:  "drop view",
			secondSQL: "drop function if exists utilities.row_allowed",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			diffResult, err := differ.New(differ.DefaultOptions()).Compare(
				test.current, test.desired,
			)
			require.NoError(t, err)

			opts := testOptions()
			opts.MaxOperationsPerFile = 1
			generated, err := generator.New(opts).Generate(diffResult)
			require.NoError(t, err)
			require.Len(t, generated.Migrations, 2)
			require.Contains(
				t, strings.ToLower(generated.Migrations[0].UpFile.Content), test.firstSQL,
			)
			require.Contains(
				t, strings.ToLower(generated.Migrations[1].UpFile.Content), test.secondSQL,
			)
		})
	}
}

func viewFunctionFixtures() ([]schema.View, []schema.View, schema.Function) {
	baseViews := []schema.View{
		{
			Schema:     "reporting",
			Name:       "visible_records",
			Definition: "SELECT 1::bigint AS record_id",
		},
		{
			Schema:     "reporting",
			Name:       "eligible_records",
			Definition: "SELECT 2::bigint AS record_id",
		},
	}
	dependentViews := []schema.View{
		{
			Schema:     "reporting",
			Name:       "visible_records",
			Definition: "SELECT 1::bigint AS record_id WHERE reporting.row_allowed(1)",
		},
		{
			Schema:     "reporting",
			Name:       "eligible_records",
			Definition: "SELECT 2::bigint AS record_id WHERE reporting.row_allowed(2)",
		},
	}
	function := schema.Function{
		Schema:        "reporting",
		Name:          "row_allowed",
		ArgumentTypes: []string{"bigint"},
		ArgumentNames: []string{"record_id"},
		ReturnType:    "boolean",
		Language:      "sql",
		Body:          "SELECT record_id > 0",
		Volatility:    schema.VolatilityStable,
		Comment:       "Whether a record may appear in reports",
	}

	return baseViews, dependentViews, function
}

func generateViewFunctionMigration(
	t *testing.T,
	current, desired *schema.Database,
) generator.MigrationPair {
	t.Helper()

	diffResult, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.MaxOperationsPerFile = 10
	generated, err := generator.New(opts).Generate(diffResult)
	require.NoError(t, err)
	require.Len(t, generated.Migrations, 1)
	require.NotNil(t, generated.Migrations[0].DownFile)

	return generated.Migrations[0]
}

func requireSQLPosition(t *testing.T, sql, statement string) int {
	t.Helper()

	position := strings.Index(sql, statement)
	require.GreaterOrEqual(t, position, 0, "missing %q in migration", statement)

	return position
}
