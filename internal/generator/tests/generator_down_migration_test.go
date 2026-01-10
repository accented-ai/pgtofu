package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDropViewDownMigrationRecreatesView(t *testing.T) { //nolint:dupl
	t.Parallel()

	view := schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "active_items",
		Definition: "SELECT id, name FROM items WHERE active = true",
		Comment:    "Shows active items",
	}

	current := &schema.Database{Views: []schema.View{view}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeDropView,
			ObjectName: "public.active_items",
		}},
	}

	builder := generator.NewDDLBuilder(result, true)

	upStmt, err := builder.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, upStmt.SQL, "DROP VIEW")
	assert.Contains(t, upStmt.SQL, "active_items")
	assert.True(t, upStmt.IsUnsafe)

	downStmt, err := builder.BuildDownStatement(result.Changes[0])
	require.NoError(t, err, "DOWN migration for DROP_VIEW should not error")
	assert.Contains(t, downStmt.SQL, "CREATE VIEW")
	assert.Contains(t, downStmt.SQL, "active_items")
	assert.Contains(t, downStmt.SQL, "SELECT id, name FROM items WHERE active = true")
	assert.Contains(t, downStmt.SQL, "COMMENT ON VIEW")
	assert.Contains(t, downStmt.SQL, "Shows active items")
}

func TestDropMaterializedViewDownMigrationRecreatesView(t *testing.T) { //nolint:dupl
	t.Parallel()

	mv := schema.MaterializedView{
		Schema:     schema.DefaultSchema,
		Name:       "item_stats",
		Definition: "SELECT COUNT(*) as total FROM items",
		Comment:    "Item statistics",
	}

	current := &schema.Database{MaterializedViews: []schema.MaterializedView{mv}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeDropMaterializedView,
			ObjectName: "public.item_stats",
		}},
	}

	builder := generator.NewDDLBuilder(result, true)

	upStmt, err := builder.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, upStmt.SQL, "DROP MATERIALIZED VIEW")
	assert.Contains(t, upStmt.SQL, "item_stats")
	assert.True(t, upStmt.IsUnsafe)

	downStmt, err := builder.BuildDownStatement(result.Changes[0])
	require.NoError(t, err, "DOWN migration for DROP_MATERIALIZED_VIEW should not error")
	assert.Contains(t, downStmt.SQL, "CREATE MATERIALIZED VIEW")
	assert.Contains(t, downStmt.SQL, "item_stats")
	assert.Contains(t, downStmt.SQL, "SELECT COUNT(*) as total FROM items")
	assert.Contains(t, downStmt.SQL, "COMMENT ON MATERIALIZED VIEW")
	assert.Contains(t, downStmt.SQL, "Item statistics")
}

func TestDropFunctionDownMigrationRecreatesFunction(t *testing.T) {
	t.Parallel()

	fn := schema.Function{
		Schema:            schema.DefaultSchema,
		Name:              "update_timestamp",
		ArgumentTypes:     []string{},
		ReturnType:        "TRIGGER",
		Language:          "plpgsql",
		Body:              "BEGIN NEW.updated_at = NOW(); RETURN NEW; END;",
		Volatility:        "VOLATILE",
		IsSecurityDefiner: false,
		IsStrict:          false,
	}

	current := &schema.Database{Functions: []schema.Function{fn}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeDropFunction,
			ObjectName: "public.update_timestamp()",
		}},
	}

	builder := generator.NewDDLBuilder(result, true)

	upStmt, err := builder.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, upStmt.SQL, "DROP FUNCTION")
	assert.Contains(t, upStmt.SQL, "update_timestamp")
	assert.True(t, upStmt.IsUnsafe)

	downStmt, err := builder.BuildDownStatement(result.Changes[0])
	require.NoError(t, err, "DOWN migration for DROP_FUNCTION should not error")
	assert.Contains(t, downStmt.SQL, "CREATE")
	assert.Contains(t, downStmt.SQL, "FUNCTION")
	assert.Contains(t, downStmt.SQL, "UPDATE_TIMESTAMP")
	assert.Contains(t, downStmt.SQL, "RETURNS TRIGGER")
	assert.Contains(t, downStmt.SQL, "NEW.updated_at = NOW()")
}

func TestDropFunctionWithArgsDownMigration(t *testing.T) {
	t.Parallel()

	fn := schema.Function{
		Schema:            schema.DefaultSchema,
		Name:              "calculate_total",
		ArgumentTypes:     []string{"INTEGER", "NUMERIC"},
		ReturnType:        "NUMERIC",
		Language:          "plpgsql",
		Body:              "BEGIN RETURN quantity * price; END;",
		Volatility:        "IMMUTABLE",
		IsSecurityDefiner: false,
		IsStrict:          true,
	}

	current := &schema.Database{Functions: []schema.Function{fn}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeDropFunction,
			ObjectName: "public.calculate_total(INTEGER,NUMERIC)",
		}},
	}

	builder := generator.NewDDLBuilder(result, true)

	downStmt, err := builder.BuildDownStatement(result.Changes[0])
	require.NoError(t, err, "DOWN migration for DROP_FUNCTION with args should not error")
	assert.Contains(t, downStmt.SQL, "CREATE")
	assert.Contains(t, downStmt.SQL, "FUNCTION")
	assert.Contains(t, downStmt.SQL, "CALCULATE_TOTAL")
	assert.Contains(t, downStmt.SQL, "RETURNS NUMERIC")
}

func TestDropViewWithComplexDefinitionDownMigration(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema: schema.DefaultSchema,
		Name:   "item_summary",
		Definition: `SELECT
    i.id,
    i.name,
    COUNT(o.id) as order_count
FROM items i
LEFT JOIN orders o ON i.id = o.item_id
WHERE i.active = TRUE
GROUP BY i.id, i.name`,
	}

	current := &schema.Database{Views: []schema.View{view}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeDropView,
			ObjectName: "public.item_summary",
		}},
	}

	builder := generator.NewDDLBuilder(result, true)

	downStmt, err := builder.BuildDownStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, downStmt.SQL, "CREATE VIEW")
	assert.Contains(t, downStmt.SQL, "item_summary")
	assert.Contains(t, downStmt.SQL, "LEFT JOIN orders")
}

func TestDropViewInNonPublicSchemaDownMigration(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema:     "analytics",
		Name:       "daily_report",
		Definition: "SELECT date, SUM(amount) FROM transactions GROUP BY date",
	}

	current := &schema.Database{Views: []schema.View{view}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeDropView,
			ObjectName: "analytics.daily_report",
		}},
	}

	builder := generator.NewDDLBuilder(result, true)

	downStmt, err := builder.BuildDownStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, downStmt.SQL, "CREATE VIEW")
	assert.Contains(t, downStmt.SQL, "analytics.daily_report")
}

func TestFullMigrationDropViewHasValidDownMigration(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "status_report",
		Definition: "SELECT id, status FROM records",
		Comment:    "Status report view",
	}

	current := &schema.Database{Views: []schema.View{view}}
	desired := &schema.Database{}

	d := differ.New(differ.DefaultOptions())

	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.GenerateDownMigrations = true

	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)
	require.NotEmpty(t, genResult.Migrations)

	for _, warning := range genResult.Warnings {
		assert.NotContains(t, warning, "Failed to build DOWN statement",
			"should not have warnings about failed DOWN statements")
		assert.NotContains(t, warning, "view not found",
			"should not have 'view not found' warnings")
	}

	require.NotNil(t, genResult.Migrations[0].DownFile)

	downContent := genResult.Migrations[0].DownFile.Content
	assert.Contains(t, downContent, "CREATE VIEW")
	assert.Contains(t, downContent, "status_report")
	assert.NotContains(t, downContent, "Manual rollback required")
}

func TestFullMigrationDropFunctionHasValidDownMigration(t *testing.T) {
	t.Parallel()

	fn := schema.Function{
		Schema:            schema.DefaultSchema,
		Name:              "get_status",
		ArgumentTypes:     []string{},
		ReturnType:        "TEXT",
		Language:          "plpgsql",
		Body:              "BEGIN RETURN 'active'; END;",
		Volatility:        "STABLE",
		IsSecurityDefiner: false,
		IsStrict:          false,
	}

	current := &schema.Database{Functions: []schema.Function{fn}}
	desired := &schema.Database{}

	d := differ.New(differ.DefaultOptions())

	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.GenerateDownMigrations = true

	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)
	require.NotEmpty(t, genResult.Migrations)

	for _, warning := range genResult.Warnings {
		assert.NotContains(t, warning, "Failed to build DOWN statement",
			"should not have warnings about failed DOWN statements")
		assert.NotContains(t, warning, "function not found",
			"should not have 'function not found' warnings")
	}

	require.NotNil(t, genResult.Migrations[0].DownFile)

	downContent := genResult.Migrations[0].DownFile.Content
	assert.Contains(t, downContent, "CREATE")
	assert.Contains(t, downContent, "get_status")
	assert.NotContains(t, downContent, "Manual rollback required")
}

func TestFullMigrationDropMaterializedViewHasValidDownMigration(t *testing.T) {
	t.Parallel()

	mv := schema.MaterializedView{
		Schema:     schema.DefaultSchema,
		Name:       "cached_stats",
		Definition: "SELECT COUNT(*) FROM items",
		Comment:    "Cached statistics",
	}

	current := &schema.Database{MaterializedViews: []schema.MaterializedView{mv}}
	desired := &schema.Database{}

	d := differ.New(differ.DefaultOptions())

	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.GenerateDownMigrations = true

	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)
	require.NotEmpty(t, genResult.Migrations)

	for _, warning := range genResult.Warnings {
		assert.NotContains(t, warning, "Failed to build DOWN statement",
			"should not have warnings about failed DOWN statements")
		assert.NotContains(t, warning, "materialized view not found",
			"should not have 'materialized view not found' warnings")
	}

	require.NotNil(t, genResult.Migrations[0].DownFile)

	downContent := genResult.Migrations[0].DownFile.Content
	assert.Contains(t, downContent, "CREATE MATERIALIZED VIEW")
	assert.Contains(t, downContent, "cached_stats")
	assert.NotContains(t, downContent, "Manual rollback required")
}
