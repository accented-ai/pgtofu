package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestAddViewWithComment(t *testing.T) {
	t.Parallel()

	view := &schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "active_users",
		Definition: "SELECT * FROM users WHERE active = true",
		Comment:    "Shows only active users for monitoring",
	}

	current := &schema.Database{}
	desired := &schema.Database{Views: []schema.View{*view}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddView,
			ObjectName: "public.active_users",
		}},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "CREATE VIEW")
	assert.Contains(t, stmt.SQL, "COMMENT ON VIEW public.active_users IS",
		"ADD_VIEW should include COMMENT ON VIEW when view has a comment")
	assert.Contains(t, stmt.SQL, "Shows only active users for monitoring")
}

func TestAddMaterializedViewWithComment(t *testing.T) {
	t.Parallel()

	mv := &schema.MaterializedView{
		Schema:     schema.DefaultSchema,
		Name:       "user_stats",
		Definition: "SELECT COUNT(*) FROM users",
		Comment:    "Materialized view of user statistics",
	}

	current := &schema.Database{}
	desired := &schema.Database{MaterializedViews: []schema.MaterializedView{*mv}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddMaterializedView,
			ObjectName: "public.user_stats",
		}},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "CREATE MATERIALIZED VIEW")
	assert.Contains(t, stmt.SQL, "COMMENT ON MATERIALIZED VIEW public.user_stats IS",
		"ADD_MATERIALIZED_VIEW should include COMMENT ON MATERIALIZED VIEW when view has a comment")
	assert.Contains(t, stmt.SQL, "Materialized view of user statistics")
}

func TestViewRecreationPreservesComment(t *testing.T) {
	t.Parallel()

	table := schema.Table{
		Schema: schema.DefaultSchema,
		Name:   "tasks",
		Columns: []schema.Column{
			{Name: "id", DataType: "INTEGER", Position: 1},
			{Name: "status", DataType: "VARCHAR(50)", Position: 2},
		},
	}

	view := schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "task_status",
		Definition: "SELECT id, status FROM tasks",
		Comment:    "Monitoring view for task progress",
	}

	current := &schema.Database{
		Tables: []schema.Table{{
			Schema: schema.DefaultSchema,
			Name:   "tasks",
			Columns: []schema.Column{
				{Name: "id", DataType: "INTEGER", Position: 1},
				{Name: "status", DataType: "VARCHAR(20)", Position: 2},
			},
		}},
		Views: []schema.View{{
			Schema:     schema.DefaultSchema,
			Name:       "task_status",
			Definition: "SELECT id, status FROM tasks",
			Comment:    "Monitoring view for task progress",
		}},
	}

	desired := &schema.Database{
		Tables: []schema.Table{table},
		Views:  []schema.View{view},
	}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	var addViewChange *differ.Change

	for i := range diffResult.Changes {
		if diffResult.Changes[i].Type == differ.ChangeTypeAddView {
			addViewChange = &diffResult.Changes[i]
			break
		}
	}

	require.NotNil(t, addViewChange, "expected an ADD_VIEW change for recreation")

	builder := generator.NewDDLBuilder(diffResult, true)
	stmt, err := builder.BuildUpStatement(*addViewChange)

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "CREATE VIEW")
	assert.Contains(t, stmt.SQL, "COMMENT ON VIEW public.task_status IS",
		"Recreated view should include its comment")
	assert.Contains(t, stmt.SQL, "Monitoring view for task progress")
}

func TestFullMigrationIncludesViewComment(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "active_users",
		Definition: "SELECT * FROM users WHERE active = true",
		Comment:    "Shows only active users for monitoring",
	}

	current := &schema.Database{}
	desired := &schema.Database{
		Views: []schema.View{view},
	}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)
	require.NotEmpty(t, genResult.Migrations)

	upContent := genResult.Migrations[0].UpFile.Content
	assert.Contains(t, upContent, "CREATE VIEW")
	assert.Contains(t, upContent, "COMMENT ON VIEW public.active_users IS",
		"Generated migration should include view comment")
	assert.Contains(t, upContent, "Shows only active users for monitoring")
}

func TestNewViewCommentDownMigrationNoWarning(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Views: []schema.View{{
			Schema:     schema.DefaultSchema,
			Name:       "health_status",
			Definition: "SELECT * FROM items WHERE active = true",
			Comment:    "Health monitoring view for active items",
		}},
	}

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
			"should not have warnings about failed DOWN statements for new view comments")
		assert.NotContains(t, warning, "view not found",
			"should not have 'view not found' warnings for new view comments")
	}

	require.NotNil(t, genResult.Migrations[0].DownFile,
		"down migration should be generated")
	downContent := genResult.Migrations[0].DownFile.Content
	assert.Contains(t, downContent, "DROP VIEW",
		"down migration should drop the view")
	assert.NotContains(t, downContent, "Manual rollback required",
		"down migration should not require manual rollback for new view comment")
}

func TestNewViewWithDependenciesCommentDownMigrationNoWarning(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{{
			Schema: schema.DefaultSchema,
			Name:   "records",
			Columns: []schema.Column{
				{Name: "id", DataType: "BIGINT", Position: 1},
				{Name: "source", DataType: "VARCHAR(50)", Position: 2},
			},
		}},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "records",
				Columns: []schema.Column{
					{Name: "id", DataType: "BIGINT", Position: 1},
					{Name: "source", DataType: "VARCHAR(50)", Position: 2},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "entities",
				Columns: []schema.Column{
					{Name: "id", DataType: "BIGINT", Position: 1},
					{Name: "source", DataType: "VARCHAR(50)", Position: 2},
					{Name: "is_active", DataType: "BOOLEAN", Position: 3},
				},
			},
		},
		Views: []schema.View{{
			Schema: schema.DefaultSchema,
			Name:   "active_entity_status",
			Definition: `SELECT e.source, e.is_active, r.id as record_id
FROM entities e
LEFT JOIN records r ON e.source = r.source
WHERE e.is_active = TRUE`,
			Comment: "Health monitoring view for active entities",
		}},
	}

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
			"should not have warnings about failed DOWN statements for new view comments")
		assert.NotContains(t, warning, "view not found",
			"should not have 'view not found' warnings for new view comments")
	}
}

func TestBuildDownStatementForNewViewCommentChange(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Views: []schema.View{{
			Schema:     schema.DefaultSchema,
			Name:       "health_status",
			Definition: "SELECT * FROM items WHERE active = true",
			Comment:    "Health monitoring view",
		}},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{{
			Type:        differ.ChangeTypeModifyView,
			ObjectName:  "public.health_status",
			Description: "Modify view comment: public.health_status",
			Details: map[string]any{
				"view": &schema.View{
					Schema:     schema.DefaultSchema,
					Name:       "health_status",
					Definition: "SELECT * FROM items WHERE active = true",
					Comment:    "Health monitoring view",
				},
				"old_comment": "",
				"new_comment": "Health monitoring view",
			},
		}},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err,
		"BuildDownStatement should not error for comment-only change on new view")
	assert.NotContains(t, stmt.SQL, "Manual rollback required",
		"should not require manual rollback for comment-only change on new view")
}

func TestNewViewCommentInSeparateBatchNoWarning(t *testing.T) {
	t.Parallel()

	currentColumns := []schema.Column{
		{Name: "id", DataType: "BIGINT", Position: 1},
		{Name: "source", DataType: "VARCHAR(50)", Position: 2},
		{Name: "item_type", DataType: "VARCHAR(50)", Position: 3},
	}

	for i := 4; i <= 15; i++ {
		currentColumns = append(currentColumns, schema.Column{
			Name: "field" + string(rune('a'+i-4)), DataType: "TEXT", Position: i,
		})
	}

	current := &schema.Database{
		Tables: []schema.Table{{
			Schema:  schema.DefaultSchema,
			Name:    "records",
			Columns: currentColumns,
		}},
	}

	desiredColumns := []schema.Column{
		{Name: "id", DataType: "BIGINT", Position: 1},
		{Name: "source", DataType: "VARCHAR(50)", Position: 2},
		{Name: "item_type", DataType: "VARCHAR(20)", Position: 3},
	}

	for i := 4; i <= 15; i++ {
		desiredColumns = append(desiredColumns, schema.Column{
			Name: "field" + string(rune('a'+i-4)), DataType: "TEXT", Position: i,
		})
	}

	desiredColumns = append(desiredColumns, schema.Column{
		Name: "new_field", DataType: "TEXT", Position: 16,
	})

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "records",
				Columns: desiredColumns,
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "entities",
				Columns: []schema.Column{
					{Name: "id", DataType: "BIGINT", Position: 1},
					{Name: "source", DataType: "VARCHAR(50)", Position: 2},
					{Name: "is_active", DataType: "BOOLEAN", Position: 3},
				},
			},
		},
		Views: []schema.View{{
			Schema: schema.DefaultSchema,
			Name:   "active_entity_status",
			Definition: `SELECT e.source, e.is_active, r.id
FROM entities e
LEFT JOIN records r ON e.source = r.source
WHERE e.is_active = TRUE`,
			Comment: "Health monitoring view for active entities",
		}},
	}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.GenerateDownMigrations = true
	opts.MaxOperationsPerFile = 10
	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)
	require.NotEmpty(t, genResult.Migrations)

	for _, warning := range genResult.Warnings {
		assert.NotContains(t, warning, "Failed to build DOWN statement",
			"should not have warnings about failed DOWN statements for new view comments")
		assert.NotContains(t, warning, "view not found",
			"should not have 'view not found' warnings for new view comments")
	}
}

func TestViewCommentOnlyChangeDoesNotDuplicateCreateView(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Views: []schema.View{{
			Schema:     schema.DefaultSchema,
			Name:       "active_users",
			Definition: "SELECT * FROM users WHERE active = true",
			Comment:    "Old comment",
		}},
	}

	desired := &schema.Database{
		Views: []schema.View{{
			Schema:     schema.DefaultSchema,
			Name:       "active_users",
			Definition: "SELECT * FROM users WHERE active = true",
			Comment:    "New comment",
		}},
	}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	require.Len(t, diffResult.Changes, 1)
	assert.Equal(t, differ.ChangeTypeModifyView, diffResult.Changes[0].Type)

	builder := generator.NewDDLBuilder(diffResult, true)
	stmt, err := builder.BuildUpStatement(diffResult.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "COMMENT ON VIEW")
	assert.Contains(t, stmt.SQL, "New comment")
	assert.NotContains(t, strings.ToUpper(stmt.SQL), "CREATE VIEW",
		"Comment-only change should not recreate the view")
}
