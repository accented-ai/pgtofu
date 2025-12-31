package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDDLBuilder_MaterializedViewOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		changeType     differ.ChangeType
		mv             *schema.MaterializedView
		wantSQL        []string
		wantUnsafe     bool
		wantRequiresTx bool
	}{
		{
			name:       "add materialized view",
			changeType: differ.ChangeTypeAddMaterializedView,
			mv: &schema.MaterializedView{
				Schema:     schema.DefaultSchema,
				Name:       "user_stats",
				Definition: "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id",
			},
			wantSQL:        []string{"CREATE MATERIALIZED VIEW", "user_stats", "AS"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add materialized view with schema",
			changeType: differ.ChangeTypeAddMaterializedView,
			mv: &schema.MaterializedView{
				Schema:     "app",
				Name:       "user_stats",
				Definition: "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id",
			},
			wantSQL:        []string{"CREATE MATERIALIZED VIEW app.user_stats", "AS"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add materialized view with comment",
			changeType: differ.ChangeTypeAddMaterializedView,
			mv: &schema.MaterializedView{
				Schema:     schema.DefaultSchema,
				Name:       "commented_mv",
				Definition: "SELECT * FROM users",
				Comment:    "This is a materialized view",
			},
			wantSQL: []string{
				"CREATE MATERIALIZED VIEW",
				"commented_mv",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "drop materialized view",
			changeType: differ.ChangeTypeDropMaterializedView,
			mv: &schema.MaterializedView{
				Schema:     schema.DefaultSchema,
				Name:       "old_mv",
				Definition: "SELECT * FROM old_table",
			},
			wantSQL:        []string{"DROP MATERIALIZED VIEW", "IF EXISTS", "old_mv", "CASCADE"},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "modify materialized view",
			changeType: differ.ChangeTypeModifyMaterializedView,
			mv: &schema.MaterializedView{
				Schema:     schema.DefaultSchema,
				Name:       "modified_mv",
				Definition: "SELECT * FROM new_table",
			},
			wantSQL: []string{
				"DROP MATERIALIZED VIEW",
				"IF EXISTS",
				"modified_mv",
				"CREATE MATERIALIZED VIEW",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "modify materialized view comment only",
			changeType: differ.ChangeTypeModifyMaterializedView,
			mv: &schema.MaterializedView{
				Schema:     schema.DefaultSchema,
				Name:       "mv_with_comment",
				Definition: "SELECT * FROM users",
				Comment:    "Updated comment",
			},
			wantSQL:        []string{"COMMENT ON MATERIALIZED VIEW", "mv_with_comment"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var current, desired *schema.Database

			switch tt.changeType {
			case differ.ChangeTypeDropMaterializedView:
				current = &schema.Database{MaterializedViews: []schema.MaterializedView{*tt.mv}}
				desired = &schema.Database{}
			case differ.ChangeTypeModifyMaterializedView:
				oldMV := &schema.MaterializedView{
					Schema:     tt.mv.Schema,
					Name:       tt.mv.Name,
					Definition: "SELECT * FROM old_table",
					Comment:    "Old comment",
				}
				current = &schema.Database{MaterializedViews: []schema.MaterializedView{*oldMV}}
				desired = &schema.Database{MaterializedViews: []schema.MaterializedView{*tt.mv}}
			default:
				current = &schema.Database{}
				desired = &schema.Database{MaterializedViews: []schema.MaterializedView{*tt.mv}}
			}

			objectName := schema.QualifiedName(tt.mv.Schema, tt.mv.Name)
			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{{Type: tt.changeType, ObjectName: objectName}},
			}

			if tt.changeType == differ.ChangeTypeModifyMaterializedView && tt.mv.Comment != "" &&
				len(current.MaterializedViews) > 0 {
				if current.MaterializedViews[0].Comment != tt.mv.Comment {
					result.Changes[0].Details = map[string]any{
						"old_comment": current.MaterializedViews[0].Comment,
						"new_comment": tt.mv.Comment,
					}
				}
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)

			for _, want := range tt.wantSQL {
				assert.Contains(t, stmt.SQL, want)
			}

			assert.Equal(t, tt.wantUnsafe, stmt.IsUnsafe)
			assert.Equal(t, tt.wantRequiresTx, stmt.RequiresTx)
		})
	}
}

func TestDDLBuilder_MaterializedViewModifyWithCommentChange(t *testing.T) {
	t.Parallel()

	mv := &schema.MaterializedView{
		Schema:     schema.DefaultSchema,
		Name:       "test_mv",
		Definition: "SELECT * FROM users",
		Comment:    "New comment",
	}

	current := &schema.Database{
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     schema.DefaultSchema,
				Name:       "test_mv",
				Definition: "SELECT * FROM users",
				Comment:    "Old comment",
			},
		},
	}
	desired := &schema.Database{MaterializedViews: []schema.MaterializedView{*mv}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeModifyMaterializedView,
				ObjectName: schema.QualifiedName(mv.Schema, mv.Name),
				Details: map[string]any{
					"old_comment": "Old comment",
					"new_comment": "New comment",
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "COMMENT ON MATERIALIZED VIEW")
	assert.Contains(t, stmt.SQL, "test_mv")
	assert.Contains(t, stmt.SQL, "New comment")
	assert.NotContains(t, stmt.SQL, "DROP MATERIALIZED VIEW")
}

func TestDDLBuilder_MaterializedViewModifyWithDefinitionChange(t *testing.T) {
	t.Parallel()

	currentMV := &schema.MaterializedView{
		Schema:     schema.DefaultSchema,
		Name:       "stats_mv",
		Definition: "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
	}
	newMV := &schema.MaterializedView{
		Schema:     schema.DefaultSchema,
		Name:       "stats_mv",
		Definition: "SELECT user_id, COUNT(*) as order_count, SUM(total) as total FROM orders GROUP BY user_id",
	}

	current := &schema.Database{MaterializedViews: []schema.MaterializedView{*currentMV}}
	desired := &schema.Database{MaterializedViews: []schema.MaterializedView{*newMV}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeModifyMaterializedView,
				ObjectName: schema.QualifiedName(newMV.Schema, newMV.Name),
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "DROP MATERIALIZED VIEW")
	assert.Contains(t, stmt.SQL, "CREATE MATERIALIZED VIEW")
	assert.Contains(t, stmt.SQL, "SUM(total)")
	assert.True(t, stmt.IsUnsafe)
}

func TestDDLBuilder_MaterializedViewDropComment(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     schema.DefaultSchema,
				Name:       "commented_mv",
				Definition: "SELECT * FROM users",
				Comment:    "Old comment",
			},
		},
	}
	desired := &schema.Database{
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     schema.DefaultSchema,
				Name:       "commented_mv",
				Definition: "SELECT * FROM users",
				Comment:    "",
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeModifyMaterializedView,
				ObjectName: schema.QualifiedName(schema.DefaultSchema, "commented_mv"),
				Details: map[string]any{
					"old_comment": "Old comment",
					"new_comment": "",
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "COMMENT ON MATERIALIZED VIEW")
	assert.Contains(t, stmt.SQL, "IS NULL")
}

func TestDDLBuilder_MaterializedViewIdempotent(t *testing.T) {
	t.Parallel()

	mv := &schema.MaterializedView{
		Schema:     schema.DefaultSchema,
		Name:       "test_mv",
		Definition: "SELECT * FROM users",
	}

	current := &schema.Database{MaterializedViews: []schema.MaterializedView{*mv}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeDropMaterializedView,
				ObjectName: schema.QualifiedName(mv.Schema, mv.Name),
			},
		},
	}

	builderIdempotent := generator.NewDDLBuilder(result, true)
	builderNonIdempotent := generator.NewDDLBuilder(result, false)

	stmtIdempotent, err := builderIdempotent.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	stmtNonIdempotent, err := builderNonIdempotent.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	assert.Contains(t, stmtIdempotent.SQL, "IF EXISTS")
	assert.NotContains(t, stmtNonIdempotent.SQL, "IF EXISTS")
}

func TestDDLBuilder_MaterializedViewRevertModify(t *testing.T) {
	t.Parallel()

	currentMV := &schema.MaterializedView{
		Schema:     schema.DefaultSchema,
		Name:       "reverted_mv",
		Definition: "SELECT * FROM old_table",
	}

	current := &schema.Database{MaterializedViews: []schema.MaterializedView{*currentMV}}
	desired := &schema.Database{
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     schema.DefaultSchema,
				Name:       "reverted_mv",
				Definition: "SELECT * FROM new_table",
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeModifyMaterializedView,
				ObjectName: schema.QualifiedName(currentMV.Schema, currentMV.Name),
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "DROP MATERIALIZED VIEW")
	assert.Contains(t, stmt.SQL, "CREATE MATERIALIZED VIEW")
	assert.Contains(t, stmt.SQL, "old_table")
	assert.NotContains(t, stmt.SQL, "new_table")
}
