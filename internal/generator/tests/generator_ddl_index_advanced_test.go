package generator_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDDLBuilder_AdvancedIndexOperations(t *testing.T) { //nolint:maintidx
	t.Parallel()

	tests := []struct {
		name       string
		changeType differ.ChangeType
		index      *schema.Index
		wantSQL    []string
	}{
		{
			name:       "add GIN index",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_users_tags_gin",
				TableName: "users",
				Columns:   []string{"tags"},
				Type:      "gin",
			},
			wantSQL: []string{
				"CREATE INDEX",
				"idx_users_tags_gin",
				"ON public.users",
				"USING gin",
				"(tags)",
			},
		},
		{
			name:       "add GiST index",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_locations_point_gist",
				TableName: "locations",
				Columns:   []string{"point"},
				Type:      "gist",
			},
			wantSQL: []string{
				"CREATE INDEX",
				"idx_locations_point_gist",
				"ON public.locations",
				"USING gist",
				"(point)",
			},
		},
		{
			name:       "add BRIN index",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_timestamps_brin",
				TableName: "events",
				Columns:   []string{"created_at"},
				Type:      "brin",
			},
			wantSQL: []string{
				"CREATE INDEX",
				"idx_timestamps_brin",
				"ON public.events",
				"USING brin",
				"(created_at)",
			},
		},
		{
			name:       "add SP-GiST index",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_points_spgist",
				TableName: "points",
				Columns:   []string{"location"},
				Type:      "spgist",
			},
			wantSQL: []string{
				"CREATE INDEX",
				"idx_points_spgist",
				"ON public.points",
				"USING spgist",
				"(location)",
			},
		},
		{
			name:       "add HASH index",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_users_email_hash",
				TableName: "users",
				Columns:   []string{"email"},
				Type:      "hash",
			},
			wantSQL: []string{
				"CREATE INDEX",
				"idx_users_email_hash",
				"ON public.users",
				"USING hash",
				"(email)",
			},
		},
		{
			name:       "add expression index",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_users_lower_email",
				TableName: "users",
				Columns:   []string{"LOWER(email)"},
				Type:      "btree",
			},
			wantSQL: []string{"CREATE INDEX", "idx_users_lower_email", "ON public.users"},
		},
		{
			name:       "add expression index with function",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_users_full_name",
				TableName: "users",
				Columns:   []string{"CONCAT(first_name, ' ', last_name)"},
				Type:      "btree",
			},
			wantSQL: []string{"CREATE INDEX", "idx_users_full_name", "ON public.users"},
		},
		{
			name:       "add multi-column index",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_users_name_email",
				TableName: "users",
				Columns:   []string{"last_name", "first_name", "email"},
				Type:      "btree",
			},
			wantSQL: []string{
				"CREATE INDEX",
				"idx_users_name_email",
				"ON public.users",
				"(last_name, first_name, email)",
			},
		},
		{
			name:       "add partial index with WHERE clause",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_active_users",
				TableName: "users",
				Columns:   []string{"created_at"},
				Type:      "btree",
				Where:     "active = true",
			},
			wantSQL: []string{
				"CREATE INDEX",
				"idx_active_users",
				"ON public.users",
				"(created_at)",
				"WHERE active = true",
			},
		},
		{
			name:       "add covering index with INCLUDE",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:         schema.DefaultSchema,
				Name:           "idx_users_email_cover",
				TableName:      "users",
				Columns:        []string{"email"},
				IncludeColumns: []string{"name", "created_at"},
				Type:           "btree",
			},
			wantSQL: []string{
				"CREATE INDEX",
				"idx_users_email_cover",
				"ON public.users",
				"(email)",
				"INCLUDE",
				"(name, created_at)",
			},
		},
		{
			name:       "add unique covering index",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:         schema.DefaultSchema,
				Name:           "idx_users_email_unique_cover",
				TableName:      "users",
				Columns:        []string{"email"},
				IncludeColumns: []string{"name"},
				IsUnique:       true,
				Type:           "btree",
			},
			wantSQL: []string{
				"CREATE UNIQUE INDEX",
				"idx_users_email_unique_cover",
				"ON public.users",
				"(email)",
				"INCLUDE",
			},
		},
		{
			name:       "add partial GIN index",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_users_tags_active_gin",
				TableName: "users",
				Columns:   []string{"tags"},
				Type:      "gin",
				Where:     "active = true",
			},
			wantSQL: []string{
				"CREATE INDEX",
				"idx_users_tags_active_gin",
				"ON public.users",
				"USING gin",
				"(tags)",
				"WHERE active = true",
			},
		},
		{
			name:       "add index with schema-qualified table",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    "app",
				Name:      "idx_users_email",
				TableName: "users",
				Columns:   []string{"email"},
				Type:      "btree",
			},
			wantSQL: []string{"CREATE INDEX", "idx_users_email", "ON app.users", "(email)"},
		},
		{
			name:       "GIN index with JSONB expression",
			changeType: differ.ChangeTypeAddIndex,
			index: &schema.Index{
				Schema:    "content",
				Name:      "idx_content_items_metadata_genre",
				TableName: "items",
				Columns:   []string{"(metadata -> 'genre')"},
				Type:      "gin",
			},
			wantSQL: []string{
				"CREATE INDEX",
				"idx_content_items_metadata_genre",
				"ON content.items",
				"USING gin",
				"(metadata -> 'genre')",
			},
		},
		{
			name:       "drop index",
			changeType: differ.ChangeTypeDropIndex,
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_old_index",
				TableName: "users",
				Columns:   []string{"old_column"},
				Type:      "btree",
			},
			wantSQL: []string{"DROP INDEX", "IF EXISTS", "idx_old_index"},
		},
		{
			name:       "drop index with schema",
			changeType: differ.ChangeTypeDropIndex,
			index: &schema.Index{
				Schema:    "app",
				Name:      "idx_users_email",
				TableName: "users",
				Columns:   []string{"email"},
				Type:      "btree",
			},
			wantSQL: []string{"DROP INDEX", "IF EXISTS", "app.idx_users_email"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var current, desired *schema.Database
			if tt.changeType == differ.ChangeTypeDropIndex {
				current = &schema.Database{
					Tables: []schema.Table{
						{
							Schema:  tt.index.Schema,
							Name:    tt.index.TableName,
							Indexes: []schema.Index{*tt.index},
						},
					},
				}
				desired = &schema.Database{
					Tables: []schema.Table{
						{
							Schema:  tt.index.Schema,
							Name:    tt.index.TableName,
							Indexes: []schema.Index{},
						},
					},
				}
			} else {
				current = &schema.Database{
					Tables: []schema.Table{
						{
							Schema:  tt.index.Schema,
							Name:    tt.index.TableName,
							Indexes: []schema.Index{},
						},
					},
				}
				desired = &schema.Database{
					Tables: []schema.Table{
						{
							Schema:  tt.index.Schema,
							Name:    tt.index.TableName,
							Indexes: []schema.Index{*tt.index},
						},
					},
				}
			}

			objectName := "public." + tt.index.Name
			if tt.index.Schema != "" && tt.index.Schema != schema.DefaultSchema {
				objectName = fmt.Sprintf("%s.%s", tt.index.Schema, tt.index.Name)
			}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{
					{
						Type:       tt.changeType,
						ObjectName: objectName,
						Details: map[string]any{
							"index": tt.index,
						},
					},
				},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)

			for _, want := range tt.wantSQL {
				assert.Contains(t, stmt.SQL, want)
			}

			assert.True(t, stmt.RequiresTx)
		})
	}
}

func TestDDLBuilder_IndexIdempotent(t *testing.T) {
	t.Parallel()

	index := &schema.Index{
		Schema:    schema.DefaultSchema,
		Name:      "test_index",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      "btree",
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "users",
				Indexes: []schema.Index{*index},
			},
		},
	}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "users",
				Indexes: []schema.Index{},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeDropIndex,
				ObjectName: fmt.Sprintf("%s.%s", index.Schema, index.Name),
				Details: map[string]any{
					"index": index,
				},
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

func TestDDLBuilder_IndexDownMigration(t *testing.T) {
	t.Parallel()

	index := &schema.Index{
		Schema:    schema.DefaultSchema,
		Name:      "users_email_idx",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      "btree",
	}

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "users",
				Indexes: []schema.Index{*index},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeAddIndex,
				ObjectName: fmt.Sprintf("%s.%s", index.Schema, index.Name),
				Details: map[string]any{
					"index": index,
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "DROP INDEX")
	assert.Contains(t, stmt.SQL, "users_email_idx")
}

func TestDDLBuilder_IndexWithQuotedName(t *testing.T) {
	t.Parallel()

	index := &schema.Index{
		Schema:    schema.DefaultSchema,
		Name:      "Index-Name",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      "btree",
	}

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "users",
				Indexes: []schema.Index{*index},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeAddIndex,
				ObjectName: fmt.Sprintf("%s.%s", index.Schema, index.Name),
				Details: map[string]any{
					"index": index,
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, `"Index-Name"`)
}

func TestDDLBuilder_IndexDefaultBtreeType(t *testing.T) {
	t.Parallel()

	index := &schema.Index{
		Schema:    schema.DefaultSchema,
		Name:      "idx_default",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      "btree",
	}

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "users",
				Indexes: []schema.Index{*index},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeAddIndex,
				ObjectName: fmt.Sprintf("%s.%s", index.Schema, index.Name),
				Details: map[string]any{
					"index": index,
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "CREATE INDEX")
	assert.NotContains(t, stmt.SQL, "USING btree")
}
