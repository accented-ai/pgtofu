package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

const userTable = "public.users"

func TestDDLBuilder_AddColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		column     *schema.Column
		wantUnsafe bool
	}{
		{
			name: "nullable column is safe",
			column: &schema.Column{
				Name:       "email",
				DataType:   "varchar(255)",
				IsNullable: true,
				Position:   2,
			},
			wantUnsafe: false,
		},
		{
			name: "not null with default is safe",
			column: &schema.Column{
				Name:       "status",
				DataType:   "text",
				IsNullable: false,
				Default:    "'active'",
				Position:   2,
			},
			wantUnsafe: false,
		},
		{
			name: "not null without default is unsafe",
			column: &schema.Column{
				Name:       "required_field",
				DataType:   "text",
				IsNullable: false,
				Position:   2,
			},
			wantUnsafe: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			current := &schema.Database{
				Tables: []schema.Table{
					{Schema: schema.DefaultSchema, Name: "users", Columns: []schema.Column{
						{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					}},
				},
			}

			desired := &schema.Database{
				Tables: []schema.Table{
					{Schema: schema.DefaultSchema, Name: "users", Columns: []schema.Column{
						{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
						*tt.column,
					}},
				},
			}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{
					{
						Type:       differ.ChangeTypeAddColumn,
						ObjectName: "public.users." + tt.column.Name,
						Details: map[string]any{
							"table":  userTable,
							"column": tt.column,
						},
					},
				},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)
			assert.Contains(t, stmt.SQL, "ALTER TABLE")
			assert.Contains(t, stmt.SQL, "ADD COLUMN")
			assert.Contains(t, stmt.SQL, tt.column.Name)
			assert.Equal(t, tt.wantUnsafe, stmt.IsUnsafe)
			assert.True(t, stmt.RequiresTx)
		})
	}
}

func TestDDLBuilder_ColumnModifications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		changeType differ.ChangeType
		details    map[string]any
		wantSQL    string
		wantUnsafe bool
	}{
		{
			name:       "modify column type",
			changeType: differ.ChangeTypeModifyColumnType,
			details: map[string]any{
				"table":       userTable,
				"column_name": "age",
				"new_type":    "bigint",
			},
			wantSQL:    "ALTER COLUMN age TYPE bigint",
			wantUnsafe: true,
		},
		{
			name:       "add not null constraint",
			changeType: differ.ChangeTypeModifyColumnNullability,
			details: map[string]any{
				"table":        userTable,
				"column_name":  "email",
				"new_nullable": false,
			},
			wantSQL:    "SET NOT NULL",
			wantUnsafe: true,
		},
		{
			name:       "drop not null constraint",
			changeType: differ.ChangeTypeModifyColumnNullability,
			details: map[string]any{
				"table":        userTable,
				"column_name":  "phone",
				"new_nullable": true,
			},
			wantSQL:    "DROP NOT NULL",
			wantUnsafe: false,
		},
		{
			name:       "set default value",
			changeType: differ.ChangeTypeModifyColumnDefault,
			details: map[string]any{
				"table":       userTable,
				"column_name": "status",
				"new_default": "'active'",
			},
			wantSQL: "SET DEFAULT 'active'",
		},
		{
			name:       "drop default value",
			changeType: differ.ChangeTypeModifyColumnDefault,
			details: map[string]any{
				"table":       userTable,
				"column_name": "status",
				"new_default": "",
			},
			wantSQL: "DROP DEFAULT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			current := &schema.Database{
				Tables: []schema.Table{{Schema: schema.DefaultSchema, Name: "users"}},
			}
			desired := &schema.Database{
				Tables: []schema.Table{{Schema: schema.DefaultSchema, Name: "users"}},
			}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{{Type: tt.changeType, Details: tt.details}},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)
			assert.Contains(t, stmt.SQL, tt.wantSQL)
			assert.Equal(t, tt.wantUnsafe, stmt.IsUnsafe)
		})
	}
}

func TestDDLBuilder_AddColumn_NormalizesDefaultCasts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		column      *schema.Column
		wantContain string
		wantAbsent  string
	}{
		{
			name: "text array cast is uppercased",
			column: &schema.Column{
				Name: "tags", DataType: "text", IsArray: true, IsNullable: false,
				Default: "ARRAY[]::text[]", Position: 2,
			},
			wantContain: "DEFAULT ARRAY[]::TEXT[]",
			wantAbsent:  "::text[]",
		},
		{
			name: "integer array cast is uppercased, not mangled",
			column: &schema.Column{
				Name: "scores", DataType: "integer", IsArray: true, IsNullable: false,
				Default: "ARRAY[]::integer[]", Position: 2,
			},
			wantContain: "DEFAULT ARRAY[]::INTEGER[]",
			wantAbsent:  "ARRAY[][]",
		},
		{
			name: "string literal cast is still stripped",
			column: &schema.Column{
				Name: "status", DataType: "text", IsNullable: false,
				Default: "'pending'::text", Position: 2,
			},
			wantContain: "DEFAULT 'pending'",
			wantAbsent:  "::text",
		},
		{
			name: "cast inside a string literal is left untouched",
			column: &schema.Column{
				Name: "label", DataType: "text", IsNullable: false,
				Default: "'a::b'::text", Position: 2,
			},
			wantContain: "DEFAULT 'a::b'",
			wantAbsent:  "::B",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			desired := &schema.Database{
				Tables: []schema.Table{
					{Schema: schema.DefaultSchema, Name: "items", Columns: []schema.Column{
						{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
						*tt.column,
					}},
				},
			}

			result := &differ.DiffResult{
				Current: &schema.Database{},
				Desired: desired,
				Changes: []differ.Change{
					{
						Type:       differ.ChangeTypeAddColumn,
						ObjectName: "public.items." + tt.column.Name,
						Details: map[string]any{
							"table":  "public.items",
							"column": tt.column,
						},
					},
				},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)
			assert.Contains(t, stmt.SQL, tt.wantContain)
			assert.NotContains(t, stmt.SQL, tt.wantAbsent)
		})
	}
}

func TestDDLBuilder_SetDefault_NormalizesCasts(t *testing.T) {
	t.Parallel()

	db := &schema.Database{
		Tables: []schema.Table{{Schema: schema.DefaultSchema, Name: "items"}},
	}

	result := &differ.DiffResult{
		Current: db,
		Desired: db,
		Changes: []differ.Change{{
			Type: differ.ChangeTypeModifyColumnDefault,
			Details: map[string]any{
				"table":       "public.items",
				"column_name": "tags",
				"new_default": "ARRAY[]::text[]",
			},
		}},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "SET DEFAULT ARRAY[]::TEXT[]")
	assert.NotContains(t, stmt.SQL, "::text[]")
}
