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

func TestDDLBuilder_ConstraintOperations(t *testing.T) { //nolint:maintidx
	t.Parallel()

	tests := []struct {
		name           string
		changeType     differ.ChangeType
		table          *schema.Table
		constraint     *schema.Constraint
		wantSQL        []string
		wantUnsafe     bool
		wantRequiresTx bool
	}{
		{
			name:       "add PRIMARY KEY constraint",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "users",
			},
			constraint: &schema.Constraint{
				Name:    "users_pkey",
				Type:    "PRIMARY KEY",
				Columns: []string{"id"},
			},
			wantSQL: []string{
				"ALTER TABLE",
				"users",
				"ADD CONSTRAINT",
				"users_pkey",
				"PRIMARY KEY",
				"(id)",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add UNIQUE constraint",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "users",
			},
			constraint: &schema.Constraint{
				Name:    "users_email_unique",
				Type:    "UNIQUE",
				Columns: []string{"email"},
			},
			wantSQL: []string{
				"ALTER TABLE",
				"users",
				"ADD CONSTRAINT",
				"users_email_unique",
				"UNIQUE",
				"(email)",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add UNIQUE constraint with multiple columns",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "user_roles",
			},
			constraint: &schema.Constraint{
				Name:    "user_roles_user_role_unique",
				Type:    "UNIQUE",
				Columns: []string{"user_id", "role_id"},
			},
			wantSQL: []string{
				"ALTER TABLE",
				"user_roles",
				"ADD CONSTRAINT",
				"UNIQUE",
				"(user_id, role_id)",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add FOREIGN KEY constraint",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "orders",
			},
			constraint: &schema.Constraint{
				Name:              "orders_user_id_fkey",
				Type:              "FOREIGN KEY",
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
			},
			wantSQL: []string{
				"ALTER TABLE",
				"orders",
				"ADD CONSTRAINT",
				"orders_user_id_fkey",
				"FOREIGN KEY",
				"REFERENCES users",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "add FOREIGN KEY with ON DELETE CASCADE",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "order_items",
			},
			constraint: &schema.Constraint{
				Name:              "order_items_order_id_fkey",
				Type:              "FOREIGN KEY",
				Columns:           []string{"order_id"},
				ReferencedTable:   "orders",
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
			},
			wantSQL: []string{
				"ALTER TABLE",
				"order_items",
				"ADD CONSTRAINT",
				"FOREIGN KEY",
				"REFERENCES orders",
				"ON DELETE CASCADE",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "add FOREIGN KEY with ON DELETE SET NULL",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "posts",
			},
			constraint: &schema.Constraint{
				Name:              "posts_author_id_fkey",
				Type:              "FOREIGN KEY",
				Columns:           []string{"author_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
				OnDelete:          "SET NULL",
			},
			wantSQL: []string{
				"ALTER TABLE",
				"posts",
				"ADD CONSTRAINT",
				"FOREIGN KEY",
				"REFERENCES users",
				"ON DELETE SET NULL",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "add FOREIGN KEY with ON UPDATE CASCADE",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "comments",
			},
			constraint: &schema.Constraint{
				Name:              "comments_post_id_fkey",
				Type:              "FOREIGN KEY",
				Columns:           []string{"post_id"},
				ReferencedTable:   "posts",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "CASCADE",
			},
			wantSQL: []string{
				"ALTER TABLE",
				"comments",
				"ADD CONSTRAINT",
				"FOREIGN KEY",
				"REFERENCES posts",
				"ON UPDATE CASCADE",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "add FOREIGN KEY with multiple columns",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "composite_fk_table",
			},
			constraint: &schema.Constraint{
				Name:              "composite_fk",
				Type:              "FOREIGN KEY",
				Columns:           []string{"col1", "col2"},
				ReferencedTable:   "referenced_table",
				ReferencedColumns: []string{"ref_col1", "ref_col2"},
			},
			wantSQL: []string{
				"ALTER TABLE",
				"composite_fk_table",
				"ADD CONSTRAINT",
				"FOREIGN KEY",
				"(col1, col2)",
				"REFERENCES referenced_table",
				"(ref_col1, ref_col2)",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "add FOREIGN KEY with schema-qualified referenced table",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "cross_schema_table",
			},
			constraint: &schema.Constraint{
				Name:              "cross_schema_fkey",
				Type:              "FOREIGN KEY",
				Columns:           []string{"user_id"},
				ReferencedSchema:  "app",
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
			},
			wantSQL: []string{
				"ALTER TABLE",
				"cross_schema_table",
				"ADD CONSTRAINT",
				"FOREIGN KEY",
				"REFERENCES app.users",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "add CHECK constraint",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "products",
			},
			constraint: &schema.Constraint{
				Name:       "products_price_check",
				Type:       "CHECK",
				Definition: "(price > 0)",
			},
			wantSQL: []string{
				"ALTER TABLE",
				"products",
				"ADD CONSTRAINT",
				"products_price_check",
				"CHECK",
				"(price > 0)",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add CHECK constraint with complex expression",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "orders",
			},
			constraint: &schema.Constraint{
				Name:       "orders_valid_status",
				Type:       "CHECK",
				Definition: "(status IN ('pending', 'processing', 'completed', 'cancelled'))",
			},
			wantSQL: []string{
				"ALTER TABLE",
				"orders",
				"ADD CONSTRAINT",
				"orders_valid_status",
				"CHECK",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add EXCLUDE constraint",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "reservations",
			},
			constraint: &schema.Constraint{
				Name:       "reservations_no_overlap",
				Type:       "EXCLUDE",
				Definition: "USING gist (room_id WITH =, period WITH &&)",
			},
			wantSQL: []string{
				"ALTER TABLE",
				"reservations",
				"ADD CONSTRAINT",
				"reservations_no_overlap",
				"EXCLUDE",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add deferrable constraint",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "deferred_table",
			},
			constraint: &schema.Constraint{
				Name:              "deferred_fkey",
				Type:              "FOREIGN KEY",
				Columns:           []string{"ref_id"},
				ReferencedTable:   "referenced",
				ReferencedColumns: []string{"id"},
				IsDeferrable:      true,
			},
			wantSQL: []string{
				"ALTER TABLE",
				"deferred_table",
				"ADD CONSTRAINT",
				"deferred_fkey",
				"FOREIGN KEY",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "drop constraint",
			changeType: differ.ChangeTypeDropConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "users",
			},
			constraint: &schema.Constraint{
				Name:    "users_email_unique",
				Type:    "UNIQUE",
				Columns: []string{"email"},
			},
			wantSQL: []string{
				"ALTER TABLE",
				"users",
				"DROP CONSTRAINT",
				"IF EXISTS",
				"users_email_unique",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "drop PRIMARY KEY constraint",
			changeType: differ.ChangeTypeDropConstraint,
			table: &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "users",
			},
			constraint: &schema.Constraint{
				Name:    "users_pkey",
				Type:    "PRIMARY KEY",
				Columns: []string{"id"},
			},
			wantSQL: []string{
				"ALTER TABLE",
				"users",
				"DROP CONSTRAINT",
				"IF EXISTS",
				"users_pkey",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "add constraint with schema-qualified table",
			changeType: differ.ChangeTypeAddConstraint,
			table: &schema.Table{
				Schema: "app",
				Name:   "users",
			},
			constraint: &schema.Constraint{
				Name:    "app_users_email_unique",
				Type:    "UNIQUE",
				Columns: []string{"email"},
			},
			wantSQL: []string{
				"ALTER TABLE app.users",
				"ADD CONSTRAINT",
				"app_users_email_unique",
				"UNIQUE",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var current, desired *schema.Database
			if tt.changeType == differ.ChangeTypeDropConstraint {
				current = &schema.Database{
					Tables: []schema.Table{
						{
							Schema:      tt.table.Schema,
							Name:        tt.table.Name,
							Constraints: []schema.Constraint{*tt.constraint},
						},
					},
				}
				desired = &schema.Database{
					Tables: []schema.Table{
						{
							Schema:      tt.table.Schema,
							Name:        tt.table.Name,
							Constraints: []schema.Constraint{},
						},
					},
				}
			} else {
				current = &schema.Database{
					Tables: []schema.Table{
						{
							Schema:      tt.table.Schema,
							Name:        tt.table.Name,
							Constraints: []schema.Constraint{},
						},
					},
				}
				desired = &schema.Database{
					Tables: []schema.Table{
						{
							Schema:      tt.table.Schema,
							Name:        tt.table.Name,
							Constraints: []schema.Constraint{*tt.constraint},
						},
					},
				}
			}

			objectName := "public." + tt.table.Name
			if tt.table.Schema != "" && tt.table.Schema != schema.DefaultSchema {
				objectName = fmt.Sprintf("%s.%s", tt.table.Schema, tt.table.Name)
			}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{
					{
						Type:       tt.changeType,
						ObjectName: objectName,
						Details: map[string]any{
							"table":      objectName,
							"constraint": tt.constraint,
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

			assert.Equal(t, tt.wantUnsafe, stmt.IsUnsafe)
			assert.Equal(t, tt.wantRequiresTx, stmt.RequiresTx)
		})
	}
}

func TestDDLBuilder_ConstraintIdempotent(t *testing.T) {
	t.Parallel()

	constraint := &schema.Constraint{
		Name:    "test_constraint",
		Type:    "UNIQUE",
		Columns: []string{"email"},
	}

	tableName := userTable
	result := &differ.DiffResult{
		Current: &schema.Database{
			Tables: []schema.Table{
				{
					Schema:      schema.DefaultSchema,
					Name:        "users",
					Constraints: []schema.Constraint{*constraint},
				},
			},
		},
		Desired: &schema.Database{},
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeDropConstraint,
				ObjectName: tableName,
				Details: map[string]any{
					"table":      tableName,
					"constraint": constraint,
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

func TestDDLBuilder_ConstraintDownMigration(t *testing.T) {
	t.Parallel()

	constraint := &schema.Constraint{
		Name:    "users_email_unique",
		Type:    "UNIQUE",
		Columns: []string{"email"},
	}

	tableName := userTable
	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:      schema.DefaultSchema,
				Name:        "users",
				Constraints: []schema.Constraint{*constraint},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeAddConstraint,
				ObjectName: tableName,
				Details: map[string]any{
					"table":      tableName,
					"constraint": constraint,
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "ALTER TABLE")
	assert.Contains(t, stmt.SQL, "users")
	assert.Contains(t, stmt.SQL, "DROP CONSTRAINT")
	assert.Contains(t, stmt.SQL, "IF EXISTS")
	assert.Contains(t, stmt.SQL, "users_email_unique")
}

func TestDDLBuilder_ConstraintWithQuotedName(t *testing.T) {
	t.Parallel()

	constraint := &schema.Constraint{
		Name:       "Constraint-Name",
		Type:       "CHECK",
		Definition: "(value > 0)",
	}

	tableName := userTable
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{
			Tables: []schema.Table{
				{
					Schema:      schema.DefaultSchema,
					Name:        "users",
					Constraints: []schema.Constraint{*constraint},
				},
			},
		},
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeAddConstraint,
				ObjectName: tableName,
				Details: map[string]any{
					"table":      tableName,
					"constraint": constraint,
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, `"Constraint-Name"`)
}

func TestDDLBuilder_CheckConstraintNumericTypeCastNormalization(t *testing.T) {
	t.Parallel()

	constraint := &schema.Constraint{
		Name: "items_rating_check",
		Type: "CHECK",
		Definition: "CHECK (((rating >= (0)::double precision) AND " +
			"(rating <= (1)::double precision)))",
	}

	tableName := "public.items"
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{
			Tables: []schema.Table{
				{
					Schema: schema.DefaultSchema,
					Name:   "items",
					Columns: []schema.Column{
						{Name: "rating", DataType: "double precision", Position: 1},
					},
					Constraints: []schema.Constraint{*constraint},
				},
			},
		},
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeAddConstraint,
				ObjectName: tableName,
				Details: map[string]any{
					"table":      tableName,
					"constraint": constraint,
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.NotContains(t, stmt.SQL, "::double precision",
		"Should not contain lowercase type casts")
	assert.NotContains(t, stmt.SQL, "(0)",
		"Should not contain parenthesized numeric literals")
	assert.NotContains(t, stmt.SQL, "(1)",
		"Should not contain parenthesized numeric literals")
	assert.Contains(t, stmt.SQL, ">=")
	assert.Contains(t, stmt.SQL, "<=")
}
