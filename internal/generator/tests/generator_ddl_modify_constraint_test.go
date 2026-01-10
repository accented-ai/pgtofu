package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDDLBuilder_ModifyConstraint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		currentConstraint *schema.Constraint
		desiredConstraint *schema.Constraint
		wantUpSQL         []string
		wantDownSQL       []string
	}{
		{
			name: "modify CHECK constraint expression",
			currentConstraint: &schema.Constraint{
				Name:            "items_status_check",
				Type:            "CHECK",
				Columns:         []string{"status"},
				Definition:      "CHECK (status IN ('pending', 'completed'))",
				CheckExpression: "CHECK (status IN ('pending', 'completed'))",
			},
			desiredConstraint: &schema.Constraint{
				Name:            "items_status_check",
				Type:            "CHECK",
				Columns:         []string{"status"},
				Definition:      "CHECK (status IN ('pending', 'processing', 'completed'))",
				CheckExpression: "CHECK (status IN ('pending', 'processing', 'completed'))",
			},
			wantUpSQL: []string{
				"DROP CONSTRAINT",
				"items_status_check",
				"ADD CONSTRAINT",
				"items_status_check",
				"CHECK",
				"'processing'",
			},
			wantDownSQL: []string{
				"DROP CONSTRAINT",
				"items_status_check",
				"ADD CONSTRAINT",
				"items_status_check",
				"CHECK",
				"'pending'",
				"'completed'",
			},
		},
		{
			name: "modify FOREIGN KEY on delete action",
			currentConstraint: &schema.Constraint{
				Name:              "items_category_fkey",
				Type:              "FOREIGN KEY",
				Columns:           []string{"category_id"},
				ReferencedTable:   "categories",
				ReferencedColumns: []string{"id"},
				OnDelete:          "NO ACTION",
			},
			desiredConstraint: &schema.Constraint{
				Name:              "items_category_fkey",
				Type:              "FOREIGN KEY",
				Columns:           []string{"category_id"},
				ReferencedTable:   "categories",
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
			},
			wantUpSQL: []string{
				"DROP CONSTRAINT",
				"items_category_fkey",
				"ADD CONSTRAINT",
				"items_category_fkey",
				"FOREIGN KEY",
				"ON DELETE CASCADE",
			},
			wantDownSQL: []string{
				"DROP CONSTRAINT",
				"items_category_fkey",
				"ADD CONSTRAINT",
				"items_category_fkey",
				"FOREIGN KEY",
				"REFERENCES categories",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			table := &schema.Table{
				Schema: schema.DefaultSchema,
				Name:   "items",
			}
			tableName := "public.items"

			current := &schema.Database{
				Tables: []schema.Table{
					{
						Schema:      table.Schema,
						Name:        table.Name,
						Constraints: []schema.Constraint{*tt.currentConstraint},
					},
				},
			}
			desired := &schema.Database{
				Tables: []schema.Table{
					{
						Schema:      table.Schema,
						Name:        table.Name,
						Constraints: []schema.Constraint{*tt.desiredConstraint},
					},
				},
			}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{
					{
						Type:       differ.ChangeTypeModifyConstraint,
						ObjectName: tableName,
						Details: map[string]any{
							"table":   tableName,
							"current": tt.currentConstraint,
							"desired": tt.desiredConstraint,
						},
					},
				},
			}

			builder := generator.NewDDLBuilder(result, true)

			upStmt, err := builder.BuildUpStatement(result.Changes[0])
			require.NoError(t, err)

			for _, want := range tt.wantUpSQL {
				assert.Contains(t, upStmt.SQL, want)
			}

			assert.True(t, upStmt.IsUnsafe)
			assert.True(t, upStmt.RequiresTx)

			downStmt, err := builder.BuildDownStatement(result.Changes[0])
			require.NoError(t, err)

			for _, want := range tt.wantDownSQL {
				assert.Contains(t, downStmt.SQL, want)
			}

			assert.True(t, downStmt.IsUnsafe)
			assert.True(t, downStmt.RequiresTx)
		})
	}
}
