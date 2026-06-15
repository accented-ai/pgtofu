package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CheckConstraint_QuotedIdentifierSimpleComparison(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "position", DataType: "integer", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "items_position_check",
						Type:            "CHECK",
						Columns:         []string{"position"},
						Definition:      `CHECK (("position" > 0))`,
						CheckExpression: `CHECK (("position" > 0))`,
					},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "position", DataType: "integer", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "items_position_check",
						Type:            "CHECK",
						Columns:         []string{"position"},
						Definition:      "CHECK (position > 0)",
						CheckExpression: "CHECK (position > 0)",
					},
				},
			},
		},
	}

	assertNoChanges(t, current, desired)
}
