package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CheckConstraint_NotInNormalization(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "orders",
				Columns: []schema.Column{
					{Name: "status", DataType: "text", Position: 1},
					{Name: "approved_by", DataType: "uuid", Position: 2, IsNullable: true},
					{Name: "approved_at", DataType: "timestamptz", Position: 3, IsNullable: true},
				},
				Constraints: []schema.Constraint{
					{
						Name: "orders_check1",
						Type: "CHECK",
						Definition: "CHECK (status NOT IN ('cancelled', 'refunded', 'disputed') " +
							"OR (approved_by IS NOT NULL AND approved_at IS NOT NULL))",
						CheckExpression: "status NOT IN ('cancelled', 'refunded', 'disputed') " +
							"OR (approved_by IS NOT NULL AND approved_at IS NOT NULL)",
					},
				},
			},
		},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "orders",
				Columns: []schema.Column{
					{Name: "status", DataType: "text", Position: 1},
					{Name: "approved_by", DataType: "uuid", Position: 2, IsNullable: true},
					{Name: "approved_at", DataType: "timestamptz", Position: 3, IsNullable: true},
				},
				Constraints: []schema.Constraint{
					{
						Name: "orders_check1",
						Type: "CHECK",
						Definition: "CHECK (((status <> ALL (ARRAY['cancelled'::text, 'refunded'::text, " +
							"'disputed'::text])) OR ((approved_by IS NOT NULL) AND " +
							"(approved_at IS NOT NULL))))",
						CheckExpression: "((status <> ALL (ARRAY['cancelled'::text, 'refunded'::text, " +
							"'disputed'::text])) OR ((approved_by IS NOT NULL) AND " +
							"(approved_at IS NOT NULL)))",
					},
				},
			},
		},
	}

	assertNoChanges(t, current, desired)
}
