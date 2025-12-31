package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CheckConstraint_NotEqualOperator(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "connections",
				Columns: []schema.Column{
					{Name: "requester_id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "addressee_id", DataType: "uuid", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "connections_check",
						Type:            "CHECK",
						Columns:         []string{},
						Definition:      "CHECK (requester_id != addressee_id)",
						CheckExpression: "CHECK (requester_id != addressee_id)",
					},
				},
			},
		},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "connections",
				Columns: []schema.Column{
					{Name: "requester_id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "addressee_id", DataType: "uuid", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "connections_check",
						Type:            "CHECK",
						Columns:         []string{"requester_id", "addressee_id"},
						Definition:      "CHECK ((requester_id <> addressee_id))",
						CheckExpression: "CHECK ((requester_id <> addressee_id))",
					},
				},
			},
		},
	}

	assertNoChanges(t, current, desired)
}
