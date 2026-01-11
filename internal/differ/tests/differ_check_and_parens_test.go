package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CheckConstraint_NestedParenthesesPreserved(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "accounts",
				Columns: []schema.Column{
					{Name: "a", DataType: "boolean", IsNullable: false, Position: 1},
					{Name: "b", DataType: "boolean", IsNullable: false, Position: 2},
					{Name: "c", DataType: "boolean", IsNullable: false, Position: 3},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "logic_check",
						Type:            "CHECK",
						Columns:         []string{"a", "b", "c"},
						Definition:      "CHECK (((a AND b) OR c))",
						CheckExpression: "CHECK (((a AND b) OR c))",
					},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "accounts",
				Columns: []schema.Column{
					{Name: "a", DataType: "boolean", IsNullable: false, Position: 1},
					{Name: "b", DataType: "boolean", IsNullable: false, Position: 2},
					{Name: "c", DataType: "boolean", IsNullable: false, Position: 3},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "logic_check",
						Type:            "CHECK",
						Columns:         []string{"a", "b", "c"},
						Definition:      "CHECK (a OR b OR c)",
						CheckExpression: "CHECK (a OR b OR c)",
					},
				},
			},
		},
	}

	d := differ.New(nil)

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	if len(result.Changes) == 0 {
		t.Errorf("Expected a change to be detected when constraints have different logic")
	}
}
