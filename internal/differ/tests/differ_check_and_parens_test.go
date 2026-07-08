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

func TestDiffer_CheckConstraint_RedundantBooleanTermParentheses(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "jobs",
				Columns: []schema.Column{
					{Name: "is_active", DataType: "boolean", IsNullable: false, Position: 1},
					{Name: "inactive_reason", DataType: "text", IsNullable: true, Position: 2},
					{
						Name:       "finished_at",
						DataType:   "timestamp with time zone",
						IsNullable: true,
						Position:   3,
					},
					{Name: "replacement_id", DataType: "uuid", IsNullable: true, Position: 4},
				},
				Constraints: []schema.Constraint{
					{
						Name: "jobs_lifecycle_check",
						Type: schema.ConstraintCheck,
						Definition: "CHECK (((is_active AND (inactive_reason IS NULL) AND " +
							"(finished_at IS NULL) AND (replacement_id IS NULL)) OR " +
							"((NOT is_active) AND (inactive_reason = 'replaced'::text) AND " +
							"(finished_at IS NOT NULL) AND (replacement_id IS NOT NULL)) OR " +
							"((NOT is_active) AND (inactive_reason = 'archived'::text) AND " +
							"(finished_at IS NULL) AND (replacement_id IS NULL))))",
						CheckExpression: "CHECK (((is_active AND (inactive_reason IS NULL) AND " +
							"(finished_at IS NULL) AND (replacement_id IS NULL)) OR " +
							"((NOT is_active) AND (inactive_reason = 'replaced'::text) AND " +
							"(finished_at IS NOT NULL) AND (replacement_id IS NOT NULL)) OR " +
							"((NOT is_active) AND (inactive_reason = 'archived'::text) AND " +
							"(finished_at IS NULL) AND (replacement_id IS NULL))))",
					},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "jobs",
				Columns: []schema.Column{
					{Name: "is_active", DataType: "boolean", IsNullable: false, Position: 1},
					{Name: "inactive_reason", DataType: "text", IsNullable: true, Position: 2},
					{
						Name:       "finished_at",
						DataType:   "timestamp with time zone",
						IsNullable: true,
						Position:   3,
					},
					{Name: "replacement_id", DataType: "uuid", IsNullable: true, Position: 4},
				},
				Constraints: []schema.Constraint{
					{
						Name: "jobs_lifecycle_check",
						Type: schema.ConstraintCheck,
						Definition: "CHECK ((is_active AND inactive_reason IS NULL AND " +
							"finished_at IS NULL AND replacement_id IS NULL) OR " +
							"(NOT is_active AND inactive_reason = 'replaced' AND " +
							"finished_at IS NOT NULL AND replacement_id IS NOT NULL) OR " +
							"(NOT is_active AND inactive_reason = 'archived' AND " +
							"finished_at IS NULL AND replacement_id IS NULL))",
						CheckExpression: "CHECK ((is_active AND inactive_reason IS NULL AND " +
							"finished_at IS NULL AND replacement_id IS NULL) OR " +
							"(NOT is_active AND inactive_reason = 'replaced' AND " +
							"finished_at IS NOT NULL AND replacement_id IS NOT NULL) OR " +
							"(NOT is_active AND inactive_reason = 'archived' AND " +
							"finished_at IS NULL AND replacement_id IS NULL))",
					},
				},
			},
		},
	}

	assertNoChanges(t, current, desired)
}
