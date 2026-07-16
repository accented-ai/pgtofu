package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CheckConstraint_RealWorldIdempotency(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{
						Name:       "rating",
						DataType:   "double precision",
						IsNullable: false,
						Position:   1,
					},
					{Name: "status", DataType: "text", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "items_rating_check",
						Type:    "CHECK",
						Columns: []string{"rating"},
						Definition: "CHECK (((rating >= (0)::double precision) AND " +
							"(rating <= (1)::double precision)))",
						CheckExpression: "CHECK (((rating >= (0)::double precision) AND " +
							"(rating <= (1)::double precision)))",
					},
					{
						Name:    "items_status_check",
						Type:    "CHECK",
						Columns: []string{"status"},
						Definition: "CHECK ((status = ANY (ARRAY['pending'::text, 'processing'::text, " +
							"'completed'::text, 'cancelled'::text, 'failed'::text])))",
						CheckExpression: "CHECK ((status = ANY (ARRAY['pending'::text, 'processing'::text, " +
							"'completed'::text, 'cancelled'::text, 'failed'::text])))",
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
					{
						Name:       "rating",
						DataType:   "double precision",
						IsNullable: false,
						Position:   1,
					},
					{Name: "status", DataType: "text", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "items_rating_check",
						Type:            "CHECK",
						Columns:         []string{"rating"},
						Definition:      "CHECK (rating BETWEEN 0 AND 1)",
						CheckExpression: "CHECK (rating BETWEEN 0 AND 1)",
					},
					{
						Name:            "items_status_check",
						Type:            "CHECK",
						Columns:         []string{"status"},
						Definition:      "CHECK (status IN ('pending', 'processing', 'completed', 'cancelled', 'failed'))",
						CheckExpression: "CHECK (status IN ('pending', 'processing', 'completed', 'cancelled', 'failed'))",
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

	if len(result.Changes) != 0 {
		t.Errorf(
			"Expected no changes for idempotent CHECK constraints, got %d changes:",
			len(result.Changes),
		)

		for _, change := range result.Changes {
			t.Errorf("  [%s] %s: %s", change.Severity, change.Type, change.Description)

			if details, ok := change.Details["constraint"]; ok {
				if c, ok := details.(*schema.Constraint); ok {
					t.Errorf("    Definition: %s", c.Definition)
				}
			}
		}
	}
}

func TestDiffer_CheckConstraint_FunctionCastIdempotency(t *testing.T) {
	t.Parallel()

	current := checkConstraintDB(
		"CHECK (((jsonb_typeof(error_summary) = 'array'::text) AND " +
			"(octet_length((error_summary)::text) <= 131072)))",
	)
	desired := checkConstraintDB(
		"CHECK (JSONB_TYPEOF(error_summary) = 'array' AND " +
			"OCTET_LENGTH(error_summary::TEXT) <= 131072)",
	)

	assertNoChanges(t, current, desired)
}

func TestDiffer_CheckConstraint_InPredicateComparisonIdempotency(t *testing.T) {
	t.Parallel()

	current := checkConstraintDB(
		"CHECK (((status = ANY (ARRAY['terminated'::text, " +
			"'already_terminal'::text])) = (terminated_at IS NOT NULL)))",
	)
	desired := checkConstraintDB(
		"CHECK ((status IN ('terminated', 'already_terminal')) = " +
			"(terminated_at IS NOT NULL))",
	)

	assertNoChanges(t, current, desired)
}

func checkConstraintDB(expression string) *schema.Database {
	return &schema.Database{Tables: []schema.Table{
		{
			Schema: schema.DefaultSchema,
			Name:   "items",
			Constraints: []schema.Constraint{
				{
					Name:            "items_check",
					Type:            "CHECK",
					Definition:      expression,
					CheckExpression: expression,
				},
			},
		},
	}}
}
