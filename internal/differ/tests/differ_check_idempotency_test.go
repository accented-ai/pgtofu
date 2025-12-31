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
