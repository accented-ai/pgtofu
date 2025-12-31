package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CheckConstraint_IntegerLiteralsOnFloat(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "stats",
				Columns: []schema.Column{
					{
						Name:       "percentage",
						DataType:   "double precision",
						IsNullable: false,
						Position:   1,
					},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "stats_percentage_check",
						Type:            "CHECK",
						Columns:         []string{"percentage"},
						Definition:      "CHECK (percentage BETWEEN 0 AND 100)",
						CheckExpression: "CHECK (percentage BETWEEN 0 AND 100)",
					},
				},
			},
		},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "stats",
				Columns: []schema.Column{
					{
						Name:       "percentage",
						DataType:   "double precision",
						IsNullable: false,
						Position:   1,
					},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "stats_percentage_check",
						Type:    "CHECK",
						Columns: []string{"percentage"},
						Definition: "CHECK (((percentage >= (0)::double precision) AND " +
							"(percentage <= (100)::double precision)))",
						CheckExpression: "CHECK (((percentage >= (0)::double precision) AND " +
							"(percentage <= (100)::double precision)))",
					},
				},
			},
		},
	}

	assertNoChanges(t, current, desired)
}
