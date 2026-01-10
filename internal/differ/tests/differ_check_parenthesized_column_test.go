package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CheckConstraint_ParenthesizedColumnInClause(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{
						Name:       "status",
						DataType:   "character varying(20)",
						IsNullable: false,
						Position:   1,
					},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "items_status_check",
						Type:            "CHECK",
						Columns:         []string{"status"},
						CheckExpression: "CHECK (status IN ('pending', 'processing', 'completed'))",
						Definition:      "CHECK (status IN ('pending', 'processing', 'completed'))",
					},
				},
			},
		},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{
						Name:       "status",
						DataType:   "character varying(20)",
						IsNullable: false,
						Position:   1,
					},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "items_status_check",
						Type:    "CHECK",
						Columns: []string{"status"},
						CheckExpression: "CHECK (((status)::text = ANY (ARRAY['pending'::text, 'processing'::text, " +
							"'completed'::text])))",
						Definition: "CHECK (((status)::text = ANY (ARRAY['pending'::text, 'processing'::text, " +
							"'completed'::text])))",
					},
				},
			},
		},
	}

	assertNoChanges(t, current, desired)
}

func TestDiffer_CheckConstraint_MultipleParenthesizedColumnsInClause(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "category", DataType: "varchar(20)", IsNullable: false, Position: 1},
					{Name: "priority", DataType: "varchar(10)", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "items_category_check",
						Type:            "CHECK",
						Columns:         []string{"category"},
						CheckExpression: "CHECK (category IN ('electronics', 'clothing', 'food'))",
						Definition:      "CHECK (category IN ('electronics', 'clothing', 'food'))",
					},
					{
						Name:            "items_priority_check",
						Type:            "CHECK",
						Columns:         []string{"priority"},
						CheckExpression: "CHECK (priority IN ('low', 'medium', 'high'))",
						Definition:      "CHECK (priority IN ('low', 'medium', 'high'))",
					},
				},
			},
		},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "category", DataType: "varchar(20)", IsNullable: false, Position: 1},
					{Name: "priority", DataType: "varchar(10)", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "items_category_check",
						Type:    "CHECK",
						Columns: []string{"category"},
						CheckExpression: "CHECK (((category)::text = ANY (ARRAY['electronics'::text, " +
							"'clothing'::text, 'food'::text])))",
						Definition: "CHECK (((category)::text = ANY (ARRAY['electronics'::text, " +
							"'clothing'::text, 'food'::text])))",
					},
					{
						Name:    "items_priority_check",
						Type:    "CHECK",
						Columns: []string{"priority"},
						CheckExpression: "CHECK (((priority)::text = ANY (ARRAY['low'::text, 'medium'::text, " +
							"'high'::text])))",
						Definition: "CHECK (((priority)::text = ANY (ARRAY['low'::text, 'medium'::text, " +
							"'high'::text])))",
					},
				},
			},
		},
	}

	assertNoChanges(t, current, desired)
}
