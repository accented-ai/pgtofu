package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CheckConstraint_BetweenExpansion(t *testing.T) {
	t.Parallel()

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
				},
				Constraints: []schema.Constraint{
					{
						Name:            "items_rating_check",
						Type:            "CHECK",
						Columns:         []string{"rating"},
						CheckExpression: "CHECK (rating BETWEEN 0 AND 1)",
						Definition:      "CHECK (rating BETWEEN 0 AND 1)",
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
						Name:       "rating",
						DataType:   "double precision",
						IsNullable: false,
						Position:   1,
					},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "items_rating_check",
						Type:    "CHECK",
						Columns: []string{"rating"},
						CheckExpression: "CHECK ((rating >= (0)::double precision) AND " +
							"(rating <= (1)::double precision))",
						Definition: "CHECK ((rating >= (0)::double precision) AND " +
							"(rating <= (1)::double precision))",
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
			"Expected no changes for BETWEEN check constraint, got %d changes:",
			len(result.Changes),
		)

		for _, change := range result.Changes {
			t.Errorf("  [%s] %s: %s", change.Severity, change.Type, change.Description)
		}
	}
}

func TestDiffer_CheckConstraint_InClause(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "status", DataType: "text", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "items_status_check",
						Type:            "CHECK",
						Columns:         []string{"status"},
						CheckExpression: "CHECK (status IN ('pending', 'processing', 'completed', 'cancelled', 'failed'))",
						Definition:      "CHECK (status IN ('pending', 'processing', 'completed', 'cancelled', 'failed'))",
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
					{Name: "status", DataType: "text", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "items_status_check",
						Type:    "CHECK",
						Columns: []string{"status"},
						CheckExpression: "CHECK ((status = ANY (ARRAY['pending'::text, 'processing'::text, " +
							"'completed'::text, 'cancelled'::text, 'failed'::text])))",
						Definition: "CHECK ((status = ANY (ARRAY['pending'::text, 'processing'::text, " +
							"'completed'::text, 'cancelled'::text, 'failed'::text])))",
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

	if len(result.Changes) > 0 {
		for _, change := range result.Changes {
			t.Logf("Change detected: [%s] %s: %s", change.Severity, change.Type, change.Description)
		}
	}
}

func TestDiffer_ForeignKey_SchemaQualification(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "users_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"id"},
					},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "posts",
				Columns: []schema.Column{
					{Name: "user_id", DataType: "uuid", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:              "posts_user_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"user_id"},
						ReferencedSchema:  schema.DefaultSchema,
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
						OnDelete:          "CASCADE",
						OnUpdate:          "NO ACTION",
					},
				},
			},
		},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "users_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"id"},
					},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "posts",
				Columns: []schema.Column{
					{Name: "user_id", DataType: "uuid", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:              "posts_user_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"user_id"},
						ReferencedSchema:  schema.DefaultSchema,
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
						OnDelete:          "CASCADE",
						OnUpdate:          "NO ACTION",
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
			"Expected no changes for FK with schema qualification, got %d changes:",
			len(result.Changes),
		)

		for _, change := range result.Changes {
			t.Errorf("  [%s] %s: %s", change.Severity, change.Type, change.Description)
		}
	}
}

func TestDiffer_ForeignKey_NoActionDefault(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "parent",
				Columns: []schema.Column{
					{Name: "id", DataType: "integer", IsNullable: false, Position: 1},
				},
			},
			{
				Schema: "public",
				Name:   "child",
				Columns: []schema.Column{
					{Name: "parent_id", DataType: "integer", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:              "child_parent_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"parent_id"},
						ReferencedTable:   "parent",
						ReferencedColumns: []string{"id"},
						OnDelete:          "",
						OnUpdate:          "",
					},
				},
			},
		},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "parent",
				Columns: []schema.Column{
					{Name: "id", DataType: "integer", IsNullable: false, Position: 1},
				},
			},
			{
				Schema: "public",
				Name:   "child",
				Columns: []schema.Column{
					{Name: "parent_id", DataType: "integer", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:              "child_parent_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"parent_id"},
						ReferencedTable:   "parent",
						ReferencedColumns: []string{"id"},
						OnDelete:          "NO ACTION",
						OnUpdate:          "NO ACTION",
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
		t.Errorf("Expected no changes for default NO ACTION, got %d changes:", len(result.Changes))

		for _, change := range result.Changes {
			t.Errorf("  [%s] %s: %s", change.Severity, change.Type, change.Description)
		}
	}
}

func TestDiffer_ForeignKey_OnDeleteCascadeSetNull(t *testing.T) {
	t.Parallel()

	db := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "users_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"id"},
					},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "categories",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "posts",
				Columns: []schema.Column{
					{Name: "user_id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "category_id", DataType: "uuid", IsNullable: true, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:              "posts_user_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"user_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
						OnDelete:          "CASCADE",
						OnUpdate:          "NO ACTION",
					},
					{
						Name:              "posts_category_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"category_id"},
						ReferencedTable:   "categories",
						ReferencedColumns: []string{"id"},
						OnDelete:          "SET NULL",
						OnUpdate:          "NO ACTION",
					},
				},
			},
		},
	}

	assertNoChanges(t, db, db)
}

func TestDiffer_ConstraintName_Truncation(t *testing.T) {
	t.Parallel()

	db := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "col1", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "col2", DataType: "text", IsNullable: false, Position: 2},
					{Name: "col3", DataType: "text", IsNullable: false, Position: 3},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "items_col1_col2_col3_very_long_name_truncated_here",
						Type:    "UNIQUE",
						Columns: []string{"col1", "col2", "col3"},
					},
				},
			},
		},
	}

	assertNoChanges(t, db, db)
}

func TestDiffer_CheckConstraint_InClause_PostgresExpansion(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "status", DataType: "text", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:            "items_status_check",
						Type:            "CHECK",
						Columns:         []string{"status"},
						CheckExpression: "CHECK (status IN ('pending', 'completed', 'cancelled'))",
						Definition:      "CHECK (status IN ('pending', 'completed', 'cancelled'))",
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
					{Name: "status", DataType: "text", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "items_status_check",
						Type:    "CHECK",
						Columns: []string{"status"},
						CheckExpression: "CHECK ((status = ANY (ARRAY['pending'::text, 'completed'::text, " +
							"'cancelled'::text])))",
						Definition: "CHECK ((status = ANY (ARRAY['pending'::text, 'completed'::text, " +
							"'cancelled'::text])))",
					},
				},
			},
		},
	}

	assertNoChanges(t, current, desired)
}
