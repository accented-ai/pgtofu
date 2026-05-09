package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func stubAppTable(name string) schema.Table {
	return schema.Table{
		Schema: "app",
		Name:   name,
		Columns: []schema.Column{
			{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
		},
		Constraints: []schema.Constraint{
			{Name: name + "_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
		},
	}
}

// TestBatchingKeepsFKAfterReferencedNewTable exercises the case where an existing
// table is heavily modified (enough operations to trigger MaxOperationsPerFile-based
// splitting) and one of the modifications is a FOREIGN KEY that references a NEW
// table created in the same diff. The FK must not land in an earlier migration
// than the CREATE TABLE for its referenced table.
func TestBatchingKeepsFKAfterReferencedNewTable(t *testing.T) { //nolint:maintidx
	t.Parallel()

	// "items" is an existing table being heavily modified.
	// "groups" is a NEW table introduced by this diff.
	// "owners" is a NEW table that "groups" references (to ensure topological depth).
	// "users" / "categories" / "regions" exist already and are referenced unchanged.

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "app",
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "user_id", DataType: "uuid", IsNullable: false, Position: 2},
					{Name: "category_id", DataType: "uuid", IsNullable: false, Position: 3},
					{Name: "region_id", DataType: "uuid", IsNullable: false, Position: 4},
					{Name: "language", DataType: "text", IsNullable: false, Position: 5},
					{
						Name: "tags", DataType: "integer[]", IsNullable: false,
						Position: 6, Default: "'{}'",
					},
					{Name: "primary_value", DataType: "text", IsNullable: false, Position: 7},
					{Name: "label", DataType: "text", IsNullable: false, Position: 8},
					{Name: "tier", DataType: "text", IsNullable: false, Position: 9},
					{
						Name: "kind", DataType: "text", IsNullable: false,
						Position: 10, Default: "'standard'",
					},
					{Name: "note", DataType: "text", IsNullable: false, Position: 11},
					{
						Name: "summary", DataType: "text", IsNullable: false,
						Position: 12, Default: "''",
					},
					{Name: "score", DataType: "smallint", IsNullable: false, Position: 13},
					{Name: "source_ref", DataType: "text", IsNullable: true, Position: 14},
					{
						Name: "produced_at", DataType: "timestamptz", IsNullable: false,
						Position: 15, Default: "CURRENT_TIMESTAMP",
					},
					{
						Name: "created_at", DataType: "timestamptz", IsNullable: false,
						Position: 16, Default: "CURRENT_TIMESTAMP",
					},
					{Name: "updated_at", DataType: "timestamptz", IsNullable: true, Position: 17},
				},
				Constraints: []schema.Constraint{
					{Name: "items_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
					{
						Name:              "items_user_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"user_id"},
						ReferencedSchema:  "app",
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
						OnDelete:          "CASCADE",
					},
					{
						Name:              "items_category_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"category_id"},
						ReferencedSchema:  "app",
						ReferencedTable:   "categories",
						ReferencedColumns: []string{"id"},
						OnDelete:          "CASCADE",
					},
					{
						Name:              "items_region_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"region_id"},
						ReferencedSchema:  "app",
						ReferencedTable:   "regions",
						ReferencedColumns: []string{"id"},
						OnDelete:          "CASCADE",
					},
					{
						Name:            "items_tier_check",
						Type:            "CHECK",
						CheckExpression: "tier IN ('low', 'mid', 'high')",
					},
					{
						Name:            "items_score_check",
						Type:            "CHECK",
						CheckExpression: "score BETWEEN 0 AND 100",
					},
				},
				Indexes: []schema.Index{
					{
						Schema:    "app",
						Name:      "idx_items_user_category",
						TableName: "items",
						Columns:   []string{"user_id", "category_id"},
					},
					{
						Schema:    "app",
						Name:      "idx_items_category",
						TableName: "items",
						Columns:   []string{"category_id"},
					},
					{
						Schema:    "app",
						Name:      "idx_items_region",
						TableName: "items",
						Columns:   []string{"region_id"},
					},
				},
			},
			stubAppTable("users"),
			stubAppTable("categories"),
			stubAppTable("regions"),
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "app",
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "group_id", DataType: "uuid", IsNullable: false, Position: 2},
					{Name: "ordering", DataType: "smallint", IsNullable: false, Position: 3},
					{Name: "label", DataType: "text", IsNullable: false, Position: 4},
					{Name: "tier", DataType: "text", IsNullable: false, Position: 5},
					{
						Name: "kind", DataType: "text", IsNullable: false,
						Position: 6, Default: "'standard'",
					},
					{Name: "note", DataType: "text", IsNullable: false, Position: 7},
					{
						Name: "summary", DataType: "text", IsNullable: false,
						Position: 8, Default: "''",
					},
					{
						Name: "review_status", DataType: "text", IsNullable: false,
						Position: 9, Default: "'pending'",
					},
					{Name: "reviewed_by", DataType: "text", IsNullable: true, Position: 10},
					{Name: "reviewed_at", DataType: "timestamptz", IsNullable: true, Position: 11},
					{Name: "rejection_reason", DataType: "text", IsNullable: true, Position: 12},
					{Name: "review_notes", DataType: "text", IsNullable: true, Position: 13},
					{
						Name: "created_at", DataType: "timestamptz", IsNullable: false,
						Position: 14, Default: "CURRENT_TIMESTAMP",
					},
					{Name: "updated_at", DataType: "timestamptz", IsNullable: true, Position: 15},
				},
				Constraints: []schema.Constraint{
					{Name: "items_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
					{
						Name:              "items_group_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"group_id"},
						ReferencedSchema:  "app",
						ReferencedTable:   "groups",
						ReferencedColumns: []string{"id"},
						OnDelete:          "CASCADE",
					},
					{
						Name:    "items_group_ordering_key",
						Type:    "UNIQUE",
						Columns: []string{"group_id", "ordering"},
					},
					{
						Name:    "items_group_label_key",
						Type:    "UNIQUE",
						Columns: []string{"group_id", "label"},
					},
					{
						Name:            "items_tier_check",
						Type:            "CHECK",
						CheckExpression: "tier IN ('low', 'mid', 'high')",
					},
					{
						Name:            "items_label_check",
						Type:            "CHECK",
						CheckExpression: "label <> ''",
					},
					{
						Name:            "items_ordering_check",
						Type:            "CHECK",
						CheckExpression: "ordering > 0",
					},
					{
						Name:            "items_review_status_check",
						Type:            "CHECK",
						CheckExpression: "review_status IN ('pending', 'approved', 'rejected')",
					},
				},
				Indexes: []schema.Index{
					{
						Schema:    "app",
						Name:      "idx_items_group_status",
						TableName: "items",
						Columns:   []string{"group_id", "review_status"},
					},
				},
			},
			// NEW: groups (referenced by items.group_id_fkey)
			{
				Schema: "app",
				Name:   "groups",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "owner_id", DataType: "uuid", IsNullable: false, Position: 2},
					{Name: "category_id", DataType: "uuid", IsNullable: false, Position: 3},
				},
				Constraints: []schema.Constraint{
					{Name: "groups_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
					{
						Name:              "groups_owner_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"owner_id"},
						ReferencedSchema:  "app",
						ReferencedTable:   "owners",
						ReferencedColumns: []string{"id"},
						OnDelete:          "CASCADE",
					},
					{
						Name:              "groups_category_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"category_id"},
						ReferencedSchema:  "app",
						ReferencedTable:   "categories",
						ReferencedColumns: []string{"id"},
						OnDelete:          "CASCADE",
					},
				},
			},
			// NEW: owners (referenced by groups.owner_id_fkey)
			{
				Schema: "app",
				Name:   "owners",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "user_id", DataType: "uuid", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{Name: "owners_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
					{
						Name:              "owners_user_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"user_id"},
						ReferencedSchema:  "app",
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
						OnDelete:          "CASCADE",
					},
				},
			},
			stubAppTable("users"),
			stubAppTable("categories"),
			stubAppTable("regions"),
		},
	}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)

	fkMigration := -1
	groupsMigration := -1

	for i, m := range genResult.Migrations {
		if m.UpFile == nil {
			continue
		}

		content := m.UpFile.Content

		if strings.Contains(content, "items_group_id_fkey") &&
			strings.Contains(content, "FOREIGN KEY") {
			fkMigration = i
		}

		if strings.Contains(content, "CREATE TABLE app.groups") {
			groupsMigration = i
		}
	}

	require.NotEqual(t, -1, fkMigration, "FK constraint must appear in a migration")
	require.NotEqual(t, -1, groupsMigration, "groups CREATE TABLE must appear in a migration")

	require.LessOrEqual(t, groupsMigration, fkMigration,
		"CREATE TABLE app.groups (migration %d) must run before "+
			"FK items_group_id_fkey (migration %d)",
		groupsMigration, fkMigration)
}
