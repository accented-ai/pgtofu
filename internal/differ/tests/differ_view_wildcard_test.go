package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_ViewWildcardColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		current string
		desired string
		changed bool
	}{
		{
			name:    "table columns in catalog order",
			current: "SELECT item.id, item.label FROM inventory.items item",
			desired: "SELECT item.* FROM inventory.items item",
		},
		{
			name:    "unqualified wildcard with known relation",
			current: "SELECT item.id, item.label FROM inventory.items item",
			desired: "SELECT * FROM inventory.items item",
		},
		{
			name:    "relation column aliases",
			current: "SELECT item.key, item.label FROM inventory.items AS item(key)",
			desired: "SELECT item.* FROM inventory.items AS item(key)",
		},
		{
			name:    "CTE column aliases",
			current: "WITH input(key, label) AS (SELECT 1, 'entry') SELECT input.key, input.label FROM input",
			desired: "WITH input(key, label) AS (SELECT 1, 'entry') SELECT * FROM input",
		},
		{
			name: "nested CTE shadows outer names",
			current: `WITH input AS (SELECT 1 AS id), nested AS (
                WITH input AS (SELECT 'entry' AS label) SELECT input.label FROM input
            ) SELECT nested.label FROM nested`,
			desired: `WITH input AS (SELECT 1 AS id), nested AS (
                WITH input AS (SELECT 'entry' AS label) SELECT * FROM input
            ) SELECT * FROM nested`,
		},
		{
			name:    "does not guess search path",
			current: "SELECT item.id, item.label FROM items item",
			desired: "SELECT item.* FROM items item",
			changed: true,
		},
		{
			name:    "does not guess unknown relation columns",
			current: "SELECT item.id, item.label FROM inventory.unknown_items item",
			desired: "SELECT item.* FROM inventory.unknown_items item",
			changed: true,
		},
		{
			name:    "preserves projection order",
			current: "SELECT item.label, item.id FROM inventory.items item",
			desired: "SELECT item.* FROM inventory.items item",
			changed: true,
		},
		{
			name:    "preserves omitted columns",
			current: "SELECT item.id FROM inventory.items item",
			desired: "SELECT item.* FROM inventory.items item",
			changed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tables := []schema.Table{{Schema: "inventory", Name: "items", Columns: []schema.Column{
				{Name: "label", Position: 2, DataType: "text"},
				{Name: "id", Position: 1, DataType: "bigint"},
			}}}
			current := &schema.Database{Tables: tables, Views: []schema.View{
				{Schema: "inventory", Name: "item_labels", Definition: tt.current},
			}}
			desired := &schema.Database{Tables: tables, Views: []schema.View{
				{Schema: "inventory", Name: "item_labels", Definition: tt.desired},
			}}

			assertViewDefinitionChange(t, current, desired, tt.changed)
		})
	}
}

func TestDiffer_ViewWildcardUsesEachSchema(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{{Schema: "inventory", Name: "items", Columns: []schema.Column{
			{Name: "id", Position: 1, DataType: "bigint"},
		}}},
		Views: []schema.View{
			{
				Schema:     "inventory",
				Name:       "item_labels",
				Definition: "SELECT item.id FROM inventory.items item",
			},
		},
	}
	desired := &schema.Database{
		Tables: []schema.Table{{Schema: "inventory", Name: "items", Columns: []schema.Column{
			{Name: "id", Position: 1, DataType: "bigint"},
			{Name: "label", Position: 2, DataType: "text", IsNullable: true},
		}}},
		Views: []schema.View{
			{
				Schema:     "inventory",
				Name:       "item_labels",
				Definition: "SELECT item.* FROM inventory.items item",
			},
		},
	}

	assertViewDefinitionChange(t, current, desired, true)
}

func TestDiffer_MaterializedViewWildcardConvergence(t *testing.T) {
	t.Parallel()

	tables := []schema.Table{{Schema: "inventory", Name: "items", Columns: []schema.Column{
		{Name: "id", Position: 1, DataType: "bigint"},
		{Name: "label", Position: 2, DataType: "text"},
	}}}
	current := &schema.Database{
		Tables: tables,
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     "inventory",
				Name:       "item_labels",
				Definition: "SELECT item.id, item.label FROM inventory.items item",
			},
		},
	}
	desired := &schema.Database{
		Tables: tables,
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     "inventory",
				Name:       "item_labels",
				Definition: "SELECT item.* FROM inventory.items item",
			},
		},
	}

	assertViewDefinitionChange(t, current, desired, false)
}

func TestViewComparator_AnonymousOutputNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		current string
		desired string
		equal   bool
	}{
		{
			name:    "anonymous literal",
			current: `SELECT 1 AS "?column?"`,
			desired: "SELECT 1",
			equal:   true,
		},
		{
			name:    "anonymous expression",
			current: `SELECT 1 + 2 AS "?column?"`,
			desired: "SELECT 1 + 2",
			equal:   true,
		},
		{
			name:    "renamed column",
			current: `SELECT id AS "?column?" FROM items`,
			desired: "SELECT id FROM items",
		},
		{
			name:    "renamed function",
			current: `SELECT count(*) AS "?column?" FROM items`,
			desired: "SELECT count(*) FROM items",
		},
		{
			name:    "renamed NULLIF expression",
			current: `SELECT NULLIF(1, 1) AS "?column?"`,
			desired: "SELECT NULLIF(1, 1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			comparator := differ.NewViewComparator(differ.DefaultOptions())
			if equal := comparator.AreEqual(
				schema.View{Definition: tt.current},
				schema.View{Definition: tt.desired},
			); equal != tt.equal {
				t.Fatalf("expected equality %v, got %v", tt.equal, equal)
			}
		})
	}
}
