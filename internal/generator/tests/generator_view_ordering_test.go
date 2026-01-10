package generator_test

import (
	"strings"
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestViewRecreationOrderingWithNewColumns(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "orders",
				Columns: []schema.Column{
					{Name: "region", DataType: "varchar(50)", IsNullable: false, Position: 1},
					{Name: "product", DataType: "varchar(50)", IsNullable: false, Position: 2},
					{Name: "status", DataType: "varchar(50)", IsNullable: false, Position: 3},
					{Name: "old_amount", DataType: "bigint", IsNullable: true, Position: 4},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "orders_pkey_old",
						Type:    "PRIMARY KEY",
						Columns: []string{"region", "product", "status"},
					},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     "public",
				Name:       "order_summary",
				Definition: "SELECT region, product, status, old_amount FROM public.orders",
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "orders",
				Columns: []schema.Column{
					{Name: "category", DataType: "varchar(50)", IsNullable: false, Position: 1},
					{Name: "product", DataType: "varchar(50)", IsNullable: false, Position: 2},
					{Name: "status", DataType: "varchar(20)", IsNullable: false, Position: 3},
					{Name: "new_amount", DataType: "timestamptz", IsNullable: true, Position: 4},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "orders_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"category", "product", "status"},
					},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     "public",
				Name:       "order_summary",
				Definition: "SELECT category, product, status, new_amount FROM public.orders",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected differ error: %v", err)
	}

	opts := generator.DefaultOptions()
	opts.PreviewMode = true
	opts.GenerateDownMigrations = false
	gen := generator.New(opts)

	genResult, err := gen.Generate(result)
	if err != nil {
		t.Fatalf("unexpected generator error: %v", err)
	}

	if len(genResult.Migrations) == 0 {
		t.Fatal("expected at least one migration")
	}

	content := genResult.Migrations[0].UpFile.Content

	dropViewIdx := strings.Index(content, "DROP VIEW")
	createViewIdx := strings.Index(content, "CREATE VIEW")
	addColumnIdx := strings.Index(content, "ADD COLUMN category")

	if dropViewIdx == -1 {
		t.Fatal("DROP VIEW not found in migration")
	}

	if createViewIdx == -1 {
		t.Fatal("CREATE VIEW not found in migration")
	}

	if addColumnIdx == -1 {
		t.Fatal("ADD COLUMN category not found in migration")
	}

	if dropViewIdx >= addColumnIdx {
		t.Errorf(
			"DROP VIEW (pos %d) should come before ADD COLUMN category (pos %d)",
			dropViewIdx,
			addColumnIdx,
		)
	}

	if addColumnIdx >= createViewIdx {
		t.Errorf(
			"ADD COLUMN category (pos %d) should come before CREATE VIEW (pos %d)",
			addColumnIdx,
			createViewIdx,
		)
	}
}

func TestMaterializedViewRecreationOrderingWithNewColumns(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "value", DataType: "varchar(50)", IsNullable: true, Position: 2},
				},
			},
		},
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     schema.DefaultSchema,
				Name:       "metric_summary",
				Definition: "SELECT id, value FROM metrics",
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "value", DataType: "text", IsNullable: true, Position: 2},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: true, Position: 3},
				},
			},
		},
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     schema.DefaultSchema,
				Name:       "metric_summary",
				Definition: "SELECT id, value, recorded_at FROM metrics",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected differ error: %v", err)
	}

	opts := generator.DefaultOptions()
	opts.PreviewMode = true
	opts.GenerateDownMigrations = false
	gen := generator.New(opts)

	genResult, err := gen.Generate(result)
	if err != nil {
		t.Fatalf("unexpected generator error: %v", err)
	}

	if len(genResult.Migrations) == 0 {
		t.Fatal("expected at least one migration")
	}

	content := genResult.Migrations[0].UpFile.Content

	dropMVIdx := strings.Index(content, "DROP MATERIALIZED VIEW")
	createMVIdx := strings.Index(content, "CREATE MATERIALIZED VIEW")
	addColumnIdx := strings.Index(content, "ADD COLUMN recorded_at")

	if dropMVIdx == -1 {
		t.Fatal("DROP MATERIALIZED VIEW not found in migration")
	}

	if createMVIdx == -1 {
		t.Fatal("CREATE MATERIALIZED VIEW not found in migration")
	}

	if addColumnIdx == -1 {
		t.Fatal("ADD COLUMN recorded_at not found in migration")
	}

	if dropMVIdx >= addColumnIdx {
		t.Errorf(
			"DROP MATERIALIZED VIEW (pos %d) should come before ADD COLUMN (pos %d)",
			dropMVIdx,
			addColumnIdx,
		)
	}

	if addColumnIdx >= createMVIdx {
		t.Errorf(
			"ADD COLUMN (pos %d) should come before CREATE MATERIALIZED VIEW (pos %d)",
			addColumnIdx,
			createMVIdx,
		)
	}
}
