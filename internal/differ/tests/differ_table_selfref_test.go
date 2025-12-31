package differ_test

import (
	"slices"
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestSelfReferentialForeignKey(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "parent_item_id", DataType: "uuid", IsNullable: true, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:              "items_parent_item_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"parent_item_id"},
						ReferencedTable:   "items",
						ReferencedSchema:  schema.DefaultSchema,
						ReferencedColumns: []string{"id"},
					},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Changes) == 0 {
		t.Fatal("expected at least one change")
	}

	hasTableChange := false

	for _, change := range result.Changes {
		if change.Type == differ.ChangeTypeAddTable && change.ObjectName == "public.items" {
			hasTableChange = true

			if len(change.DependsOn) > 0 {
				for _, dep := range change.DependsOn {
					if dep == "public.items" {
						t.Error(
							"self-referential foreign key should not create a dependency on itself",
						)
					}
				}
			}
		}
	}

	if !hasTableChange {
		t.Error("expected ADD_TABLE change for items table")
	}
}

func TestCrossSchemaDependencies(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "app",
				Name:   "codes",
				Columns: []schema.Column{
					{Name: "code", DataType: "text", IsNullable: false, Position: 1},
				},
			},
			{
				Schema: "content",
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "code", DataType: "text", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:              "items_code_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"code"},
						ReferencedTable:   "codes",
						ReferencedSchema:  "app",
						ReferencedColumns: []string{"code"},
					},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var contentTableChange *differ.Change

	for i := range result.Changes {
		if result.Changes[i].Type == differ.ChangeTypeAddTable &&
			result.Changes[i].ObjectName == "content.items" {
			contentTableChange = &result.Changes[i]
			break
		}
	}

	if contentTableChange == nil {
		t.Fatal("expected ADD_TABLE change for content.items")
	}

	foundAppDependency := slices.Contains(contentTableChange.DependsOn, "app.codes")
	if !foundAppDependency {
		t.Error("expected content.items to depend on app.codes")
	}

	var appTableIndex, contentTableIndex int

	for i, change := range result.Changes {
		if change.ObjectName == "app.codes" {
			appTableIndex = i
		}

		if change.ObjectName == "content.items" {
			contentTableIndex = i
		}
	}

	if appTableIndex >= contentTableIndex {
		t.Errorf(
			"app.codes (index %d) should come before content.items (index %d)",
			appTableIndex,
			contentTableIndex,
		)
	}
}
