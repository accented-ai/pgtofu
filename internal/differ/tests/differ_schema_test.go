package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CompareSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		current          *schema.Database
		desired          *schema.Database
		expectedChanges  int
		expectedTypes    []differ.ChangeType
		expectedSeverity []differ.ChangeSeverity
	}{
		{
			name:    "add new schema",
			current: &schema.Database{Schemas: []schema.Schema{}},
			desired: &schema.Database{
				Schemas: []schema.Schema{
					{Name: "app"},
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeAddSchema},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe},
		},
		{
			name: "add multiple schemas",
			current: &schema.Database{
				Schemas: []schema.Schema{
					{Name: schema.DefaultSchema},
				},
			},
			desired: &schema.Database{
				Schemas: []schema.Schema{
					{Name: schema.DefaultSchema},
					{Name: "app"},
					{Name: "shop"},
				},
			},
			expectedChanges: 2,
			expectedTypes: []differ.ChangeType{
				differ.ChangeTypeAddSchema,
				differ.ChangeTypeAddSchema,
			},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe, differ.SeveritySafe},
		},
		{
			name: "drop schema",
			current: &schema.Database{
				Schemas: []schema.Schema{
					{Name: "old_schema"},
				},
			},
			desired:          &schema.Database{Schemas: []schema.Schema{}},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeDropSchema},
			expectedSeverity: []differ.ChangeSeverity{differ.SeverityBreaking},
		},
		{
			name: "no changes",
			current: &schema.Database{
				Schemas: []schema.Schema{
					{Name: "app"},
					{Name: "shop"},
				},
			},
			desired: &schema.Database{
				Schemas: []schema.Schema{
					{Name: "app"},
					{Name: "shop"},
				},
			},
			expectedChanges:  0,
			expectedTypes:    []differ.ChangeType{},
			expectedSeverity: []differ.ChangeSeverity{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := differ.New(differ.DefaultOptions())

			result, err := d.Compare(tt.current, tt.desired)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(result.Changes) != tt.expectedChanges {
				t.Errorf("expected %d changes, got %d", tt.expectedChanges, len(result.Changes))

				for i, ch := range result.Changes {
					t.Logf("  change %d: %s - %s", i, ch.Type, ch.Description)
				}
			}

			for i, expectedType := range tt.expectedTypes {
				if i >= len(result.Changes) {
					continue
				}

				if result.Changes[i].Type != expectedType {
					t.Errorf(
						"expected change type %s, got %s",
						expectedType,
						result.Changes[i].Type,
					)
				}
			}

			for i, expectedSev := range tt.expectedSeverity {
				if i >= len(result.Changes) {
					continue
				}

				if result.Changes[i].Severity != expectedSev {
					t.Errorf(
						"expected severity %s, got %s",
						expectedSev,
						result.Changes[i].Severity,
					)
				}
			}
		})
	}
}

func TestDiffer_SchemaDependencyOrdering(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Schemas: []schema.Schema{},
	}
	desired := &schema.Database{
		Schemas: []schema.Schema{
			{Name: "app"},
		},
		Tables: []schema.Table{
			{
				Schema: "app",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", Position: 1},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Changes) != 2 {
		t.Fatalf("expected 2 changes, got %d", len(result.Changes))
	}

	if result.Changes[0].Type != differ.ChangeTypeAddSchema {
		t.Errorf("expected first change to be ADD_SCHEMA, got %s", result.Changes[0].Type)
	}

	if result.Changes[1].Type != differ.ChangeTypeAddTable {
		t.Errorf("expected second change to be ADD_TABLE, got %s", result.Changes[1].Type)
	}
}
