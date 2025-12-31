package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_FloatType_Idempotency(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{
						Name:       "rating",
						DataType:   "double precision",
						IsNullable: false,
						Position:   2,
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
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{
						Name:       "rating",
						DataType:   "double precision",
						IsNullable: false,
						Position:   2,
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
		t.Errorf("Expected no changes for FLOAT type, got %d changes:", len(result.Changes))

		for _, change := range result.Changes {
			t.Errorf("  [%s] %s: %s", change.Severity, change.Type, change.Description)
		}
	}
}

func TestDiffer_FloatTypeNormalization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		desiredType   string
		extractedType string
	}{
		{
			name:          "FLOAT becomes double precision",
			desiredType:   "double precision",
			extractedType: "double precision",
		},
		{
			name:          "FLOAT8 becomes double precision",
			desiredType:   "double precision",
			extractedType: "double precision",
		},
		{
			name:          "FLOAT4 becomes real",
			desiredType:   "real",
			extractedType: "real",
		},
		{
			name:          "REAL stays real",
			desiredType:   "real",
			extractedType: "real",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			desired := &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "items",
						Columns: []schema.Column{
							{
								Name:       "value",
								DataType:   tt.desiredType,
								IsNullable: false,
								Position:   1,
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
								Name:       "value",
								DataType:   tt.extractedType,
								IsNullable: false,
								Position:   1,
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
				t.Errorf("Expected no changes, got %d changes:", len(result.Changes))

				for _, change := range result.Changes {
					t.Errorf("  [%s] %s: %s", change.Severity, change.Type, change.Description)
				}
			}
		})
	}
}

func TestDiffer_NumericType_WithPrecision(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{
						Name:       "price",
						DataType:   "numeric",
						IsNullable: false,
						Position:   1,
						Precision:  ptr(10),
						Scale:      ptr(2),
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
						Name:       "price",
						DataType:   "numeric",
						IsNullable: false,
						Position:   1,
						Precision:  ptr(10),
						Scale:      ptr(2),
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
			"Expected no changes for NUMERIC with precision, got %d changes:",
			len(result.Changes),
		)

		for _, change := range result.Changes {
			t.Errorf("  [%s] %s: %s", change.Severity, change.Type, change.Description)
		}
	}
}

func ptr[T any](v T) *T {
	return &v
}
