package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CompareViews(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		current         *schema.Database
		desired         *schema.Database
		expectedChanges int
		expectedTypes   []differ.ChangeType
	}{
		{
			name:    "add view",
			current: &schema.Database{Views: []schema.View{}},
			desired: &schema.Database{
				Views: []schema.View{
					{
						Schema:     schema.DefaultSchema,
						Name:       "user_emails",
						Definition: "SELECT id, email FROM users",
					},
				},
			},
			expectedChanges: 1,
			expectedTypes:   []differ.ChangeType{differ.ChangeTypeAddView},
		},
		{
			name: "modify view",
			current: &schema.Database{
				Views: []schema.View{
					{
						Schema:     schema.DefaultSchema,
						Name:       "user_emails",
						Definition: "SELECT id, email FROM users",
					},
				},
			},
			desired: &schema.Database{
				Views: []schema.View{
					{
						Schema:     schema.DefaultSchema,
						Name:       "user_emails",
						Definition: "SELECT id, email, created_at FROM users",
					},
				},
			},
			expectedChanges: 1,
			expectedTypes:   []differ.ChangeType{differ.ChangeTypeModifyView},
		},
		{
			name: "drop view",
			current: &schema.Database{
				Views: []schema.View{
					{
						Schema:     schema.DefaultSchema,
						Name:       "user_emails",
						Definition: "SELECT id, email FROM users",
					},
				},
			},
			desired:         &schema.Database{Views: []schema.View{}},
			expectedChanges: 1,
			expectedTypes:   []differ.ChangeType{differ.ChangeTypeDropView},
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
			}

			for i, expectedType := range tt.expectedTypes {
				if i >= len(result.Changes) {
					t.Errorf(
						"expected change type %s at index %d, but no change found",
						expectedType,
						i,
					)

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
		})
	}
}
