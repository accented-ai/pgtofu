package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CompareIndexes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		current         *schema.Database
		desired         *schema.Database
		expectedChanges int
		expectedTypes   []differ.ChangeType
	}{
		{
			name: "add index",
			current: &schema.Database{
				Tables: []schema.Table{
					{Schema: schema.DefaultSchema, Name: "users", Indexes: []schema.Index{}},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "users",
								Name:      "idx_users_email",
								Columns:   []string{"email"},
								Type:      "btree",
							},
						},
					},
				},
			},
			expectedChanges: 1,
			expectedTypes:   []differ.ChangeType{differ.ChangeTypeAddIndex},
		},
		{
			name: "drop index",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "users",
								Name:      "idx_old",
								Columns:   []string{"old_column"},
								Type:      "btree",
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{Schema: schema.DefaultSchema, Name: "users", Indexes: []schema.Index{}},
				},
			},
			expectedChanges: 1,
			expectedTypes:   []differ.ChangeType{differ.ChangeTypeDropIndex},
		},
		{
			name: "no change when columns differ only by identifier quoting",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "items",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "items",
								Name:      "idx_items_position",
								Columns:   []string{`"position"`},
								Type:      "btree",
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "items",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "items",
								Name:      "idx_items_position",
								Columns:   []string{"position"},
								Type:      "btree",
							},
						},
					},
				},
			},
			expectedChanges: 0,
			expectedTypes:   nil,
		},
		{
			name: "no change when multi-column index has quoted reserved word",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "events",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "events",
								Name:      "idx_events_type_position",
								Columns:   []string{"type_id", `"position"`},
								Type:      "btree",
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "events",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "events",
								Name:      "idx_events_type_position",
								Columns:   []string{"type_id", "position"},
								Type:      "btree",
							},
						},
					},
				},
			},
			expectedChanges: 0,
			expectedTypes:   nil,
		},
		{
			name: "modify index",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "users",
								Name:      "idx_users",
								Columns:   []string{"email"},
								Type:      "btree",
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "users",
								Name:      "idx_users",
								Columns:   []string{"email", "name"},
								Type:      "btree",
							},
						},
					},
				},
			},
			expectedChanges: 1,
			expectedTypes:   []differ.ChangeType{differ.ChangeTypeModifyIndex},
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
