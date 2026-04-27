package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CompareIndexes(t *testing.T) { //nolint:maintidx
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
		{
			name: "no change for HNSW index with matching opclass and storage params",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: "vectors",
						Name:   "documents",
						Indexes: []schema.Index{
							{
								Schema:    "vectors",
								TableName: "documents",
								Name:      "documents_hnsw_idx",
								Columns:   []string{"embedding halfvec_cosine_ops"},
								Type:      "hnsw",
								StorageParams: map[string]string{
									"m":               "16",
									"ef_construction": "64",
								},
								Where: "model_id = 'primary' AND task_type = 'search'",
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: "vectors",
						Name:   "documents",
						Indexes: []schema.Index{
							{
								Schema:    "vectors",
								TableName: "documents",
								Name:      "documents_hnsw_idx",
								Columns:   []string{"embedding halfvec_cosine_ops"},
								Type:      "hnsw",
								StorageParams: map[string]string{
									"m":               "16",
									"ef_construction": "64",
								},
								Where: "model_id = 'primary' AND task_type = 'search'",
							},
						},
					},
				},
			},
			expectedChanges: 0,
			expectedTypes:   nil,
		},
		{
			name: "modify when HNSW storage params differ",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: "vectors",
						Name:   "documents",
						Indexes: []schema.Index{
							{
								Schema:    "vectors",
								TableName: "documents",
								Name:      "documents_hnsw_idx",
								Columns:   []string{"embedding halfvec_cosine_ops"},
								Type:      "hnsw",
								StorageParams: map[string]string{
									"m":               "16",
									"ef_construction": "64",
								},
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: "vectors",
						Name:   "documents",
						Indexes: []schema.Index{
							{
								Schema:    "vectors",
								TableName: "documents",
								Name:      "documents_hnsw_idx",
								Columns:   []string{"embedding halfvec_cosine_ops"},
								Type:      "hnsw",
								StorageParams: map[string]string{
									"m":               "32",
									"ef_construction": "64",
								},
							},
						},
					},
				},
			},
			expectedChanges: 1,
			expectedTypes:   []differ.ChangeType{differ.ChangeTypeModifyIndex},
		},
		{
			name: "no change when extractor WHERE has casts and extra parens",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: "vectors",
						Name:   "documents",
						Indexes: []schema.Index{
							{
								Schema:    "vectors",
								TableName: "documents",
								Name:      "documents_hnsw_idx",
								Columns:   []string{"embedding halfvec_cosine_ops"},
								Type:      "hnsw",
								StorageParams: map[string]string{
									"m":               "16",
									"ef_construction": "64",
								},
								Where: "((model_id = 'primary'::text) AND (task_type = 'search'::text))",
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: "vectors",
						Name:   "documents",
						Indexes: []schema.Index{
							{
								Schema:    "vectors",
								TableName: "documents",
								Name:      "documents_hnsw_idx",
								Columns:   []string{"embedding halfvec_cosine_ops"},
								Type:      "hnsw",
								StorageParams: map[string]string{
									"m":               "16",
									"ef_construction": "64",
								},
								Where: "model_id = 'primary' AND task_type = 'search'",
							},
						},
					},
				},
			},
			expectedChanges: 0,
			expectedTypes:   nil,
		},
		{
			name: "no change when NullsNotDistinct matches",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "items",
						Indexes: []schema.Index{
							{
								Schema:           schema.DefaultSchema,
								TableName:        "items",
								Name:             "uq_items_identity",
								Columns:          []string{"a", "b"},
								Type:             "btree",
								IsUnique:         true,
								NullsNotDistinct: true,
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
								Schema:           schema.DefaultSchema,
								TableName:        "items",
								Name:             "uq_items_identity",
								Columns:          []string{"a", "b"},
								Type:             "btree",
								IsUnique:         true,
								NullsNotDistinct: true,
							},
						},
					},
				},
			},
			expectedChanges: 0,
			expectedTypes:   nil,
		},
		{
			name: "modify when NullsNotDistinct differs",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "items",
						Indexes: []schema.Index{
							{
								Schema:           schema.DefaultSchema,
								TableName:        "items",
								Name:             "uq_items_identity",
								Columns:          []string{"a", "b"},
								Type:             "btree",
								IsUnique:         true,
								NullsNotDistinct: false,
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
								Schema:           schema.DefaultSchema,
								TableName:        "items",
								Name:             "uq_items_identity",
								Columns:          []string{"a", "b"},
								Type:             "btree",
								IsUnique:         true,
								NullsNotDistinct: true,
							},
						},
					},
				},
			},
			expectedChanges: 1,
			expectedTypes:   []differ.ChangeType{differ.ChangeTypeModifyIndex},
		},
		{
			name: "no change when parser has explicit ASC but extractor omits it",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "orders",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "orders",
								Name:      "idx_orders_customer_seq",
								Columns:   []string{"customer_id", "seq"},
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
						Name:   "orders",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "orders",
								Name:      "idx_orders_customer_seq",
								Columns:   []string{"customer_id", "seq ASC"},
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
			name: "no change when DESC NULLS FIRST matches bare DESC",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "events",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "events",
								Name:      "idx_events_created",
								Columns:   []string{"created_at DESC"},
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
								Name:      "idx_events_created",
								Columns:   []string{"created_at DESC NULLS FIRST"},
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
			name: "modify when sort direction actually differs",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "events",
						Indexes: []schema.Index{
							{
								Schema:    schema.DefaultSchema,
								TableName: "events",
								Name:      "idx_events_created",
								Columns:   []string{"created_at"},
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
								Name:      "idx_events_created",
								Columns:   []string{"created_at DESC"},
								Type:      "btree",
							},
						},
					},
				},
			},
			expectedChanges: 1,
			expectedTypes:   []differ.ChangeType{differ.ChangeTypeModifyIndex},
		},
		{
			name: "modify when opclass differs",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: "vectors",
						Name:   "documents",
						Indexes: []schema.Index{
							{
								Schema:    "vectors",
								TableName: "documents",
								Name:      "documents_hnsw_idx",
								Columns:   []string{"embedding halfvec_cosine_ops"},
								Type:      "hnsw",
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: "vectors",
						Name:   "documents",
						Indexes: []schema.Index{
							{
								Schema:    "vectors",
								TableName: "documents",
								Name:      "documents_hnsw_idx",
								Columns:   []string{"embedding halfvec_l2_ops"},
								Type:      "hnsw",
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
