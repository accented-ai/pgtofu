package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CompareConstraints(t *testing.T) { //nolint:maintidx
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
			name: "add foreign key with CASCADE",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
						},
					},
					{
						Schema: schema.DefaultSchema,
						Name:   "posts",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
							{Name: "user_id", DataType: "bigint", IsNullable: true, Position: 2},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
						},
					},
					{
						Schema: schema.DefaultSchema,
						Name:   "posts",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
							{Name: "user_id", DataType: "bigint", IsNullable: true, Position: 2},
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
						},
					},
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeAddConstraint},
			expectedSeverity: []differ.ChangeSeverity{differ.SeverityDataMigrationRequired},
		},
		{
			name: "add CHECK constraint",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
							{Name: "age", DataType: "integer", IsNullable: true, Position: 2},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
							{Name: "age", DataType: "integer", IsNullable: true, Position: 2},
						},
						Constraints: []schema.Constraint{
							{
								Name:            "users_age_check",
								Type:            "CHECK",
								Columns:         []string{"age"},
								CheckExpression: "age >= 0 AND age <= 150",
							},
						},
					},
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeAddConstraint},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe},
		},
		{
			name: "add DEFERRABLE INITIALLY DEFERRED constraint",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "entities",
						Columns: []schema.Column{
							{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
							{Name: "type", DataType: "text", IsNullable: false, Position: 2},
							{
								Name:       "canonical_form",
								DataType:   "text",
								IsNullable: false,
								Position:   3,
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "entities",
						Columns: []schema.Column{
							{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
							{Name: "type", DataType: "text", IsNullable: false, Position: 2},
							{
								Name:       "canonical_form",
								DataType:   "text",
								IsNullable: false,
								Position:   3,
							},
						},
						Constraints: []schema.Constraint{
							{
								Name:              "entities_unique",
								Type:              "UNIQUE",
								Columns:           []string{"type", "canonical_form"},
								IsDeferrable:      true,
								InitiallyDeferred: true,
							},
						},
					},
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeAddConstraint},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe},
		},
		{
			name: "add composite primary key",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "memberships",
						Columns: []schema.Column{
							{Name: "team_id", DataType: "varchar", IsNullable: false, Position: 1},
							{
								Name:       "member_id",
								DataType:   "varchar",
								IsNullable: false,
								Position:   2,
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "memberships",
						Columns: []schema.Column{
							{Name: "team_id", DataType: "varchar", IsNullable: false, Position: 1},
							{
								Name:       "member_id",
								DataType:   "varchar",
								IsNullable: false,
								Position:   2,
							},
						},
						Constraints: []schema.Constraint{
							{
								Name:    "memberships_pkey",
								Type:    "PRIMARY KEY",
								Columns: []string{"team_id", "member_id"},
							},
						},
					},
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeAddConstraint},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe},
		},
		{
			name: "drop UNIQUE constraint",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
							{Name: "email", DataType: "varchar", IsNullable: false, Position: 2},
						},
						Constraints: []schema.Constraint{
							{
								Name:    "users_email_unique",
								Type:    "UNIQUE",
								Columns: []string{"email"},
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
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
							{Name: "email", DataType: "varchar", IsNullable: false, Position: 2},
						},
					},
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeDropConstraint},
			expectedSeverity: []differ.ChangeSeverity{differ.SeverityBreaking},
		},
		{
			name: "modify foreign key ON DELETE action",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "posts",
						Columns: []schema.Column{
							{Name: "user_id", DataType: "bigint", IsNullable: true, Position: 1},
						},
						Constraints: []schema.Constraint{
							{
								Name:              "posts_user_id_fkey",
								Type:              "FOREIGN KEY",
								Columns:           []string{"user_id"},
								ReferencedTable:   "users",
								ReferencedColumns: []string{"id"},
								OnDelete:          "NO ACTION",
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "posts",
						Columns: []schema.Column{
							{Name: "user_id", DataType: "bigint", IsNullable: true, Position: 1},
						},
						Constraints: []schema.Constraint{
							{
								Name:              "posts_user_id_fkey",
								Type:              "FOREIGN KEY",
								Columns:           []string{"user_id"},
								ReferencedTable:   "users",
								ReferencedColumns: []string{"id"},
								OnDelete:          "CASCADE",
							},
						},
					},
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeModifyConstraint},
			expectedSeverity: []differ.ChangeSeverity{differ.SeverityPotentiallyBreaking},
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
					t.Logf("  change %d: %s", i, ch.Type)
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
