package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CompareTables(t *testing.T) { //nolint:maintidx
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
			name:    "add new table",
			current: &schema.Database{Tables: []schema.Table{}},
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
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeAddTable},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe},
		},
		{
			name: "drop table",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "old_table",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
						},
					},
				},
			},
			desired:          &schema.Database{Tables: []schema.Table{}},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeDropTable},
			expectedSeverity: []differ.ChangeSeverity{differ.SeverityBreaking},
		},
		{
			name: "add nullable column",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
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
							{Name: "email", DataType: "varchar", IsNullable: true, Position: 2},
						},
					},
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeAddColumn},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe},
		},
		{
			name: "add non-nullable column without default",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
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
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeAddColumn},
			expectedSeverity: []differ.ChangeSeverity{differ.SeverityDataMigrationRequired},
		},
		{
			name: "safe type widening - integer to bigint",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{Name: "id", DataType: "integer", IsNullable: false, Position: 1},
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
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeModifyColumnType},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe},
		},
		{
			name: "make column not null",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{Name: "email", DataType: "varchar", IsNullable: true, Position: 1},
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
							{Name: "email", DataType: "varchar", IsNullable: false, Position: 1},
						},
					},
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeModifyColumnNullability},
			expectedSeverity: []differ.ChangeSeverity{differ.SeverityDataMigrationRequired},
		},
		{
			name: "add column with comment",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
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
							{
								Name:       "email",
								DataType:   "varchar",
								IsNullable: true,
								Position:   2,
								Comment:    "User email address",
							},
						},
					},
				},
			},
			expectedChanges: 2,
			expectedTypes: []differ.ChangeType{
				differ.ChangeTypeAddColumn,
				differ.ChangeTypeModifyColumnComment,
			},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe, differ.SeveritySafe},
		},
		{
			name:    "add table with comment",
			current: &schema.Database{Tables: []schema.Table{}},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema:  schema.DefaultSchema,
						Name:    "users",
						Comment: "User accounts table",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
						},
					},
				},
			},
			expectedChanges: 2,
			expectedTypes: []differ.ChangeType{
				differ.ChangeTypeAddTable,
				differ.ChangeTypeModifyTableComment,
			},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe, differ.SeveritySafe},
		},
		{
			name:    "add table with column comments",
			current: &schema.Database{Tables: []schema.Table{}},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{
								Name:       "id",
								DataType:   "bigint",
								IsNullable: false,
								Position:   1,
								Comment:    "Primary key",
							},
							{
								Name:       "email",
								DataType:   "varchar",
								IsNullable: true,
								Position:   2,
								Comment:    "Email address",
							},
						},
					},
				},
			},
			expectedChanges: 3,
			expectedTypes: []differ.ChangeType{
				differ.ChangeTypeAddTable,
				differ.ChangeTypeModifyColumnComment,
				differ.ChangeTypeModifyColumnComment,
			},
			expectedSeverity: []differ.ChangeSeverity{
				differ.SeveritySafe,
				differ.SeveritySafe,
				differ.SeveritySafe,
			},
		},
		{
			name: "modify column comment on existing column",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema: schema.DefaultSchema,
						Name:   "users",
						Columns: []schema.Column{
							{
								Name:       "email",
								DataType:   "varchar",
								IsNullable: true,
								Position:   1,
								Comment:    "Old comment",
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
							{
								Name:       "email",
								DataType:   "varchar",
								IsNullable: true,
								Position:   1,
								Comment:    "New comment",
							},
						},
					},
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeModifyColumnComment},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe},
		},
		{
			name: "modify table comment on existing table",
			current: &schema.Database{
				Tables: []schema.Table{
					{
						Schema:  schema.DefaultSchema,
						Name:    "users",
						Comment: "Old comment",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
						},
					},
				},
			},
			desired: &schema.Database{
				Tables: []schema.Table{
					{
						Schema:  schema.DefaultSchema,
						Name:    "users",
						Comment: "New comment",
						Columns: []schema.Column{
							{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
						},
					},
				},
			},
			expectedChanges:  1,
			expectedTypes:    []differ.ChangeType{differ.ChangeTypeModifyTableComment},
			expectedSeverity: []differ.ChangeSeverity{differ.SeveritySafe},
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

func TestDiffer_DetectsCharLengthChange(t *testing.T) {
	t.Parallel()

	length2 := 2
	length3 := 3

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "core",
				Name:   "language_codes",
				Columns: []schema.Column{
					{
						Name:       "code",
						DataType:   "character",
						IsNullable: false,
						Position:   1,
						MaxLength:  &length2,
					},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "core",
				Name:   "language_codes",
				Columns: []schema.Column{
					{
						Name:       "code",
						DataType:   "CHAR",
						IsNullable: false,
						Position:   1,
						MaxLength:  &length3,
					},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected compare error: %v", err)
	}

	if len(result.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(result.Changes))
	}

	change := result.Changes[0]
	if change.Type != differ.ChangeTypeModifyColumnType {
		t.Fatalf("expected change type ModifyColumnType, got %s", change.Type)
	}

	if change.Severity != differ.SeveritySafe {
		t.Fatalf("expected severity Safe, got %s", change.Severity)
	}

	oldType, _ := change.Details["old_type"].(string)
	newType, _ := change.Details["new_type"].(string)

	if oldType != "CHAR(2)" {
		t.Fatalf("expected old_type CHAR(2), got %s", oldType)
	}

	if newType != "CHAR(3)" {
		t.Fatalf("expected new_type CHAR(3), got %s", newType)
	}
}
