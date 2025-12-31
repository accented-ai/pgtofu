package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_ConstraintBackedIndexes(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "email", DataType: "text", IsNullable: false, Position: 2},
					{Name: "username", DataType: "text", IsNullable: false, Position: 3},
				},
				Constraints: []schema.Constraint{
					{
						Name:       "users_pkey",
						Type:       schema.ConstraintPrimaryKey,
						Columns:    []string{"id"},
						Definition: "PRIMARY KEY (id)",
					},
					{
						Name:       "users_email_key",
						Type:       schema.ConstraintUnique,
						Columns:    []string{"email"},
						Definition: "UNIQUE (email)",
					},
					{
						Name:       "users_username_key",
						Type:       schema.ConstraintUnique,
						Columns:    []string{"username"},
						Definition: "UNIQUE (username)",
					},
				},
				Indexes: []schema.Index{
					{
						Schema:    schema.DefaultSchema,
						TableName: "users",
						Name:      "users_pkey",
						Columns:   []string{"id"},
						Type:      "btree",
						IsUnique:  true,
						IsPrimary: true,
					},
					{
						Schema:    schema.DefaultSchema,
						TableName: "users",
						Name:      "users_email_key",
						Columns:   []string{"email"},
						Type:      "btree",
						IsUnique:  true,
						IsPrimary: false,
					},
					{
						Schema:    schema.DefaultSchema,
						TableName: "users",
						Name:      "users_username_key",
						Columns:   []string{"username"},
						Type:      "btree",
						IsUnique:  true,
						IsPrimary: false,
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

	var (
		addTableCount int
		addIndexCount int
	)

	for _, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddTable:
			addTableCount++
		case differ.ChangeTypeAddIndex:
			addIndexCount++

			t.Errorf(
				"Unexpected ADD_INDEX change: %s (constraint-backed indexes should be filtered out)",
				change.Description,
			)
		}
	}

	if addTableCount != 1 {
		t.Errorf("expected 1 ADD_TABLE change, got %d", addTableCount)
	}

	if addIndexCount != 0 {
		t.Errorf(
			"expected 0 ADD_INDEX changes, got %d (constraint-backed indexes should not appear)",
			addIndexCount,
		)
	}
}

func TestDiffer_StandaloneIndexesStillDetected(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "email", DataType: "text", IsNullable: false, Position: 2},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "email", DataType: "text", IsNullable: false, Position: 2},
				},
				Indexes: []schema.Index{
					{
						Schema:    schema.DefaultSchema,
						TableName: "users",
						Name:      "idx_users_email",
						Columns:   []string{"email"},
						Type:      "btree",
						IsUnique:  false,
						IsPrimary: false,
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

	addIndexCount := 0

	for _, change := range result.Changes {
		if change.Type == differ.ChangeTypeAddIndex {
			addIndexCount++
		}
	}

	if addIndexCount != 1 {
		t.Errorf("expected 1 ADD_INDEX change for standalone index, got %d", addIndexCount)
	}
}
