package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_PartitionTables_Idempotency(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "user_id", DataType: "uuid", IsNullable: false, Position: 2},
					{Name: "session_id", DataType: "uuid", IsNullable: false, Position: 3},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "items_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"id", "user_id", "session_id"},
					},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "HASH",
					Columns: []string{"user_id", "session_id"},
					Partitions: []schema.Partition{
						{
							Name:       "items_p0",
							Definition: "FOR VALUES WITH (modulus 16, remainder 0)",
						},
						{
							Name:       "items_p1",
							Definition: "FOR VALUES WITH (modulus 16, remainder 1)",
						},
						{
							Name:       "items_p2",
							Definition: "FOR VALUES WITH (modulus 16, remainder 2)",
						},
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
					{Name: "user_id", DataType: "uuid", IsNullable: false, Position: 2},
					{Name: "session_id", DataType: "uuid", IsNullable: false, Position: 3},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "items_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"id", "user_id", "session_id"},
					},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "HASH",
					Columns: []string{"user_id", "session_id"},
					Partitions: []schema.Partition{
						{
							Name:       "items_p0",
							Definition: "FOR VALUES WITH (modulus 16, remainder 0)",
						},
						{
							Name:       "items_p1",
							Definition: "FOR VALUES WITH (modulus 16, remainder 1)",
						},
						{
							Name:       "items_p2",
							Definition: "FOR VALUES WITH (modulus 16, remainder 2)",
						},
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
}

func TestDiffer_PartitionTables_NoPartitionTablesInExtracted(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "HASH",
					Columns: []string{"id"},
					Partitions: []schema.Partition{
						{
							Name:       "items_p0",
							Definition: "FOR VALUES WITH (modulus 4, remainder 0)",
						},
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
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "HASH",
					Columns: []string{"id"},
					Partitions: []schema.Partition{
						{
							Name:       "items_p0",
							Definition: "FOR VALUES WITH (modulus 4, remainder 0)",
						},
					},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "items_p0",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
	}

	d := differ.New(nil)

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	if len(result.Changes) == 0 {
		t.Fatal("Expected changes for partition table, got none")
	}

	foundDropTable := false

	for _, change := range result.Changes {
		if change.Type == differ.ChangeTypeDropTable {
			if details, ok := change.Details["table"].(*schema.Table); ok {
				if details.Schema == schema.DefaultSchema && details.Name == "items_p0" {
					foundDropTable = true
					break
				}
			}
		}
	}

	if !foundDropTable {
		t.Error("Expected DROP_TABLE change for partition table items_p0")
	}
}
