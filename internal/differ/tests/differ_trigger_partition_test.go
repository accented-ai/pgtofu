package differ_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestTriggerComparator_IgnoresInheritedPartitionTriggers(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "orders",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "updated_at", DataType: "timestamptz", IsNullable: true, Position: 2},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type: "HASH",
					Partitions: []schema.Partition{
						{Name: "orders_p0"},
						{Name: "orders_p1"},
					},
				},
			},
		},
		Triggers: []schema.Trigger{
			{
				Schema:         schema.DefaultSchema,
				Name:           "set_updated_at",
				TableName:      "orders",
				Timing:         "BEFORE",
				Events:         []string{"UPDATE"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "update_timestamp",
			},
			{
				Schema:         schema.DefaultSchema,
				Name:           "set_updated_at",
				TableName:      "orders_p0",
				Timing:         "BEFORE",
				Events:         []string{"UPDATE"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "update_timestamp",
			},
			{
				Schema:         schema.DefaultSchema,
				Name:           "set_updated_at",
				TableName:      "orders_p1",
				Timing:         "BEFORE",
				Events:         []string{"UPDATE"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "update_timestamp",
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "orders",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "updated_at", DataType: "timestamptz", IsNullable: true, Position: 2},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type: "HASH",
					Partitions: []schema.Partition{
						{Name: "orders_p0"},
						{Name: "orders_p1"},
					},
				},
			},
		},
		Triggers: []schema.Trigger{
			{
				Schema:         schema.DefaultSchema,
				Name:           "set_updated_at",
				TableName:      "orders",
				Timing:         "BEFORE",
				Events:         []string{"UPDATE"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "update_timestamp",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())
	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	dropTriggers := result.GetChangesByType(differ.ChangeTypeDropTrigger)
	require.Empty(t, dropTriggers, "should not drop inherited partition triggers")

	addTriggers := result.GetChangesByType(differ.ChangeTypeAddTrigger)
	require.Empty(t, addTriggers, "should not add any triggers")
}
