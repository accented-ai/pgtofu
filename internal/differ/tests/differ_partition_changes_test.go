package differ_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_PartitionChanges_AddPartition(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "events",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "event_date", DataType: "date", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "events_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"id", "event_date"},
					},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "RANGE",
					Columns: []string{"event_date"},
					Partitions: []schema.Partition{
						{
							Name:       "events_2025_q1",
							Definition: "FOR VALUES FROM ('2025-01-01') TO ('2025-04-01')",
						},
						{
							Name:       "events_2025_q2",
							Definition: "FOR VALUES FROM ('2025-04-01') TO ('2025-07-01')",
						},
					},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "events",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "event_date", DataType: "date", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "events_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"id", "event_date"},
					},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "RANGE",
					Columns: []string{"event_date"},
					Partitions: []schema.Partition{
						{
							Name:       "events_2025_q1",
							Definition: "FOR VALUES FROM ('2025-01-01') TO ('2025-04-01')",
						},
						{
							Name:       "events_2025_q2",
							Definition: "FOR VALUES FROM ('2025-04-01') TO ('2025-07-01')",
						},
						{
							Name:       "events_2025_q3",
							Definition: "FOR VALUES FROM ('2025-07-01') TO ('2025-10-01')",
						},
						{
							Name:       "events_2025_q4",
							Definition: "FOR VALUES FROM ('2025-10-01') TO ('2026-01-01')",
						},
					},
				},
			},
		},
	}

	d := differ.New(nil)

	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	addPartitionChanges := result.GetChangesByType(differ.ChangeTypeAddPartition)
	assert.Len(t, addPartitionChanges, 2, "Expected 2 ADD_PARTITION changes")

	partitionNames := make(map[string]bool)

	for _, change := range addPartitionChanges {
		partition, ok := change.Details["partition"].(*schema.Partition)
		require.True(t, ok, "Expected partition in details")

		partitionNames[partition.Name] = true

		assert.Equal(t, differ.SeveritySafe, change.Severity)
		assert.Equal(t, "partition", change.ObjectType)
	}

	assert.True(t, partitionNames["events_2025_q3"], "Expected events_2025_q3")
	assert.True(t, partitionNames["events_2025_q4"], "Expected events_2025_q4")
}

func TestDiffer_PartitionChanges_DropPartition(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "created_at", DataType: "timestamptz", IsNullable: false, Position: 2},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "RANGE",
					Columns: []string{"created_at"},
					Partitions: []schema.Partition{
						{
							Name:       "items_2024_q1",
							Definition: "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')",
						},
						{
							Name:       "items_2024_q2",
							Definition: "FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')",
						},
						{
							Name:       "items_2024_q3",
							Definition: "FOR VALUES FROM ('2024-07-01') TO ('2024-10-01')",
						},
					},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "created_at", DataType: "timestamptz", IsNullable: false, Position: 2},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "RANGE",
					Columns: []string{"created_at"},
					Partitions: []schema.Partition{
						{
							Name:       "items_2024_q2",
							Definition: "FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')",
						},
						{
							Name:       "items_2024_q3",
							Definition: "FOR VALUES FROM ('2024-07-01') TO ('2024-10-01')",
						},
					},
				},
			},
		},
	}

	d := differ.New(nil)

	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	dropPartitionChanges := result.GetChangesByType(differ.ChangeTypeDropPartition)
	assert.Len(t, dropPartitionChanges, 1, "Expected 1 DROP_PARTITION change")

	change := dropPartitionChanges[0]
	partition, ok := change.Details["partition"].(*schema.Partition)
	require.True(t, ok, "Expected partition in details")

	assert.Equal(t, "items_2024_q1", partition.Name)
	assert.Equal(t, differ.SeverityBreaking, change.Severity)
	assert.Equal(t, "partition", change.ObjectType)
}

func TestDiffer_PartitionChanges_AddAndDropPartition(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "logs",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "log_date", DataType: "date", IsNullable: false, Position: 2},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "RANGE",
					Columns: []string{"log_date"},
					Partitions: []schema.Partition{
						{
							Name:       "logs_2024_01",
							Definition: "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')",
						},
						{
							Name:       "logs_2024_02",
							Definition: "FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')",
						},
					},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "logs",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "log_date", DataType: "date", IsNullable: false, Position: 2},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "RANGE",
					Columns: []string{"log_date"},
					Partitions: []schema.Partition{
						{
							Name:       "logs_2024_02",
							Definition: "FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')",
						},
						{
							Name:       "logs_2024_03",
							Definition: "FOR VALUES FROM ('2024-03-01') TO ('2024-04-01')",
						},
					},
				},
			},
		},
	}

	d := differ.New(nil)

	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	addChanges := result.GetChangesByType(differ.ChangeTypeAddPartition)
	dropChanges := result.GetChangesByType(differ.ChangeTypeDropPartition)

	assert.Len(t, addChanges, 1, "Expected 1 ADD_PARTITION change")
	assert.Len(t, dropChanges, 1, "Expected 1 DROP_PARTITION change")

	addPartition, ok := addChanges[0].Details["partition"].(*schema.Partition)
	require.True(t, ok)
	assert.Equal(t, "logs_2024_03", addPartition.Name)

	dropPartition, ok := dropChanges[0].Details["partition"].(*schema.Partition)
	require.True(t, ok)
	assert.Equal(t, "logs_2024_01", dropPartition.Name)
}

func TestDiffer_PartitionChanges_NoChangesWhenIdentical(t *testing.T) {
	t.Parallel()

	tableWithPartitions := schema.Table{
		Schema: schema.DefaultSchema,
		Name:   "events",
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
			{Name: "event_date", DataType: "date", IsNullable: false, Position: 2},
		},
		PartitionStrategy: &schema.PartitionStrategy{
			Type:    "RANGE",
			Columns: []string{"event_date"},
			Partitions: []schema.Partition{
				{
					Name:       "events_2025_q1",
					Definition: "FOR VALUES FROM ('2025-01-01') TO ('2025-04-01')",
				},
				{
					Name:       "events_2025_q2",
					Definition: "FOR VALUES FROM ('2025-04-01') TO ('2025-07-01')",
				},
			},
		},
	}

	current := &schema.Database{Tables: []schema.Table{tableWithPartitions}}
	desired := &schema.Database{Tables: []schema.Table{tableWithPartitions}}

	d := differ.New(nil)

	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	addChanges := result.GetChangesByType(differ.ChangeTypeAddPartition)
	dropChanges := result.GetChangesByType(differ.ChangeTypeDropPartition)

	assert.Empty(t, addChanges, "Expected no ADD_PARTITION changes")
	assert.Empty(t, dropChanges, "Expected no DROP_PARTITION changes")
}

func TestDiffer_PartitionChanges_HashPartitions(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "user_id", DataType: "uuid", IsNullable: false, Position: 2},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "HASH",
					Columns: []string{"user_id"},
					Partitions: []schema.Partition{
						{
							Name:       "items_p0",
							Definition: "FOR VALUES WITH (modulus 4, remainder 0)",
						},
						{
							Name:       "items_p1",
							Definition: "FOR VALUES WITH (modulus 4, remainder 1)",
						},
					},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "user_id", DataType: "uuid", IsNullable: false, Position: 2},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "HASH",
					Columns: []string{"user_id"},
					Partitions: []schema.Partition{
						{
							Name:       "items_p0",
							Definition: "FOR VALUES WITH (modulus 4, remainder 0)",
						},
						{
							Name:       "items_p1",
							Definition: "FOR VALUES WITH (modulus 4, remainder 1)",
						},
						{
							Name:       "items_p2",
							Definition: "FOR VALUES WITH (modulus 4, remainder 2)",
						},
						{
							Name:       "items_p3",
							Definition: "FOR VALUES WITH (modulus 4, remainder 3)",
						},
					},
				},
			},
		},
	}

	d := differ.New(nil)

	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	addChanges := result.GetChangesByType(differ.ChangeTypeAddPartition)
	assert.Len(t, addChanges, 2, "Expected 2 ADD_PARTITION changes for hash partitions")

	partitionNames := make(map[string]bool)

	for _, change := range addChanges {
		partition, ok := change.Details["partition"].(*schema.Partition)
		require.True(t, ok)

		partitionNames[partition.Name] = true
	}

	assert.True(t, partitionNames["items_p2"])
	assert.True(t, partitionNames["items_p3"])
}

func TestDiffer_PartitionChanges_NonPartitionedTable(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "regular_table",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "regular_table",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
	}

	d := differ.New(nil)

	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	addChanges := result.GetChangesByType(differ.ChangeTypeAddPartition)
	dropChanges := result.GetChangesByType(differ.ChangeTypeDropPartition)

	assert.Empty(t, addChanges, "Expected no partition changes for non-partitioned table")
	assert.Empty(t, dropChanges, "Expected no partition changes for non-partitioned table")
}
