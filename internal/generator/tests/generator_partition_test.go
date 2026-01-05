package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestGenerator_AddPartition(t *testing.T) {
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
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "RANGE",
					Columns: []string{"event_date"},
					Partitions: []schema.Partition{
						{
							Name:       "events_2025_q1",
							Definition: "FOR VALUES FROM ('2025-01-01') TO ('2025-04-01')",
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

	d := differ.New(nil)
	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := generator.DefaultOptions()
	opts.PreviewMode = true
	opts.Idempotent = true

	gen := generator.New(opts)
	genResult, err := gen.Generate(result)
	require.NoError(t, err)
	require.NotEmpty(t, genResult.Migrations)

	upContent := genResult.Migrations[0].UpFile.Content
	assert.Contains(t, upContent, "CREATE TABLE IF NOT EXISTS")
	assert.Contains(t, upContent, "events_2025_q2")
	assert.Contains(t, upContent, "PARTITION OF")
	assert.Contains(t, upContent, "public.events")
	assert.Contains(t, upContent, "FOR VALUES FROM ('2025-04-01') TO ('2025-07-01')")

	downContent := genResult.Migrations[0].DownFile.Content
	assert.Contains(t, downContent, "DROP TABLE IF EXISTS")
	assert.Contains(t, downContent, "events_2025_q2")
}

func TestGenerator_DropPartition(t *testing.T) {
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
							Name:       "logs_2024_q1",
							Definition: "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')",
						},
						{
							Name:       "logs_2024_q2",
							Definition: "FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')",
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
							Name:       "logs_2024_q2",
							Definition: "FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')",
						},
					},
				},
			},
		},
	}

	d := differ.New(nil)
	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := generator.DefaultOptions()
	opts.PreviewMode = true
	opts.Idempotent = true

	gen := generator.New(opts)
	genResult, err := gen.Generate(result)
	require.NoError(t, err)
	require.NotEmpty(t, genResult.Migrations)

	upContent := genResult.Migrations[0].UpFile.Content
	assert.Contains(t, upContent, "DROP TABLE IF EXISTS")
	assert.Contains(t, upContent, "logs_2024_q1")

	downContent := genResult.Migrations[0].DownFile.Content
	assert.Contains(t, downContent, "CREATE TABLE IF NOT EXISTS")
	assert.Contains(t, downContent, "logs_2024_q1")
	assert.Contains(t, downContent, "PARTITION OF")
}

func TestGenerator_MultiplePartitionChanges(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "analytics",
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "recorded_at", DataType: "date", IsNullable: false, Position: 2},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "RANGE",
					Columns: []string{"recorded_at"},
					Partitions: []schema.Partition{
						{
							Name:       "metrics_2025_q3",
							Definition: "FOR VALUES FROM ('2025-07-01') TO ('2025-10-01')",
						},
						{
							Name:       "metrics_2025_q4",
							Definition: "FOR VALUES FROM ('2025-10-01') TO ('2026-01-01')",
						},
					},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "analytics",
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "recorded_at", DataType: "date", IsNullable: false, Position: 2},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "RANGE",
					Columns: []string{"recorded_at"},
					Partitions: []schema.Partition{
						{
							Name:       "metrics_2025_q3",
							Definition: "FOR VALUES FROM ('2025-07-01') TO ('2025-10-01')",
						},
						{
							Name:       "metrics_2025_q4",
							Definition: "FOR VALUES FROM ('2025-10-01') TO ('2026-01-01')",
						},
						{
							Name:       "metrics_2026_q1",
							Definition: "FOR VALUES FROM ('2026-01-01') TO ('2026-04-01')",
						},
						{
							Name:       "metrics_2026_q2",
							Definition: "FOR VALUES FROM ('2026-04-01') TO ('2026-07-01')",
						},
						{
							Name:       "metrics_2026_q3",
							Definition: "FOR VALUES FROM ('2026-07-01') TO ('2026-10-01')",
						},
						{
							Name:       "metrics_2026_q4",
							Definition: "FOR VALUES FROM ('2026-10-01') TO ('2027-01-01')",
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
	assert.Len(t, addChanges, 4, "Expected 4 new partitions")

	opts := generator.DefaultOptions()
	opts.PreviewMode = true
	opts.Idempotent = true

	gen := generator.New(opts)
	genResult, err := gen.Generate(result)
	require.NoError(t, err)
	require.NotEmpty(t, genResult.Migrations)

	upContent := genResult.Migrations[0].UpFile.Content

	assert.Contains(t, upContent, "metrics_2026_q1")
	assert.Contains(t, upContent, "metrics_2026_q2")
	assert.Contains(t, upContent, "metrics_2026_q3")
	assert.Contains(t, upContent, "metrics_2026_q4")

	assert.Equal(t, 4, strings.Count(upContent, "PARTITION OF"))
	assert.Equal(t, 4, strings.Count(upContent, "CREATE TABLE IF NOT EXISTS"))
}
