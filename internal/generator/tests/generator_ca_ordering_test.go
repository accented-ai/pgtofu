package generator_test

import (
	"strings"
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestGeneratorOrdersCABeforeDropColumn(t *testing.T) {
	t.Parallel()

	hypertable := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "metrics",
		TimeColumnName:     "recorded_at",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"old_col"},
			OrderByColumns:   []schema.OrderByColumn{{Column: "recorded_at", Direction: "DESC"}},
		},
	}

	desiredHypertable := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "metrics",
		TimeColumnName:     "recorded_at",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"new_col"},
			OrderByColumns:   []schema.OrderByColumn{{Column: "recorded_at", Direction: "DESC"}},
		},
	}

	currentCA := schema.ContinuousAggregate{
		Schema:           schema.DefaultSchema,
		ViewName:         "metrics_hourly",
		HypertableSchema: schema.DefaultSchema,
		HypertableName:   "metrics",
		Query: "SELECT old_col, time_bucket('1 hour', recorded_at) AS bucket, " +
			"COUNT(*) FROM metrics GROUP BY old_col, bucket",
	}

	desiredCA := schema.ContinuousAggregate{
		Schema:           schema.DefaultSchema,
		ViewName:         "metrics_hourly",
		HypertableSchema: schema.DefaultSchema,
		HypertableName:   "metrics",
		Query: "SELECT new_col, time_bucket('1 hour', recorded_at) AS bucket, " +
			"COUNT(*) FROM metrics GROUP BY new_col, bucket",
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "metrics",
				Comment: "Stores measurements",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigserial", IsNullable: false, Position: 1},
					{
						Name:       "old_col",
						DataType:   "varchar(20)",
						IsNullable: false,
						Position:   2,
						Comment:    "Original column",
					},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false, Position: 3},
					{Name: "value", DataType: "smallint", IsNullable: false, Position: 4},
					{Name: "prev_value", DataType: "smallint", IsNullable: false, Position: 5},
					{Name: "active", DataType: "boolean", IsNullable: false, Position: 6},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "metrics_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"old_col", "recorded_at"},
					},
				},
				Indexes: []schema.Index{
					{
						Schema:    schema.DefaultSchema,
						Name:      "idx_metrics_old_col_time",
						TableName: "metrics",
						Columns:   []string{"old_col", "recorded_at"},
					},
					{
						Schema:    schema.DefaultSchema,
						Name:      "idx_metrics_active",
						TableName: "metrics",
						Columns:   []string{"old_col", "recorded_at"},
						Where:     "active = TRUE",
					},
				},
			},
			{
				Schema:  schema.DefaultSchema,
				Name:    "state",
				Comment: "Tracks state",
				Columns: []schema.Column{
					{
						Name:       "old_col",
						DataType:   "varchar(20)",
						IsNullable: false,
						Position:   1,
						Comment:    "Original column",
					},
					{Name: "current_value", DataType: "smallint", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{Name: "state_pkey", Type: "PRIMARY KEY", Columns: []string{"old_col"}},
				},
			},
		},
		Hypertables:          []schema.Hypertable{hypertable},
		ContinuousAggregates: []schema.ContinuousAggregate{currentCA},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "metrics",
				Comment: "Stores measurements",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigserial", IsNullable: false, Position: 1},
					{
						Name:       "new_col",
						DataType:   "varchar(20)",
						IsNullable: false,
						Position:   2,
						Comment:    "Renamed column",
					},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false, Position: 3},
					{Name: "value", DataType: "smallint", IsNullable: false, Position: 4},
					{Name: "prev_value", DataType: "smallint", IsNullable: false, Position: 5},
					{Name: "active", DataType: "boolean", IsNullable: false, Position: 6},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "metrics_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"new_col", "recorded_at"},
					},
					{
						Name:       "metrics_value_check",
						Type:       "CHECK",
						Definition: "value IN (0, 1, 2)",
					},
					{
						Name:       "metrics_prev_check",
						Type:       "CHECK",
						Definition: "prev_value IN (0, 1, 2)",
					},
				},
				Indexes: []schema.Index{
					{
						Schema:    schema.DefaultSchema,
						Name:      "idx_metrics_new_col_time",
						TableName: "metrics",
						Columns:   []string{"new_col", "recorded_at"},
					},
					{
						Schema:    schema.DefaultSchema,
						Name:      "idx_metrics_active",
						TableName: "metrics",
						Columns:   []string{"new_col", "recorded_at"},
						Where:     "active = TRUE",
					},
				},
			},
			{
				Schema:  schema.DefaultSchema,
				Name:    "state",
				Comment: "Tracks state",
				Columns: []schema.Column{
					{
						Name:       "new_col",
						DataType:   "varchar(20)",
						IsNullable: false,
						Position:   1,
						Comment:    "Renamed column",
					},
					{Name: "current_value", DataType: "smallint", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{Name: "state_pkey", Type: "PRIMARY KEY", Columns: []string{"new_col"}},
					{
						Name:       "state_value_check",
						Type:       "CHECK",
						Definition: "current_value IN (0, 1, 2)",
					},
				},
			},
		},
		Hypertables:          []schema.Hypertable{desiredHypertable},
		ContinuousAggregates: []schema.ContinuousAggregate{desiredCA},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("diff error: %v", err)
	}

	t.Log("Changes from differ (with Order):")

	for i, change := range result.Changes {
		t.Logf("  %d: Order=%d Type=%s Desc=%s", i, change.Order, change.Type, change.Description)
	}

	opts := generator.DefaultOptions()
	opts.PreviewMode = true
	opts.IncludeComments = true

	gen := generator.New(opts)

	genResult, err := gen.Generate(result)
	if err != nil {
		t.Fatalf("generate error: %v", err)
	}

	if len(genResult.Migrations) == 0 {
		t.Fatal("no migrations generated")
	}

	migration := genResult.Migrations[0]
	content := migration.UpFile.Content

	t.Log("Generated migration content:")
	t.Log(content)

	dropColumnMetricsIndex := strings.Index(
		content,
		"ALTER TABLE public.metrics DROP COLUMN IF EXISTS old_col",
	)
	modifyCAIndex := strings.Index(
		content,
		"DROP MATERIALIZED VIEW IF EXISTS public.metrics_hourly",
	)

	if dropColumnMetricsIndex == -1 {
		t.Fatal("DROP COLUMN for metrics.old_col not found in migration")
	}

	if modifyCAIndex == -1 {
		t.Fatal("DROP MATERIALIZED VIEW (part of modify CA) not found in migration")
	}

	if dropColumnMetricsIndex < modifyCAIndex {
		t.Errorf(
			"DROP COLUMN for metrics.old_col at position %d, DROP MATERIALIZED VIEW at position %d.\n"+
				"The continuous aggregate must be modified BEFORE dropping the column it depends on.",
			dropColumnMetricsIndex,
			modifyCAIndex,
		)
	}
}
