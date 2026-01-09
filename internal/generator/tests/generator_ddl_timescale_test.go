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

func TestDDLBuilder_TimescaleOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		changeType differ.ChangeType
		setup      func() (*schema.Database, *schema.Database)
		wantSQL    []string
	}{
		{
			name:       "add hypertable",
			changeType: differ.ChangeTypeAddHypertable,
			setup: func() (*schema.Database, *schema.Database) {
				current := &schema.Database{}
				desired := &schema.Database{
					Hypertables: []schema.Hypertable{
						{
							Schema:            schema.DefaultSchema,
							TableName:         "metrics",
							TimeColumnName:    "created_at",
							PartitionInterval: "1 day",
						},
					},
				}

				return current, desired
			},
			wantSQL: []string{"create_hypertable", "metrics", "created_at", "1 day"},
		},
		{
			name:       "add compression policy",
			changeType: differ.ChangeTypeAddCompressionPolicy,
			setup: func() (*schema.Database, *schema.Database) {
				current := &schema.Database{}
				desired := &schema.Database{
					Hypertables: []schema.Hypertable{
						{
							Schema:             schema.DefaultSchema,
							TableName:          "metrics",
							CompressionEnabled: true,
							CompressionSettings: &schema.CompressionSettings{
								SegmentByColumns: []string{"device_id"},
								OrderByColumns: []schema.OrderByColumn{
									{Column: "time", Direction: "DESC"},
								},
							},
						},
					},
				}

				return current, desired
			},
			wantSQL: []string{"timescaledb.compress", "segmentby", "orderby"},
		},
		{
			name:       "add retention policy",
			changeType: differ.ChangeTypeAddRetentionPolicy,
			setup: func() (*schema.Database, *schema.Database) {
				current := &schema.Database{}
				desired := &schema.Database{
					Hypertables: []schema.Hypertable{
						{
							Schema:    schema.DefaultSchema,
							TableName: "metrics",
							RetentionPolicy: &schema.RetentionPolicy{
								DropAfter: "90 days",
							},
						},
					},
				}

				return current, desired
			},
			wantSQL: []string{"add_retention_policy", "90 days"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			current, desired := tt.setup()
			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{{Type: tt.changeType, ObjectName: "public.metrics"}},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)

			for _, want := range tt.wantSQL {
				assert.Contains(t, strings.ToLower(stmt.SQL), strings.ToLower(want))
			}
		})
	}
}

func TestDDLBuilder_ContinuousAggregateIncludesComment(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "recent_logs_summary",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "logs",
				Query: `SELECT
    team,
    actor,
    time_bucket('1 hour', occurred_at) AS bucket
FROM logs
GROUP BY team, actor, bucket`,
				Comment: "Continuous aggregate of logs per hour for monitoring and alerting",
			},
		},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeAddContinuousAggregate,
		ObjectName: "public.recent_logs_summary",
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)
	require.NoError(t, err)

	assert.Contains(t, stmt.SQL, "COMMENT ON VIEW public.recent_logs_summary IS")
	assert.Contains(
		t,
		stmt.SQL,
		"Continuous aggregate of logs per hour for monitoring and alerting",
	)
}

func TestDDLBuilder_DropHypertableDownMigration(t *testing.T) {
	t.Parallel()

	hypertable := &schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "metrics",
		TimeColumnName:     "created_at",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"device_id"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "created_at", Direction: "DESC"},
			},
		},
	}

	current := &schema.Database{
		Hypertables: []schema.Hypertable{*hypertable},
	}
	desired := &schema.Database{}

	change := differ.Change{
		Type:       differ.ChangeTypeDropHypertable,
		ObjectName: "public.metrics",
		Details:    map[string]any{"hypertable": hypertable},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)

	downStmt, err := builder.BuildDownStatement(change)
	require.NoError(t, err, "BuildDownStatement should not fail for DROP_HYPERTABLE")

	assert.Contains(t, strings.ToLower(downStmt.SQL), "create_hypertable")
	assert.Contains(t, strings.ToLower(downStmt.SQL), "metrics")
	assert.Contains(t, strings.ToLower(downStmt.SQL), "created_at")
	assert.Contains(t, strings.ToLower(downStmt.SQL), "1 day")
}

func TestDDLBuilder_DropHypertableDownMigrationWithCompression(t *testing.T) {
	t.Parallel()

	hypertable := &schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "sensor_readings",
		TimeColumnName:     "recorded_at",
		PartitionInterval:  "7 days",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"sensor_id"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "recorded_at", Direction: "DESC"},
			},
		},
	}

	current := &schema.Database{
		Hypertables: []schema.Hypertable{*hypertable},
	}
	desired := &schema.Database{}

	change := differ.Change{
		Type:       differ.ChangeTypeDropHypertable,
		ObjectName: "public.sensor_readings",
		Details:    map[string]any{"hypertable": hypertable},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	downStmt, err := builder.BuildDownStatement(change)
	require.NoError(t, err)

	sql := strings.ToLower(downStmt.SQL)

	assert.Contains(t, sql, "create_hypertable")
	assert.Contains(t, sql, "sensor_readings")
	assert.Contains(t, sql, "recorded_at")

	assert.Contains(t, sql, "timescaledb.compress")
	assert.Contains(t, sql, "segmentby")
	assert.Contains(t, sql, "sensor_id")
	assert.Contains(t, sql, "orderby")
}

func TestDDLBuilder_DropHypertableDownMigrationWithRetention(t *testing.T) {
	t.Parallel()

	hypertable := &schema.Hypertable{
		Schema:            schema.DefaultSchema,
		TableName:         "events",
		TimeColumnName:    "event_time",
		PartitionInterval: "1 day",
		RetentionPolicy: &schema.RetentionPolicy{
			DropAfter: "30 days",
		},
	}

	current := &schema.Database{
		Hypertables: []schema.Hypertable{*hypertable},
	}
	desired := &schema.Database{}

	change := differ.Change{
		Type:       differ.ChangeTypeDropHypertable,
		ObjectName: "public.events",
		Details:    map[string]any{"hypertable": hypertable},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	downStmt, err := builder.BuildDownStatement(change)
	require.NoError(t, err)

	sql := strings.ToLower(downStmt.SQL)

	assert.Contains(t, sql, "create_hypertable")
	assert.Contains(t, sql, "events")

	assert.Contains(t, sql, "add_retention_policy")
	assert.Contains(t, sql, "30 days")
}

func TestDDLBuilder_DropCompressionPolicyDownMigration(t *testing.T) {
	t.Parallel()

	hypertable := &schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "metrics",
		TimeColumnName:     "created_at",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"device_id"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "created_at", Direction: "DESC"},
			},
		},
	}

	current := &schema.Database{
		Hypertables: []schema.Hypertable{*hypertable},
	}
	desiredHT := *hypertable
	desiredHT.CompressionEnabled = false
	desiredHT.CompressionSettings = nil
	desired := &schema.Database{
		Hypertables: []schema.Hypertable{desiredHT},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeDropCompressionPolicy,
		ObjectName: "public.metrics",
		Details:    map[string]any{"hypertable": hypertable},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	downStmt, err := builder.BuildDownStatement(change)
	require.NoError(t, err)

	sql := strings.ToLower(downStmt.SQL)
	assert.Contains(t, sql, "timescaledb.compress")
	assert.Contains(t, sql, "segmentby")
	assert.Contains(t, sql, "device_id")
}

func TestDDLBuilder_DropRetentionPolicyDownMigration(t *testing.T) {
	t.Parallel()

	policy := &schema.RetentionPolicy{
		DropAfter: "90 days",
	}

	hypertable := &schema.Hypertable{
		Schema:          schema.DefaultSchema,
		TableName:       "logs",
		TimeColumnName:  "created_at",
		RetentionPolicy: policy,
	}

	current := &schema.Database{
		Hypertables: []schema.Hypertable{*hypertable},
	}
	desiredHT := *hypertable
	desiredHT.RetentionPolicy = nil
	desired := &schema.Database{
		Hypertables: []schema.Hypertable{desiredHT},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeDropRetentionPolicy,
		ObjectName: "public.logs",
		Details:    map[string]any{"policy": policy},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	downStmt, err := builder.BuildDownStatement(change)
	require.NoError(t, err)

	sql := strings.ToLower(downStmt.SQL)
	assert.Contains(t, sql, "add_retention_policy")
	assert.Contains(t, sql, "90 days")
}

func TestDDLBuilder_CompressionPolicyDedupesSettings(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Hypertables: []schema.Hypertable{
			{
				Schema:             schema.DefaultSchema,
				TableName:          "logs",
				TimeColumnName:     "event_time",
				CompressionEnabled: true,
				CompressionSettings: &schema.CompressionSettings{
					SegmentByColumns: []string{"team", "actor", "team", "actor"},
					OrderByColumns: []schema.OrderByColumn{
						{Column: "event_time", Direction: "DESC"},
						{Column: "event_time", Direction: "DESC"},
					},
				},
			},
		},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeAddCompressionPolicy,
		ObjectName: "public.logs",
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)
	require.NoError(t, err)

	assert.Contains(t, stmt.SQL, "timescaledb.compress_segmentby = 'team,actor'")
	assert.NotContains(t, stmt.SQL, "team,actor,team")
	assert.Contains(t, stmt.SQL, "timescaledb.compress_orderby = 'event_time DESC'")
	assert.NotContains(t, stmt.SQL, "event_time DESC,event_time DESC")
}
