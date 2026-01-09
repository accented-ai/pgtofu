package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CompareTimescaleDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		current         *schema.Database
		desired         *schema.Database
		expectedChanges int
		expectedTypes   []differ.ChangeType
	}{
		{
			name: "add hypertable",
			current: &schema.Database{
				Tables:      []schema.Table{{Schema: schema.DefaultSchema, Name: "metrics"}},
				Hypertables: []schema.Hypertable{},
			},
			desired: &schema.Database{
				Tables: []schema.Table{{Schema: schema.DefaultSchema, Name: "metrics"}},
				Hypertables: []schema.Hypertable{
					{
						Schema:            schema.DefaultSchema,
						TableName:         "metrics",
						TimeColumnName:    "time",
						PartitionInterval: "1 day",
					},
				},
			},
			expectedChanges: 1,
			expectedTypes:   []differ.ChangeType{differ.ChangeTypeAddHypertable},
		},
		{
			name: "add hypertable with compression",
			current: &schema.Database{
				Tables: []schema.Table{{Schema: schema.DefaultSchema, Name: "metrics"}},
			},
			desired: &schema.Database{
				Tables: []schema.Table{{Schema: schema.DefaultSchema, Name: "metrics"}},
				Hypertables: []schema.Hypertable{
					{
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
					},
				},
			},
			expectedChanges: 2,
			expectedTypes: []differ.ChangeType{
				differ.ChangeTypeAddHypertable,
				differ.ChangeTypeAddCompressionPolicy,
			},
		},
		{
			name: "compression settings normalized",
			current: &schema.Database{
				Hypertables: []schema.Hypertable{
					{
						Schema:             schema.DefaultSchema,
						TableName:          "logs",
						CompressionEnabled: true,
						CompressionSettings: &schema.CompressionSettings{
							SegmentByColumns: []string{"team", "actor"},
							OrderByColumns: []schema.OrderByColumn{
								{Column: "event_time", Direction: "DESC"},
							},
						},
					},
				},
			},
			desired: &schema.Database{
				Hypertables: []schema.Hypertable{
					{
						Schema:             schema.DefaultSchema,
						TableName:          "logs",
						CompressionEnabled: true,
						CompressionSettings: &schema.CompressionSettings{
							SegmentByColumns: []string{"team", "actor", "team"},
							OrderByColumns: []schema.OrderByColumn{
								{Column: "event_time", Direction: "desc"},
								{Column: "event_time ", Direction: "DESC"},
							},
						},
					},
				},
			},
			expectedChanges: 0,
			expectedTypes:   []differ.ChangeType{},
		},
		{
			name: "add continuous aggregate",
			current: &schema.Database{
				Hypertables: []schema.Hypertable{
					{Schema: schema.DefaultSchema, TableName: "metrics"},
				},
				ContinuousAggregates: []schema.ContinuousAggregate{},
			},
			desired: &schema.Database{
				Hypertables: []schema.Hypertable{
					{Schema: schema.DefaultSchema, TableName: "metrics"},
				},
				ContinuousAggregates: []schema.ContinuousAggregate{
					{
						Schema:           schema.DefaultSchema,
						ViewName:         "metrics_hourly",
						HypertableSchema: schema.DefaultSchema,
						HypertableName:   "metrics",
						Query:            "SELECT time_bucket('1 hour', time) as bucket FROM metrics",
					},
				},
			},
			expectedChanges: 1,
			expectedTypes:   []differ.ChangeType{differ.ChangeTypeAddContinuousAggregate},
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

func TestContinuousAggregateFormattingIdempotent(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Hypertables: []schema.Hypertable{
			{Schema: schema.DefaultSchema, TableName: "logs"},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "recent_logs_summary",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "logs",
				Query: `SELECT
	team,
	actor,
	time_bucket('01:00:00'::interval, occurred_at) AS bucket,
	count(*) AS event_count,
	min(occurred_at) AS earliest_event,
	max(occurred_at) AS latest_event,
	min(event_id) AS first_event_id,
	max(event_id) AS last_event_id,
	max(created_at) AS last_recorded
	FROM logs
	GROUP BY team, actor, (time_bucket('01:00:00'::interval, occurred_at));`,
				RefreshPolicy: &schema.RefreshPolicy{
					StartOffset:      "2 days",
					EndOffset:        "01:00:00",
					ScheduleInterval: "01:00:00",
				},
				WithData:     true,
				Materialized: true,
				Finalized:    true,
			},
		},
	}

	desired := &schema.Database{
		Hypertables: []schema.Hypertable{
			{Schema: schema.DefaultSchema, TableName: "logs"},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "recent_logs_summary",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "logs",
				Query: `SELECT
    team,
    actor,
    time_bucket('1 hour', occurred_at) AS bucket,
    COUNT(*) as event_count,
    MIN(occurred_at) as earliest_event,
    MAX(occurred_at) as latest_event,
    MIN(event_id) as first_event_id,
    MAX(event_id) as last_event_id,
    MAX(created_at) as last_recorded
FROM logs
GROUP BY team, actor, bucket`,
				RefreshPolicy: &schema.RefreshPolicy{
					StartOffset:      "2 days",
					EndOffset:        "1 hour",
					ScheduleInterval: "1 hour",
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Changes) != 0 {
		t.Fatalf("expected no changes, got %d: %+v", len(result.Changes), result.Changes)
	}
}

func TestContinuousAggregateIntervalNormalization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		viewName          string
		extractedInterval string // HH:MM:SS format from PostgreSQL
		parsedInterval    string // Human-readable format from SQL file
		extractedStartOff string
		extractedEndOff   string
		extractedSchedInt string
		parsedStartOff    string
		parsedEndOff      string
		parsedSchedInt    string
	}{
		{
			name:              "10 second bucket",
			viewName:          "sensor_readings_10s",
			extractedInterval: "00:00:10",
			parsedInterval:    "10 seconds",
			extractedStartOff: "01:00:00",
			extractedEndOff:   "00:00:10",
			extractedSchedInt: "00:00:10",
			parsedStartOff:    "1 hour",
			parsedEndOff:      "10 seconds",
			parsedSchedInt:    "10 seconds",
		},
		{
			name:              "1 minute bucket",
			viewName:          "sensor_readings_1m",
			extractedInterval: "00:01:00",
			parsedInterval:    "1 minute",
			extractedStartOff: "03:00:00",
			extractedEndOff:   "00:01:00",
			extractedSchedInt: "00:01:00",
			parsedStartOff:    "3 hours",
			parsedEndOff:      "1 minute",
			parsedSchedInt:    "1 minute",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			extracted := buildContinuousAggTestDB(
				tt.viewName,
				tt.extractedInterval,
				tt.extractedStartOff,
				tt.extractedEndOff,
				tt.extractedSchedInt,
				true,
			)

			parsed := buildContinuousAggTestDB(
				tt.viewName,
				tt.parsedInterval,
				tt.parsedStartOff,
				tt.parsedEndOff,
				tt.parsedSchedInt,
				false,
			)

			d := differ.New(differ.DefaultOptions())

			result, err := d.Compare(extracted, parsed)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(result.Changes) != 0 {
				t.Errorf(
					"expected no changes (schemas should be equivalent), got %d changes",
					len(result.Changes),
				)

				for _, c := range result.Changes {
					t.Logf("Change: %s - %s", c.Type, c.Description)
				}
			}
		})
	}
}

func buildContinuousAggTestDB(
	viewName, interval, startOff, endOff, schedInt string,
	fromPostgres bool,
) *schema.Database {
	var query string

	if fromPostgres {
		query = `SELECT sensor_readings.device_id,
    public.time_bucket('` + interval + `'::interval, sensor_readings.recorded_at) AS bucket,
    sum(sensor_readings.value) AS total_value
   FROM public.sensor_readings
  WHERE sensor_readings.granularity = 'second'::character varying AND sensor_readings.multiplier = 1
  GROUP BY sensor_readings.device_id, (public.time_bucket('` + interval + `'::interval, sensor_readings.recorded_at))`
	} else {
		query = `SELECT
    device_id,
    time_bucket('` + interval + `', recorded_at) AS bucket,
    sum(value) AS total_value
FROM public.sensor_readings
WHERE granularity = 'second' AND multiplier = 1
GROUP BY device_id, time_bucket('` + interval + `', recorded_at)`
	}

	db := &schema.Database{
		Hypertables: []schema.Hypertable{
			{Schema: schema.DefaultSchema, TableName: "sensor_readings"},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         viewName,
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "sensor_readings",
				Query:            query,
				RefreshPolicy: &schema.RefreshPolicy{
					StartOffset:      startOff,
					EndOffset:        endOff,
					ScheduleInterval: schedInt,
				},
				WithData:     fromPostgres,
				Materialized: fromPostgres,
			},
		},
	}

	return db
}

func TestContinuousAggregateWithFilterClause(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Hypertables: []schema.Hypertable{
			{Schema: schema.DefaultSchema, TableName: "metrics"},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "metrics_hourly",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "metrics",
				Query: `SELECT metrics.category,
    public.time_bucket('01:00:00'::interval, metrics.created_at) AS bucket,
    count(*) AS total_count,
    count(*) FILTER (WHERE metrics.is_active) AS active_count,
    count(*) FILTER (WHERE (metrics.status = 2)) AS status_two,
    count(*) FILTER (WHERE (metrics.status = 0)) AS status_zero,
    avg(metrics.value) AS avg_value,
    avg(metrics.latency) AS avg_latency,
    max(metrics.latency) AS max_latency
   FROM public.metrics
  GROUP BY metrics.category, (public.time_bucket('01:00:00'::interval, metrics.created_at))`,
				RefreshPolicy: &schema.RefreshPolicy{
					StartOffset:      "3 hours",
					EndOffset:        "1 hour",
					ScheduleInterval: "1 hour",
				},
				WithData:     true,
				Materialized: true,
			},
		},
	}

	desired := &schema.Database{
		Hypertables: []schema.Hypertable{
			{Schema: schema.DefaultSchema, TableName: "metrics"},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "metrics_hourly",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "metrics",
				Query: `SELECT
    category,
    time_bucket('1 hour', created_at) AS bucket,
    COUNT(*) AS total_count,
    COUNT(*) FILTER (WHERE is_active) AS active_count,
    COUNT(*) FILTER (WHERE status = 2) AS status_two,
    COUNT(*) FILTER (WHERE status = 0) AS status_zero,
    AVG(value) AS avg_value,
    AVG(latency) AS avg_latency,
    MAX(latency) AS max_latency
FROM metrics
GROUP BY category, bucket`,
				RefreshPolicy: &schema.RefreshPolicy{
					StartOffset:      "3 hours",
					EndOffset:        "1 hour",
					ScheduleInterval: "1 hour",
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Changes) != 0 {
		t.Fatalf("expected no changes, got %d: %+v", len(result.Changes), result.Changes)
	}
}
