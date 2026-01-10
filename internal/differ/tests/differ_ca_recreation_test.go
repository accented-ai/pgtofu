package differ_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

const (
	metricsHourlyQuery = `SELECT device_id, time_bucket('1 hour', recorded_at) AS bucket,
		sum(value) AS total FROM metrics GROUP BY device_id, bucket`
	eventsDailyQuery = `SELECT source, time_bucket('1 day', event_time) AS bucket,
		sum(count) AS total FROM events GROUP BY source, bucket`
	readings10sQuery = `SELECT sensor_id, time_bucket('10 seconds', timestamp) AS bucket,
		sum(value) AS total FROM readings GROUP BY sensor_id, bucket`
	readings1mQuery = `SELECT sensor_id, time_bucket('1 minute', timestamp) AS bucket,
		sum(value) AS total FROM readings GROUP BY sensor_id, bucket`
	readings1hQuery = `SELECT sensor_id, time_bucket('1 hour', timestamp) AS bucket,
		sum(value) AS total FROM readings GROUP BY sensor_id, bucket`
)

func TestDiffer_ContinuousAggregateRecreationForColumnTypeChange(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "device_id", DataType: "text", IsNullable: false},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric(20,8)", IsNullable: false},
				},
			},
		},
		Hypertables: []schema.Hypertable{
			{
				Schema:            schema.DefaultSchema,
				TableName:         "metrics",
				TimeColumnName:    "recorded_at",
				PartitionInterval: "1 day",
			},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "metrics_hourly",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "metrics",
				Query:            metricsHourlyQuery,
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "device_id", DataType: "text", IsNullable: false},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric(30,8)", IsNullable: false},
				},
			},
		},
		Hypertables: []schema.Hypertable{
			{
				Schema:            schema.DefaultSchema,
				TableName:         "metrics",
				TimeColumnName:    "recorded_at",
				PartitionInterval: "1 day",
			},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "metrics_hourly",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "metrics",
				Query:            metricsHourlyQuery,
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	changeTypes := make(map[differ.ChangeType]int)
	for _, c := range result.Changes {
		changeTypes[c.Type]++
	}

	assert.Equal(t, 1, changeTypes[differ.ChangeTypeModifyColumnType],
		"should have 1 column type change")
	assert.Equal(t, 1, changeTypes[differ.ChangeTypeDropContinuousAggregate],
		"should have 1 DROP continuous aggregate")
	assert.Equal(t, 1, changeTypes[differ.ChangeTypeAddContinuousAggregate],
		"should have 1 ADD continuous aggregate for recreation")

	var dropCAOrder, columnChangeOrder, addCAOrder int

	for i, c := range result.Changes {
		switch c.Type {
		case differ.ChangeTypeDropContinuousAggregate:
			dropCAOrder = i
		case differ.ChangeTypeModifyColumnType:
			columnChangeOrder = i
		case differ.ChangeTypeAddContinuousAggregate:
			addCAOrder = i
		}
	}

	assert.Less(t, dropCAOrder, columnChangeOrder,
		"DROP CA should come before column type change")
	assert.Less(t, columnChangeOrder, addCAOrder,
		"column type change should come before ADD CA")
}

func TestDiffer_ContinuousAggregateRecreationForNullabilityChange(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "events",
				Columns: []schema.Column{
					{Name: "source", DataType: "text", IsNullable: false},
					{Name: "event_time", DataType: "timestamptz", IsNullable: false},
					{Name: "count", DataType: "integer", IsNullable: true},
				},
			},
		},
		Hypertables: []schema.Hypertable{
			{
				Schema:            schema.DefaultSchema,
				TableName:         "events",
				TimeColumnName:    "event_time",
				PartitionInterval: "1 day",
			},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "events_daily",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "events",
				Query:            eventsDailyQuery,
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "events",
				Columns: []schema.Column{
					{Name: "source", DataType: "text", IsNullable: false},
					{Name: "event_time", DataType: "timestamptz", IsNullable: false},
					{Name: "count", DataType: "integer", IsNullable: false},
				},
			},
		},
		Hypertables: []schema.Hypertable{
			{
				Schema:            schema.DefaultSchema,
				TableName:         "events",
				TimeColumnName:    "event_time",
				PartitionInterval: "1 day",
			},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "events_daily",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "events",
				Query:            eventsDailyQuery,
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	changeTypes := make(map[differ.ChangeType]int)
	for _, c := range result.Changes {
		changeTypes[c.Type]++
	}

	assert.Equal(t, 1, changeTypes[differ.ChangeTypeModifyColumnNullability],
		"should have 1 nullability change")
	assert.Equal(t, 1, changeTypes[differ.ChangeTypeDropContinuousAggregate],
		"should have 1 DROP continuous aggregate")
	assert.Equal(t, 1, changeTypes[differ.ChangeTypeAddContinuousAggregate],
		"should have 1 ADD continuous aggregate for recreation")
}

func TestDiffer_MultipleContinuousAggregatesRecreation(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "readings",
				Columns: []schema.Column{
					{Name: "sensor_id", DataType: "text", IsNullable: false},
					{Name: "timestamp", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric(20,8)", IsNullable: false},
				},
			},
		},
		Hypertables: []schema.Hypertable{
			{
				Schema:            schema.DefaultSchema,
				TableName:         "readings",
				TimeColumnName:    "timestamp",
				PartitionInterval: "1 day",
			},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "readings_10s",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "readings",
				Query:            readings10sQuery,
			},
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "readings_1m",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "readings",
				Query:            readings1mQuery,
			},
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "readings_1h",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "readings",
				Query:            readings1hQuery,
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "readings",
				Columns: []schema.Column{
					{Name: "sensor_id", DataType: "text", IsNullable: false},
					{Name: "timestamp", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric(30,8)", IsNullable: false},
				},
			},
		},
		Hypertables: []schema.Hypertable{
			{
				Schema:            schema.DefaultSchema,
				TableName:         "readings",
				TimeColumnName:    "timestamp",
				PartitionInterval: "1 day",
			},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "readings_10s",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "readings",
				Query:            readings10sQuery,
			},
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "readings_1m",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "readings",
				Query:            readings1mQuery,
			},
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "readings_1h",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "readings",
				Query:            readings1hQuery,
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	changeTypes := make(map[differ.ChangeType]int)
	for _, c := range result.Changes {
		changeTypes[c.Type]++
	}

	assert.Equal(t, 1, changeTypes[differ.ChangeTypeModifyColumnType],
		"should have 1 column type change")
	assert.Equal(t, 3, changeTypes[differ.ChangeTypeDropContinuousAggregate],
		"should have 3 DROP continuous aggregates")
	assert.Equal(t, 3, changeTypes[differ.ChangeTypeAddContinuousAggregate],
		"should have 3 ADD continuous aggregates for recreation")
}

func TestDiffer_NoContinuousAggregateRecreationForUnrelatedTable(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "device_id", DataType: "text", IsNullable: false},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric(20,8)", IsNullable: false},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "other_table",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false},
					{Name: "data", DataType: "text", IsNullable: true},
				},
			},
		},
		Hypertables: []schema.Hypertable{
			{
				Schema:            schema.DefaultSchema,
				TableName:         "metrics",
				TimeColumnName:    "recorded_at",
				PartitionInterval: "1 day",
			},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "metrics_hourly",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "metrics",
				Query:            metricsHourlyQuery,
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "device_id", DataType: "text", IsNullable: false},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric(20,8)", IsNullable: false},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "other_table",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false},
					{Name: "data", DataType: "varchar(500)", IsNullable: true},
				},
			},
		},
		Hypertables: []schema.Hypertable{
			{
				Schema:            schema.DefaultSchema,
				TableName:         "metrics",
				TimeColumnName:    "recorded_at",
				PartitionInterval: "1 day",
			},
		},
		ContinuousAggregates: []schema.ContinuousAggregate{
			{
				Schema:           schema.DefaultSchema,
				ViewName:         "metrics_hourly",
				HypertableSchema: schema.DefaultSchema,
				HypertableName:   "metrics",
				Query:            metricsHourlyQuery,
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	changeTypes := make(map[differ.ChangeType]int)
	for _, c := range result.Changes {
		changeTypes[c.Type]++
	}

	assert.Equal(t, 1, changeTypes[differ.ChangeTypeModifyColumnType],
		"should have 1 column type change for other_table")
	assert.Equal(t, 0, changeTypes[differ.ChangeTypeDropContinuousAggregate],
		"should NOT have DROP continuous aggregate - change is on unrelated table")
	assert.Equal(t, 0, changeTypes[differ.ChangeTypeAddContinuousAggregate],
		"should NOT have ADD continuous aggregate - change is on unrelated table")
}
