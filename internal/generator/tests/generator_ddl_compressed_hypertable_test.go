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

func TestDDLBuilder_ModifyColumnTypeOnCompressedHypertable(t *testing.T) {
	t.Parallel()

	hypertable := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "metrics",
		TimeColumnName:     "recorded_at",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"device_id"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "recorded_at", Direction: "DESC"},
			},
		},
	}

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
		Hypertables: []schema.Hypertable{hypertable},
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
		Hypertables: []schema.Hypertable{hypertable},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeModifyColumnType,
		ObjectName: "public.metrics.value",
		Details: map[string]any{
			"table":       "public.metrics",
			"column_name": "value",
			"old_type":    "numeric(20,8)",
			"new_type":    "numeric(30,8)",
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.Contains(t, sql, "warning")
	assert.Contains(t, sql, "decompress_chunk")
	assert.Contains(t, sql, "timescaledb.compress = false")
	assert.Contains(t, sql, "alter column")
	assert.Contains(t, sql, "type numeric(30,8)")
	assert.Contains(t, sql, "timescaledb.compress")
	assert.Contains(t, sql, "segmentby")
	assert.True(t, stmt.IsUnsafe, "operations on compressed hypertables should be marked unsafe")

	disablePos := strings.Index(sql, "timescaledb.compress = false")
	alterPos := strings.Index(sql, "alter column")
	enablePos := strings.LastIndex(sql, "timescaledb.compress,")

	assert.Less(t, disablePos, alterPos, "compression should be disabled before alter column")
	assert.Less(t, alterPos, enablePos, "compression should be re-enabled after alter column")
}

func TestDDLBuilder_ModifyColumnNullabilityOnCompressedHypertable(t *testing.T) {
	t.Parallel()

	hypertable := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "events",
		TimeColumnName:     "event_time",
		PartitionInterval:  "7 days",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"source"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "event_time", Direction: "DESC"},
			},
		},
	}

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
		Hypertables: []schema.Hypertable{hypertable},
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
		Hypertables: []schema.Hypertable{hypertable},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeModifyColumnNullability,
		ObjectName: "public.events.count",
		Details: map[string]any{
			"table":        "public.events",
			"column_name":  "count",
			"old_nullable": true,
			"new_nullable": false,
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.Contains(t, sql, "timescaledb.compress = false")
	assert.Contains(t, sql, "set not null")
	assert.Contains(t, sql, "timescaledb.compress")
}

func TestDDLBuilder_AddConstraintOnCompressedHypertable(t *testing.T) {
	t.Parallel()

	hypertable := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "readings",
		TimeColumnName:     "timestamp",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"sensor_id"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "timestamp", Direction: "DESC"},
			},
		},
	}

	constraint := schema.Constraint{
		Name:       "readings_value_check",
		Type:       "CHECK",
		Definition: "(value >= 0)",
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "readings",
				Columns: []schema.Column{
					{Name: "sensor_id", DataType: "text", IsNullable: false},
					{Name: "timestamp", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric", IsNullable: false},
				},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "readings",
				Columns: []schema.Column{
					{Name: "sensor_id", DataType: "text", IsNullable: false},
					{Name: "timestamp", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric", IsNullable: false},
				},
				Constraints: []schema.Constraint{constraint},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeAddConstraint,
		ObjectName: "public.readings.readings_value_check",
		Details: map[string]any{
			"table":      "public.readings",
			"constraint": &constraint,
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.Contains(t, sql, "timescaledb.compress = false")
	assert.Contains(t, sql, "add constraint")
	assert.Contains(t, sql, "check")
	assert.Contains(t, sql, "timescaledb.compress")
}

func TestDDLBuilder_NoCompressionWrapForRegularTable(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false},
					{Name: "score", DataType: "integer", IsNullable: true},
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
					{Name: "id", DataType: "bigint", IsNullable: false},
					{Name: "score", DataType: "bigint", IsNullable: true},
				},
			},
		},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeModifyColumnType,
		ObjectName: "public.users.score",
		Details: map[string]any{
			"table":       "public.users",
			"column_name": "score",
			"old_type":    "integer",
			"new_type":    "bigint",
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.NotContains(t, sql, "timescaledb.compress")
	assert.Contains(t, sql, "alter column")
	assert.Contains(t, sql, "type bigint")
}

func TestDDLBuilder_NoCompressionWrapForUncompressedHypertable(t *testing.T) {
	t.Parallel()

	hypertable := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "logs",
		TimeColumnName:     "created_at",
		PartitionInterval:  "1 day",
		CompressionEnabled: false,
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "logs",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false},
					{Name: "created_at", DataType: "timestamptz", IsNullable: false},
					{Name: "message", DataType: "text", IsNullable: true},
				},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "logs",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false},
					{Name: "created_at", DataType: "timestamptz", IsNullable: false},
					{Name: "message", DataType: "varchar(1000)", IsNullable: true},
				},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeModifyColumnType,
		ObjectName: "public.logs.message",
		Details: map[string]any{
			"table":       "public.logs",
			"column_name": "message",
			"old_type":    "text",
			"new_type":    "varchar(1000)",
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.NotContains(t, sql, "timescaledb.compress = false")
	assert.Contains(t, sql, "alter column")
}

func TestDDLBuilder_DownMigrationForCompressedHypertableColumnChange(t *testing.T) {
	t.Parallel()

	hypertable := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "metrics",
		TimeColumnName:     "recorded_at",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"device_id"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "recorded_at", Direction: "DESC"},
			},
		},
	}

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
		Hypertables: []schema.Hypertable{hypertable},
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
		Hypertables: []schema.Hypertable{hypertable},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeModifyColumnType,
		ObjectName: "public.metrics.value",
		Details: map[string]any{
			"table":       "public.metrics",
			"column_name": "value",
			"old_type":    "numeric(20,8)",
			"new_type":    "numeric(30,8)",
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(change)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.Contains(t, sql, "timescaledb.compress = false")
	assert.Contains(t, sql, "alter column")
	assert.Contains(t, sql, "type numeric(20,8)")
	assert.Contains(t, sql, "timescaledb.compress")
}

func TestDDLBuilder_ModifyColumnDefaultOnCompressedHypertableNoWrap(t *testing.T) {
	t.Parallel()

	hypertable := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "events",
		TimeColumnName:     "event_time",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"source"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "event_time", Direction: "DESC"},
			},
		},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "events",
				Columns: []schema.Column{
					{Name: "source", DataType: "text", IsNullable: false},
					{Name: "event_time", DataType: "timestamptz", IsNullable: false},
					{Name: "count", DataType: "integer", IsNullable: false, Default: "0"},
				},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "events",
				Columns: []schema.Column{
					{Name: "source", DataType: "text", IsNullable: false},
					{Name: "event_time", DataType: "timestamptz", IsNullable: false},
					{Name: "count", DataType: "integer", IsNullable: false, Default: "1"},
				},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeModifyColumnDefault,
		ObjectName: "public.events.count",
		Details: map[string]any{
			"table":       "public.events",
			"column_name": "count",
			"old_default": "0",
			"new_default": "1",
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.NotContains(t, sql, "timescaledb.compress = false",
		"SET DEFAULT should not require compression disable")
	assert.Contains(t, sql, "set default 1")
}

func TestDDLBuilder_AddColumnOnCompressedHypertable(t *testing.T) {
	t.Parallel()

	hypertable := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "metrics",
		TimeColumnName:     "recorded_at",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"device_id"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "recorded_at", Direction: "DESC"},
			},
		},
	}

	newColumn := schema.Column{
		Name:       "status",
		DataType:   "text",
		IsNullable: false,
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "device_id", DataType: "text", IsNullable: false},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric", IsNullable: false},
				},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "device_id", DataType: "text", IsNullable: false},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric", IsNullable: false},
					newColumn,
				},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeAddColumn,
		ObjectName: "public.metrics.status",
		Details: map[string]any{
			"table":  "public.metrics",
			"column": &newColumn,
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.Contains(t, sql, "warning")
	assert.Contains(t, sql, "decompress_chunk")
	assert.Contains(t, sql, "timescaledb.compress = false")
	assert.Contains(t, sql, "add column")
	assert.Contains(t, sql, "status")
	assert.Contains(t, sql, "timescaledb.compress")
	assert.Contains(t, sql, "segmentby")
	assert.True(t, stmt.IsUnsafe,
		"adding NOT NULL column on compressed hypertable should be unsafe")

	disablePos := strings.Index(sql, "timescaledb.compress = false")
	addColumnPos := strings.Index(sql, "add column")
	enablePos := strings.LastIndex(sql, "timescaledb.compress,")

	assert.Less(t, disablePos, addColumnPos, "compression should be disabled before add column")
	assert.Less(t, addColumnPos, enablePos, "compression should be re-enabled after add column")
}

func TestDDLBuilder_AddNullableColumnOnCompressedHypertable(t *testing.T) {
	t.Parallel()

	hypertable := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "events",
		TimeColumnName:     "event_time",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"source"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "event_time", Direction: "DESC"},
			},
		},
	}

	newColumn := schema.Column{
		Name:       "metadata",
		DataType:   "jsonb",
		IsNullable: true,
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "events",
				Columns: []schema.Column{
					{Name: "source", DataType: "text", IsNullable: false},
					{Name: "event_time", DataType: "timestamptz", IsNullable: false},
				},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "events",
				Columns: []schema.Column{
					{Name: "source", DataType: "text", IsNullable: false},
					{Name: "event_time", DataType: "timestamptz", IsNullable: false},
					newColumn,
				},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeAddColumn,
		ObjectName: "public.events.metadata",
		Details: map[string]any{
			"table":  "public.events",
			"column": &newColumn,
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.Contains(t, sql, "timescaledb.compress = false")
	assert.Contains(t, sql, "add column")
	assert.Contains(t, sql, "metadata")
}

func TestDDLBuilder_DropColumnOnCompressedHypertable(t *testing.T) {
	t.Parallel()

	hypertable := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "readings",
		TimeColumnName:     "timestamp",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"sensor_id"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "timestamp", Direction: "DESC"},
			},
		},
	}

	droppedColumn := schema.Column{
		Name:       "old_field",
		DataType:   "text",
		IsNullable: true,
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "readings",
				Columns: []schema.Column{
					{Name: "sensor_id", DataType: "text", IsNullable: false},
					{Name: "timestamp", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric", IsNullable: false},
					droppedColumn,
				},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "readings",
				Columns: []schema.Column{
					{Name: "sensor_id", DataType: "text", IsNullable: false},
					{Name: "timestamp", DataType: "timestamptz", IsNullable: false},
					{Name: "value", DataType: "numeric", IsNullable: false},
				},
			},
		},
		Hypertables: []schema.Hypertable{hypertable},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeDropColumn,
		ObjectName: "public.readings.old_field",
		Details: map[string]any{
			"table":  "public.readings",
			"column": &droppedColumn,
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.Contains(t, sql, "warning")
	assert.Contains(t, sql, "decompress_chunk")
	assert.Contains(t, sql, "timescaledb.compress = false")
	assert.Contains(t, sql, "drop column")
	assert.Contains(t, sql, "old_field")
	assert.Contains(t, sql, "timescaledb.compress")
	assert.Contains(t, sql, "segmentby")
	assert.True(t, stmt.IsUnsafe, "dropping column on compressed hypertable should be unsafe")

	disablePos := strings.Index(sql, "timescaledb.compress = false")
	dropColumnPos := strings.Index(sql, "drop column")
	enablePos := strings.LastIndex(sql, "timescaledb.compress,")

	assert.Less(t, disablePos, dropColumnPos, "compression should be disabled before drop column")
	assert.Less(t, dropColumnPos, enablePos, "compression should be re-enabled after drop column")
}

func TestDDLBuilder_NoCompressionWrapForAddColumnOnRegularTable(t *testing.T) {
	t.Parallel()

	newColumn := schema.Column{
		Name:       "email",
		DataType:   "text",
		IsNullable: false,
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false},
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
					{Name: "id", DataType: "bigint", IsNullable: false},
					newColumn,
				},
			},
		},
	}

	change := differ.Change{
		Type:       differ.ChangeTypeAddColumn,
		ObjectName: "public.users.email",
		Details: map[string]any{
			"table":  "public.users",
			"column": &newColumn,
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{change},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(change)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.NotContains(t, sql, "timescaledb.compress")
	assert.Contains(t, sql, "add column")
	assert.Contains(t, sql, "email")
}

func TestDDLBuilder_SkipReEnableWhenModifyCompressionPolicyExists(t *testing.T) {
	t.Parallel()

	currentHT := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "metrics",
		TimeColumnName:     "recorded_at",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"old_col"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "recorded_at", Direction: "DESC"},
			},
		},
	}

	desiredHT := schema.Hypertable{
		Schema:             schema.DefaultSchema,
		TableName:          "metrics",
		TimeColumnName:     "recorded_at",
		PartitionInterval:  "1 day",
		CompressionEnabled: true,
		CompressionSettings: &schema.CompressionSettings{
			SegmentByColumns: []string{"new_col"},
			OrderByColumns: []schema.OrderByColumn{
				{Column: "recorded_at", Direction: "DESC"},
			},
		},
	}

	droppedColumn := schema.Column{
		Name:       "old_col",
		DataType:   "text",
		IsNullable: false,
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					droppedColumn,
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false},
				},
			},
		},
		Hypertables: []schema.Hypertable{currentHT},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "new_col", DataType: "text", IsNullable: false},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false},
				},
			},
		},
		Hypertables: []schema.Hypertable{desiredHT},
	}

	dropColumnChange := differ.Change{
		Type:       differ.ChangeTypeDropColumn,
		ObjectName: "public.metrics.old_col",
		Details: map[string]any{
			"table":  "public.metrics",
			"column": &droppedColumn,
		},
	}

	modifyCompressionChange := differ.Change{
		Type:       differ.ChangeTypeModifyCompressionPolicy,
		ObjectName: "public.metrics",
		Details: map[string]any{
			"current": &currentHT,
			"desired": &desiredHT,
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{dropColumnChange, modifyCompressionChange},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(dropColumnChange)

	require.NoError(t, err)

	sql := strings.ToLower(stmt.SQL)
	assert.Contains(t, sql, "timescaledb.compress = false", "should disable compression")
	assert.Contains(t, sql, "drop column", "should have drop column statement")

	disableCount := strings.Count(sql, "timescaledb.compress")
	assert.Equal(t, 1, disableCount,
		"should only have one compression statement (disable only, no re-enable)")
}
