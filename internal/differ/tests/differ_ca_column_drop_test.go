package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

//nolint:dupl // Test data structures are similar but serve different test purposes
func TestModifyCABeforeDropColumnWithMultipleTables(t *testing.T) {
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

	caQuery := "SELECT %s, time_bucket('1 hour', recorded_at) AS bucket, COUNT(*) " +
		"FROM metrics GROUP BY %s, bucket"

	currentCA := schema.ContinuousAggregate{
		Schema:           schema.DefaultSchema,
		ViewName:         "metrics_hourly",
		HypertableSchema: schema.DefaultSchema,
		HypertableName:   "metrics",
		Query: "SELECT old_col, time_bucket('1 hour', recorded_at) AS bucket, " +
			"COUNT(*) FROM metrics GROUP BY old_col, bucket",
	}
	_ = caQuery

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
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigserial", IsNullable: false, Position: 1},
					{Name: "old_col", DataType: "varchar(20)", IsNullable: false, Position: 2},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false, Position: 3},
					{Name: "value", DataType: "numeric", IsNullable: false, Position: 4},
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
						Name:      "idx_metrics_old_col",
						TableName: "metrics",
						Columns:   []string{"old_col", "recorded_at"},
					},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "configs",
				Columns: []schema.Column{
					{Name: "old_col", DataType: "varchar(20)", IsNullable: false, Position: 1},
					{Name: "setting", DataType: "text", IsNullable: true, Position: 2},
				},
				Constraints: []schema.Constraint{
					{Name: "configs_pkey", Type: "PRIMARY KEY", Columns: []string{"old_col"}},
				},
			},
		},
		Hypertables:          []schema.Hypertable{hypertable},
		ContinuousAggregates: []schema.ContinuousAggregate{currentCA},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigserial", IsNullable: false, Position: 1},
					{Name: "new_col", DataType: "varchar(20)", IsNullable: false, Position: 2},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false, Position: 3},
					{Name: "value", DataType: "numeric", IsNullable: false, Position: 4},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "metrics_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"new_col", "recorded_at"},
					},
				},
				Indexes: []schema.Index{
					{
						Schema:    schema.DefaultSchema,
						Name:      "idx_metrics_new_col",
						TableName: "metrics",
						Columns:   []string{"new_col", "recorded_at"},
					},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "configs",
				Columns: []schema.Column{
					{Name: "new_col", DataType: "varchar(20)", IsNullable: false, Position: 1},
					{Name: "setting", DataType: "text", IsNullable: true, Position: 2},
				},
				Constraints: []schema.Constraint{
					{Name: "configs_pkey", Type: "PRIMARY KEY", Columns: []string{"new_col"}},
				},
			},
		},
		Hypertables:          []schema.Hypertable{desiredHypertable},
		ContinuousAggregates: []schema.ContinuousAggregate{desiredCA},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var (
		modifyCAIndex          = -1
		dropColumnMetricsIndex = -1
	)

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeModifyContinuousAggregate:
			modifyCAIndex = i
		case differ.ChangeTypeDropColumn:
			tableName, _ := change.Details["table"].(string)
			if tableName == "public.metrics" {
				dropColumnMetricsIndex = i
			}
		}
	}

	if modifyCAIndex == -1 {
		t.Fatal("MODIFY_CONTINUOUS_AGGREGATE change not found")
	}

	if dropColumnMetricsIndex == -1 {
		t.Fatal("DROP_COLUMN for metrics not found")
	}

	if modifyCAIndex >= dropColumnMetricsIndex {
		t.Errorf(
			"MODIFY_CONTINUOUS_AGGREGATE (index %d) should come before DROP_COLUMN on metrics (index %d). "+
				"The continuous aggregate must be modified before dropping the column it depends on.",
			modifyCAIndex,
			dropColumnMetricsIndex,
		)
		t.Log("Changes order:")

		for i, change := range result.Changes {
			t.Logf("  %d: %s (%s) Order=%d", i, change.Type, change.Description, change.Order)
		}
	}
}

func TestModifyCABeforeDropColumnSingleTable(t *testing.T) {
	t.Parallel()

	hypertable := schema.Hypertable{
		Schema:            schema.DefaultSchema,
		TableName:         "metrics",
		TimeColumnName:    "recorded_at",
		PartitionInterval: "1 day",
	}

	currentCA := schema.ContinuousAggregate{
		Schema:           schema.DefaultSchema,
		ViewName:         "metrics_hourly",
		HypertableSchema: schema.DefaultSchema,
		HypertableName:   "metrics",
		Query: "SELECT old_col, time_bucket('1 hour', recorded_at) AS bucket " +
			"FROM metrics GROUP BY old_col, bucket",
	}

	desiredCA := schema.ContinuousAggregate{
		Schema:           schema.DefaultSchema,
		ViewName:         "metrics_hourly",
		HypertableSchema: schema.DefaultSchema,
		HypertableName:   "metrics",
		Query: "SELECT new_col, time_bucket('1 hour', recorded_at) AS bucket " +
			"FROM metrics GROUP BY new_col, bucket",
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigserial", IsNullable: false, Position: 1},
					{Name: "old_col", DataType: "varchar(20)", IsNullable: false, Position: 2},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false, Position: 3},
					{Name: "value", DataType: "smallint", IsNullable: false, Position: 4},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "metrics_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"old_col", "recorded_at"},
					},
				},
			},
		},
		Hypertables:          []schema.Hypertable{hypertable},
		ContinuousAggregates: []schema.ContinuousAggregate{currentCA},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "metrics",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigserial", IsNullable: false, Position: 1},
					{Name: "new_col", DataType: "varchar(20)", IsNullable: false, Position: 2},
					{Name: "recorded_at", DataType: "timestamptz", IsNullable: false, Position: 3},
					{Name: "value", DataType: "smallint", IsNullable: false, Position: 4},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "metrics_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"new_col", "recorded_at"},
					},
					{Name: "metrics_value_check", Type: "CHECK", Definition: "value IN (0, 1, 2)"},
				},
			},
		},
		Hypertables:          []schema.Hypertable{hypertable},
		ContinuousAggregates: []schema.ContinuousAggregate{desiredCA},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var (
		modifyCAIndex   = -1
		dropColumnIndex = -1
		addColumnIndex  = -1
	)

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeModifyContinuousAggregate:
			modifyCAIndex = i
		case differ.ChangeTypeDropColumn:
			dropColumnIndex = i
		case differ.ChangeTypeAddColumn:
			addColumnIndex = i
		}
	}

	if modifyCAIndex == -1 {
		t.Fatal("MODIFY_CONTINUOUS_AGGREGATE change not found")
	}

	if dropColumnIndex == -1 {
		t.Fatal("DROP_COLUMN change not found")
	}

	if addColumnIndex == -1 {
		t.Fatal("ADD_COLUMN change not found")
	}

	if modifyCAIndex >= dropColumnIndex {
		t.Errorf(
			"MODIFY_CONTINUOUS_AGGREGATE (index %d) should come before DROP_COLUMN (index %d)",
			modifyCAIndex,
			dropColumnIndex,
		)
		t.Log("Changes order:")

		for i, change := range result.Changes {
			t.Logf("  %d: %s (%s)", i, change.Type, change.Description)
		}
	}

	if addColumnIndex >= modifyCAIndex {
		t.Errorf(
			"ADD_COLUMN (index %d) should come before MODIFY_CONTINUOUS_AGGREGATE (index %d)",
			addColumnIndex,
			modifyCAIndex,
		)
	}
}
