package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerialTypeNullability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sql            string
		columnName     string
		wantDataType   string
		wantIsNullable bool
	}{
		{
			name: "SERIAL is NOT NULL",
			sql: `CREATE TABLE counters (
				id SERIAL PRIMARY KEY,
				value INTEGER
			);`,
			columnName:     "id",
			wantDataType:   "INTEGER",
			wantIsNullable: false,
		},
		{
			name: "BIGSERIAL is NOT NULL",
			sql: `CREATE TABLE events (
				id BIGSERIAL,
				event_type TEXT NOT NULL
			);`,
			columnName:     "id",
			wantDataType:   "BIGINT",
			wantIsNullable: false,
		},
		{
			name: "SMALLSERIAL is NOT NULL",
			sql: `CREATE TABLE lookup_codes (
				id SMALLSERIAL,
				code TEXT NOT NULL
			);`,
			columnName:     "id",
			wantDataType:   "SMALLINT",
			wantIsNullable: false,
		},
		{
			name: "SERIAL with explicit NOT NULL (redundant but valid)",
			sql: `CREATE TABLE items (
				id SERIAL NOT NULL,
				name TEXT
			);`,
			columnName:     "id",
			wantDataType:   "INTEGER",
			wantIsNullable: false,
		},
		{
			name: "BIGSERIAL with PRIMARY KEY",
			sql: `CREATE TABLE records (
				id BIGSERIAL PRIMARY KEY,
				data JSONB
			);`,
			columnName:     "id",
			wantDataType:   "BIGINT",
			wantIsNullable: false,
		},
		{
			name: "SERIAL with UNIQUE constraint",
			sql: `CREATE TABLE tokens (
				id SERIAL UNIQUE,
				token TEXT NOT NULL
			);`,
			columnName:     "id",
			wantDataType:   "INTEGER",
			wantIsNullable: false,
		},
		{
			name: "lowercase serial",
			sql: `CREATE TABLE metrics (
				id serial,
				metric_name TEXT
			);`,
			columnName:     "id",
			wantDataType:   "INTEGER",
			wantIsNullable: false,
		},
		{
			name: "lowercase bigserial",
			sql: `CREATE TABLE logs (
				id bigserial,
				message TEXT
			);`,
			columnName:     "id",
			wantDataType:   "BIGINT",
			wantIsNullable: false,
		},
		{
			name: "lowercase smallserial",
			sql: `CREATE TABLE tags (
				id smallserial,
				tag_name TEXT
			);`,
			columnName:     "id",
			wantDataType:   "SMALLINT",
			wantIsNullable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQL(t, tt.sql)
			table := requireSingleTable(t, db)

			col := table.GetColumn(tt.columnName)
			require.NotNil(t, col, "column %s not found", tt.columnName)

			assert.Equal(t, tt.wantDataType, col.DataType,
				"column %s DataType mismatch", tt.columnName)
			assert.Equal(t, tt.wantIsNullable, col.IsNullable,
				"column %s IsNullable mismatch", tt.columnName)
		})
	}
}

func TestSerialVsRegularIntegerNullability(t *testing.T) {
	t.Parallel()

	sql := `CREATE TABLE comparison (
		serial_col SERIAL,
		bigserial_col BIGSERIAL,
		smallserial_col SMALLSERIAL,
		integer_col INTEGER,
		bigint_col BIGINT,
		smallint_col SMALLINT,
		integer_not_null INTEGER NOT NULL
	);`

	db := parseSQL(t, sql)
	table := requireSingleTable(t, db)

	tests := []struct {
		columnName     string
		wantDataType   string
		wantIsNullable bool
	}{
		{"serial_col", "INTEGER", false},
		{"bigserial_col", "BIGINT", false},
		{"smallserial_col", "SMALLINT", false},
		{"integer_col", "INTEGER", true},
		{"bigint_col", "BIGINT", true},
		{"smallint_col", "SMALLINT", true},
		{"integer_not_null", "INTEGER", false},
	}

	for _, tt := range tests {
		col := table.GetColumn(tt.columnName)
		require.NotNil(t, col, "column %s not found", tt.columnName)

		assert.Equal(t, tt.wantDataType, col.DataType,
			"column %s DataType mismatch", tt.columnName)
		assert.Equal(t, tt.wantIsNullable, col.IsNullable,
			"column %s IsNullable mismatch", tt.columnName)
	}
}

func TestMultipleSerialColumns(t *testing.T) {
	t.Parallel()

	sql := `CREATE TABLE audit_log (
		id BIGSERIAL PRIMARY KEY,
		sequence_num SERIAL,
		batch_id SMALLSERIAL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);`

	db := parseSQL(t, sql)
	table := requireSingleTable(t, db)

	require.Len(t, table.Columns, 4)

	idCol := table.GetColumn("id")
	require.NotNil(t, idCol)
	assert.Equal(t, "BIGINT", idCol.DataType)
	assert.False(t, idCol.IsNullable, "BIGSERIAL column should be NOT NULL")

	seqCol := table.GetColumn("sequence_num")
	require.NotNil(t, seqCol)
	assert.Equal(t, "INTEGER", seqCol.DataType)
	assert.False(t, seqCol.IsNullable, "SERIAL column should be NOT NULL")

	batchCol := table.GetColumn("batch_id")
	require.NotNil(t, batchCol)
	assert.Equal(t, "SMALLINT", batchCol.DataType)
	assert.False(t, batchCol.IsNullable, "SMALLSERIAL column should be NOT NULL")
}

func TestSerialWithSchemaQualification(t *testing.T) {
	t.Parallel()

	sql := `CREATE TABLE public.orders (
		id BIGSERIAL PRIMARY KEY,
		customer_id BIGINT NOT NULL,
		total_amount NUMERIC(12, 2)
	);`

	db := parseSQL(t, sql)
	table := requireSingleTable(t, db)

	assert.Equal(t, "orders", table.Name)
	assert.Equal(t, "public", table.Schema)

	idCol := table.GetColumn("id")
	require.NotNil(t, idCol)
	assert.Equal(t, "BIGINT", idCol.DataType)
	assert.False(t, idCol.IsNullable, "BIGSERIAL column should be NOT NULL")
}

func TestSerialDefaultValueMarker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		sql               string
		columnName        string
		wantDefaultPrefix string
	}{
		{
			name: "SERIAL gets sequence default",
			sql: `CREATE TABLE items (
				id SERIAL PRIMARY KEY
			);`,
			columnName:        "id",
			wantDefaultPrefix: "nextval",
		},
		{
			name: "BIGSERIAL gets sequence default",
			sql: `CREATE TABLE items (
				id BIGSERIAL PRIMARY KEY
			);`,
			columnName:        "id",
			wantDefaultPrefix: "nextval",
		},
		{
			name: "SMALLSERIAL gets sequence default",
			sql: `CREATE TABLE items (
				id SMALLSERIAL PRIMARY KEY
			);`,
			columnName:        "id",
			wantDefaultPrefix: "nextval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQL(t, tt.sql)
			table := requireSingleTable(t, db)

			col := table.GetColumn(tt.columnName)
			require.NotNil(t, col, "column %s not found", tt.columnName)

			assert.Contains(t, col.Default, tt.wantDefaultPrefix,
				"SERIAL column should have nextval default after finalization")
		})
	}
}

func TestSerialInPartitionedTable(t *testing.T) {
	t.Parallel()

	sql := `CREATE TABLE time_series_data (
		id BIGSERIAL,
		recorded_at TIMESTAMPTZ NOT NULL,
		value NUMERIC(20, 8) NOT NULL,
		PRIMARY KEY (recorded_at, id)
	) PARTITION BY RANGE (recorded_at);`

	db := parseSQL(t, sql)
	table := requireSingleTable(t, db)

	idCol := table.GetColumn("id")
	require.NotNil(t, idCol)
	assert.Equal(t, "BIGINT", idCol.DataType)
	assert.False(t, idCol.IsNullable, "BIGSERIAL column should be NOT NULL")

	require.NotNil(t, table.PartitionStrategy)
	assert.Equal(t, "RANGE", table.PartitionStrategy.Type)
}

func TestSerialWithCheckConstraint(t *testing.T) {
	t.Parallel()

	sql := `CREATE TABLE positive_values (
		id SERIAL CHECK (id > 0),
		value INTEGER CHECK (value >= 0)
	);`

	db := parseSQL(t, sql)
	table := requireSingleTable(t, db)

	idCol := table.GetColumn("id")
	require.NotNil(t, idCol)
	assert.Equal(t, "INTEGER", idCol.DataType)
	assert.False(t, idCol.IsNullable, "SERIAL column should be NOT NULL")

	hasCheck := false

	for _, c := range table.Constraints {
		if c.Type == "CHECK" {
			hasCheck = true
			break
		}
	}

	assert.True(t, hasCheck, "CHECK constraint should be parsed")
}
