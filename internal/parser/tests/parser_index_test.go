package parser_test

import (
	"strings"
	"testing"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestParseCreateIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupSQL   string
		indexSQL   string
		wantIndex  string
		wantUnique bool
		wantWhere  bool
	}{
		{
			name:       "simple index",
			setupSQL:   `CREATE TABLE users (id BIGINT, email VARCHAR(255));`,
			indexSQL:   `CREATE INDEX idx_users_email ON users (email);`,
			wantIndex:  "idx_users_email",
			wantUnique: false,
			wantWhere:  false,
		},
		{
			name:       "unique index",
			setupSQL:   `CREATE TABLE users (id BIGINT, email VARCHAR(255));`,
			indexSQL:   `CREATE UNIQUE INDEX idx_users_email_unique ON users (email);`,
			wantIndex:  "idx_users_email_unique",
			wantUnique: true,
			wantWhere:  false,
		},
		{
			name:       "partial index",
			setupSQL:   `CREATE TABLE items (status VARCHAR(50), is_active BOOLEAN);`,
			indexSQL:   `CREATE INDEX idx_items_active ON items (status, is_active) WHERE is_active = TRUE;`,
			wantIndex:  "idx_items_active",
			wantUnique: false,
			wantWhere:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQLWithSetup(t, tt.setupSQL, tt.indexSQL)
			table := requireSingleTable(t, db)

			if len(table.Indexes) != 1 {
				t.Fatalf("expected 1 index, got %d", len(table.Indexes))
			}

			idx := table.Indexes[0]
			if idx.Name != tt.wantIndex {
				t.Errorf("index name = %v, want %v", idx.Name, tt.wantIndex)
			}

			if idx.IsUnique != tt.wantUnique {
				t.Errorf("index IsUnique = %v, want %v", idx.IsUnique, tt.wantUnique)
			}

			if (idx.Where != "") != tt.wantWhere {
				t.Errorf("index has WHERE = %v, want %v", idx.Where != "", tt.wantWhere)
			}
		})
	}
}

func TestParseAdvancedIndexes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupSQL       string
		indexSQL       string
		wantIndex      string
		wantType       string
		wantGIN        bool
		wantInclude    bool
		wantExpression bool
	}{
		{
			name:           "GIN index on JSONB",
			setupSQL:       `CREATE TABLE items (id UUID PRIMARY KEY, attributes JSONB);`,
			indexSQL:       `CREATE INDEX idx_items_attributes_gin ON items USING gin (attributes);`,
			wantIndex:      "idx_items_attributes_gin",
			wantType:       "gin",
			wantGIN:        true,
			wantInclude:    false,
			wantExpression: false,
		},
		{
			name:           "covering index with INCLUDE",
			setupSQL:       `CREATE TABLE users (id BIGINT, email VARCHAR(255), name VARCHAR(255), created_at TIMESTAMPTZ);`,
			indexSQL:       `CREATE INDEX idx_users_email_cover ON users (email) INCLUDE (name, created_at);`,
			wantIndex:      "idx_users_email_cover",
			wantType:       "btree",
			wantGIN:        false,
			wantInclude:    true,
			wantExpression: false,
		},
		{
			name:           "expression index",
			setupSQL:       `CREATE TABLE users (id BIGINT, email VARCHAR(255));`,
			indexSQL:       `CREATE INDEX idx_users_email_lower ON users (lower(email));`,
			wantIndex:      "idx_users_email_lower",
			wantType:       "btree",
			wantGIN:        false,
			wantInclude:    false,
			wantExpression: true,
		},
		{
			name:           "GIN index with text_pattern_ops",
			setupSQL:       `CREATE TABLE items (id UUID, name TEXT);`,
			indexSQL:       `CREATE INDEX idx_items_name ON items (name text_pattern_ops);`,
			wantIndex:      "idx_items_name",
			wantType:       "btree",
			wantGIN:        false,
			wantInclude:    false,
			wantExpression: false,
		},
		{
			name:           "HNSW index with halfvec opclass",
			setupSQL:       `CREATE TABLE embeddings (id UUID, v HALFVEC(3072));`,
			indexSQL:       `CREATE INDEX idx_embeddings_hnsw ON embeddings USING hnsw (v halfvec_cosine_ops);`,
			wantIndex:      "idx_embeddings_hnsw",
			wantType:       "hnsw",
			wantGIN:        false,
			wantInclude:    false,
			wantExpression: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQLWithSetup(t, tt.setupSQL, tt.indexSQL)
			table := requireSingleTable(t, db)

			var idx *schema.Index

			for i := range table.Indexes {
				if table.Indexes[i].Name == tt.wantIndex {
					idx = &table.Indexes[i]
					break
				}
			}

			if idx == nil {
				t.Fatalf(
					"index %s not found in table, got %d indexes",
					tt.wantIndex,
					len(table.Indexes),
				)
			}

			if idx.Name != tt.wantIndex {
				t.Errorf("index name = %v, want %v", idx.Name, tt.wantIndex)
			}

			if idx.Type != tt.wantType {
				t.Errorf("index type = %v, want %v", idx.Type, tt.wantType)
			}

			if (idx.Type == "gin") != tt.wantGIN {
				t.Errorf("GIN index = %v, want %v", idx.Type == "gin", tt.wantGIN)
			}

			if (len(idx.IncludeColumns) > 0) != tt.wantInclude {
				t.Errorf(
					"has INCLUDE columns = %v, want %v",
					len(idx.IncludeColumns) > 0,
					tt.wantInclude,
				)
			}

			if idx.IsExpression() != tt.wantExpression {
				t.Errorf("is expression index = %v, want %v", idx.IsExpression(), tt.wantExpression)
			}
		})
	}
}

func TestParseIndexOperatorClassAndStorageParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		setupSQL          string
		indexSQL          string
		wantIndex         string
		wantType          string
		wantColumns       []string
		wantStorageParams map[string]string
		wantWhere         string
	}{
		{
			name:     "HNSW index with opclass and storage params",
			setupSQL: `CREATE TABLE embeddings (id UUID, embedding HALFVEC(3072));`,
			indexSQL: `CREATE INDEX idx_emb_hnsw ON embeddings
				USING hnsw (embedding halfvec_cosine_ops)
				WITH (m = 16, ef_construction = 64);`,
			wantIndex:         "idx_emb_hnsw",
			wantType:          "hnsw",
			wantColumns:       []string{"embedding halfvec_cosine_ops"},
			wantStorageParams: map[string]string{"m": "16", "ef_construction": "64"},
			wantWhere:         "",
		},
		{
			name:     "partial HNSW index with opclass storage params and WHERE",
			setupSQL: `CREATE TABLE documents (id UUID, model_id TEXT, task_type TEXT, embedding HALFVEC(3072));`,
			indexSQL: `CREATE INDEX documents_search_idx ON documents
				USING hnsw (embedding halfvec_cosine_ops)
				WITH (m = 16, ef_construction = 64)
				WHERE model_id = 'primary' AND task_type = 'search';`,
			wantIndex:         "documents_search_idx",
			wantType:          "hnsw",
			wantColumns:       []string{"embedding halfvec_cosine_ops"},
			wantStorageParams: map[string]string{"m": "16", "ef_construction": "64"},
			wantWhere:         "model_id = 'primary' AND task_type = 'search'",
		},
		{
			name:              "btree index with fillfactor storage param",
			setupSQL:          `CREATE TABLE t (id UUID, email TEXT);`,
			indexSQL:          `CREATE INDEX idx_t_email ON t (email) WITH (fillfactor = 70);`,
			wantIndex:         "idx_t_email",
			wantType:          "btree",
			wantColumns:       []string{"email"},
			wantStorageParams: map[string]string{"fillfactor": "70"},
			wantWhere:         "",
		},
		{
			name:              "quoted storage param value",
			setupSQL:          `CREATE TABLE t (id UUID, v HALFVEC(3));`,
			indexSQL:          `CREATE INDEX idx_t_v ON t USING hnsw (v halfvec_cosine_ops) WITH (m='16');`,
			wantIndex:         "idx_t_v",
			wantType:          "hnsw",
			wantColumns:       []string{"v halfvec_cosine_ops"},
			wantStorageParams: map[string]string{"m": "16"},
			wantWhere:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQLWithSetup(t, tt.setupSQL, tt.indexSQL)
			table := requireSingleTable(t, db)

			idx := findIndexByName(table.Indexes, tt.wantIndex)
			if idx == nil {
				t.Fatalf("index %s not found; got %d indexes", tt.wantIndex, len(table.Indexes))
			}

			assertIndexShape(
				t,
				idx,
				tt.wantType,
				tt.wantColumns,
				tt.wantStorageParams,
				tt.wantWhere,
			)
		})
	}
}

func findIndexByName(indexes []schema.Index, name string) *schema.Index {
	for i := range indexes {
		if indexes[i].Name == name {
			return &indexes[i]
		}
	}

	return nil
}

func assertIndexShape(
	t *testing.T,
	idx *schema.Index,
	wantType string,
	wantColumns []string,
	wantStorageParams map[string]string,
	wantWhere string,
) {
	t.Helper()

	if idx.Type != wantType {
		t.Errorf("index type = %v, want %v", idx.Type, wantType)
	}

	if len(idx.Columns) != len(wantColumns) {
		t.Fatalf("columns length = %d, want %d", len(idx.Columns), len(wantColumns))
	}

	for i, want := range wantColumns {
		if idx.Columns[i] != want {
			t.Errorf("columns[%d] = %q, want %q", i, idx.Columns[i], want)
		}
	}

	assertStorageParams(t, idx.StorageParams, wantStorageParams)

	if strings.TrimSpace(idx.Where) != wantWhere {
		t.Errorf("where = %q, want %q", idx.Where, wantWhere)
	}
}

func assertStorageParams(t *testing.T, got, want map[string]string) {
	t.Helper()

	if len(got) != len(want) {
		t.Errorf("storage params count = %d, want %d (got %v)", len(got), len(want), got)
	}

	for k, v := range want {
		actual, ok := got[k]
		if !ok {
			t.Errorf("missing storage param %q", k)
			continue
		}

		if actual != v {
			t.Errorf("storage param %q = %q, want %q", k, actual, v)
		}
	}
}

func TestParseNullsNotDistinct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		setupSQL             string
		indexSQL             string
		wantIndex            string
		wantNullsNotDistinct bool
		wantColumns          []string
	}{
		{
			name:                 "unique index with NULLS NOT DISTINCT",
			setupSQL:             `CREATE TABLE t (id UUID, a TEXT, b TEXT);`,
			indexSQL:             `CREATE UNIQUE INDEX uq_t_ab ON t (a, b) NULLS NOT DISTINCT;`,
			wantIndex:            "uq_t_ab",
			wantNullsNotDistinct: true,
			wantColumns:          []string{"a", "b"},
		},
		{
			name:                 "unique index without NULLS NOT DISTINCT",
			setupSQL:             `CREATE TABLE t (id UUID, a TEXT);`,
			indexSQL:             `CREATE UNIQUE INDEX uq_t_a ON t (a);`,
			wantIndex:            "uq_t_a",
			wantNullsNotDistinct: false,
			wantColumns:          []string{"a"},
		},
		{
			name:                 "NULLS NOT DISTINCT with WHERE clause",
			setupSQL:             `CREATE TABLE t (id UUID, a TEXT, active BOOLEAN);`,
			indexSQL:             `CREATE UNIQUE INDEX uq_t_a_active ON t (a) NULLS NOT DISTINCT WHERE active = TRUE;`,
			wantIndex:            "uq_t_a_active",
			wantNullsNotDistinct: true,
			wantColumns:          []string{"a"},
		},
		{
			name:                 "NULLS NOT DISTINCT with INCLUDE",
			setupSQL:             `CREATE TABLE t (id UUID, a TEXT, b TEXT, c TEXT);`,
			indexSQL:             `CREATE UNIQUE INDEX uq_t_a_incl ON t (a) NULLS NOT DISTINCT INCLUDE (b, c);`,
			wantIndex:            "uq_t_a_incl",
			wantNullsNotDistinct: true,
			wantColumns:          []string{"a"},
		},
		{
			name:                 "NULLS NOT DISTINCT with storage params",
			setupSQL:             `CREATE TABLE t (id UUID, a TEXT);`,
			indexSQL:             `CREATE UNIQUE INDEX uq_t_a_fill ON t (a) NULLS NOT DISTINCT WITH (fillfactor = 70);`,
			wantIndex:            "uq_t_a_fill",
			wantNullsNotDistinct: true,
			wantColumns:          []string{"a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQLWithSetup(t, tt.setupSQL, tt.indexSQL)
			table := requireSingleTable(t, db)

			idx := findIndexByName(table.Indexes, tt.wantIndex)
			if idx == nil {
				t.Fatalf("index %s not found; got %d indexes", tt.wantIndex, len(table.Indexes))
			}

			if idx.NullsNotDistinct != tt.wantNullsNotDistinct {
				t.Errorf(
					"NullsNotDistinct = %v, want %v",
					idx.NullsNotDistinct,
					tt.wantNullsNotDistinct,
				)
			}

			if len(idx.Columns) != len(tt.wantColumns) {
				t.Fatalf("columns length = %d, want %d", len(idx.Columns), len(tt.wantColumns))
			}

			for i, want := range tt.wantColumns {
				if idx.Columns[i] != want {
					t.Errorf("columns[%d] = %q, want %q", i, idx.Columns[i], want)
				}
			}
		})
	}
}

func TestParseIndexOnContinuousAggregate(t *testing.T) {
	t.Parallel()

	setupSQL := `
CREATE TABLE public.sensor_data (
    sensor_id VARCHAR(50) NOT NULL,
    reading_time TIMESTAMPTZ NOT NULL,
    value NUMERIC(20, 8) NOT NULL,
    PRIMARY KEY (sensor_id, reading_time)
);

SELECT create_hypertable('sensor_data', 'reading_time');

CREATE MATERIALIZED VIEW sensor_data_hourly
WITH (timescaledb.continuous) AS
SELECT
    sensor_id,
    time_bucket('1 hour', reading_time) AS bucket,
    sum(value) AS total_value
FROM sensor_data
GROUP BY sensor_id, time_bucket('1 hour', reading_time)
WITH NO DATA;
`

	indexSQL := `CREATE INDEX idx_sensor_data_hourly_sensor ON sensor_data_hourly (sensor_id, bucket DESC);`

	db := parseSQLWithSetup(t, setupSQL, indexSQL)

	if len(db.ContinuousAggregates) != 1 {
		t.Fatalf("expected 1 continuous aggregate, got %d", len(db.ContinuousAggregates))
	}

	ca := db.ContinuousAggregates[0]
	if ca.ViewName != "sensor_data_hourly" {
		t.Errorf("CA view name = %v, want sensor_data_hourly", ca.ViewName)
	}

	if len(ca.Indexes) != 1 {
		t.Fatalf("expected 1 index on CA, got %d", len(ca.Indexes))
	}

	idx := ca.Indexes[0]
	if idx.Name != "idx_sensor_data_hourly_sensor" {
		t.Errorf("index name = %v, want idx_sensor_data_hourly_sensor", idx.Name)
	}

	if idx.TableName != "sensor_data_hourly" {
		t.Errorf("index table name = %v, want sensor_data_hourly", idx.TableName)
	}

	if len(idx.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(idx.Columns))
	}
}

func TestParseSchemaQualifiedIndex(t *testing.T) { //nolint:gocognit
	t.Parallel()

	tests := []struct {
		name        string
		setupSQL    string
		indexSQL    string
		wantIndex   string
		wantSchema  string
		wantTable   string
		wantColumns []string
	}{
		{
			name:     "schema-qualified table",
			setupSQL: `CREATE TABLE items (id UUID PRIMARY KEY, col1 TEXT, col2 TEXT);`,
			indexSQL: `CREATE INDEX idx_items_col1_col2
				ON items (col1, col2);`,
			wantIndex:   "idx_items_col1_col2",
			wantSchema:  schema.DefaultSchema,
			wantTable:   "items",
			wantColumns: []string{"col1", "col2"},
		},
		{
			name: "schema-qualified with foreign key containing ON DELETE",
			setupSQL: `CREATE TABLE items (id UUID PRIMARY KEY);
				CREATE TABLE posts (
					id UUID PRIMARY KEY,
					item_id UUID NOT NULL REFERENCES items (id) ON DELETE CASCADE,
					col1 TEXT
				);`,
			indexSQL:    `CREATE INDEX idx_posts_col1 ON posts (col1);`,
			wantIndex:   "idx_posts_col1",
			wantSchema:  schema.DefaultSchema,
			wantTable:   "posts",
			wantColumns: []string{"col1"},
		},
		{
			name: "multiple ON keywords in foreign keys",
			setupSQL: `CREATE TABLE items (id UUID PRIMARY KEY);
				CREATE TABLE connections (
					id UUID PRIMARY KEY,
					source_id UUID NOT NULL REFERENCES items (id) ON DELETE CASCADE,
					target_id UUID NOT NULL REFERENCES items (id) ON DELETE CASCADE,
					col1 TEXT,
					col2 TEXT
				);`,
			indexSQL: `CREATE INDEX idx_connections_col1_col2
				ON connections (col1, col2);`,
			wantIndex:   "idx_connections_col1_col2",
			wantSchema:  schema.DefaultSchema,
			wantTable:   "connections",
			wantColumns: []string{"col1", "col2"},
		},
		{
			name:        "index with text_pattern_ops on schema-qualified table",
			setupSQL:    `CREATE TABLE items (id UUID PRIMARY KEY, name TEXT);`,
			indexSQL:    `CREATE INDEX idx_items_name ON items (name text_pattern_ops);`,
			wantIndex:   "idx_items_name",
			wantSchema:  schema.DefaultSchema,
			wantTable:   "items",
			wantColumns: []string{"name"},
		},
		{
			name:        "public schema explicit",
			setupSQL:    `CREATE TABLE public.users (id UUID PRIMARY KEY, email TEXT);`,
			indexSQL:    `CREATE INDEX idx_users_email ON public.users (email);`,
			wantIndex:   "idx_users_email",
			wantSchema:  schema.DefaultSchema,
			wantTable:   "users",
			wantColumns: []string{"email"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQLWithSetup(t, tt.setupSQL, tt.indexSQL)

			var foundIndex *schema.Index

			for _, table := range db.Tables {
				if table.Schema == tt.wantSchema && table.Name == tt.wantTable {
					for i := range table.Indexes {
						if table.Indexes[i].Name == tt.wantIndex {
							foundIndex = &table.Indexes[i]
							break
						}
					}

					break
				}
			}

			if foundIndex == nil {
				t.Fatalf(
					"index %s not found on table %s.%s",
					tt.wantIndex,
					tt.wantSchema,
					tt.wantTable,
				)
			}

			if foundIndex.Schema != tt.wantSchema {
				t.Errorf("index schema = %v, want %v", foundIndex.Schema, tt.wantSchema)
			}

			if foundIndex.TableName != tt.wantTable {
				t.Errorf("index table name = %v, want %v", foundIndex.TableName, tt.wantTable)
			}

			if len(foundIndex.Columns) != len(tt.wantColumns) {
				t.Errorf(
					"index columns length = %v, want %v",
					len(foundIndex.Columns),
					len(tt.wantColumns),
				)
			}

			for i, wantCol := range tt.wantColumns {
				if i < len(foundIndex.Columns) {
					gotCol := foundIndex.Columns[i]
					if gotCol != wantCol && !strings.Contains(gotCol, wantCol) {
						t.Errorf("index column[%d] = %v, want %v", i, gotCol, wantCol)
					}
				}
			}
		})
	}
}
