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
