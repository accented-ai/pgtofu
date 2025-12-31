package parser_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestParseCreateTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sql          string
		wantTable    string
		wantSchema   string
		wantColCount int
	}{
		{
			name: "simple table",
			sql: `CREATE TABLE users (
				id BIGINT PRIMARY KEY,
				name VARCHAR(100) NOT NULL,
				email VARCHAR(255) UNIQUE
			);`,
			wantTable:    "users",
			wantSchema:   schema.DefaultSchema,
			wantColCount: 3,
		},
		{
			name: "composite primary key",
			sql: `CREATE TABLE pairs (
				key1 VARCHAR(50) NOT NULL,
				key2 VARCHAR(50) NOT NULL,
				value VARCHAR(20) NOT NULL,
				CONSTRAINT pairs_pkey PRIMARY KEY (key1, key2)
			);`,
			wantTable:    "pairs",
			wantSchema:   schema.DefaultSchema,
			wantColCount: 3,
		},
		{
			name: "with defaults",
			sql: `CREATE TABLE items (
				id SERIAL PRIMARY KEY,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				is_active BOOLEAN DEFAULT TRUE,
				metadata JSONB DEFAULT '{}'::jsonb
			);`,
			wantTable:    "items",
			wantSchema:   schema.DefaultSchema,
			wantColCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQL(t, tt.sql)
			table := requireSingleTable(t, db)

			if table.Name != tt.wantTable {
				t.Errorf("table name = %v, want %v", table.Name, tt.wantTable)
			}

			if table.Schema != tt.wantSchema {
				t.Errorf("table schema = %v, want %v", table.Schema, tt.wantSchema)
			}

			if len(table.Columns) != tt.wantColCount {
				t.Errorf("column count = %v, want %v", len(table.Columns), tt.wantColCount)
			}
		})
	}
}

func TestParseComplexConstraints(t *testing.T) { //nolint:gocognit
	t.Parallel()

	tests := []struct {
		name                string
		sql                 string
		wantTable           string
		wantConstraintCount int
		wantFK              bool
		wantCheck           bool
		wantDeferrable      bool
	}{
		{
			name: "foreign key with CASCADE",
			sql: `CREATE TABLE posts (
				id BIGINT PRIMARY KEY,
				user_id BIGINT NOT NULL,
				CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id)
					REFERENCES users(id) ON DELETE CASCADE
			);`,
			wantTable:           "posts",
			wantConstraintCount: 2, // PK + FK
			wantFK:              true,
			wantCheck:           false,
			wantDeferrable:      false,
		},
		{
			name: "CHECK constraint with complex expression",
			sql: `CREATE TABLE users (
				id BIGINT PRIMARY KEY,
				age INTEGER,
				difficulty_level INTEGER,
				CONSTRAINT users_age_check CHECK (age >= 0 AND age <= 150),
				CONSTRAINT users_difficulty_check CHECK (difficulty_level BETWEEN 1 AND 10)
			);`,
			wantTable:           "users",
			wantConstraintCount: 3, // PK + 2 CHECK
			wantFK:              false,
			wantCheck:           true,
			wantDeferrable:      false,
		},
		{
			name: "DEFERRABLE INITIALLY DEFERRED constraint",
			sql: `CREATE TABLE items (
				id UUID PRIMARY KEY,
				type TEXT NOT NULL,
				name TEXT NOT NULL,
				CONSTRAINT items_unique UNIQUE (type, name)
					DEFERRABLE INITIALLY DEFERRED
			);`,
			wantTable:           "items",
			wantConstraintCount: 2, // PK + UNIQUE
			wantFK:              false,
			wantCheck:           false,
			wantDeferrable:      true,
		},
		{
			name: "composite primary key",
			sql: `CREATE TABLE pairs (
				team_id VARCHAR(50) NOT NULL,
				member_id VARCHAR(50) NOT NULL,
				role VARCHAR(20) NOT NULL,
				CONSTRAINT pairs_pkey PRIMARY KEY (team_id, member_id)
			);`,
			wantTable:           "pairs",
			wantConstraintCount: 1,
			wantFK:              false,
			wantCheck:           false,
			wantDeferrable:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQL(t, tt.sql)
			table := requireSingleTable(t, db)

			if table.Name != tt.wantTable {
				t.Errorf("table name = %v, want %v", table.Name, tt.wantTable)
			}

			if len(table.Constraints) != tt.wantConstraintCount {
				t.Errorf(
					"constraint count = %v, want %v",
					len(table.Constraints),
					tt.wantConstraintCount,
				)
			}

			if tt.wantFK {
				hasFK := false

				for _, c := range table.Constraints {
					if c.Type == schema.ForeignKey {
						hasFK = true
						break
					}
				}

				if !hasFK {
					t.Error("expected foreign key constraint, but none found")
				}
			}

			if tt.wantCheck {
				hasCheck := false

				for _, c := range table.Constraints {
					if c.Type == "CHECK" {
						hasCheck = true
						break
					}
				}

				if !hasCheck {
					t.Error("expected CHECK constraint, but none found")
				}
			}

			if tt.wantDeferrable {
				hasDeferrable := false

				for _, c := range table.Constraints {
					if c.IsDeferrable {
						hasDeferrable = true
						break
					}
				}

				if !hasDeferrable {
					t.Error("expected DEFERRABLE constraint, but none found")
				}
			}
		})
	}
}

func TestParseInlineCheckConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sql            string
		wantCheckCount int
		wantDefinition string
	}{
		{
			name: "simple inline CHECK",
			sql: `CREATE TABLE items (
				id TEXT PRIMARY KEY,
				price FLOAT CHECK (price > 0)
			);`,
			wantCheckCount: 1,
			wantDefinition: "CHECK (price > 0)",
		},
		{
			name: "inline CHECK with multi-line IN list",
			sql: `CREATE TABLE items (
				id TEXT PRIMARY KEY,
				type TEXT NOT NULL CHECK (type IN (
					'type_a',
					'type_b',
					'type_c',
					'type_d',
					'type_e'
				))
			);`,
			wantCheckCount: 1,
			wantDefinition: "CHECK (type IN (" +
				"\n\t\t\t\t'type_a'," +
				"\n\t\t\t\t'type_b'," +
				"\n\t\t\t\t'type_c'," +
				"\n\t\t\t\t'type_d'," +
				"\n\t\t\t\t'type_e'" +
				"\n\t\t\t))",
		},
		{
			name: "multiple inline CHECK constraints",
			sql: `CREATE TABLE connections (
				source_id UUID NOT NULL,
				target_id UUID NOT NULL,
				status TEXT NOT NULL CHECK (status IN ('pending', 'accepted', 'blocked')),
				confidence FLOAT DEFAULT 0.5 CHECK (confidence BETWEEN 0 AND 1)
			);`,
			wantCheckCount: 2,
		},
		{
			name: "inline CHECK with nested parentheses",
			sql: `CREATE TABLE items (
				id TEXT PRIMARY KEY,
				status TEXT CHECK (status IN ('pending', 'processing', 'completed') OR status IS NULL)
			);`,
			wantCheckCount: 1,
			wantDefinition: "CHECK (status IN ('pending', 'processing', 'completed') OR status IS NULL)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQL(t, tt.sql)
			table := requireSingleTable(t, db)

			checkConstraints := []schema.Constraint{}

			for _, c := range table.Constraints {
				if c.Type == "CHECK" {
					checkConstraints = append(checkConstraints, c)
				}
			}

			if len(checkConstraints) != tt.wantCheckCount {
				t.Errorf(
					"CHECK constraint count = %v, want %v",
					len(checkConstraints),
					tt.wantCheckCount,
				)
			}

			if tt.wantDefinition != "" && len(checkConstraints) > 0 {
				got := strings.TrimSpace(checkConstraints[0].Definition)
				want := strings.TrimSpace(tt.wantDefinition)

				got = regexp.MustCompile(`\s+`).ReplaceAllString(got, " ")
				want = regexp.MustCompile(`\s+`).ReplaceAllString(want, " ")

				if got != want {
					t.Errorf(
						"CHECK definition = %q, want %q",
						checkConstraints[0].Definition,
						tt.wantDefinition,
					)
				}
			}

			for _, c := range checkConstraints {
				if c.Type == "CHECK" {
					openCount := strings.Count(c.Definition, "(")

					closeCount := strings.Count(c.Definition, ")")
					if openCount != closeCount {
						t.Errorf(
							"unbalanced parentheses in CHECK constraint %q: %d open, %d close",
							c.Definition,
							openCount,
							closeCount,
						)
					}
				}
			}
		})
	}
}

func TestParseMultiLineTableComment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sql         string
		wantComment string
		wantTable   string
		wantSchema  string
	}{
		{
			name: "single-line comment",
			sql: `CREATE TABLE users (
				id BIGINT PRIMARY KEY
			);
			COMMENT ON TABLE users IS 'User accounts table';`,
			wantTable:   "users",
			wantSchema:  schema.DefaultSchema,
			wantComment: "User accounts table",
		},
		{
			name: "object name contains 'is' substring",
			sql: `CREATE TABLE history (
				id UUID PRIMARY KEY
			);
			COMMENT ON TABLE history IS 'History of events';`,
			wantTable:   "history",
			wantSchema:  schema.DefaultSchema,
			wantComment: "History of events",
		},
		{
			name: "multi-line comment with adjacent string literals",
			sql: `CREATE TABLE items (
				id UUID PRIMARY KEY
			);
			COMMENT ON TABLE items IS
			'Represents a specific item derived from an association '
			'between two items. It defines how an association is presented to the user '
			'for processing and testing.';`,
			wantTable:  "items",
			wantSchema: schema.DefaultSchema,
			wantComment: "Represents a specific item derived from an association " +
				"between two items. It defines how an association is presented to the user " +
				"for processing and testing.",
		},
		{
			name: "multi-line comment with escaped quotes",
			sql: `CREATE TABLE items (
				id BIGINT PRIMARY KEY
			);
			COMMENT ON TABLE items IS
			'Item catalog with ''special'' characters '
			'and multi-line descriptions.';`,
			wantTable:   "items",
			wantSchema:  schema.DefaultSchema,
			wantComment: "Item catalog with 'special' characters and multi-line descriptions.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQL(t, tt.sql)
			table := requireSingleTable(t, db)

			if table.Name != tt.wantTable {
				t.Errorf("table name = %v, want %v", table.Name, tt.wantTable)
			}

			if table.Schema != tt.wantSchema {
				t.Errorf("table schema = %v, want %v", table.Schema, tt.wantSchema)
			}

			if table.Comment != tt.wantComment {
				t.Errorf("table comment = %q, want %q", table.Comment, tt.wantComment)
			}
		})
	}
}

func TestParseArrayDefaultValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sql         string
		columnName  string
		wantDefault string
	}{
		{
			name: "ARRAY[]::TEXT[] default",
			sql: `CREATE TABLE items (
				tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]
			);`,
			columnName:  "tags",
			wantDefault: "ARRAY[]::TEXT[]",
		},
		{
			name: "ARRAY[] default",
			sql: `CREATE TABLE items (
				tags TEXT[] DEFAULT ARRAY[]
			);`,
			columnName:  "tags",
			wantDefault: "ARRAY[]",
		},
		{
			name: "ARRAY[]::INTEGER[] default",
			sql: `CREATE TABLE items (
				ids INTEGER[] DEFAULT ARRAY[]::INTEGER[]
			);`,
			columnName:  "ids",
			wantDefault: "ARRAY[]::INTEGER[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQL(t, tt.sql)
			table := requireSingleTable(t, db)

			col := table.GetColumn(tt.columnName)
			if col == nil {
				t.Fatalf("column %s not found", tt.columnName)
			}

			if col.Default != tt.wantDefault {
				t.Errorf("column default = %q, want %q", col.Default, tt.wantDefault)
			}
		})
	}
}

func TestParseArrayTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sql          string
		columnName   string
		wantDataType string
		wantIsArray  bool
	}{
		{
			name: "TEXT[] array",
			sql: `CREATE TABLE items (
				tags TEXT[]
			);`,
			columnName:   "tags",
			wantDataType: "TEXT",
			wantIsArray:  true,
		},
		{
			name: "UUID[] array",
			sql: `CREATE TABLE items (
				ids UUID[]
			);`,
			columnName:   "ids",
			wantDataType: "UUID",
			wantIsArray:  true,
		},
		{
			name: "INTEGER[] array",
			sql: `CREATE TABLE items (
				numbers INTEGER[]
			);`,
			columnName:   "numbers",
			wantDataType: "INTEGER",
			wantIsArray:  true,
		},
		{
			name: "non-array TEXT",
			sql: `CREATE TABLE items (
				description TEXT
			);`,
			columnName:   "description",
			wantDataType: "TEXT",
			wantIsArray:  false,
		},
		{
			name: "array with NOT NULL",
			sql: `CREATE TABLE items (
				tags TEXT[] NOT NULL
			);`,
			columnName:   "tags",
			wantDataType: "TEXT",
			wantIsArray:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQL(t, tt.sql)
			table := requireSingleTable(t, db)

			col := table.GetColumn(tt.columnName)
			if col == nil {
				t.Fatalf("column %s not found", tt.columnName)
			}

			if col.DataType != tt.wantDataType {
				t.Errorf("column DataType = %q, want %q", col.DataType, tt.wantDataType)
			}

			if col.IsArray != tt.wantIsArray {
				t.Errorf("column IsArray = %v, want %v", col.IsArray, tt.wantIsArray)
			}
		})
	}
}

func TestParseCrossSchemaReferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sql        string
		wantSchema string
		wantTable  string
		wantFKRef  string
	}{
		{
			name: "cross-schema foreign key",
			sql: `CREATE TABLE items (
				id UUID PRIMARY KEY,
				source_id TEXT NOT NULL,
				CONSTRAINT items_source_fkey FOREIGN KEY (source_id)
					REFERENCES sources(id)
			);`,
			wantSchema: schema.DefaultSchema,
			wantTable:  "items",
			wantFKRef:  "public.sources",
		},
		{
			name: "different schema table",
			sql: `CREATE TABLE items (
				id UUID PRIMARY KEY,
				code TEXT NOT NULL,
				CONSTRAINT items_code_fkey FOREIGN KEY (code)
					REFERENCES codes(code)
			);`,
			wantSchema: schema.DefaultSchema,
			wantTable:  "items",
			wantFKRef:  "public.codes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQL(t, tt.sql)
			table := requireSingleTable(t, db)

			if table.Schema != tt.wantSchema {
				t.Errorf("table schema = %v, want %v", table.Schema, tt.wantSchema)
			}

			if table.Name != tt.wantTable {
				t.Errorf("table name = %v, want %v", table.Name, tt.wantTable)
			}

			if tt.wantFKRef != "" {
				hasFK := false

				for _, c := range table.Constraints {
					if c.Type == schema.ForeignKey {
						hasFK = true

						refTable := c.QualifiedReferencedTable()
						if refTable != tt.wantFKRef {
							t.Errorf("FK reference = %v, want %v", refTable, tt.wantFKRef)
						}

						break
					}
				}

				if !hasFK {
					t.Error("expected foreign key constraint, but none found")
				}
			}
		})
	}
}

func TestParsePartitions(t *testing.T) {
	t.Parallel()

	sql := `CREATE TABLE items (
    id UUID NOT NULL DEFAULT UUID_GENERATE_V4(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);

CREATE TABLE items_2025_07 PARTITION OF items
FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');

CREATE TABLE items_2025_10 PARTITION OF items
FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');`

	db := parseSQL(t, sql)
	table := requireSingleTable(t, db)

	if table.Name != "items" {
		t.Errorf("table name = %v, want items", table.Name)
	}

	if table.Schema != schema.DefaultSchema {
		t.Errorf("table schema = %v, want %v", table.Schema, schema.DefaultSchema)
	}

	if table.PartitionStrategy == nil {
		t.Fatal("expected partition strategy, got nil")
	}

	if table.PartitionStrategy.Type != "RANGE" {
		t.Errorf("partition type = %v, want RANGE", table.PartitionStrategy.Type)
	}

	if len(table.PartitionStrategy.Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(table.PartitionStrategy.Partitions))
	}

	partition1 := table.PartitionStrategy.Partitions[0]
	if partition1.Name != "items_2025_07" {
		t.Errorf("partition1 name = %v, want items_2025_07", partition1.Name)
	}

	expectedDef1 := "FOR VALUES FROM ('2025-07-01') TO ('2025-10-01')"
	if partition1.Definition != expectedDef1 {
		t.Errorf("partition1 definition = %v, want %v", partition1.Definition, expectedDef1)
	}

	partition2 := table.PartitionStrategy.Partitions[1]
	if partition2.Name != "items_2025_10" {
		t.Errorf("partition2 name = %v, want items_2025_10", partition2.Name)
	}

	expectedDef2 := "FOR VALUES FROM ('2025-10-01') TO ('2026-01-01')"
	if partition2.Definition != expectedDef2 {
		t.Errorf("partition2 definition = %v, want %v", partition2.Definition, expectedDef2)
	}
}

func TestParsePartitionsDeferred(t *testing.T) {
	t.Parallel()

	sql := `CREATE TABLE items_p0 PARTITION OF items
    FOR VALUES WITH (MODULUS 16, REMAINDER 0);

CREATE TABLE items_p1 PARTITION OF items
    FOR VALUES WITH (MODULUS 16, REMAINDER 1);

CREATE TABLE items (
    id UUID NOT NULL DEFAULT UUID_GENERATE_V4(),
    user_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, id)
) PARTITION BY HASH (user_id);`

	p := parser.New()
	db := &schema.Database{}

	if err := p.ParseSQL(sql, db); err != nil {
		t.Fatalf("ParseSQL() error = %v", err)
	}

	if err := p.ProcessDeferredPartitions(db); err != nil {
		t.Fatalf("ProcessDeferredPartitions() error = %v", err)
	}

	table := requireSingleTable(t, db)
	if table.Name != "items" {
		t.Errorf("table name = %v, want items", table.Name)
	}

	if table.Schema != schema.DefaultSchema {
		t.Errorf("table schema = %v, want %v", table.Schema, schema.DefaultSchema)
	}

	if table.PartitionStrategy == nil {
		t.Fatal("expected partition strategy, got nil")
	}

	if table.PartitionStrategy.Type != "HASH" {
		t.Errorf("partition type = %v, want HASH", table.PartitionStrategy.Type)
	}

	if len(table.PartitionStrategy.Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(table.PartitionStrategy.Partitions))
	}

	partition1 := table.PartitionStrategy.Partitions[0]
	if partition1.Name != "items_p0" {
		t.Errorf("partition1 name = %v, want items_p0", partition1.Name)
	}

	expectedDef1 := "FOR VALUES WITH (MODULUS 16, REMAINDER 0)"
	if partition1.Definition != expectedDef1 {
		t.Errorf("partition1 definition = %v, want %v", partition1.Definition, expectedDef1)
	}

	partition2 := table.PartitionStrategy.Partitions[1]
	if partition2.Name != "items_p1" {
		t.Errorf("partition2 name = %v, want items_p1", partition2.Name)
	}

	expectedDef2 := "FOR VALUES WITH (MODULUS 16, REMAINDER 1)"
	if partition2.Definition != expectedDef2 {
		t.Errorf("partition2 definition = %v, want %v", partition2.Definition, expectedDef2)
	}
}

func TestParsePartitionsHashWithMultipleRemainders(t *testing.T) {
	t.Parallel()

	sql := `CREATE TABLE items (
    id UUID NOT NULL DEFAULT UUID_GENERATE_V4(),
    user_id UUID NOT NULL,
    PRIMARY KEY (user_id, id)
) PARTITION BY HASH (user_id);

CREATE TABLE items_p0 PARTITION OF items
    FOR VALUES WITH (MODULUS 16, REMAINDER 0);

CREATE TABLE items_p1 PARTITION OF items
    FOR VALUES WITH (MODULUS 16, REMAINDER 1);

CREATE TABLE items_p2 PARTITION OF items
    FOR VALUES WITH (MODULUS 16, REMAINDER 2);`

	p := parser.New()
	db := &schema.Database{}

	if err := p.ParseSQL(sql, db); err != nil {
		t.Fatalf("ParseSQL() error = %v", err)
	}

	if err := p.ProcessDeferredPartitions(db); err != nil {
		t.Fatalf("ProcessDeferredPartitions() error = %v", err)
	}

	table := requireSingleTable(t, db)
	if table.PartitionStrategy == nil {
		t.Fatal("expected partition strategy, got nil")
	}

	if len(table.PartitionStrategy.Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(table.PartitionStrategy.Partitions))
	}

	expectedPartitions := map[string]string{
		"items_p0": "FOR VALUES WITH (MODULUS 16, REMAINDER 0)",
		"items_p1": "FOR VALUES WITH (MODULUS 16, REMAINDER 1)",
		"items_p2": "FOR VALUES WITH (MODULUS 16, REMAINDER 2)",
	}

	for _, partition := range table.PartitionStrategy.Partitions {
		expectedDef, exists := expectedPartitions[partition.Name]
		if !exists {
			t.Errorf("unexpected partition name: %s", partition.Name)
			continue
		}

		if partition.Definition != expectedDef {
			t.Errorf(
				"partition %s definition = %v, want %v",
				partition.Name,
				partition.Definition,
				expectedDef,
			)
		}
	}
}
