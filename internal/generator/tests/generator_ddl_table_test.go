package generator_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDDLBuilder_AddTable(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "email", DataType: "varchar(255)", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{Name: "users_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
					{Name: "users_email_unique", Type: "UNIQUE", Columns: []string{"email"}},
				},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: userTable},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "CREATE TABLE")
	assert.Contains(t, stmt.SQL, "users")
	assert.Contains(t, stmt.SQL, "id")
	assert.Contains(t, stmt.SQL, "email")
	assert.Contains(t, stmt.SQL, "PRIMARY KEY")
	assert.Contains(t, stmt.SQL, "UNIQUE")
	assert.True(t, stmt.RequiresTx)
	assert.False(t, stmt.IsUnsafe)
	assert.NotContains(t, stmt.SQL, ";;", "Should not have double semicolons")
}

func TestDDLBuilder_AddTable_PreservesCharLength(t *testing.T) {
	t.Parallel()

	length2 := 2
	length3 := 3
	length4 := 4

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "core",
				Name:   "language_codes",
				Columns: []schema.Column{
					{
						Name:       "code",
						DataType:   "char",
						IsNullable: false,
						Position:   1,
						MaxLength:  &length2,
					},
					{
						Name:       "iso_639_3",
						DataType:   "char",
						IsNullable: false,
						Position:   2,
						MaxLength:  &length3,
					},
					{
						Name:       "script",
						DataType:   "char",
						IsNullable: false,
						Position:   3,
						MaxLength:  &length4,
					},
					{
						Name:       "label",
						DataType:   "text",
						IsNullable: false,
						Position:   4,
					},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "language_codes_pkey",
						Type:    schema.ConstraintPrimaryKey,
						Columns: []string{"code"},
					},
				},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: "core.language_codes"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "code CHAR(2) NOT NULL")
	assert.Contains(t, stmt.SQL, "iso_639_3 CHAR(3) NOT NULL")
	assert.Contains(t, stmt.SQL, "script CHAR(4) NOT NULL")
}

func TestDDLBuilder_AddTable_Spacing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		schema   string
		table    string
		expected string
	}{
		{
			name:     "default schema",
			schema:   schema.DefaultSchema,
			table:    "users",
			expected: "CREATE TABLE public.users (",
		},
		{
			name:     "custom schema",
			schema:   "app",
			table:    "images",
			expected: "CREATE TABLE app.images (",
		},
		{
			name:     "quoted schema",
			schema:   "MySchema",
			table:    "Users",
			expected: "CREATE TABLE \"MySchema\".\"Users\" (",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			current := &schema.Database{}
			desired := &schema.Database{
				Tables: []schema.Table{
					{
						Schema: tt.schema,
						Name:   tt.table,
						Columns: []schema.Column{
							{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
						},
					},
				},
			}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{
					{
						Type:       differ.ChangeTypeAddTable,
						ObjectName: generator.QualifiedName(tt.schema, tt.table),
					},
				},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)
			assert.Contains(t, stmt.SQL, tt.expected)
			assert.NotContains(t, stmt.SQL, "CREATE TABLE"+tt.table+"(")
			assert.NotContains(t, stmt.SQL, "CREATE TABLEapp.")
		})
	}
}

func TestDDLBuilder_AddIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		index *schema.Index
	}{
		{
			name: "simple btree index",
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_users_email",
				TableName: "users",
				Columns:   []string{"email"},
				Type:      "btree",
			},
		},
		{
			name: "unique index",
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_users_username",
				TableName: "users",
				Columns:   []string{"username"},
				IsUnique:  true,
				Type:      "btree",
			},
		},
		{
			name: "partial index",
			index: &schema.Index{
				Schema:    schema.DefaultSchema,
				Name:      "idx_active_users",
				TableName: "users",
				Columns:   []string{"created_at"},
				Type:      "btree",
				Where:     "active = true",
			},
		},
		{
			name: "covering index",
			index: &schema.Index{
				Schema:         schema.DefaultSchema,
				Name:           "idx_users_email_cover",
				TableName:      "users",
				Columns:        []string{"email"},
				IncludeColumns: []string{"name", "created_at"},
				Type:           "btree",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			current := &schema.Database{
				Tables: []schema.Table{{Schema: schema.DefaultSchema, Name: "users"}},
			}

			desired := &schema.Database{
				Tables: []schema.Table{
					{
						Schema:  schema.DefaultSchema,
						Name:    "users",
						Indexes: []schema.Index{*tt.index},
					},
				},
			}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{
					{
						Type:       differ.ChangeTypeAddIndex,
						ObjectName: "public." + tt.index.Name,
						Details:    map[string]any{"index": tt.index},
					},
				},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)
			assert.Contains(t, stmt.SQL, "CREATE")
			assert.Contains(t, stmt.SQL, "INDEX")
			assert.Contains(t, stmt.SQL, tt.index.Name)

			if tt.index.IsUnique {
				assert.Contains(t, stmt.SQL, "UNIQUE")
			}

			if tt.index.Where != "" {
				assert.Contains(t, stmt.SQL, "WHERE")
			}

			if len(tt.index.IncludeColumns) > 0 {
				assert.Contains(t, stmt.SQL, "INCLUDE")
			}
		})
	}
}

func TestDDLBuilder_TableCommentRevertForNewTable(t *testing.T) {
	t.Parallel()

	tbl := schema.Table{
		Schema:  schema.DefaultSchema,
		Name:    "users",
		Comment: "Users table",
		Columns: []schema.Column{{Name: "id", DataType: "uuid", IsNullable: false, Position: 1}},
	}

	current := &schema.Database{}
	desired := &schema.Database{Tables: []schema.Table{tbl}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeModifyTableComment,
				ObjectName: differ.TableKey(tbl.Schema, tbl.Name),
				Details: map[string]any{
					"table":       generator.QualifiedName(tbl.Schema, tbl.Name),
					"old_comment": "",
					"new_comment": tbl.Comment,
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "COMMENT ON TABLE public.users IS NULL;")
}

func TestDDLBuilder_ColumnCommentRevertForNewColumn(t *testing.T) {
	t.Parallel()

	tbl := schema.Table{
		Schema: schema.DefaultSchema,
		Name:   "posts",
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
			{Name: "note", DataType: "text", IsNullable: true, Position: 2, Comment: "note"},
		},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "posts",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
	}
	desired := &schema.Database{Tables: []schema.Table{tbl}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeModifyColumnComment,
				ObjectName: differ.TableKey(tbl.Schema, tbl.Name),
				Details: map[string]any{
					"table":       generator.QualifiedName(tbl.Schema, tbl.Name),
					"column_name": "note",
					"old_comment": "",
					"new_comment": "note",
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "COMMENT ON COLUMN public.posts.note IS NULL;")
}

func TestDownSkipsTableAndColumnCommentWhenDroppingTable(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "codes",
				Comment: "Codes table",
				Columns: []schema.Column{
					{Name: "code", DataType: "text", Comment: "Code value"},
				},
			},
		},
	}
	current := &schema.Database{Tables: nil}

	res := &differ.DiffResult{Current: current, Desired: desired}
	table := &desired.Tables[0]
	tableKey := differ.TableKey(table.Schema, table.Name)
	res.Changes = append(res.Changes,
		differ.Change{
			Type:        differ.ChangeTypeAddTable,
			Severity:    differ.SeveritySafe,
			Description: "Add table: " + table.QualifiedName(),
			ObjectType:  "table",
			ObjectName:  tableKey,
			Details:     map[string]any{"table": table},
		},
	)
	res.Changes = append(res.Changes,
		differ.Change{
			Type:        differ.ChangeTypeModifyTableComment,
			Severity:    differ.SeveritySafe,
			Description: "Add table comment: " + table.QualifiedName(),
			ObjectType:  "table",
			ObjectName:  tableKey,
			Details: map[string]any{
				"table":       table.QualifiedName(),
				"old_comment": "",
				"new_comment": table.Comment,
			},
		},
	)
	res.Changes = append(res.Changes,
		differ.Change{
			Type:        differ.ChangeTypeModifyColumnComment,
			Severity:    differ.SeveritySafe,
			Description: "Add column comment: " + table.QualifiedName() + ".code",
			ObjectType:  "column",
			ObjectName:  tableKey,
			Details: map[string]any{
				"table":       table.QualifiedName(),
				"column_name": "code",
				"old_comment": "",
				"new_comment": "ISO code",
			},
		},
	)

	opts := testOptions()
	opts.GenerateDownMigrations = true
	g := generator.New(opts)

	gen, err := g.Generate(res)
	require.NoError(t, err)
	require.Len(t, gen.Migrations, 1)
	require.NotNil(t, gen.Migrations[0].DownFile)
	out := gen.Migrations[0].DownFile.Content

	if strings.Contains(out, "COMMENT ON TABLE public.codes IS NULL") {
		t.Fatalf(
			"down migration should not contain table comment revert when table is dropped; got:\n%s",
			out,
		)
	}

	if strings.Contains(out, "COMMENT ON COLUMN public.codes.code IS NULL") {
		t.Fatalf(
			"down migration should not contain column comment revert when table is dropped; got:\n%s",
			out,
		)
	}

	if !strings.Contains(out, "DROP TABLE") {
		t.Fatalf("down migration should drop the table; got:\n%s", out)
	}
}

func TestDDLBuilder_Indentation(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "test_table",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "name", DataType: "varchar(255)", IsNullable: true, Position: 2},
				},
				Constraints: []schema.Constraint{
					{Name: "test_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
				},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: "public.test_table"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)

	assert.Contains(
		t,
		stmt.SQL,
		"    id BIGINT NOT NULL",
		"Column definitions should be indented with 4 spaces",
	)
	assert.Contains(
		t,
		stmt.SQL,
		"    name VARCHAR(255)",
		"Column definitions should be indented with 4 spaces",
	)
	assert.Contains(
		t,
		stmt.SQL,
		"    CONSTRAINT test_pkey PRIMARY KEY",
		"Constraint definitions should be indented with 4 spaces",
	)

	lines := strings.SplitSeq(stmt.SQL, "\n")
	for line := range lines {
		if strings.TrimSpace(line) != "" && !strings.HasPrefix(line, "CREATE") &&
			!strings.HasPrefix(line, ")") &&
			!strings.HasPrefix(line, ";") {
			if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") {
				t.Errorf("Found line with 2-space indentation (should be 4 spaces): %q", line)
			}
		}
	}
}

func TestDDLBuilder_AddTableWithPartitions(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "created_at", DataType: "timestamptz", IsNullable: false, Position: 2},
				},
				PartitionStrategy: &schema.PartitionStrategy{
					Type:    "RANGE",
					Columns: []string{"created_at"},
					Partitions: []schema.Partition{
						{
							Name:       "items_2025_07",
							Definition: "FOR VALUES FROM ('2025-07-01') TO ('2025-10-01')",
						},
						{
							Name:       "items_2025_10",
							Definition: "FOR VALUES FROM ('2025-10-01') TO ('2026-01-01')",
						},
					},
				},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: "public.items"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "PARTITION BY RANGE (created_at)")
	assert.Contains(
		t,
		stmt.SQL,
		"CREATE TABLE IF NOT EXISTS public.items_2025_07 PARTITION OF public.items",
	)
	assert.Contains(t, stmt.SQL, "FOR VALUES FROM ('2025-07-01') TO ('2025-10-01')")
	assert.Contains(
		t,
		stmt.SQL,
		"CREATE TABLE IF NOT EXISTS public.items_2025_10 PARTITION OF public.items",
	)
	assert.Contains(t, stmt.SQL, "FOR VALUES FROM ('2025-10-01') TO ('2026-01-01')")

	lines := strings.Split(stmt.SQL, "\n")
	foundPartition1 := false
	foundPartition2 := false

	for i, line := range lines {
		if strings.Contains(line, "items_2025_07") {
			foundPartition1 = true

			if i+1 < len(lines) {
				assert.Contains(
					t,
					lines[i+1],
					"FOR VALUES FROM",
					"Partition definition should be on next line",
				)
			}
		}

		if strings.Contains(line, "items_2025_10") {
			foundPartition2 = true

			if i+1 < len(lines) {
				assert.Contains(
					t,
					lines[i+1],
					"FOR VALUES FROM",
					"Partition definition should be on next line",
				)
			}
		}
	}

	assert.True(t, foundPartition1, "Should find first partition")
	assert.True(t, foundPartition2, "Should find second partition")

	partition1End := strings.Index(stmt.SQL, "FOR VALUES FROM ('2025-07-01') TO ('2025-10-01')")
	assert.Greater(t, partition1End, -1, "Should find first partition definition")

	afterPartition1 := stmt.SQL[partition1End+len("FOR VALUES FROM ('2025-07-01') TO ('2025-10-01')"):]
	afterPartition1 = strings.TrimSpace(afterPartition1)
	assert.True(
		t,
		strings.HasPrefix(afterPartition1, ";"),
		"First partition should end with semicolon",
	)

	partition2End := strings.Index(stmt.SQL, "FOR VALUES FROM ('2025-10-01') TO ('2026-01-01')")
	assert.Greater(t, partition2End, -1, "Should find second partition definition")

	afterPartition2 := stmt.SQL[partition2End+len("FOR VALUES FROM ('2025-10-01') TO ('2026-01-01')"):]
	afterPartition2 = strings.TrimSpace(afterPartition2)
	assert.True(
		t,
		strings.HasPrefix(afterPartition2, ";"),
		"Second partition should end with semicolon",
	)
}

func TestDDLBuilder_AddTableWithCheckConstraint(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "connections",
				Columns: []schema.Column{
					{Name: "source_id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "target_id", DataType: "uuid", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Type:       "CHECK",
						Definition: "CHECK (source_id != target_id)",
					},
				},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: "public.connections"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "CHECK (source_id != target_id)")
	assert.NotContains(
		t, stmt.SQL, "CHECK CHECK", "Should not have duplicate CHECK keyword", //nolint:dupword
	)
}

func TestDDLBuilder_DoesNotInlineCheckConstraint(t *testing.T) {
	t.Parallel()

	sql := `
CREATE TABLE test_table (
    id UUID NOT NULL,
    percentage FLOAT NOT NULL DEFAULT 0 CHECK (percentage BETWEEN 0 AND 100),
    name TEXT NOT NULL,
    CONSTRAINT test_table_pkey PRIMARY KEY (id)
);
`

	p := parser.New()
	db := &schema.Database{}

	require.NoError(t, p.ParseSQL(sql, db))
	require.Empty(t, p.GetErrors())
	require.Len(t, db.Tables, 1)

	table := db.Tables[0]
	table.Sort()
	require.Equal(t, "0", table.Columns[1].Default)

	current := &schema.Database{}
	desired := &schema.Database{Tables: []schema.Table{table}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: "public.test_table"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(
		t,
		stmt.SQL,
		"CONSTRAINT test_table_percentage_check CHECK (percentage BETWEEN 0 AND 100)",
	)
	assert.NotContains(
		t,
		stmt.SQL,
		"percentage FLOAT NOT NULL DEFAULT 0 CHECK (percentage BETWEEN 0 AND 100)",
		"CHECK constraint should not be duplicated inline with the column definition",
	)
}

func TestDDLBuilder_AddTableWithMultiLineCheckConstraint(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "text", IsNullable: false, Position: 1},
					{Name: "type", DataType: "text", IsNullable: false, Position: 2},
					{Name: "level", DataType: "text", IsNullable: false, Position: 3},
					{Name: "confidence", DataType: "float", IsNullable: false, Position: 4},
				},
				Constraints: []schema.Constraint{
					{Name: "items_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
					{
						Type: "CHECK",
						Definition: "CHECK (type IN (" +
							"'type_a', 'type_b', 'type_c', 'type_d', 'type_e'))",
					},
					{
						Type:       "CHECK",
						Definition: "CHECK (level IN ('level_a', 'level_b', 'level_c', 'level_d'))",
					},
					{
						Type:       "CHECK",
						Definition: "CHECK (confidence BETWEEN 0 AND 1)",
					},
				},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: "public.items"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)

	assert.Contains(t, stmt.SQL, "CHECK (type IN")
	assert.Contains(t, stmt.SQL, "CHECK (level IN")
	assert.Contains(t, stmt.SQL, "CHECK (confidence BETWEEN 0 AND 1)")
	assert.NotContains(
		t,
		stmt.SQL,
		"CHECK CHECK", //nolint:dupword
		"Should not have duplicate CHECK keyword",
	)

	openCount := strings.Count(stmt.SQL, "(")
	closeCount := strings.Count(stmt.SQL, ")")
	assert.Equal(t, openCount, closeCount, "Parentheses should be balanced in generated SQL")

	checkIndices := regexp.MustCompile(`(?i)\bCHECK\s*\(`).FindAllStringIndex(stmt.SQL, -1)
	for _, idx := range checkIndices {
		start := idx[1] - 1
		depth := 0
		found := false

		for i := start; i < len(stmt.SQL); i++ {
			if stmt.SQL[i] == '(' {
				depth++
			} else if stmt.SQL[i] == ')' {
				depth--
				if depth == 0 {
					found = true
					break
				}
			}
		}

		assert.True(
			t,
			found,
			"CHECK constraint starting at position %d should have balanced parentheses",
			start,
		)
	}
}

func TestDDLBuilder_CheckConstraintTrailingWhitespace(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "text", IsNullable: false, Position: 1},
					{Name: "level", DataType: "text", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Type: "CHECK",
						Definition: "CHECK (level IN (" +
							"\n        'level_a',     " +
							"\n        'level_b',     " +
							"\n        'level_c',     " +
							"\n        'level_d'     " +
							"\n    ))",
					},
				},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: "public.items"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)

	assert.Contains(t, stmt.SQL, "'level_a',")
	assert.Contains(t, stmt.SQL, "'level_b',")
	assert.Contains(t, stmt.SQL, "'level_c',")
	assert.Contains(t, stmt.SQL, "'level_d'")

	lines := strings.SplitSeq(stmt.SQL, "\n")
	for line := range lines {
		if strings.Contains(line, "'level_a',") || strings.Contains(line, "'level_b',") ||
			strings.Contains(line, "'level_c',") || strings.Contains(line, "'level_d'") {
			trimmed := strings.TrimRight(line, " \t")
			assert.Equal(t, line, trimmed, "Line should not have trailing whitespace: %q", line)
		}
	}
}

func TestDDLBuilder_AddTableWithMultiLineComment(t *testing.T) {
	t.Parallel()

	table := schema.Table{
		Schema: schema.DefaultSchema,
		Name:   "items",
		Columns: []schema.Column{
			{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
		},
		Comment: "Represents a specific item derived from an association\n" +
			"between two entities. It defines how an association is presented to the user\n" +
			"for processing and testing.",
	}

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{table},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: "public.items"},
			{
				Type:       differ.ChangeTypeModifyTableComment,
				ObjectName: differ.TableKey(table.Schema, table.Name),
				Details: map[string]any{
					"table":       generator.QualifiedName(table.Schema, table.Name),
					"old_comment": "",
					"new_comment": table.Comment,
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)

	addTableStmt, err := builder.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)
	assert.NotContains(t, addTableStmt.SQL, "COMMENT ON TABLE")

	commentStmt, err := builder.BuildUpStatement(result.Changes[1])
	require.NoError(t, err)
	assert.Contains(t, commentStmt.SQL, "COMMENT ON TABLE public.items IS")
	assert.Contains(
		t,
		commentStmt.SQL,
		"'Represents a specific item derived from an association'",
	)
	assert.Contains(
		t,
		commentStmt.SQL,
		"'between two entities. It defines how an association is presented to the user'",
	)
	assert.Contains(t, commentStmt.SQL, "'for processing and testing.'")
}

func TestDDLBuilder_AddTableWithArrayDefault(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{
						Name:       "tags",
						DataType:   "text",
						IsNullable: false,
						IsArray:    true,
						Default:    "ARRAY[]::TEXT[]",
						Position:   2,
					},
				},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: "public.items"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)

	assert.Contains(t, stmt.SQL, "tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]")
}

func TestDDLBuilder_DropTableDownMigration(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "app",
				Name:   "favorites",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "user_id", DataType: "uuid", IsNullable: false, Position: 2},
					{Name: "item_id", DataType: "uuid", IsNullable: false, Position: 3},
					{
						Name: "status", DataType: "text", IsNullable: false,
						Default: "'active'", Position: 4,
					},
				},
				Constraints: []schema.Constraint{
					{Name: "favorites_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
				},
			},
		},
	}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeDropTable, ObjectName: "app.favorites"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)

	upStmt, err := builder.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, upStmt.SQL, "DROP TABLE")
	assert.Contains(t, upStmt.SQL, "app.favorites")
	assert.True(t, upStmt.IsUnsafe)

	downStmt, err := builder.BuildDownStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, downStmt.SQL, "CREATE TABLE")
	assert.Contains(t, downStmt.SQL, "app.favorites")
	assert.Contains(t, downStmt.SQL, "id UUID NOT NULL")
	assert.Contains(t, downStmt.SQL, "user_id UUID NOT NULL")
	assert.Contains(t, downStmt.SQL, "item_id UUID NOT NULL")
	assert.Contains(t, downStmt.SQL, "status TEXT NOT NULL DEFAULT 'active'")
	assert.Contains(t, downStmt.SQL, "PRIMARY KEY")
}
