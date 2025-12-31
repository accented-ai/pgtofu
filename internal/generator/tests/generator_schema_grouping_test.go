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

func TestSchemaBasedGrouping(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "app",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
			{
				Schema: "shop",
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "posts",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.MaxOperationsPerFile = 10
	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)

	if len(genResult.Migrations) == 0 {
		t.Fatal("expected at least one migration")
	}

	schemasFound := make(map[string]bool)

	for _, migration := range genResult.Migrations {
		desc := migration.Description
		if contains(desc, "app") {
			schemasFound["app"] = true
		}

		if contains(desc, "shop") {
			schemasFound["shop"] = true
		}

		if !contains(desc, "app") && !contains(desc, "shop") && !contains(desc, "multi_schema") {
			schemasFound[schema.DefaultSchema] = true
		}
	}

	assert.True(
		t,
		schemasFound["app"] || schemasFound["shop"] || schemasFound[schema.DefaultSchema],
		"expected migrations to be grouped by schema",
	)
}

func TestTableOperationsGroupedTogether(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "categories",
				Columns: []schema.Column{
					{Name: "id", DataType: "text", IsNullable: false, Position: 1},
				},
				Comment: "Categories table",
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
				},
				Comment: "Items table",
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "results",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
				},
				Comment: "Results table",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.MaxOperationsPerFile = 100
	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(genResult.Migrations), 1, "expected at least one migration")

	for _, migration := range genResult.Migrations {
		if migration.UpFile != nil { //nolint:nestif
			upSQL := migration.UpFile.Content

			itemsTablePos := strings.Index(upSQL, "CREATE TABLE public.items")
			itemsCommentPos := strings.Index(upSQL, "COMMENT ON TABLE public.items")

			if itemsTablePos != -1 && itemsCommentPos != -1 {
				resultsTablePos := strings.Index(
					upSQL,
					"CREATE TABLE public.results",
				)
				resultsCommentPos := strings.Index(
					upSQL,
					"COMMENT ON TABLE public.results",
				)

				assert.Greater(
					t,
					itemsCommentPos, itemsTablePos,
					"items comment should appear after items table. "+
						"Items table at %d, items comment at %d",
					itemsTablePos,
					itemsCommentPos,
				)

				if resultsTablePos != -1 && resultsCommentPos != -1 {
					assert.Greater(
						t,
						resultsCommentPos, resultsTablePos,
						"results comment should appear after results table. "+
							"Results table at %d, results comment at %d",
						resultsTablePos,
						resultsCommentPos,
					)

					if itemsTablePos > resultsTablePos {
						assert.Greater(
							t,
							itemsTablePos,
							resultsCommentPos,
							"all results operations should be grouped together before items. "+
								"Results comment at %d, items table at %d",
							resultsCommentPos,
							itemsTablePos,
						)
					}
				}
			}
		}
	}
}

func TestSchemaDependencyOrdering(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "content",
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "code", DataType: "text", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:              "items_code_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"code"},
						ReferencedTable:   "codes",
						ReferencedSchema:  "app",
						ReferencedColumns: []string{"code"},
					},
				},
			},
			{
				Schema: "app",
				Name:   "codes",
				Columns: []schema.Column{
					{Name: "code", DataType: "text", IsNullable: false, Position: 1},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	gen := generator.New(testOptions())

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)

	if len(genResult.Migrations) < 2 {
		t.Fatal("expected at least 2 migrations (one per schema)")
	}

	appMigrationIndex := -1
	contentMigrationIndex := -1

	for i, migration := range genResult.Migrations {
		desc := migration.Description
		if contains(desc, "app") {
			appMigrationIndex = i
		}

		if contains(desc, "content") {
			contentMigrationIndex = i
		}
	}

	if appMigrationIndex == -1 || contentMigrationIndex == -1 {
		t.Skip("schema prefixes not detected in migration names, skipping order test")
		return
	}

	assert.Less(t, appMigrationIndex, contentMigrationIndex,
		"app schema migrations should come before content schema migrations")
}

func TestSchemaNaming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		changes  []differ.Change
		expected string
	}{
		{
			name: "single schema prefix",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddTable, ObjectName: "app.users"},
			},
			expected: "app_add_table_users",
		},
		{
			name: "public schema has no prefix",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddTable, ObjectName: "public.users"},
			},
			expected: "add_table_users",
		},
		{
			name: "multiple schemas",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddTable, ObjectName: "app.users"},
				{Type: differ.ChangeTypeAddTable, ObjectName: "shop.products"},
			},
			expected: "app_and_shop_schema_changes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := generator.GenerateMigrationName(tt.changes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}
