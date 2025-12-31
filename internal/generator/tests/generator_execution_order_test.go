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

func TestOptimalExecutionOrder(t *testing.T) { //nolint:cyclop,gocognit
	t.Parallel()

	current := &schema.Database{
		Extensions: []schema.Extension{
			{Name: "pgcrypto"},
		},
	}
	desired := &schema.Database{
		Schemas: []schema.Schema{
			{Name: "app"},
		},
		Extensions: []schema.Extension{
			{Name: "uuid-ossp"},
		},
		CustomTypes: []schema.CustomType{
			{
				Schema: "app",
				Name:   "status",
				Type:   "enum",
				Values: []string{"active", "inactive"},
			},
		},
		Tables: []schema.Table{
			{
				Schema: "app",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "status", DataType: "app.status", IsNullable: false, Position: 2},
				},
			},
		},
		Functions: []schema.Function{
			{
				Schema:        "app",
				Name:          "generate_user_id",
				ArgumentTypes: []string{},
				ReturnType:    "uuid",
				Language:      "sql",
				Definition:    "SELECT uuid_generate_v4()",
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

	require.GreaterOrEqual(t, len(genResult.Migrations), 1, "expected at least one migration")

	schemaCreationIndex := -1
	extensionAddIndex := -1
	extensionDropIndex := -1
	customTypeIndex := -1
	tableIndex := -1
	functionIndex := -1

	for i, migration := range genResult.Migrations {
		if migration.UpFile == nil {
			continue
		}

		upSQL := migration.UpFile.Content
		if strings.Contains(upSQL, "CREATE SCHEMA") && strings.Contains(upSQL, "app") {
			schemaCreationIndex = i
		}

		if strings.Contains(upSQL, "CREATE EXTENSION") && strings.Contains(upSQL, "uuid-ossp") {
			extensionAddIndex = i
		}

		if strings.Contains(upSQL, "DROP EXTENSION") && strings.Contains(upSQL, "pgcrypto") {
			extensionDropIndex = i
		}

		if strings.Contains(upSQL, "CREATE TYPE") && strings.Contains(upSQL, "status") {
			customTypeIndex = i
		}

		if strings.Contains(upSQL, "CREATE TABLE") && strings.Contains(upSQL, "users") {
			tableIndex = i
		}

		if strings.Contains(upSQL, "CREATE FUNCTION") &&
			strings.Contains(upSQL, "generate_user_id") {
			functionIndex = i
		}
	}

	if schemaCreationIndex >= 0 {
		if extensionAddIndex >= 0 {
			assert.Less(t, schemaCreationIndex, extensionAddIndex,
				"schema creation should come before extension creation")
		}

		if customTypeIndex >= 0 {
			assert.Less(t, schemaCreationIndex, customTypeIndex,
				"schema creation should come before custom type creation")
		}

		if tableIndex >= 0 {
			assert.Less(t, schemaCreationIndex, tableIndex,
				"schema creation should come before table creation")
		}
	}

	if extensionAddIndex >= 0 {
		if customTypeIndex >= 0 {
			assert.Less(t, extensionAddIndex, customTypeIndex,
				"extension creation should come before custom type creation")
		}

		if tableIndex >= 0 {
			assert.Less(t, extensionAddIndex, tableIndex,
				"extension creation should come before table creation")
		}

		if functionIndex >= 0 {
			assert.Less(t, extensionAddIndex, functionIndex,
				"extension creation should come before function creation")
		}
	}

	if customTypeIndex >= 0 && tableIndex >= 0 {
		if customTypeIndex == tableIndex {
			migration := genResult.Migrations[customTypeIndex]
			upSQL := migration.UpFile.Content
			typePos := strings.Index(upSQL, "CREATE TYPE")

			tablePos := strings.Index(upSQL, "CREATE TABLE")
			if typePos >= 0 && tablePos >= 0 {
				assert.Less(t, typePos, tablePos,
					"custom type creation should come before table creation within the same file")
			}
		} else {
			assert.Less(t, customTypeIndex, tableIndex,
				"custom type creation should come before table creation")
		}
	}

	t.Logf(
		"Execution order: Schema=%d, ExtensionAdd=%d, ExtensionDrop=%d, CustomType=%d, Table=%d, Function=%d",
		schemaCreationIndex,
		extensionAddIndex,
		extensionDropIndex,
		customTypeIndex,
		tableIndex,
		functionIndex,
	)
}

func TestGeneratorTablesBeforeViews(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
				},
			},
		},
		Views: []schema.View{
			{
				Schema: schema.DefaultSchema,
				Name:   "all_items",
				Definition: `
SELECT id
FROM public.items AS i
UNION ALL
SELECT id
FROM public.items AS i2
`,
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
	require.NotEmpty(t, genResult.Migrations)
	require.NotNil(t, genResult.Migrations[0].UpFile)

	upSQL := genResult.Migrations[0].UpFile.Content
	tablePos := strings.Index(upSQL, "CREATE TABLE public.items")
	viewPos := strings.Index(upSQL, "CREATE VIEW public.all_items")

	require.GreaterOrEqual(t, tablePos, 0, "table statement not found")
	require.GreaterOrEqual(t, viewPos, 0, "view statement not found")
	assert.Less(t, tablePos, viewPos, "table should be created before view")
}

func TestGeneratorFunctionsBeforeTriggers(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
		Functions: []schema.Function{
			{
				Schema:        schema.DefaultSchema,
				Name:          "notify_items",
				Language:      "plpgsql",
				ReturnType:    "trigger",
				Body:          "BEGIN RETURN NEW; END;",
				Volatility:    schema.VolatilityVolatile,
				ArgumentTypes: []string{},
			},
		},
		Triggers: []schema.Trigger{
			{
				Schema:         schema.DefaultSchema,
				Name:           "items_notify",
				TableName:      "items",
				Timing:         "BEFORE",
				Events:         []string{"INSERT"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "notify_items",
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
	require.NotEmpty(t, genResult.Migrations)
	require.NotNil(t, genResult.Migrations[0].UpFile)

	upSQL := genResult.Migrations[0].UpFile.Content
	upperSQL := strings.ToUpper(upSQL)
	functionPos := strings.Index(upperSQL, "CREATE OR REPLACE FUNCTION PUBLIC.NOTIFY_ITEMS()")
	triggerPos := strings.Index(upperSQL, "CREATE TRIGGER ITEMS_NOTIFY")

	require.GreaterOrEqual(t, functionPos, 0, "function statement not found")
	require.GreaterOrEqual(t, triggerPos, 0, "trigger statement not found")
	assert.Less(t, functionPos, triggerPos, "function should be created before trigger")
}
