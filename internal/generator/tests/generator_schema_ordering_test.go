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

func TestSchemaCreationBeforeTableCreation(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Schemas: []schema.Schema{
			{Name: "app"},
		},
		Tables: []schema.Table{
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

	opts := testOptions()
	opts.MaxOperationsPerFile = 10
	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(genResult.Migrations), 1, "expected at least one migration")

	schemaCreationFound := false
	tableCreationFound := false
	schemaCreationIndex := -1
	tableCreationIndex := -1

	for i, migration := range genResult.Migrations {
		if migration.UpFile != nil {
			upSQL := migration.UpFile.Content
			if strings.Contains(upSQL, "CREATE SCHEMA") && strings.Contains(upSQL, "app") {
				schemaCreationFound = true
				schemaCreationIndex = i
			}

			if strings.Contains(upSQL, "CREATE TABLE") &&
				strings.Contains(upSQL, "codes") {
				tableCreationFound = true
				tableCreationIndex = i
			}
		}
	}

	require.True(t, schemaCreationFound, "schema creation should be found")
	require.True(t, tableCreationFound, "table creation should be found")

	if schemaCreationIndex == tableCreationIndex {
		migration := genResult.Migrations[schemaCreationIndex]
		upSQL := migration.UpFile.Content
		schemaPos := strings.Index(upSQL, "CREATE SCHEMA")
		tablePos := strings.Index(upSQL, "CREATE TABLE")
		assert.Less(t, schemaPos, tablePos,
			"schema creation should come before table creation within the same file")
	} else {
		assert.Less(t, schemaCreationIndex, tableCreationIndex,
			"schema creation should come before table creation (schema index: %d, table index: %d)",
			schemaCreationIndex, tableCreationIndex)
	}
}

func TestSchemaCreationWithExtensionDrop(t *testing.T) {
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
		Tables: []schema.Table{
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

	opts := testOptions()
	opts.MaxOperationsPerFile = 10
	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(genResult.Migrations), 1, "expected at least one migration")

	schemaCreationFound := false
	tableCreationFound := false
	extensionDropFound := false
	schemaCreationIndex := -1
	tableCreationIndex := -1
	extensionDropIndex := -1

	for i, migration := range genResult.Migrations {
		if migration.UpFile != nil {
			upSQL := migration.UpFile.Content
			if strings.Contains(upSQL, "CREATE SCHEMA") && strings.Contains(upSQL, "app") {
				schemaCreationFound = true
				schemaCreationIndex = i
			}

			if strings.Contains(upSQL, "CREATE TABLE") &&
				strings.Contains(upSQL, "codes") {
				tableCreationFound = true
				tableCreationIndex = i
			}

			if strings.Contains(upSQL, "DROP EXTENSION") && strings.Contains(upSQL, "pgcrypto") {
				extensionDropFound = true
				extensionDropIndex = i
			}
		}
	}

	require.True(t, schemaCreationFound, "schema creation should be found")
	require.True(t, tableCreationFound, "table creation should be found")

	if schemaCreationIndex == tableCreationIndex {
		migration := genResult.Migrations[schemaCreationIndex]
		upSQL := migration.UpFile.Content
		schemaPos := strings.Index(upSQL, "CREATE SCHEMA")
		tablePos := strings.Index(upSQL, "CREATE TABLE")
		assert.Less(t, schemaPos, tablePos,
			"schema creation should come before table creation within the same file")
	} else {
		assert.Less(t, schemaCreationIndex, tableCreationIndex,
			"schema creation should come before table creation (schema index: %d, table index: %d)",
			schemaCreationIndex, tableCreationIndex)
	}

	if extensionDropFound {
		t.Logf("Extension drop found in migration %d", extensionDropIndex)
	}
}
