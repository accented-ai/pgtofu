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

func TestCyclicSchemaDependenciesOrderedByObjectDependencies(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Schemas: []schema.Schema{
			{Name: "alpha"},
			{Name: "beta"},
			{Name: "gamma"},
		},
		Tables: []schema.Table{
			{
				Schema: "alpha",
				Name:   "trinkets",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "gadget_id", DataType: "uuid", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:              "trinkets_gadget_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"gadget_id"},
						ReferencedSchema:  "beta",
						ReferencedTable:   "gadgets",
						ReferencedColumns: []string{"id"},
					},
				},
			},
			{
				Schema: "alpha",
				Name:   "widgets",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
				},
			},
			{
				Schema: "beta",
				Name:   "gadgets",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "widget_id", DataType: "uuid", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:              "gadgets_widget_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"widget_id"},
						ReferencedSchema:  "alpha",
						ReferencedTable:   "widgets",
						ReferencedColumns: []string{"id"},
					},
				},
			},
			{
				Schema: "gamma",
				Name:   "notes",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "gadget_id", DataType: "uuid", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:              "notes_gadget_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"gadget_id"},
						ReferencedSchema:  "beta",
						ReferencedTable:   "gadgets",
						ReferencedColumns: []string{"id"},
					},
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

	var allUpSQL strings.Builder

	for _, migration := range genResult.Migrations {
		require.NotNil(t, migration.UpFile)
		allUpSQL.WriteString(migration.UpFile.Content)
	}

	sql := allUpSQL.String()

	widgetsPos := createTablePos(sql, "alpha.widgets")
	gadgetsPos := createTablePos(sql, "beta.gadgets")
	trinketsPos := createTablePos(sql, "alpha.trinkets")
	notesPos := createTablePos(sql, "gamma.notes")

	require.GreaterOrEqual(t, widgetsPos, 0, "alpha.widgets must be created")
	require.GreaterOrEqual(t, gadgetsPos, 0, "beta.gadgets must be created")
	require.GreaterOrEqual(t, trinketsPos, 0, "alpha.trinkets must be created")
	require.GreaterOrEqual(t, notesPos, 0, "gamma.notes must be created")

	assert.Less(t, widgetsPos, gadgetsPos,
		"alpha.widgets must be created before beta.gadgets references it")
	assert.Less(t, gadgetsPos, trinketsPos,
		"beta.gadgets must be created before alpha.trinkets references it")
	assert.Less(t, gadgetsPos, notesPos,
		"beta.gadgets must be created before gamma.notes references it")
}

func createTablePos(sql, qualifiedName string) int {
	if pos := strings.Index(sql, "CREATE TABLE "+qualifiedName); pos >= 0 {
		return pos
	}

	return strings.Index(sql, "CREATE TABLE IF NOT EXISTS "+qualifiedName)
}
