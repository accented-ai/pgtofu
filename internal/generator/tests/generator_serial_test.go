package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestGenerator_SerialColumn_UsesSerialKeyword(t *testing.T) {
	t.Parallel()

	sourceSQL := `
CREATE TABLE steps (
    id SERIAL PRIMARY KEY
);
`

	p := parser.New()
	result, err := p.ParseDirectory("")
	require.NoError(t, err)

	db := result.Database
	err = p.ParseSQL(sourceSQL, db)
	require.NoError(t, err, "Failed to parse source SQL")

	current := &schema.Database{}

	d := differ.New(nil)
	diffResult, err := d.Compare(current, db)
	require.NoError(t, err, "Failed to compare schemas")

	gen := generator.New(testOptions())
	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err, "Failed to generate migration")

	upSQL := genResult.Migrations[0].UpFile.Content
	t.Logf("Generated UP SQL:\n%s", upSQL)

	require.Contains(t, upSQL, "id SERIAL",
		"Generated SQL should use SERIAL keyword")

	createTableStart := strings.Index(upSQL, "CREATE TABLE")

	createTableEnd := strings.Index(upSQL, ");")
	if createTableStart >= 0 && createTableEnd > createTableStart {
		createTableSQL := upSQL[createTableStart:createTableEnd]
		require.NotContains(t, createTableSQL, "nextval(",
			"Generated CREATE TABLE should not contain nextval() for SERIAL columns")
	}
}

func TestGenerator_SerialColumn_AllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		columnDef    string
		expectedType string
	}{
		{
			name:         "SERIAL",
			columnDef:    "id SERIAL PRIMARY KEY",
			expectedType: "SERIAL",
		},
		{
			name:         "BIGSERIAL",
			columnDef:    "id BIGSERIAL PRIMARY KEY",
			expectedType: "BIGSERIAL",
		},
		{
			name:         "SMALLSERIAL",
			columnDef:    "id SMALLSERIAL PRIMARY KEY",
			expectedType: "SMALLSERIAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sourceSQL := `CREATE TABLE sample (` + tt.columnDef + `);`

			p := parser.New()
			result, err := p.ParseDirectory("")
			require.NoError(t, err)

			db := result.Database
			err = p.ParseSQL(sourceSQL, db)
			require.NoError(t, err)

			current := &schema.Database{}
			d := differ.New(nil)
			diffResult, err := d.Compare(current, db)
			require.NoError(t, err)

			gen := generator.New(testOptions())
			genResult, err := gen.Generate(diffResult)
			require.NoError(t, err)

			upSQL := genResult.Migrations[0].UpFile.Content
			t.Logf("Generated SQL:\n%s", upSQL)

			require.Contains(t, upSQL, tt.expectedType,
				"Generated SQL should use %s keyword", tt.expectedType)

			require.NotContains(t, strings.Split(upSQL, ");")[0], "nextval(",
				"CREATE TABLE should not contain nextval() for %s columns", tt.expectedType)
		})
	}
}
