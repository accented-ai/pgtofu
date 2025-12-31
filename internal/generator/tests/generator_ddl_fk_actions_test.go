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

func TestGenerator_ForeignKeyActions_InCreateTable(t *testing.T) {
	t.Parallel()

	sourceSQL := `
CREATE TABLE users (
	id UUID NOT NULL PRIMARY KEY
);

CREATE TABLE posts (
	id UUID NOT NULL PRIMARY KEY,
	user_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE
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

	var allUpSQL strings.Builder
	for _, mig := range genResult.Migrations {
		allUpSQL.WriteString(mig.UpFile.Content)
		t.Logf("Migration %d SQL:\n%s\n", mig.Version, mig.UpFile.Content)
	}

	require.Contains(t, allUpSQL.String(), "ON DELETE CASCADE",
		"Generated UP SQL should contain ON DELETE CASCADE")

	require.Contains(t, allUpSQL.String(), "REFERENCES public.users",
		"Generated UP SQL should reference users")
}

func TestGenerator_ForeignKeyActions_MultipleActions(t *testing.T) {
	t.Parallel()

	sourceSQL := `
CREATE TABLE users (
	id UUID NOT NULL PRIMARY KEY
);

CREATE TABLE categories (
	id TEXT NOT NULL PRIMARY KEY
);

CREATE TABLE posts (
	id UUID NOT NULL PRIMARY KEY,
	user_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
	category_id TEXT NOT NULL REFERENCES categories (id),
	author_id UUID REFERENCES users (id) ON DELETE SET NULL
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

	var allUpSQL strings.Builder
	for _, mig := range genResult.Migrations {
		allUpSQL.WriteString(mig.UpFile.Content)
		t.Logf("Migration %d SQL:\n%s\n", mig.Version, mig.UpFile.Content)
	}

	upSQL := allUpSQL.String()

	require.Contains(t, upSQL, "ON DELETE CASCADE",
		"Generated UP SQL should contain ON DELETE CASCADE")
	require.Contains(t, upSQL, "ON DELETE SET NULL",
		"Generated UP SQL should contain ON DELETE SET NULL")

	require.Contains(t, upSQL, "REFERENCES public.users",
		"Generated UP SQL should reference users")
	require.Contains(t, upSQL, "REFERENCES public.categories",
		"Generated UP SQL should reference categories")
}

func TestGenerator_ForeignKeyActions_AlterTableAddConstraint(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false},
				},
				Constraints: []schema.Constraint{
					{Name: "users_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
				},
			},
			{
				Schema: "public",
				Name:   "posts",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false},
					{Name: "user_id", DataType: "uuid", IsNullable: false},
				},
				Constraints: []schema.Constraint{
					{Name: "posts_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false},
				},
				Constraints: []schema.Constraint{
					{Name: "users_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
				},
			},
			{
				Schema: "public",
				Name:   "posts",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false},
					{Name: "user_id", DataType: "uuid", IsNullable: false},
				},
				Constraints: []schema.Constraint{
					{Name: "posts_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
					{
						Name:              "posts_user_id_fkey",
						Type:              "FOREIGN KEY",
						Columns:           []string{"user_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
						OnDelete:          "CASCADE",
					},
				},
			},
		},
	}

	d := differ.New(nil)
	result, err := d.Compare(current, desired)
	require.NoError(t, err, "Failed to compare schemas")

	gen := generator.New(testOptions())
	genResult, err := gen.Generate(result)
	require.NoError(t, err, "Failed to generate migration")

	upSQL := genResult.Migrations[0].UpFile.Content
	t.Logf("Generated UP SQL:\n%s", upSQL)

	require.Contains(t, upSQL, "ALTER TABLE",
		"Generated UP SQL should contain ALTER TABLE")
	require.Contains(t, upSQL, "ADD CONSTRAINT",
		"Generated UP SQL should contain ADD CONSTRAINT")
	require.Contains(t, upSQL, "ON DELETE CASCADE",
		"Generated UP SQL should contain ON DELETE CASCADE")
}
