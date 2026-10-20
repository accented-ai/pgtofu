package generator_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestGeneratedColumnsPreserveComputationAcrossCreationAndRollback(t *testing.T) {
	t.Parallel()

	for _, storage := range []string{"STORED", "VIRTUAL"} {
		t.Run(storage, func(t *testing.T) {
			t.Parallel()

			base := foreignKeySchema(t, `CREATE TABLE catalog.items (id integer);`)
			populated := foreignKeySchema(t, fmt.Sprintf(`CREATE TABLE catalog.items (
				id integer, z_input text,
				a_derived boolean GENERATED ALWAYS AS (z_input IS NOT NULL AND length(z_input) > 0) %s NOT NULL
			);`, storage))
			column := populated.Tables[0].GetColumn("a_derived")
			require.NotNil(t, column)
			require.True(t, column.IsGenerated)
			require.False(t, column.IsNullable)
			require.Equal(t, storage, column.GeneratedStorage())

			definition := "a_derived BOOLEAN GENERATED ALWAYS AS " +
				"(z_input IS NOT NULL AND length(z_input) > 0) " + storage + " NOT NULL"

			for _, current := range []*schema.Database{base, {}} {
				up, down := foreignKeyMigrationSQL(t, current, populated, 1)
				require.Contains(t, up, definition)

				if len(current.Tables) != 0 {
					requireSQLBefore(t, up, "ADD COLUMN z_input", "ADD COLUMN a_derived")
					requireSQLBefore(
						t,
						down,
						"DROP COLUMN IF EXISTS a_derived",
						"DROP COLUMN IF EXISTS z_input",
					)
				}

				_, restore := foreignKeyMigrationSQL(t, populated, current, 1)
				require.Contains(t, restore, definition)
			}
		})
	}
}

func TestGeneratedColumnExpressionDoesNotBecomeAColumnConstraint(t *testing.T) {
	t.Parallel()

	db := foreignKeySchema(t, `CREATE TABLE catalog.items (
		input text, present boolean GENERATED ALWAYS AS (input IS NOT NULL) STORED,
		label text GENERATED ALWAYS AS (coalesce(input, 'UNIQUE DEFAULT CHECK')) VIRTUAL
	);`)
	for _, name := range []string{"present", "label"} {
		column := db.Tables[0].GetColumn(name)
		require.NotNil(t, column)
		require.True(t, column.IsGenerated)
		require.True(t, column.IsNullable)
		require.Empty(t, column.Default)
	}

	require.Empty(t, db.Tables[0].Constraints)
}

func TestGeneratedColumnDriftCannotReportNoChanges(t *testing.T) {
	t.Parallel()

	definitions := []string{
		`label text`,
		`label text GENERATED ALWAYS AS ('Before') STORED`,
		`label text GENERATED ALWAYS AS ('After') VIRTUAL`,
	}

	desired := foreignKeySchema(t, `CREATE TABLE catalog.items (
		label text GENERATED ALWAYS AS ('After') STORED
	);`)
	for _, definition := range definitions {
		t.Run(definition, func(t *testing.T) {
			t.Parallel()

			current := foreignKeySchema(t, "CREATE TABLE catalog.items ("+definition+");")
			_, err := differ.New(nil).Compare(current, desired)
			require.ErrorContains(
				t,
				err,
				"generated column definition differs for catalog.items.label",
			)
		})
	}

	current := foreignKeySchema(t, `CREATE TABLE catalog.items (
		label text GENERATED ALWAYS AS ((('After'::text))) STORED
	);`)
	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)
	require.False(t, result.HasChanges())
	current = foreignKeySchema(t, `CREATE TABLE catalog.items (
		label text GENERATED ALWAYS AS ('After'::varchar(2)) STORED
	);`)
	_, err = differ.New(nil).Compare(current, desired)
	require.ErrorContains(t, err, "generated column definition differs")

	result, err = differ.New(nil).Compare(&schema.Database{}, desired)
	require.NoError(t, err)

	ddl := generator.NewDDLBuilder(result, false)
	statement, err := ddl.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)
	require.Contains(t, statement.SQL, "GENERATED ALWAYS AS")
}

func TestGeneratedColumnWaitsForItsFunctionAndReleasesItOnDrop(t *testing.T) {
	t.Parallel()

	base := foreignKeySchema(t, `CREATE TABLE catalog.items (id integer);`)
	desired := foreignKeySchema(t, `
		CREATE FUNCTION catalog.nonempty(value text) RETURNS boolean
		LANGUAGE sql IMMUTABLE AS $$ SELECT length(value) > 0 $$;
		CREATE TABLE catalog.items (
			id integer, z_input text,
			a_derived boolean GENERATED ALWAYS AS (catalog.nonempty(z_input)) STORED
		);`)
	up, down := foreignKeyMigrationSQL(t, base, desired, 1)
	requireSQLBefore(
		t,
		strings.ToLower(up),
		"create or replace function catalog.nonempty",
		"add column a_derived",
	)
	requireSQLBefore(
		t,
		strings.ToLower(down),
		"drop column if exists a_derived",
		"drop function if exists",
	)

	up, down = foreignKeyMigrationSQL(t, desired, base, 1)
	requireSQLBefore(
		t,
		strings.ToLower(up),
		"drop column if exists a_derived",
		"drop function if exists",
	)
	requireSQLBefore(
		t,
		strings.ToLower(down),
		"create or replace function catalog.nonempty",
		"add column a_derived",
	)
}
