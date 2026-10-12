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

func TestPolicyMigrationsOrderReferencedObjects(t *testing.T) {
	t.Parallel()

	db := policySchema(t, `
		CREATE TABLE documents (id integer);
		CREATE TABLE grants (document_id integer);
		CREATE FUNCTION allowed() RETURNS boolean LANGUAGE sql AS $$ SELECT true $$;
		CREATE POLICY scope ON documents USING (allowed() AND EXISTS (
			SELECT 1 FROM grants WHERE document_id = documents.id));
	`)
	up, down := generatedPolicySQL(t, &schema.Database{}, db)
	assertPolicyStatementOrder(t, up, "CREATE TABLE public.documents", `CREATE POLICY "scope"`)
	assertPolicyStatementOrder(t, up, "CREATE TABLE public.grants", `CREATE POLICY "scope"`)
	assertPolicyStatementOrder(t, up, "FUNCTION public.allowed", `CREATE POLICY "scope"`)
	assertPolicyStatementOrder(t, down, `DROP POLICY`, "DROP FUNCTION")
	assertPolicyStatementOrder(t, down, `DROP POLICY`, "DROP TABLE")

	up, down = generatedPolicySQL(t, db, &schema.Database{})
	assertPolicyStatementOrder(t, up, `DROP POLICY`, "DROP FUNCTION")
	assertPolicyStatementOrder(t, up, `DROP POLICY`, "DROP TABLE")
	assertPolicyStatementOrder(t, down, "CREATE TABLE public.documents", `CREATE POLICY "scope"`)
	assertPolicyStatementOrder(t, down, "CREATE TABLE public.grants", `CREATE POLICY "scope"`)
	assertPolicyStatementOrder(t, down, "FUNCTION public.allowed", `CREATE POLICY "scope"`)
}

func TestPolicyMigrationsRecreateForColumnTypeChanges(t *testing.T) {
	t.Parallel()

	for _, expression := range []string{
		"id > 0", "EXISTS (SELECT grants.* FROM grants WHERE document_id = documents.id)",
	} {
		t.Run(expression, func(t *testing.T) {
			t.Parallel()

			current := policySchema(t, `CREATE TABLE documents (id integer);
				CREATE TABLE grants (document_id integer);
				CREATE POLICY scope ON documents USING (`+expression+`);`)
			desired := policySchema(t, `CREATE TABLE documents (id bigint);
				CREATE TABLE grants (document_id bigint);
				CREATE POLICY scope ON documents USING (`+expression+`);`)
			up, down := generatedPolicySQL(t, current, desired)
			assertPolicyStatementOrder(t, up, "DROP POLICY", "ALTER COLUMN id TYPE")
			assertPolicyStatementOrder(t, up, "ALTER COLUMN id TYPE", "CREATE POLICY")
			assertPolicyStatementOrder(t, down, "DROP POLICY", "ALTER COLUMN id TYPE")
			assertPolicyStatementOrder(t, down, "ALTER COLUMN id TYPE", "CREATE POLICY")
		})
	}
}

func TestPolicyRollbackRestoresOldFunctionBeforeReplacingPolicy(t *testing.T) {
	t.Parallel()

	current := policySchema(t, `CREATE TABLE documents (id integer);
		CREATE FUNCTION old_scope() RETURNS boolean LANGUAGE sql AS $$ SELECT true $$;
		CREATE POLICY scope ON documents USING (old_scope());`)
	desired := policySchema(t, `CREATE TABLE documents (id integer);
		CREATE FUNCTION new_scope() RETURNS boolean LANGUAGE sql AS $$ SELECT false $$;
		CREATE POLICY scope ON documents USING (new_scope());`)
	up, down := generatedPolicySQL(t, current, desired)
	assertPolicyStatementOrder(t, up, "DROP POLICY", "DROP FUNCTION public.old_scope")
	assertPolicyStatementOrder(t, up, "FUNCTION public.new_scope", "CREATE POLICY")
	assertPolicyStatementOrder(t, down, "DROP POLICY", "DROP FUNCTION public.new_scope")
	assertPolicyStatementOrder(t, down, "FUNCTION public.old_scope", "CREATE POLICY")
}

func TestPolicyMigrationsCreateReferencedColumnBeforePolicy(t *testing.T) {
	t.Parallel()

	current := policySchema(t, `CREATE TABLE documents (id integer);`)
	desired := policySchema(t, `CREATE TABLE documents (id integer, tenant_id text);
		CREATE POLICY scope ON documents USING (tenant_id = 'a');`)
	up, down := generatedPolicySQL(t, current, desired)
	assertPolicyStatementOrder(t, up, "ADD COLUMN tenant_id", "CREATE POLICY")
	assertPolicyStatementOrder(t, down, "DROP POLICY", "DROP COLUMN")
}

func generatedPolicySQL(t *testing.T, current, desired *schema.Database) (string, string) {
	t.Helper()

	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)

	options := testOptions()
	options.Idempotent = false
	generated, err := generator.New(options).Generate(result)
	require.NoError(t, err)

	for _, warning := range generated.Warnings {
		require.True(t, strings.HasPrefix(warning, "Unsafe"), warning)
	}

	var up, down strings.Builder

	for _, migration := range generated.Migrations {
		require.NotNil(t, migration.UpFile)
		up.WriteString(migration.UpFile.Content)
	}

	for i := len(generated.Migrations) - 1; i >= 0; i-- {
		require.NotNil(t, generated.Migrations[i].DownFile)
		down.WriteString(generated.Migrations[i].DownFile.Content)
	}

	return up.String(), down.String()
}

func assertPolicyStatementOrder(t *testing.T, sql, first, second string) {
	t.Helper()

	firstIndex := strings.Index(strings.ToLower(sql), strings.ToLower(first))
	secondIndex := strings.Index(strings.ToLower(sql), strings.ToLower(second))
	require.NotEqual(t, -1, firstIndex, "%s missing from %s", first, sql)
	require.NotEqual(t, -1, secondIndex, "%s missing from %s", second, sql)
	assert.Less(t, firstIndex, secondIndex, "%s must precede %s: %s", first, second, sql)
}

func TestPolicyRecreationStaysInOneMigration(t *testing.T) {
	t.Parallel()

	current := policySchema(t, `CREATE TABLE documents (id integer);
		CREATE POLICY scope ON documents AS RESTRICTIVE USING (id > 0);`)
	desired := policySchema(t, `CREATE TABLE documents (id bigint);
		CREATE POLICY scope ON documents AS RESTRICTIVE USING (id > 0);`)
	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)

	options := testOptions()
	options.MaxOperationsPerFile = 1
	generated, err := generator.New(options).Generate(result)
	require.NoError(t, err)
	require.Len(t, generated.Migrations, 1)
	assertPolicyStatementOrder(
		t,
		generated.Migrations[0].UpFile.Content,
		"DROP POLICY",
		"ALTER COLUMN",
	)
	assertPolicyStatementOrder(
		t,
		generated.Migrations[0].UpFile.Content,
		"ALTER COLUMN",
		"CREATE POLICY",
	)
}

func TestPolicyRollbackRestoresReferencedType(t *testing.T) {
	t.Parallel()

	current := policySchema(t, `CREATE TYPE access_level AS ENUM ('OPEN');
		CREATE TABLE documents (id integer);
		CREATE POLICY scope ON documents USING ('OPEN'::access_level = 'OPEN'::access_level);`)
	desired := policySchema(t, `CREATE TABLE documents (id integer);
		CREATE POLICY scope ON documents USING (true);`)
	up, down := generatedPolicySQL(t, current, desired)
	assertPolicyStatementOrder(t, up, "DROP POLICY", "DROP TYPE")
	assertPolicyStatementOrder(t, down, "CREATE TYPE", "CREATE POLICY")
}
