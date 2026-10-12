package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wasmquery "github.com/wasilibs/go-pgquery"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestGenerateRowSecurityDDL(t *testing.T) {
	t.Parallel()

	current := policySchema(t, `CREATE TABLE documents (id integer);`)
	desired := policySchema(t, `CREATE TABLE documents (id integer);
		ALTER TABLE documents ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY;`)
	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	ddl := generator.NewDDLBuilder(result, true)
	up, err := ddl.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, up.SQL, `ALTER TABLE "public"."documents" ENABLE ROW LEVEL SECURITY;`)
	assert.Contains(t, up.SQL, `ALTER TABLE "public"."documents" FORCE ROW LEVEL SECURITY;`)

	down, err := ddl.BuildDownStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, down.SQL, `DISABLE ROW LEVEL SECURITY;`)
	assert.Contains(t, down.SQL, `NO FORCE ROW LEVEL SECURITY;`)
	assert.True(t, up.RequiresTx)
	assert.True(t, down.RequiresTx)
}

func TestGeneratePolicyDDL(t *testing.T) {
	t.Parallel()

	for _, idempotent := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "idempotent"}[idempotent], func(t *testing.T) {
			t.Parallel()

			current := policySchema(t, `CREATE TABLE documents (tenant_id text);`)
			desired := policySchema(t, `CREATE TABLE documents (tenant_id text);
				CREATE POLICY "Tenant""Scope" ON documents AS RESTRICTIVE FOR UPDATE TO "PUBLIC", PUBLIC
				USING (tenant_id = '$pgtofu_policy$') WITH CHECK (tenant_id = 'O''Reilly');`)
			result, err := differ.New(nil).Compare(current, desired)
			require.NoError(t, err)
			require.Len(t, result.Changes, 1)
			ddl := generator.NewDDLBuilder(result, idempotent)
			up, err := ddl.BuildUpStatement(result.Changes[0])
			require.NoError(t, err)
			assert.Contains(t, up.SQL, `CREATE POLICY "Tenant""Scope" ON "public"."documents"`)
			assert.Contains(t, up.SQL, `AS RESTRICTIVE FOR UPDATE TO "PUBLIC", PUBLIC`)
			assert.Contains(t, up.SQL, `USING (tenant_id = '$pgtofu_policy$')`)
			assert.Contains(t, up.SQL, `WITH CHECK (tenant_id = 'O''Reilly')`)
			_, err = wasmquery.Parse(up.SQL)
			require.NoError(t, err)

			if idempotent {
				assert.Contains(t, up.SQL, "IF NOT EXISTS")
				assert.Contains(t, up.SQL, "DO $pgtofu_policy_$")
				assert.Contains(t, up.SQL, `p.polname = 'Tenant"Scope'`)
			} else {
				assert.NotContains(t, up.SQL, "IF NOT EXISTS")
			}

			down, err := ddl.BuildDownStatement(result.Changes[0])
			require.NoError(t, err)
			assert.Contains(t, down.SQL, `"Tenant""Scope" ON "public"."documents";`)
			assert.NotContains(t, down.SQL, "CASCADE")
		})
	}
}

func TestGeneratePolicyReplacementRestoresClearedClauses(t *testing.T) {
	t.Parallel()

	current := policySchema(t, `CREATE TABLE documents (tenant_id text);
		CREATE POLICY scope ON documents AS RESTRICTIVE FOR UPDATE TO writer
		USING (tenant_id = 'a') WITH CHECK (tenant_id = 'b');`)
	desired := policySchema(t, `CREATE TABLE documents (tenant_id text);
		CREATE POLICY scope ON documents FOR SELECT USING (true);`)
	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	ddl := generator.NewDDLBuilder(result, false)
	up, err := ddl.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, up.SQL, `DROP POLICY "scope" ON "public"."documents";`)
	assert.Contains(t, up.SQL, `AS PERMISSIVE FOR SELECT TO PUBLIC`)
	assert.NotContains(t, up.SQL, "WITH CHECK")

	down, err := ddl.BuildDownStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, down.SQL, `AS RESTRICTIVE FOR UPDATE TO "writer"`)
	assert.Contains(t, down.SQL, `USING (tenant_id = 'a')`)
	assert.Contains(t, down.SQL, `WITH CHECK (tenant_id = 'b')`)
}

func TestGenerateDroppedTableRestoresRowSecurity(t *testing.T) {
	t.Parallel()

	current := policySchema(t, `CREATE TABLE documents (id integer);
		ALTER TABLE documents FORCE ROW LEVEL SECURITY;`)
	result, err := differ.New(nil).Compare(current, &schema.Database{})
	require.NoError(t, err)

	changes := result.GetChangesByType(differ.ChangeTypeDropTable)
	require.Len(t, changes, 1)

	ddl := generator.NewDDLBuilder(result, false)
	down, err := ddl.BuildDownStatement(changes[0])
	require.NoError(t, err)
	assert.Contains(t, down.SQL, `FORCE ROW LEVEL SECURITY;`)
	assert.NotContains(t, down.SQL, `ENABLE ROW LEVEL SECURITY;`)
}

func policySchema(t *testing.T, sql string) *schema.Database {
	t.Helper()

	p := parser.New()
	db := &schema.Database{}
	require.NoError(t, p.ParseSQL(sql, db))
	require.Empty(t, p.GetErrors())

	return db
}
