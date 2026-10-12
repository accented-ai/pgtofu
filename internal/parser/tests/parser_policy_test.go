package parser_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestParsePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, sql string
		want      schema.Policy
	}{
		{"defaults", `CREATE POLICY tenant ON documents;`, schema.Policy{
			Name: "tenant", Command: "ALL", Permissive: true,
			Roles: []schema.PolicyRole{{Public: true}},
		}},
		{
			"quoted names and roles",
			`CREATE POLICY "Tenant""Scope" ON public.documents
			AS RESTRICTIVE FOR UPDATE TO "App Role", "PUBLIC", PUBLIC
			USING (tenant_id = current_setting('app.tenant', true))
			WITH CHECK (tenant_id = current_setting('app.tenant', true));`,
			schema.Policy{
				Name:       `Tenant"Scope`,
				Command:    "UPDATE",
				Permissive: false,
				Roles: []schema.PolicyRole{
					{Name: "App Role"},
					{Name: "PUBLIC"},
					{Public: true},
				},
				UsingExpression: "tenant_id = current_setting('app.tenant', true)",
				CheckExpression: "tenant_id = current_setting('app.tenant', true)",
			},
		},
		{
			"subquery and literal",
			`CREATE POLICY shared ON documents FOR SELECT
			USING (EXISTS (SELECT 1 FROM grants WHERE grants.tenant_id = documents.tenant_id
			AND label = 'WITH CHECK; USING'));`,
			schema.Policy{
				Name:       "shared",
				Command:    "SELECT",
				Permissive: true,
				Roles:      []schema.PolicyRole{{Public: true}},
				UsingExpression: "EXISTS (SELECT 1 FROM grants WHERE grants.tenant_id = documents.tenant_id" +
					" AND label = 'WITH CHECK; USING')",
			},
		},
		{"insert", `CREATE POLICY insert_tenant ON documents FOR INSERT TO writer
			WITH CHECK (tenant_id = 'a');`, schema.Policy{
			Name: "insert_tenant", Command: "INSERT", Permissive: true,
			Roles: []schema.PolicyRole{{Name: "writer"}}, CheckExpression: "tenant_id = 'a'",
		}},
		{
			"delete",
			`CREATE POLICY delete_tenant ON documents FOR DELETE USING (true);`,
			schema.Policy{
				Name: "delete_tenant", Command: "DELETE", Permissive: true,
				Roles: []schema.PolicyRole{{Public: true}}, UsingExpression: "true",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := parser.New()
			db := &schema.Database{}
			require.NoError(
				t,
				p.ParseSQL(`CREATE TABLE documents (tenant_id text, label text);`+tt.sql, db),
			)
			require.Empty(t, p.GetErrors())
			require.Empty(t, p.GetWarnings())
			require.Len(t, db.Tables[0].Policies, 1)
			assert.Equal(t, tt.want, db.Tables[0].Policies[0])
		})
	}
}

func TestParsePolicyRejectsInvalidDefinitions(t *testing.T) {
	t.Parallel()

	for _, sql := range []string{
		`CREATE POLICY p ON missing USING (true);`,
		`CREATE POLICY p ON documents FOR INSERT USING (true);`,
		`CREATE POLICY p ON documents FOR SELECT WITH CHECK (true);`,
		`CREATE POLICY p ON documents FOR DELETE WITH CHECK (true);`,
		`CREATE POLICY p ON documents TO CURRENT_USER USING (true);`,
		`CREATE POLICY p ON documents USING ();`,
	} {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()

			p := parser.New()
			db := &schema.Database{}
			require.NoError(t, p.ParseSQL(`CREATE TABLE documents (id integer);`+sql, db))
			require.Len(t, p.GetErrors(), 1)
			assert.Empty(t, db.Tables[0].Policies)
		})
	}
}

func TestParsePolicyNamesAreTableLocal(t *testing.T) {
	t.Parallel()

	p := parser.New()
	db := &schema.Database{}
	require.NoError(t, p.ParseSQL(`
		CREATE TABLE first (id integer);
		CREATE TABLE second (id integer);
		CREATE POLICY tenant ON first USING (true);
		CREATE POLICY tenant ON second USING (false);
		CREATE POLICY "Tenant" ON first USING (false);
		COMMENT ON POLICY tenant ON first IS 'first';
		COMMENT ON POLICY tenant ON second IS 'second';
		COMMENT ON POLICY "Tenant" ON first IS 'quoted';
		CREATE POLICY tenant ON first USING (false);
	`, db))
	require.Len(t, p.GetErrors(), 1)
	assert.Contains(t, p.GetErrors()[0].Message, "duplicate policy")
	assert.Len(t, db.Tables[0].Policies, 2)
	assert.Len(t, db.Tables[1].Policies, 1)
	assert.Equal(t, "first", db.Tables[0].Policies[0].Comment)
	assert.Equal(t, "quoted", db.Tables[0].Policies[1].Comment)
	assert.Equal(t, "second", db.Tables[1].Policies[0].Comment)
}

func TestParsePolicyDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	for _, subdir := range []string{"tables", "policies"} {
		require.NoError(t, os.Mkdir(filepath.Join(dir, subdir), 0o700))
	}

	require.NoError(t, os.WriteFile(filepath.Join(dir, "tables", "documents.sql"),
		[]byte(`CREATE TABLE documents (id integer);`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "policies", "tenant.sql"),
		[]byte(`ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
			CREATE POLICY tenant ON documents USING (true);`), 0o600))

	p := parser.New()
	result, err := p.ParseDirectory(dir)
	require.NoError(t, err)
	require.Empty(t, result.Errors)
	require.Empty(t, result.Warnings)
	assert.True(t, result.Database.Tables[0].RowSecurityEnabled)
	require.Len(t, result.Database.Tables[0].Policies, 1)
	assert.Equal(t, "tenant", result.Database.Tables[0].Policies[0].Name)
}

func TestParsePolicyTruncatesNames(t *testing.T) {
	t.Parallel()

	p := parser.New()
	db := &schema.Database{}
	require.NoError(t, p.ParseSQL(`CREATE TABLE documents (id integer);
		CREATE POLICY `+strings.Repeat("p", 80)+` ON documents USING (true);`, db))
	require.Empty(t, p.GetErrors())
	require.Len(t, db.Tables[0].Policies, 1)
	assert.Equal(t, strings.Repeat("p", 63), db.Tables[0].Policies[0].Name)
}
