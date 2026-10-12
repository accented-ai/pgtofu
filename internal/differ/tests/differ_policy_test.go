package differ_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDifferRowSecurityAndPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, current, desired string
		kinds                  []differ.ChangeType
	}{
		{
			"enable", "", `ALTER TABLE documents ENABLE ROW LEVEL SECURITY;`,
			[]differ.ChangeType{differ.ChangeTypeModifyTableRowSecurity},
		},
		{
			"force independently", "", `ALTER TABLE documents FORCE ROW LEVEL SECURITY;`,
			[]differ.ChangeType{differ.ChangeTypeModifyTableRowSecurity},
		},
		{
			"disable", `ALTER TABLE documents ENABLE ROW LEVEL SECURITY;`, "",
			[]differ.ChangeType{differ.ChangeTypeModifyTableRowSecurity},
		},
		{
			"add policy while disabled", "", `CREATE POLICY scope ON documents USING (true);`,
			[]differ.ChangeType{differ.ChangeTypeAddPolicy},
		},
		{
			"drop policy", `CREATE POLICY scope ON documents USING (true);`, "",
			[]differ.ChangeType{differ.ChangeTypeDropPolicy},
		},
		{
			"literal case", `CREATE POLICY scope ON documents USING (tenant_id = 'A');`,
			`CREATE POLICY scope ON documents USING (tenant_id = 'a');`,
			[]differ.ChangeType{differ.ChangeTypeModifyPolicy},
		},
		{
			"command", `CREATE POLICY scope ON documents FOR SELECT USING (true);`,
			`CREATE POLICY scope ON documents FOR DELETE USING (true);`,
			[]differ.ChangeType{differ.ChangeTypeModifyPolicy},
		},
		{
			"mode", `CREATE POLICY scope ON documents USING (true);`,
			`CREATE POLICY scope ON documents AS RESTRICTIVE USING (true);`,
			[]differ.ChangeType{differ.ChangeTypeModifyPolicy},
		},
		{
			"remove check", `CREATE POLICY scope ON documents USING (true) WITH CHECK (false);`,
			`CREATE POLICY scope ON documents USING (true);`,
			[]differ.ChangeType{differ.ChangeTypeModifyPolicy},
		},
		{
			"roles", `CREATE POLICY scope ON documents TO PUBLIC USING (true);`,
			`CREATE POLICY scope ON documents TO "PUBLIC" USING (true);`,
			[]differ.ChangeType{differ.ChangeTypeModifyPolicy},
		},
		{
			"role ordering", `CREATE POLICY scope ON documents TO reader, writer USING (true);`,
			`CREATE POLICY scope ON documents TO writer, reader, reader USING (true);`, nil,
		},
		{
			"catalog expression",
			`CREATE POLICY scope ON documents
			USING ((tenant_id = current_setting('app.tenant'::text, true)));`,
			`CREATE POLICY scope ON documents USING (tenant_id = current_setting('app.tenant', true));`,
			nil,
		},
		{
			"quoted policy name", `CREATE POLICY "Scope" ON documents USING (true);`,
			`CREATE POLICY scope ON documents USING (true);`,
			[]differ.ChangeType{differ.ChangeTypeAddPolicy, differ.ChangeTypeDropPolicy},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			current := policyTestSchema(t, `CREATE TABLE documents (tenant_id text);`+tt.current)
			desired := policyTestSchema(t, `CREATE TABLE documents (tenant_id text);`+tt.desired)
			result, err := differ.New(nil).Compare(current, desired)
			require.NoError(t, err)
			require.Len(t, result.Changes, len(tt.kinds))

			got := make([]differ.ChangeType, 0, len(result.Changes))
			for _, change := range result.Changes {
				got = append(got, change.Type)
				assert.Equal(t, differ.SeverityPotentiallyBreaking, change.Severity)
			}

			assert.ElementsMatch(t, tt.kinds, got)
		})
	}
}

func TestDifferPoliciesForNewAndDroppedTables(t *testing.T) {
	t.Parallel()

	db := policyTestSchema(t, `CREATE TABLE documents (id integer);
		ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
		CREATE POLICY scope ON documents USING (true);`)
	result, err := differ.New(nil).Compare(&schema.Database{}, db)
	require.NoError(t, err)
	assert.Len(t, result.GetChangesByType(differ.ChangeTypeAddPolicy), 1)
	assert.Len(t, result.GetChangesByType(differ.ChangeTypeModifyTableRowSecurity), 1)
	result, err = differ.New(nil).Compare(db, &schema.Database{})
	require.NoError(t, err)
	assert.Len(t, result.GetChangesByType(differ.ChangeTypeDropPolicy), 1)
}

func policyTestSchema(t *testing.T, sql string) *schema.Database {
	t.Helper()

	p := parser.New()
	db := &schema.Database{}
	require.NoError(t, p.ParseSQL(sql, db))
	require.Empty(t, p.GetErrors())

	return db
}
