package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDesiredSchemaReadsPoliciesAfterTables(t *testing.T) {
	t.Parallel()

	for _, policyDirectory := range []string{"policies", filepath.Join("policies", "nested")} {
		t.Run(policyDirectory, func(t *testing.T) {
			t.Parallel()

			directory := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(directory, "tables"), 0o700))
			require.NoError(t, os.MkdirAll(filepath.Join(directory, policyDirectory), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(directory, "tables", "documents.sql"),
				[]byte(`CREATE TABLE documents (organization_id text);`), 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(directory, policyDirectory, "scope.sql"),
				[]byte(`ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
					CREATE POLICY scope ON documents USING (organization_id = current_setting('app.organization', true));`),
				0o600))
			db, err := loadDesiredSchema(directory)
			require.NoError(t, err)
			require.Len(t, db.Tables, 1)
			assert.True(t, db.Tables[0].RowSecurityEnabled)
			require.Len(t, db.Tables[0].Policies, 1)
			assert.Equal(t, "scope", db.Tables[0].Policies[0].Name)
		})
	}
}
