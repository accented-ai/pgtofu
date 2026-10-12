package generator_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

//nolint:paralleltest // Exercises type and policy migrations in a dedicated schema.
func TestPostgresPolicyTypeRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	conn, e := policyIntegrationDatabase(t, ctx, "policy_type")
	initial := policySchema(t, `CREATE TYPE policy_type.access_level AS ENUM ('OPEN');
		CREATE TABLE policy_type.documents (id integer);
		CREATE POLICY scope ON policy_type.documents
			USING ('OPEN'::policy_type.access_level = 'OPEN'::policy_type.access_level);`)
	desired := policySchema(t, `CREATE TABLE policy_type.documents (id integer);
		CREATE POLICY scope ON policy_type.documents USING (true);`)
	applyPolicyMigration(t, ctx, conn, policyMigration(t, &schema.Database{}, initial), false)
	migration := policyMigration(t, initial, desired)
	require.Len(t, migration.Migrations, 1)

	_, err := conn.Exec(ctx, `CREATE POLICY external ON policy_type.documents
		USING ('OPEN'::policy_type.access_level = 'OPEN'::policy_type.access_level);`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, migration.Migrations[0].UpFile.Content)
	require.Error(t, err)

	var pgError *pgconn.PgError
	require.ErrorAs(t, err, &pgError)
	assert.Equal(t, "2BP01", pgError.Code)

	_, err = conn.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)

	var count int
	require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_policy
		WHERE polname = 'external' AND polrelid = 'policy_type.documents'::regclass`).Scan(&count))
	assert.Equal(t, 1, count)

	_, err = conn.Exec(ctx, "DROP POLICY external ON policy_type.documents")
	require.NoError(t, err)

	for _, down := range []bool{false, true} {
		applyPolicyMigration(t, ctx, conn, migration, down)
		db, err := e.Extract(ctx)
		require.NoError(t, err)

		table := db.GetTable("policy_type", "documents")
		require.NotNil(t, table)
		actual := &schema.Database{
			Tables: []schema.Table{*table},
			CustomTypes: slices.DeleteFunc(db.CustomTypes, func(customType schema.CustomType) bool {
				return customType.Schema != "policy_type"
			}),
		}

		expected := desired
		if down {
			expected = initial
		}

		result, err := differ.New(nil).Compare(actual, expected)
		require.NoError(t, err)
		assert.Empty(t, result.Changes, "%#v", result.Changes)
	}
}
