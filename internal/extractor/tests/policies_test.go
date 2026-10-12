package extractor_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/extractor"
	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/pkg/database"
)

//nolint:paralleltest // Uses a dedicated mutable database schema and role.
func TestExtractRowSecurityPolicies(t *testing.T) {
	databaseURL := os.Getenv("PGTOFU_TEST_DATABASE_URL")
	if testing.Short() || databaseURL == "" {
		t.Skip("requires PGTOFU_TEST_DATABASE_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	pool, err := database.NewPoolFromURL(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	conn, err := pgx.Connect(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		_, cleanupErr := conn.Exec(cleanupCtx, `DROP SCHEMA IF EXISTS policy_extract CASCADE;
			DROP ROLE IF EXISTS policy_extract_reader;`)
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, `
		CREATE SCHEMA policy_extract;
		CREATE ROLE policy_extract_reader;
		CREATE TABLE policy_extract.documents (tenant_id text);
		ALTER TABLE policy_extract.documents ENABLE ROW LEVEL SECURITY;
		ALTER TABLE policy_extract.documents FORCE ROW LEVEL SECURITY;
		CREATE POLICY scope ON policy_extract.documents TO PUBLIC
			USING (tenant_id = current_setting('app.tenant', true));
		CREATE POLICY writer ON policy_extract.documents AS RESTRICTIVE FOR INSERT
			TO policy_extract_reader WITH CHECK (tenant_id <> 'blocked');
		CREATE TABLE policy_extract.disabled (id integer);
		ALTER TABLE policy_extract.disabled FORCE ROW LEVEL SECURITY;
		CREATE POLICY "MixedCase" ON policy_extract.disabled FOR SELECT USING (true);
	`)
	require.NoError(t, err)
	e, err := extractor.New(ctx, pool, extractor.Options{})
	require.NoError(t, err)
	db, err := e.Extract(ctx)
	require.NoError(t, err)

	table := db.GetTable("policy_extract", "documents")
	require.NotNil(t, table)
	assert.True(t, table.RowSecurityEnabled)
	assert.True(t, table.RowSecurityForced)
	require.Len(t, table.Policies, 2)
	assert.Equal(t, "scope", table.Policies[0].Name)
	assert.Equal(t, "ALL", table.Policies[0].Command)
	assert.True(t, table.Policies[0].Permissive)
	assert.Equal(t, []schema.PolicyRole{{Public: true}}, table.Policies[0].Roles)
	assert.Contains(
		t,
		table.Policies[0].UsingExpression,
		"current_setting('app.tenant'::text, true)",
	)
	assert.Empty(t, table.Policies[0].CheckExpression)
	assert.Equal(t, "INSERT", table.Policies[1].Command)
	assert.False(t, table.Policies[1].Permissive)
	assert.Equal(t, []schema.PolicyRole{{Name: "policy_extract_reader"}}, table.Policies[1].Roles)
	assert.Empty(t, table.Policies[1].UsingExpression)
	assert.NotEmpty(t, table.Policies[1].CheckExpression)

	disabled := db.GetTable("policy_extract", "disabled")
	require.NotNil(t, disabled)
	assert.False(t, disabled.RowSecurityEnabled)
	assert.True(t, disabled.RowSecurityForced)
	require.Len(t, disabled.Policies, 1)
	assert.Equal(t, "MixedCase", disabled.Policies[0].Name)
}
