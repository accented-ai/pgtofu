package generator_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/extractor"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/pkg/database"
)

//nolint:paralleltest // Exercises migrations in a dedicated mutable schema.
func TestPostgresPolicyRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	conn, e := policyIntegrationDatabase(t, ctx, "policy_roundtrip")
	initialSQL := postgresPolicySchema("policy_roundtrip")
	initial := policySchema(t, initialSQL)
	empty := &schema.Database{}
	created := policyMigration(t, empty, initial)
	applyPolicyMigration(t, ctx, conn, created, false)
	assertPolicyConvergence(t, ctx, e, "policy_roundtrip", initial)

	variants := []string{
		strings.ReplaceAll(initialSQL, "Scope boundary", "Scope''s boundary"),
		strings.ReplaceAll(initialSQL, "id integer", "id bigint"),
		strings.ReplaceAll(initialSQL, "current_organization", "current_tenant"),
		strings.ReplaceAll(initialSQL, "FOR ALL TO", "AS RESTRICTIVE FOR UPDATE TO"),
		strings.ReplaceAll(initialSQL, "TO policy_roundtrip_service", "TO PUBLIC"),
		strings.ReplaceAll(initialSQL, "FORCE ROW LEVEL SECURITY", "NO FORCE ROW LEVEL SECURITY"),
		strings.ReplaceAll(initialSQL, "ENABLE ROW LEVEL SECURITY", "DISABLE ROW LEVEL SECURITY"),
		strings.ReplaceAll(
			initialSQL,
			"WITH CHECK (organization_id = policy_roundtrip.current_organization())",
			"",
		),
	}
	for i, sql := range variants {
		t.Run(fmt.Sprintf("change_%d", i), func(t *testing.T) {
			desired := policySchema(t, sql)
			migration := policyMigration(t, initial, desired)
			applyPolicyMigration(t, ctx, conn, migration, false)
			assertPolicyConvergence(t, ctx, e, "policy_roundtrip", desired)
			applyPolicyMigration(t, ctx, conn, migration, true)
			assertPolicyConvergence(t, ctx, e, "policy_roundtrip", initial)
		})
	}

	extraSQL := initialSQL + `CREATE POLICY added ON policy_roundtrip.documents FOR DELETE
		TO policy_roundtrip_service USING (organization_id = policy_roundtrip.current_organization());`
	extra := policySchema(t, extraSQL)
	addition := policyMigration(t, initial, extra)
	applyPolicyMigration(t, ctx, conn, addition, false)
	applyPolicyMigration(t, ctx, conn, addition, false)
	assertPolicyConvergence(t, ctx, e, "policy_roundtrip", extra)
	applyPolicyMigration(t, ctx, conn, addition, true)
	applyPolicyMigration(t, ctx, conn, addition, true)
	assertPolicyConvergence(t, ctx, e, "policy_roundtrip", initial)

	removed := policyMigration(t, initial, empty)
	applyPolicyMigration(t, ctx, conn, removed, false)
	assertPolicyConvergence(t, ctx, e, "policy_roundtrip", empty)
	applyPolicyMigration(t, ctx, conn, removed, true)
	assertPolicyConvergence(t, ctx, e, "policy_roundtrip", initial)
	applyPolicyMigration(t, ctx, conn, created, true)
	assertPolicyConvergence(t, ctx, e, "policy_roundtrip", empty)
}

//nolint:paralleltest // Exercises database roles and transaction-local scope.
func TestPostgresGeneratedPoliciesEnforceIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	conn, e := policyIntegrationDatabase(t, ctx, "policy_isolation")
	sql := postgresPolicySchema("policy_isolation")
	desired := policySchema(t, sql)
	applyPolicyMigration(t, ctx, conn, policyMigration(t, &schema.Database{}, desired), false)
	_, err := conn.Exec(ctx, `
		INSERT INTO policy_isolation.documents VALUES (1, 'A', 'OPEN'), (2, 'B', 'CLOSED');
		INSERT INTO policy_isolation.grants VALUES ('A'), ('B');
		GRANT USAGE ON SCHEMA policy_isolation TO policy_isolation_service, policy_isolation_owner;
		GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA policy_isolation TO policy_isolation_service;
		ALTER TABLE policy_isolation.documents OWNER TO policy_isolation_owner;
		SET ROLE policy_isolation_service;
	`)
	require.NoError(t, err)

	for _, organization := range []string{"A", "B", ""} {
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)

		if organization != "" {
			_, err = tx.Exec(ctx, "SELECT set_config('app.organization', $1, true)", organization)
			require.NoError(t, err)
		}

		rows, err := tx.Query(ctx, "SELECT organization_id FROM policy_isolation.documents")
		require.NoError(t, err)
		organizations, err := pgx.CollectRows(rows, pgx.RowTo[string])
		require.NoError(t, err)

		if organization == "" {
			assert.Empty(t, organizations)
		} else {
			assert.Equal(t, []string{organization}, organizations)
		}

		updated, updateErr := tx.Exec(ctx, "UPDATE policy_isolation.documents SET state = 'OPEN'")
		require.NoError(t, updateErr)

		if organization == "" {
			assert.Zero(t, updated.RowsAffected())
		} else {
			assert.Equal(t, int64(1), updated.RowsAffected())

			_, err = tx.Exec(
				ctx,
				"INSERT INTO policy_isolation.documents VALUES (3, $1, 'OPEN')",
				organization,
			)
			require.NoError(t, err)
		}

		_, err = tx.Exec(ctx, "INSERT INTO policy_isolation.documents VALUES (3, 'OTHER', 'OPEN')")
		require.Error(t, err)

		var pgError *pgconn.PgError
		require.ErrorAs(t, err, &pgError)
		assert.Equal(t, "42501", pgError.Code)
		require.NoError(t, tx.Rollback(ctx))
	}

	_, err = conn.Exec(ctx, `RESET ROLE; SET ROLE policy_isolation_owner;`)
	require.NoError(t, err)

	var count int
	require.NoError(
		t,
		conn.QueryRow(ctx, "SELECT count(*) FROM policy_isolation.documents").Scan(&count),
	)
	assert.Zero(t, count)

	_, err = conn.Exec(ctx, `RESET ROLE;`)
	require.NoError(t, err)
	noForce := policySchema(
		t,
		strings.ReplaceAll(sql, "FORCE ROW LEVEL SECURITY", "NO FORCE ROW LEVEL SECURITY"),
	)
	applyPolicyMigration(t, ctx, conn, policyMigration(t, desired, noForce), false)
	_, err = conn.Exec(ctx, `SET ROLE policy_isolation_owner;`)
	require.NoError(t, err)
	require.NoError(
		t,
		conn.QueryRow(ctx, "SELECT count(*) FROM policy_isolation.documents").Scan(&count),
	)
	assert.Equal(t, 2, count)

	_, err = conn.Exec(ctx, `RESET ROLE;`)
	require.NoError(t, err)
	assertPolicyConvergence(t, ctx, e, "policy_isolation", noForce)
}

func policyIntegrationDatabase(
	t *testing.T,
	ctx context.Context,
	name string,
) (*pgx.Conn, *extractor.Extractor) {
	t.Helper()

	url := os.Getenv("PGTOFU_TEST_DATABASE_URL")
	if testing.Short() || url == "" {
		t.Skip("requires PGTOFU_TEST_DATABASE_URL")
	}

	conn, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cleanupCancel()

		_, cleanupErr := conn.Exec(
			cleanupCtx,
			fmt.Sprintf(
				`ROLLBACK; RESET ROLE; DROP SCHEMA IF EXISTS %s CASCADE;
			DROP ROLE IF EXISTS %s; DROP ROLE IF EXISTS %s;`,
				pgx.Identifier{name}.Sanitize(),
				pgx.Identifier{
					name + "_service",
				}.Sanitize(),
				pgx.Identifier{name + "_owner"}.Sanitize(),
			),
		)
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s;
		CREATE ROLE %s NOSUPERUSER NOBYPASSRLS;
		CREATE ROLE %s NOSUPERUSER NOBYPASSRLS;`, pgx.Identifier{name}.Sanitize(),
		pgx.Identifier{name + "_service"}.Sanitize(), pgx.Identifier{name + "_owner"}.Sanitize()))
	require.NoError(t, err)
	pool, err := database.NewPoolFromURL(ctx, url)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	e, err := extractor.New(ctx, pool, extractor.Options{})
	require.NoError(t, err)

	return conn, e
}

func postgresPolicySchema(name string) string {
	return strings.ReplaceAll(`
		CREATE TABLE fixture.documents (id integer NOT NULL, organization_id text NOT NULL, state text NOT NULL);
		CREATE TABLE fixture.grants (organization_id text);
		CREATE TABLE fixture.dormant (id integer);
		ALTER TABLE fixture.dormant FORCE ROW LEVEL SECURITY;
		CREATE FUNCTION fixture.current_organization() RETURNS text LANGUAGE sql STABLE AS
			$$ SELECT current_setting('app.organization', true) $$;
		ALTER TABLE fixture.documents ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY;
		CREATE POLICY organization_scope ON fixture.documents FOR ALL TO fixture_service
			USING (organization_id = fixture.current_organization())
			WITH CHECK (organization_id = fixture.current_organization());
		CREATE POLICY "Quoted""Scope" ON fixture.documents AS RESTRICTIVE FOR SELECT TO fixture_service
			USING (state IN ('OPEN', 'CLOSED') AND EXISTS (
				SELECT 1 FROM fixture.grants g WHERE g.organization_id = documents.organization_id));
		COMMENT ON POLICY organization_scope ON fixture.documents IS 'Scope boundary';
	`, "fixture", name)
}

func policyMigration(t *testing.T, current, desired *schema.Database) *generator.GenerateResult {
	t.Helper()

	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)

	options := testOptions()
	options.MaxOperationsPerFile = 1
	generated, err := generator.New(options).Generate(result)
	require.NoError(t, err)

	for _, warning := range generated.Warnings {
		require.True(t, strings.HasPrefix(warning, "Unsafe"), warning)
	}

	return generated
}

func applyPolicyMigration(
	t *testing.T,
	ctx context.Context,
	conn *pgx.Conn,
	result *generator.GenerateResult,
	down bool,
) {
	t.Helper()

	for i := range result.Migrations {
		file := result.Migrations[i].UpFile
		if down {
			file = result.Migrations[len(result.Migrations)-1-i].DownFile
		}

		require.NotNil(t, file)
		_, err := conn.Exec(ctx, file.Content)
		require.NoError(t, err, "%s", file.Content)
	}
}

func assertPolicyConvergence(
	t *testing.T,
	ctx context.Context,
	e *extractor.Extractor,
	name string,
	desired *schema.Database,
) {
	t.Helper()

	db, err := e.Extract(ctx)
	require.NoError(t, err)

	actual := &schema.Database{}

	for _, table := range db.Tables {
		if table.Schema == name {
			actual.Tables = append(actual.Tables, table)
		}
	}

	for _, function := range db.Functions {
		if function.Schema == name {
			actual.Functions = append(actual.Functions, function)
		}
	}

	result, err := differ.New(nil).Compare(actual, desired)
	require.NoError(t, err)
	assert.Empty(t, result.Changes, "%#v", result.Changes)
}
