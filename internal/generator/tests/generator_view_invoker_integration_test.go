package generator_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

//nolint:paralleltest // Exercises generated view migrations with dedicated database roles.
func TestPostgresViewInvokerRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	conn, e := policyIntegrationDatabase(t, ctx, "view_invoker")
	initial := invokerPolicySchema(t, "'yes'")
	applyPolicyMigration(t, ctx, conn, policyMigration(t, &schema.Database{}, initial), false)
	_, err := conn.Exec(ctx, `INSERT INTO view_invoker.documents VALUES (1, 'A'), (2, 'B');
		GRANT USAGE ON SCHEMA view_invoker TO view_invoker_service;`)
	require.NoError(t, err)
	assertPolicyConvergence(t, ctx, e, "view_invoker", initial)
	assertInvokerRows(t, ctx, conn, 1)

	for _, name := range []string{"option only", "definition", "dependency recreation"} {
		t.Run(name, func(t *testing.T) {
			desired := invokerPolicySchema(t, "true")
			wantRows := 1

			switch name {
			case "option only":
				desired = invokerPolicySchema(t, "false")
				wantRows = 2
			case "definition":
				desired.Views[0].Definition += " WHERE id >= 0"
			case "dependency recreation":
				desired.Tables[0].Columns[0].DataType = "bigint"
			}

			migration := policyMigration(t, initial, desired)
			applyPolicyMigration(t, ctx, conn, migration, false)
			assertPolicyConvergence(t, ctx, e, "view_invoker", desired)
			assertInvokerRows(t, ctx, conn, wantRows)
			applyPolicyMigration(t, ctx, conn, migration, true)
			assertPolicyConvergence(t, ctx, e, "view_invoker", initial)
			assertInvokerRows(t, ctx, conn, 1)
		})
	}

	_, err = conn.Exec(ctx, `CREATE VIEW view_invoker.default_mode AS SELECT 1;
		CREATE VIEW view_invoker.false_mode WITH (security_invoker = false) AS SELECT 1;`)
	require.NoError(t, err)
	actual, err := e.Extract(ctx)
	require.NoError(t, err)

	defaults := map[string]bool{}

	for _, view := range actual.Views {
		if view.Schema == "view_invoker" &&
			(view.Name == "default_mode" || view.Name == "false_mode") {
			defaults[view.Name] = view.SecurityInvoker
		}
	}

	assert.Equal(t, map[string]bool{"default_mode": false, "false_mode": false}, defaults)
}

func invokerPolicySchema(t *testing.T, option string) *schema.Database {
	t.Helper()

	desired := viewInvokerSchema(t, option)
	p := parser.New()
	require.NoError(t, p.ParseSQL(`ALTER TABLE view_invoker.documents ENABLE ROW LEVEL SECURITY;
		CREATE POLICY tenant_scope ON view_invoker.documents FOR SELECT TO view_invoker_service
		USING (organization_id = current_setting('app.organization', true));`, desired))
	require.Empty(t, p.GetErrors())

	return desired
}

func assertInvokerRows(t *testing.T, ctx context.Context, conn *pgx.Conn, want int) {
	t.Helper()

	_, err := conn.Exec(
		ctx,
		`GRANT SELECT ON ALL TABLES IN SCHEMA view_invoker TO view_invoker_service`,
	)
	require.NoError(t, err)
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `SET LOCAL ROLE view_invoker_service;
		SELECT set_config('app.organization', 'A', true);`)
	require.NoError(t, err)

	for _, view := range []string{"visible", "selected"} {
		var count int
		require.NoError(t, tx.QueryRow(ctx,
			"SELECT count(*) FROM view_invoker."+view,
		).Scan(&count))
		assert.Equal(t, want, count, view)
	}

	require.NoError(t, tx.Rollback(ctx))
}
