package differ_test

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/extractor"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/pkg/database"
)

//nolint:paralleltest // Uses dedicated mutable view and public source objects.
func TestPostgresViewQualificationRoundTrip(t *testing.T) {
	databaseURL := os.Getenv("PGTOFU_TEST_DATABASE_URL")
	if testing.Short() || databaseURL == "" {
		t.Skip("requires PGTOFU_TEST_DATABASE_URL")
	}

	location, err := url.Parse(databaseURL)
	require.NoError(t, err)

	parameters := location.Query()
	parameters.Set("search_path", "public")
	location.RawQuery = parameters.Encode()
	databaseURL = location.String()

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	conn, err := pgx.Connect(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cleanupCancel()

		_, cleanupErr := conn.Exec(cleanupCtx, `DROP SCHEMA IF EXISTS view_qualification CASCADE;
			DROP FUNCTION IF EXISTS public.view_qualification_allowed(integer);
			DROP TABLE IF EXISTS public.view_qualification_records;`)
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, `CREATE SCHEMA view_qualification;
		CREATE TABLE public.view_qualification_records (id integer, group_id integer);
		CREATE FUNCTION public.view_qualification_allowed(integer) RETURNS boolean
			LANGUAGE sql IMMUTABLE AS 'SELECT $1 > 0';`)
	require.NoError(t, err)
	parameters.Set("pool_max_conns", "1")
	location.RawQuery = parameters.Encode()
	pool, err := database.NewPoolFromURL(ctx, location.String())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	schemaExtractor, err := extractor.New(ctx, pool, extractor.Options{})
	require.NoError(t, err)
	desired, err := schemaExtractor.Extract(ctx)
	require.NoError(t, err)

	const query = `SELECT r.id, CASE WHEN r.id > 0 THEN r.id END AS optional_id
		FROM public.view_qualification_records AS r
		JOIN public.view_qualification_records AS linked ON linked.group_id = r.group_id
		WHERE public.view_qualification_allowed(r.id) AND NOT EXISTS (
			SELECT 1 FROM public.view_qualification_records AS earlier
			WHERE earlier.group_id = linked.group_id AND earlier.id < linked.id
		)`

	const source = `CREATE VIEW view_qualification.visible AS ` + query + `;
		CREATE MATERIALIZED VIEW view_qualification.snapshot AS ` + query + `;`

	schemaParser := parser.New()
	require.NoError(t, schemaParser.ParseSQL(source, desired))
	require.Empty(t, schemaParser.GetErrors())
	require.Len(t, desired.Views, 1)
	require.Len(t, desired.MaterializedViews, 1)

	_, err = conn.Exec(ctx, source)
	require.NoError(t, err)

	var stored string
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT pg_get_viewdef('view_qualification.visible'::regclass, true)",
	).Scan(&stored))
	require.NotContains(
		t,
		stored,
		"public.",
		"the regression needs PostgreSQL's deparsed qualification",
	)

	current, err := schemaExtractor.Extract(ctx)
	require.NoError(t, err)
	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)
	assert.Empty(t, result.Changes, changeDescriptions(result.Changes))

	encoded, _ := json.Marshal(current)

	var reopened schema.Database
	require.NoError(t, json.Unmarshal(encoded, &reopened))
	result, err = differ.New(nil).Compare(&reopened, desired)
	require.NoError(t, err)
	assert.Empty(t, result.Changes, changeDescriptions(result.Changes))

	var path string
	require.NoError(t, pool.QueryRow(ctx, "SHOW search_path").Scan(&path))
	assert.Equal(t, "public", path)

	shadowSource := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(source,
		"view_qualification.visible", "view_qualification.shadowed"),
		"view_qualification.snapshot", "view_qualification.shadow_snapshot"),
		"public.", "view_qualification.")
	_, err = conn.Exec(
		ctx,
		`CREATE TABLE view_qualification.view_qualification_records (id integer, group_id integer);
		CREATE FUNCTION view_qualification.view_qualification_allowed(integer) RETURNS boolean
			LANGUAGE sql IMMUTABLE AS 'SELECT $1 > 0';`+shadowSource,
	)
	require.NoError(t, err)

	parameters.Set("search_path", "view_qualification, public")
	location.RawQuery = parameters.Encode()
	shadowPool, err := database.NewPoolFromURL(ctx, location.String())
	require.NoError(t, err)
	t.Cleanup(shadowPool.Close)
	shadowExtractor, err := extractor.New(ctx, shadowPool, extractor.Options{})
	require.NoError(t, err)
	shadowed, err := shadowExtractor.Extract(ctx)
	require.NoError(t, err)
	require.NoError(t, shadowPool.QueryRow(ctx, "SHOW search_path").Scan(&path))
	assert.Equal(t, "view_qualification, public", path)
	assertChangedViewBindings(t, shadowed)
}

func assertChangedViewBindings(t *testing.T, current *schema.Database) {
	t.Helper()

	desired := *current
	desired.Views = append([]schema.View(nil), current.Views...)
	desired.MaterializedViews = append([]schema.MaterializedView(nil), current.MaterializedViews...)

	for index := range desired.Views {
		view := &desired.Views[index]
		if view.Name == "shadowed" {
			require.NotContains(t, view.Definition, "view_qualification.")
			view.Definition = strings.ReplaceAll(
				view.BoundDefinition(),
				"view_qualification.",
				"public.",
			)
		}
	}

	for index := range desired.MaterializedViews {
		view := &desired.MaterializedViews[index]
		if view.Name == "shadow_snapshot" {
			view.Definition = strings.ReplaceAll(
				view.BoundDefinition(),
				"view_qualification.",
				"public.",
			)
		}
	}

	result, err := differ.New(nil).Compare(current, &desired)
	require.NoError(t, err)
	require.Len(t, result.Changes, 2, changeDescriptions(result.Changes))
	assert.ElementsMatch(t,
		[]differ.ChangeType{differ.ChangeTypeModifyView, differ.ChangeTypeModifyMaterializedView},
		[]differ.ChangeType{result.Changes[0].Type, result.Changes[1].Type},
	)
}

//nolint:paralleltest // Uses a dedicated mutable view schema.
func TestPostgresViewBuiltinBindingRollback(t *testing.T) {
	databaseURL := os.Getenv("PGTOFU_TEST_DATABASE_URL")
	if testing.Short() || databaseURL == "" {
		t.Skip("requires PGTOFU_TEST_DATABASE_URL")
	}

	location, err := url.Parse(databaseURL)
	require.NoError(t, err)

	parameters := location.Query()
	parameters.Set("search_path", "view_builtin_binding, pg_catalog")
	location.RawQuery = parameters.Encode()

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	conn, err := pgx.Connect(ctx, location.String())
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cleanupCancel()

		_, cleanupErr := conn.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS view_builtin_binding CASCADE")
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, `CREATE SCHEMA view_builtin_binding;
		CREATE FUNCTION view_builtin_binding.lower(text) RETURNS text
			LANGUAGE sql IMMUTABLE AS $$SELECT 'shadow:' || $1$$;
		CREATE VIEW view_builtin_binding.visible AS
			SELECT pg_catalog.lower('MiXeD'::text) AS value;`)
	require.NoError(t, err)
	parameters.Set("pool_max_conns", "1")
	location.RawQuery = parameters.Encode()
	pool, err := database.NewPoolFromURL(ctx, location.String())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	schemaExtractor, err := extractor.New(ctx, pool, extractor.Options{})
	require.NoError(t, err)
	current, err := schemaExtractor.Extract(ctx)
	require.NoError(t, err)
	require.Len(t, current.Views, 1)
	require.Contains(t, current.Views[0].Definition, "pg_catalog.lower")

	withoutViews := *current
	withoutViews.Views = nil
	result, err := differ.New(nil).Compare(current, &withoutViews)
	require.NoError(t, err)

	options := generator.DefaultOptions()
	options.PreviewMode = true
	generated, err := generator.New(options).Generate(result)
	require.NoError(t, err)

	for _, migration := range generated.Migrations {
		_, applyErr := conn.Exec(ctx, migration.UpFile.Content)
		require.NoError(t, applyErr)
	}

	for index := len(generated.Migrations) - 1; index >= 0; index-- {
		_, applyErr := conn.Exec(ctx, generated.Migrations[index].DownFile.Content)
		require.NoError(t, applyErr)
	}

	var value string
	require.NoError(
		t,
		conn.QueryRow(ctx, "SELECT value FROM view_builtin_binding.visible").Scan(&value),
	)
	assert.Equal(t, "mixed", value)
}

//nolint:paralleltest // Uses a dedicated mutable schema and reader role.
func TestPostgresViewRestrictedSchemaExtraction(t *testing.T) {
	databaseURL := os.Getenv("PGTOFU_TEST_DATABASE_URL")
	if testing.Short() || databaseURL == "" {
		t.Skip("requires PGTOFU_TEST_DATABASE_URL")
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	conn, err := pgx.Connect(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cleanupCancel()

		_, cleanupErr := conn.Exec(cleanupCtx, `DROP SCHEMA IF EXISTS "View.Catalog" CASCADE;
			DROP ROLE IF EXISTS view_catalog_reader;`)
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, `CREATE SCHEMA "View.Catalog";
		CREATE VIEW "View.Catalog"."Visible.View" AS SELECT 1 AS id;
		CREATE MATERIALIZED VIEW "View.Catalog"."Snapshot.View" AS SELECT 2 AS id;
		CREATE ROLE view_catalog_reader;`)
	require.NoError(t, err)

	var permitted bool
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT has_schema_privilege('view_catalog_reader', 'View.Catalog', 'USAGE')",
	).Scan(&permitted))
	require.False(t, permitted)

	location, err := url.Parse(databaseURL)
	require.NoError(t, err)

	parameters := location.Query()
	parameters.Set("pool_max_conns", "1")
	location.RawQuery = parameters.Encode()
	pool, err := database.NewPoolFromURL(ctx, location.String())
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	var role string
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT set_config('role', 'view_catalog_reader', false)",
	).Scan(&role))
	schemaExtractor, err := extractor.New(ctx, pool, extractor.Options{})
	require.NoError(t, err)
	current, err := schemaExtractor.Extract(ctx)
	require.NoError(t, err)
	// Ordinary views retain information_schema visibility restrictions.
	assert.Empty(t, current.Views)
	require.Len(t, current.MaterializedViews, 1)
	assert.Equal(t, "View.Catalog", current.MaterializedViews[0].Schema)
	assert.Equal(t, "Snapshot.View", current.MaterializedViews[0].Name)
	assert.NotEmpty(t, current.MaterializedViews[0].BoundDefinition())
}
