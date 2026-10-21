package differ_test

import (
	"context"
	"encoding/json"
	"fmt"
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

//nolint:paralleltest // Uses dedicated mutable functions and a table.
func TestPostgresFunctionDefaultRoundTrip(t *testing.T) {
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

		_, cleanupErr := conn.Exec(cleanupCtx, `DROP TABLE IF EXISTS public.notes;
			DROP FUNCTION IF EXISTS public.label_value(); DROP FUNCTION IF EXISTS public."LabelValue"();
			DROP DOMAIN IF EXISTS public.label_type; DROP SCHEMA IF EXISTS other CASCADE;`)
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, `CREATE SCHEMA other;
		CREATE FUNCTION public.label_value() RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'public'::text $$;
		CREATE FUNCTION other.label_value() RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'other'::text $$;
		CREATE FUNCTION public."LabelValue"() RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'Quoted'::text $$;
		CREATE DOMAIN public.label_type AS text; CREATE DOMAIN other.label_type AS text;
		CREATE SEQUENCE other.note_seq;`)
	require.NoError(t, err)

	parsedURL, err := url.Parse(databaseURL)
	require.NoError(t, err)

	parameters := parsedURL.Query()
	parameters.Set("pool_max_conns", "1")
	parameters.Set("search_path", "public, other")
	parsedURL.RawQuery = parameters.Encode()
	pool, err := database.NewPoolFromURL(ctx, parsedURL.String())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	e, err := extractor.New(ctx, pool, extractor.Options{})
	require.NoError(t, err)

	for _, function := range []string{"public.label_value", "other.label_value", "label_value"} {
		t.Run(function, func(t *testing.T) {
			_, err = conn.Exec(ctx, "DROP TABLE IF EXISTS public.notes, other.entries")
			require.NoError(t, err)

			desired, extractErr := e.Extract(ctx)
			require.NoError(t, extractErr)

			sql := fmt.Sprintf(`CREATE TABLE public.notes (
				id bigserial, label text DEFAULT %s(), remote_id integer DEFAULT nextval('other.note_seq'::regclass),
				created_at timestamptz DEFAULT CURRENT_TIMESTAMP, amount integer DEFAULT 42,
				word text DEFAULT 'MixedCase'::text, quoted text DEFAULT public."LabelValue"(),
				local_type text DEFAULT 'OPEN'::text::public.label_type::text,
				remote_type text DEFAULT 'OPEN'::text::other.label_type::text,
				display text GENERATED ALWAYS AS (label || '!') STORED);
				CREATE TABLE other.entries (id serial);`, function)
			p := parser.New()
			require.NoError(t, p.ParseSQL(sql, desired))
			require.Empty(t, p.GetErrors())

			_, err = conn.Exec(ctx, sql)
			require.NoError(t, err)

			current, extractErr := e.Extract(ctx)
			require.NoError(t, extractErr)

			encoded, encodeErr := json.Marshal(current)
			require.NoError(t, encodeErr)
			require.NoError(t, json.Unmarshal(encoded, &current))
			assert.Empty(t, current.GetTable("public", "notes").GetColumn("display").CatalogDefault)
			result, compareErr := differ.New(nil).Compare(current, desired)
			require.NoError(t, compareErr)
			assert.Empty(t, result.Changes, changeDescriptions(result.Changes))

			var path string
			require.NoError(t, pool.QueryRow(ctx, "SHOW search_path").Scan(&path))
			assert.Equal(t, "public, other", path)

			if function == "public.label_value" {
				assertDefaultMigrations(t, ctx, conn, pool, e, current)
			}
		})
	}

	assertDefaultExtractionFailure(t, ctx, conn, parsedURL)
}

func assertDefaultMigrations(
	t *testing.T,
	ctx context.Context,
	conn *pgx.Conn,
	pool *database.Pool,
	e *extractor.Extractor,
	before *schema.Database,
) {
	t.Helper()

	encoded, _ := json.Marshal(before)

	var after *schema.Database
	require.NoError(t, json.Unmarshal(encoded, &after))
	after.GetTable("public", "notes").GetColumn("label").Default = "other.label_value()"
	assert.Empty(t, after.GetTable("public", "notes").GetColumn("label").BoundDefault())
	result, err := differ.New(nil).Compare(before, after)
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)

	options := generator.DefaultOptions()
	options.PreviewMode = true
	generated, err := generator.New(options).Generate(result)
	require.NoError(t, err)
	require.Len(t, generated.Migrations, 1)
	migration := generated.Migrations[0]

	_, err = conn.Exec(ctx, `SET search_path = other, public;`)
	require.NoError(t, err)

	for _, file := range []*generator.MigrationFile{
		migration.UpFile, migration.DownFile, migration.UpFile, migration.DownFile,
	} {
		_, err = conn.Exec(ctx, file.Content)
		require.NoError(t, err)

		actual, extractErr := e.Extract(ctx)
		require.NoError(t, extractErr)

		want := before
		if file == migration.UpFile {
			want = after
		}

		difference, compareErr := differ.New(nil).Compare(actual, want)
		require.NoError(t, compareErr)
		assert.Empty(t, difference.Changes, changeDescriptions(difference.Changes))
	}

	_, err = conn.Exec(ctx, `RESET search_path`)
	require.NoError(t, err)

	var path string
	require.NoError(
		t,
		pool.QueryRow(ctx, "SELECT set_config('search_path', 'other, public', false)").Scan(&path),
	)
	shadow, err := e.Extract(ctx)
	require.NoError(t, err)
	difference, err := differ.New(nil).Compare(shadow, before)
	require.NoError(t, err)
	assert.Empty(t, difference.Changes, changeDescriptions(difference.Changes))
	require.NoError(t, pool.QueryRow(ctx, "SHOW search_path").Scan(&path))
	assert.Equal(t, "other, public", path)
	require.NoError(
		t,
		pool.QueryRow(ctx, "SELECT set_config('search_path', 'public, other', false)").Scan(&path),
	)

	for _, snapshot := range []*schema.Database{before, after} {
		removed := &schema.Database{Tables: []schema.Table{*snapshot.GetTable("public", "notes")}}

		for _, keepTable := range []bool{true, false} {
			desired := &schema.Database{}
			if keepTable {
				desired.Tables = []schema.Table{*removed.GetTable("public", "notes")}
				desired.Tables[0].Columns = nil
			}

			change, compareErr := differ.New(nil).Compare(removed, desired)
			require.NoError(t, compareErr)

			recreated, generateErr := generator.New(options).Generate(change)
			require.NoError(t, generateErr)

			var down strings.Builder
			for _, group := range recreated.Migrations {
				down.WriteString(group.DownFile.Content)
			}

			assert.Contains(t, down.String(), removed.Tables[0].GetColumn("label").DefaultSQL())
		}
	}
}

func assertDefaultExtractionFailure(
	t *testing.T,
	ctx context.Context,
	conn *pgx.Conn,
	databaseURL *url.URL,
) {
	t.Helper()

	_, err := conn.Exec(ctx, `CREATE ROLE default_reader LOGIN PASSWORD 'fixture';
		GRANT USAGE ON SCHEMA public, other TO default_reader;
		GRANT SELECT ON public.notes TO default_reader;
		REVOKE SELECT ON pg_catalog.pg_attrdef FROM PUBLIC;`)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()

		_, cleanupErr := conn.Exec(cleanupCtx, `GRANT SELECT ON pg_catalog.pg_attrdef TO PUBLIC;
			DROP OWNED BY default_reader; DROP ROLE default_reader;`)
		assert.NoError(t, cleanupErr)
	})

	readerURL := *databaseURL
	readerURL.User = url.UserPassword("default_reader", "fixture")
	pool, err := database.NewPoolFromURL(ctx, readerURL.String())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	e, err := extractor.New(ctx, pool, extractor.Options{})
	require.NoError(t, err)
	_, err = e.Extract(ctx)
	require.ErrorContains(t, err, "extract column defaults")

	var path string
	require.NoError(t, pool.QueryRow(ctx, "SHOW search_path").Scan(&path))
	assert.Equal(t, "public, other", path)
}
