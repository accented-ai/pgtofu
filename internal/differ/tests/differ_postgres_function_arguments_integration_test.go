package differ_test

import (
	"context"
	"os"
	"slices"
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

//nolint:paralleltest // Uses a dedicated mutable function schema.
func TestPostgresFunctionArgumentRoundTrip(t *testing.T) {
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

		_, cleanupErr := conn.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS function_arguments CASCADE")
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, "CREATE SCHEMA function_arguments")
	require.NoError(t, err)
	pool, err := database.NewPoolFromURL(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	e, err := extractor.New(ctx, pool, extractor.Options{})
	require.NoError(t, err)
	desired, err := e.Extract(ctx)
	require.NoError(t, err)

	const sql = `CREATE FUNCTION function_arguments.due_at(cutoff TIMESTAMPTZ) RETURNS boolean
		LANGUAGE sql STABLE STRICT PARALLEL SAFE AS $$ SELECT cutoff <= CURRENT_TIMESTAMP $$;
		CREATE FUNCTION function_arguments.anchor() RETURNS boolean
		LANGUAGE sql IMMUTABLE AS $$ SELECT true $$;
		CREATE FUNCTION function_arguments.verifier(secret BYTEA, labels TEXT[]) RETURNS boolean
		LANGUAGE sql IMMUTABLE AS $$ SELECT secret IS NOT NULL AND labels IS NOT NULL $$;`

	p := parser.New()
	require.NoError(t, p.ParseSQL(sql, desired))
	require.Empty(t, p.GetErrors())

	_, err = conn.Exec(ctx, sql)
	require.NoError(t, err)
	current, err := e.Extract(ctx)
	require.NoError(t, err)
	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)
	assert.Empty(t, result.Changes, changeDescriptions(result.Changes))

	fn := current.GetFunction("function_arguments", "due_at", []string{"timestamp with time zone"})
	require.NotNil(t, fn)
	assert.Equal(t, []string{"cutoff"}, fn.ArgumentNames)
	assert.Equal(t, []string{"IN"}, fn.ArgumentModes)
	assert.True(t, fn.IsStrict)
	assert.Equal(t, schema.ParallelSafetySafe, fn.ParallelSafety)

	after, err := e.Extract(ctx)
	require.NoError(t, err)

	after.GetFunction("function_arguments", "due_at", fn.ArgumentTypes).Body = "SELECT cutoff < CURRENT_TIMESTAMP"
	assertFunctionArgumentMigrations(t, ctx, conn, e, current, after)
	removed, err := e.Extract(ctx)
	require.NoError(t, err)

	removed.Functions = slices.DeleteFunc(removed.Functions, func(function schema.Function) bool {
		return function.Schema == "function_arguments" && function.Name == "due_at"
	})
	assertFunctionArgumentMigrations(t, ctx, conn, e, after, removed)
	assertCatalogFunctionArguments(t, ctx, conn, e)
}

func assertFunctionArgumentMigrations(
	t *testing.T,
	ctx context.Context,
	conn *pgx.Conn,
	e *extractor.Extractor,
	before, after *schema.Database,
) {
	t.Helper()

	result, err := differ.New(nil).Compare(before, after)
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)

	options := generator.DefaultOptions()
	options.PreviewMode = true
	generated, err := generator.New(options).Generate(result)
	require.NoError(t, err)
	require.Len(t, generated.Migrations, 1)

	migration := generated.Migrations[0]
	for _, step := range []struct {
		file *generator.MigrationFile
		want *schema.Database
	}{
		{migration.UpFile, after}, {migration.DownFile, before}, {migration.UpFile, after},
	} {
		_, err = conn.Exec(ctx, step.file.Content)
		require.NoError(t, err, step.file.Content)

		actual, extractErr := e.Extract(ctx)
		require.NoError(t, extractErr)

		difference, compareErr := differ.New(nil).Compare(actual, step.want)
		require.NoError(t, compareErr)
		assert.Empty(t, difference.Changes, changeDescriptions(difference.Changes))
	}
}

func assertCatalogFunctionArguments(
	t *testing.T,
	ctx context.Context,
	conn *pgx.Conn,
	e *extractor.Extractor,
) {
	t.Helper()

	_, err := conn.Exec(ctx, `
		CREATE DOMAIN function_arguments."Moment Type" AS TIMESTAMPTZ;
		CREATE FUNCTION function_arguments.multiword(
			cutoff TIMESTAMP WITH TIME ZONE, readings DOUBLE PRECISION[], memo CHARACTER VARYING(12))
		RETURNS boolean LANGUAGE sql AS $$ SELECT cutoff IS NOT NULL $$;
		CREATE FUNCTION function_arguments.named(
			TIMESTAMPTZ, "cut off" TIMESTAMPTZ, "a,b" TEXT, "a""b" INTEGER)
		RETURNS boolean LANGUAGE sql AS $$ SELECT $1 IS NOT NULL $$;
		CREATE FUNCTION function_arguments.modes(INOUT cutoff TIMESTAMPTZ, OUT elapsed DOUBLE PRECISION)
		RETURNS record LANGUAGE sql AS $$ SELECT cutoff, 1::double precision $$;
		CREATE FUNCTION function_arguments.variadic_values(VARIADIC cutoffs TIMESTAMPTZ[])
		RETURNS integer LANGUAGE sql AS $$ SELECT cardinality(cutoffs) $$;
		CREATE FUNCTION function_arguments.tabular(cutoff TIMESTAMPTZ)
		RETURNS TABLE (value TIMESTAMPTZ, label TEXT) LANGUAGE sql AS $$ SELECT cutoff, 'x'::text $$;
		CREATE FUNCTION function_arguments.table_only() RETURNS TABLE (value TIMESTAMPTZ)
		LANGUAGE sql AS $$ SELECT CURRENT_TIMESTAMP $$;
		CREATE FUNCTION function_arguments.only_output(OUT cutoff TIMESTAMPTZ)
		LANGUAGE sql AS $$ SELECT CURRENT_TIMESTAMP $$;
		CREATE FUNCTION function_arguments.defaults(
			label TEXT DEFAULT $value$a,(b)'c$value$,
			stamps TIMESTAMPTZ[] DEFAULT ARRAY[]::TIMESTAMPTZ[], amount NUMERIC(12,3) DEFAULT (1.25))
		RETURNS text LANGUAGE sql AS $$ SELECT label $$;
		CREATE FUNCTION function_arguments.typed(value function_arguments."Moment Type")
		RETURNS boolean LANGUAGE sql AS $$ SELECT value IS NOT NULL $$;
	`)
	require.NoError(t, err)
	actual, err := e.Extract(ctx)
	require.NoError(t, err)

	for _, test := range []struct {
		name  string
		types []string
		names []string
		modes []string
	}{
		{"anchor", nil, nil, nil},
		{"verifier", []string{"bytea", "text[]"}, []string{"secret", "labels"}, []string{"IN", "IN"}},
		{
			"multiword",
			[]string{"timestamp with time zone", "double precision[]", "character varying"},
			[]string{"cutoff", "readings", "memo"},
			[]string{"IN", "IN", "IN"},
		},
		{
			"named",
			[]string{"timestamp with time zone", "timestamp with time zone", "text", "integer"},
			[]string{"", `"cut off"`, `"a,b"`, `"a""b"`},
			[]string{"IN", "IN", "IN", "IN"},
		},
		{
			"modes",
			[]string{"timestamp with time zone", "double precision"},
			[]string{"cutoff", "elapsed"},
			[]string{"INOUT", "OUT"},
		},
		{"variadic_values", []string{"timestamp with time zone[]"}, []string{"cutoffs"}, []string{"VARIADIC"}},
		{"tabular", []string{"timestamp with time zone"}, []string{"cutoff"}, []string{"IN"}},
		{"table_only", nil, nil, nil},
		{"only_output", []string{"timestamp with time zone"}, []string{"cutoff"}, []string{"OUT"}},
		{
			"defaults",
			[]string{"text", "timestamp with time zone[]", "numeric"},
			[]string{"label", "stamps", "amount"},
			[]string{"IN", "IN", "IN"},
		},
		{"typed", []string{`function_arguments."Moment Type"`}, []string{"value"}, []string{"IN"}},
	} {
		fn := actual.GetFunction("function_arguments", test.name, test.types)
		require.NotNil(t, fn, test.name)
		assert.Equal(t, test.types, fn.ArgumentTypes, test.name)
		assert.Equal(t, test.names, fn.ArgumentNames, test.name)
		assert.Equal(t, test.modes, fn.ArgumentModes, test.name)

		if test.name == "tabular" || test.name == "table_only" {
			assert.Contains(t, fn.ReturnType, "TABLE(")
		}
	}

	defaults := actual.GetFunction(
		"function_arguments",
		"defaults",
		[]string{"text", "timestamp with time zone[]", "numeric"},
	)
	assert.Contains(t, defaults.Definition, "DEFAULT")
}
