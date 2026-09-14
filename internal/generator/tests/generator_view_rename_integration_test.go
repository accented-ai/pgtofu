package generator_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/extractor"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/pkg/database"
)

//nolint:paralleltest // Uses a dedicated mutable database.
func TestPostgresViewRenameRoundTrip(t *testing.T) {
	databaseURL := os.Getenv("PGTOFU_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("PGTOFU_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn, err := pgx.Connect(ctx, databaseURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		_, cleanupErr := conn.Exec(cleanupCtx, `
DROP SCHEMA IF EXISTS view_rename_reporting CASCADE;
DROP SCHEMA IF EXISTS view_rename_source CASCADE;
`)
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, `
DROP SCHEMA IF EXISTS view_rename_reporting CASCADE;
DROP SCHEMA IF EXISTS view_rename_source CASCADE;
CREATE SCHEMA view_rename_source;
CREATE SCHEMA view_rename_reporting;
CREATE TABLE view_rename_source.records (
    record_id BIGINT PRIMARY KEY,
    is_ready BOOLEAN NOT NULL
);
CREATE VIEW view_rename_source.status_source AS
SELECT
    record_id AS legacy_record_id,
    is_ready AS legacy_is_ready
FROM view_rename_source.records;
CREATE VIEW view_rename_reporting.status_summary AS
SELECT
    legacy_record_id,
    legacy_is_ready
FROM view_rename_source.status_source;
`)
	require.NoError(t, err)

	pool, err := database.NewPoolFromURL(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	schemaExtractor, err := extractor.New(ctx, pool, extractor.Options{})
	require.NoError(t, err)

	current, err := schemaExtractor.Extract(ctx)
	require.NoError(t, err)
	desired, err := schemaExtractor.Extract(ctx)
	require.NoError(t, err)

	schemaParser := parser.New()
	require.NoError(t, schemaParser.ParseSQL(viewRenameDesiredSchema, desired))
	require.Empty(t, schemaParser.GetErrors())

	diffResult, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)

	options := generator.DefaultOptions()
	options.PreviewMode = true
	options.MaxOperationsPerFile = 1
	generated, err := generator.New(options).Generate(diffResult)
	require.NoError(t, err)
	require.Len(t, generated.Migrations, 1)

	_, err = conn.Exec(ctx, generated.Migrations[0].UpFile.Content)
	require.NoError(t, err)
	assertViewColumns(t, ctx, conn, "view_rename_source", "status_source", []string{
		"source_record_id",
		"source_is_ready",
	})

	_, err = conn.Exec(ctx, generated.Migrations[0].DownFile.Content)
	require.NoError(t, err)
	assertViewColumns(t, ctx, conn, "view_rename_source", "status_source", []string{
		"legacy_record_id",
		"legacy_is_ready",
	})
}

func assertViewColumns(
	t *testing.T,
	ctx context.Context,
	conn *pgx.Conn,
	schemaName,
	viewName string,
	want []string,
) {
	t.Helper()

	rows, err := conn.Query(ctx, `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2
ORDER BY ordinal_position
`, schemaName, viewName)
	require.NoError(t, err)

	columns, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	assert.Equal(t, want, columns)
}

const viewRenameDesiredSchema = `
CREATE OR REPLACE VIEW view_rename_source.status_source AS
SELECT
    record_id AS source_record_id,
    is_ready AS source_is_ready
FROM view_rename_source.records;

CREATE OR REPLACE VIEW view_rename_reporting.status_summary AS
SELECT
    source_record_id,
    source_is_ready
FROM view_rename_source.status_source;
`
