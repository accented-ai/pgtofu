package differ_test

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
func TestPostgresCanonicalCheckRoundTripConverges(t *testing.T) {
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

		_, cleanupErr := conn.Exec(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS check_roundtrip CASCADE",
		)
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, `
		CREATE EXTENSION IF NOT EXISTS pgcrypto;
		CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
		DROP SCHEMA IF EXISTS check_roundtrip CASCADE;
	`)
	require.NoError(t, err)

	pool, err := database.NewPoolFromURL(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	schemaExtractor, err := extractor.New(ctx, pool, extractor.Options{})
	require.NoError(t, err)

	baseline, err := schemaExtractor.Extract(ctx)
	require.NoError(t, err)
	desired, err := schemaExtractor.Extract(ctx)
	require.NoError(t, err)

	schemaParser := parser.New()
	require.NoError(t, schemaParser.ParseSQL(canonicalCheckTestSchema, desired))
	require.Empty(t, schemaParser.GetErrors())

	firstDiff, err := differ.New(nil).Compare(baseline, desired)
	require.NoError(t, err)
	require.NotEmpty(t, firstDiff.Changes)

	options := generator.DefaultOptions()
	options.PreviewMode = true
	options.GenerateDownMigrations = false
	migrationGenerator := generator.New(options)
	generated, err := migrationGenerator.Generate(firstDiff)
	require.NoError(t, err)
	require.NotEmpty(t, generated.Migrations)

	for _, migration := range generated.Migrations {
		require.NotNil(t, migration.UpFile)

		_, err = conn.Exec(ctx, migration.UpFile.Content)
		require.NoError(t, err)
	}

	afterApply, err := schemaExtractor.Extract(ctx)
	require.NoError(t, err)
	secondDiff, err := differ.New(nil).Compare(afterApply, desired)
	require.NoError(t, err)

	assert.Empty(t, secondDiff.Changes, changeDescriptions(secondDiff.Changes))
}

func changeDescriptions(changes []differ.Change) []string {
	descriptions := make([]string, len(changes))
	for i := range changes {
		descriptions[i] = changes[i].Description
	}

	return descriptions
}

const canonicalCheckTestSchema = `
CREATE SCHEMA check_roundtrip;

CREATE TABLE check_roundtrip.documents (
    id UUID PRIMARY KEY,
    namespace_id UUID NOT NULL,
    source_text TEXT NOT NULL,
    document JSONB NOT NULL CHECK ((
        JSONB_TYPEOF(document) = 'object'
        AND document -> 'enabled' = 'true'::JSONB
        AND document = source_text::JSONB
    ) IS TRUE),
    content_hash TEXT NOT NULL CHECK (
        content_hash ~ '^[0-9a-f]{64}$'
        AND content_hash = ENCODE(PUBLIC.DIGEST(CONVERT_TO(
            source_text,
            'UTF8'
        ), 'sha256'), 'hex')
    ),
    CHECK (
        id = UUID_GENERATE_V5(
            namespace_id,
            'document:' || content_hash
        )
    ),
    CHECK (
        namespace_id = UUID_GENERATE_V5(
            '00000000-0000-0000-0000-000000000000'::UUID,
            'namespace:' || content_hash
        )
    )
);
`
