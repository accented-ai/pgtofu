package differ_test

import (
	"context"
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

//nolint:paralleltest // Uses a dedicated mutable view schema.
func TestPostgresViewAliasRoundTripConverges(t *testing.T) {
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

		_, cleanupErr := conn.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS view_alias_roundtrip CASCADE")
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, `CREATE SCHEMA view_alias_roundtrip;
		CREATE TABLE view_alias_roundtrip.links (id integer, record_id integer);
		CREATE TABLE view_alias_roundtrip.selections (id integer, link_id integer);`)
	require.NoError(t, err)

	pool, err := database.NewPoolFromURL(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	schemaExtractor, err := extractor.New(ctx, pool, extractor.Options{})
	require.NoError(t, err)
	desired, err := schemaExtractor.Extract(ctx)
	require.NoError(t, err)

	const query = `SELECT selected.id, link.record_id
		FROM view_alias_roundtrip.selections AS selected
		CROSS JOIN LATERAL (
			SELECT link.record_id
			FROM view_alias_roundtrip.links AS link
			WHERE link.id = selected.link_id
			OFFSET 0
		) AS link`

	const source = `CREATE VIEW view_alias_roundtrip.selected_records AS ` + query + `;
		CREATE MATERIALIZED VIEW view_alias_roundtrip.selected_records_snapshot AS ` + query + `;`

	schemaParser := parser.New()
	require.NoError(t, schemaParser.ParseSQL(source, desired))
	require.Empty(t, schemaParser.GetErrors())

	_, err = conn.Exec(ctx, source)
	require.NoError(t, err)

	for _, name := range []string{"selected_records", "selected_records_snapshot"} {
		var stored string
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT pg_get_viewdef($1::regclass, true)", "view_alias_roundtrip."+name,
		).Scan(&stored))
		assert.Regexp(t, `\blink_[0-9]+\.record_id\b`, stored,
			"PostgreSQL must rename the shadowed inner alias for this regression fixture")
	}

	current, err := schemaExtractor.Extract(ctx)
	require.NoError(t, err)
	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)
	assert.Empty(t, result.Changes, changeDescriptions(result.Changes))

	for _, test := range []struct {
		name      string
		predicate string
	}{
		{"changed correlation", "link.id = selected.id"},
		{"changed predicate", "link.id = selected.link_id AND link.record_id > 0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			after := *desired
			after.Views = append([]schema.View(nil), desired.Views...)
			after.MaterializedViews = append(
				[]schema.MaterializedView(nil), desired.MaterializedViews...,
			)

			for index := range after.Views {
				view := &after.Views[index]
				if view.Schema == "view_alias_roundtrip" {
					view.Definition = strings.Replace(view.Definition,
						"link.id = selected.link_id", test.predicate, 1)
				}
			}

			for index := range after.MaterializedViews {
				view := &after.MaterializedViews[index]
				if view.Schema == "view_alias_roundtrip" {
					view.Definition = strings.Replace(view.Definition,
						"link.id = selected.link_id", test.predicate, 1)
				}
			}

			difference, compareErr := differ.New(nil).Compare(current, &after)
			require.NoError(t, compareErr)
			require.Len(t, difference.Changes, 2, changeDescriptions(difference.Changes))
			assert.ElementsMatch(t,
				[]differ.ChangeType{
					differ.ChangeTypeModifyView, differ.ChangeTypeModifyMaterializedView,
				},
				[]differ.ChangeType{difference.Changes[0].Type, difference.Changes[1].Type},
			)

			options := generator.DefaultOptions()
			options.PreviewMode = true
			generated, generateErr := generator.New(options).Generate(difference)
			require.NoError(t, generateErr)
			require.NotEmpty(t, generated.Migrations)

			for _, migration := range generated.Migrations {
				_, applyErr := conn.Exec(ctx, migration.UpFile.Content)
				require.NoError(t, applyErr)
			}

			actual, extractErr := schemaExtractor.Extract(ctx)
			require.NoError(t, extractErr)

			converged, compareErr := differ.New(nil).Compare(actual, &after)
			require.NoError(t, compareErr)
			assert.Empty(t, converged.Changes, changeDescriptions(converged.Changes))

			_, restoreErr := conn.Exec(ctx, `DROP VIEW view_alias_roundtrip.selected_records;
				DROP MATERIALIZED VIEW view_alias_roundtrip.selected_records_snapshot;`+source)
			require.NoError(t, restoreErr)
		})
	}
}
