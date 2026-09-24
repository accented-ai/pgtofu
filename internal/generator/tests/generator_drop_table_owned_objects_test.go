package generator_test

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
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDroppedTableDropsOwnedObjectsFirst(t *testing.T) {
	t.Parallel()

	current, desired := droppedOwnedObjectsSchemas()
	result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)

	generated, err := generator.New(testOptions()).Generate(result)
	require.NoError(t, err)

	var up, down strings.Builder
	for _, migration := range generated.Migrations {
		up.WriteString(migration.UpFile.Content)
	}

	for i := len(generated.Migrations) - 1; i >= 0; i-- {
		down.WriteString(generated.Migrations[i].DownFile.Content)
	}

	for _, statement := range []string{
		"DROP TRIGGER IF EXISTS track_change ON pgtofu_owned_drop.records",
		"DROP INDEX IF EXISTS pgtofu_owned_drop.records_payload_idx",
	} {
		require.Contains(t, up.String(), statement)
		require.Less(t, strings.Index(up.String(), statement),
			strings.Index(up.String(), "DROP TABLE IF EXISTS pgtofu_owned_drop.records"))
	}

	for _, statement := range []string{
		"CREATE INDEX records_payload_idx ON pgtofu_owned_drop.records",
		"CREATE TRIGGER track_change",
	} {
		require.Contains(t, down.String(), statement)
		require.Less(t, strings.Index(down.String(), "CREATE TABLE pgtofu_owned_drop.records"),
			strings.Index(down.String(), statement))
	}
}

//nolint:paralleltest // Uses a dedicated mutable database.
func TestDroppedTableOwnedObjectsApplyAndRollback(t *testing.T) {
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

		_, cleanupErr := conn.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS pgtofu_owned_drop CASCADE")
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, `
		CREATE SCHEMA pgtofu_owned_drop;
		CREATE TABLE pgtofu_owned_drop.records (id integer, payload text);
		CREATE INDEX records_payload_idx ON pgtofu_owned_drop.records (payload);
		CREATE FUNCTION pgtofu_owned_drop.touch_record() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
		CREATE TRIGGER track_change BEFORE INSERT ON pgtofu_owned_drop.records
		FOR EACH ROW EXECUTE FUNCTION pgtofu_owned_drop.touch_record();
	`)
	require.NoError(t, err)

	current, desired := droppedOwnedObjectsSchemas()
	result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)
	generated, err := generator.New(testOptions()).Generate(result)
	require.NoError(t, err)

	for _, migration := range generated.Migrations {
		_, err = conn.Exec(ctx, migration.UpFile.Content)
		require.NoError(t, err)
	}

	for i := len(generated.Migrations) - 1; i >= 0; i-- {
		_, err = conn.Exec(ctx, generated.Migrations[i].DownFile.Content)
		require.NoError(t, err)
	}

	var triggerExists, indexExists bool

	err = conn.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'track_change'),
		       EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'pgtofu_owned_drop'
		               AND indexname = 'records_payload_idx')
	`).Scan(&triggerExists, &indexExists)
	require.NoError(t, err)
	assert.True(t, triggerExists)
	assert.True(t, indexExists)
}

func droppedOwnedObjectsSchemas() (*schema.Database, *schema.Database) {
	function := schema.Function{
		Schema: "pgtofu_owned_drop", Name: "touch_record", ReturnType: "trigger",
		Language: "plpgsql", Body: "BEGIN RETURN NEW; END",
	}
	base := schema.Database{
		Schemas:   []schema.Schema{{Name: "pgtofu_owned_drop"}},
		Functions: []schema.Function{function},
	}
	current := base
	current.Tables = []schema.Table{{
		Schema: "pgtofu_owned_drop", Name: "records",
		Columns: []schema.Column{
			{Name: "id", DataType: "integer", Position: 1, IsNullable: true},
			{Name: "payload", DataType: "text", Position: 2, IsNullable: true},
		},
		Indexes: []schema.Index{{
			Schema: "pgtofu_owned_drop", TableName: "records", Name: "records_payload_idx",
			Columns: []string{"payload"}, Type: schema.IndexTypeBTree,
		}},
	}}
	current.Triggers = []schema.Trigger{{
		Schema: "pgtofu_owned_drop", TableName: "records", Name: "track_change",
		Timing: "BEFORE", Events: []string{"INSERT"}, ForEachRow: true,
		FunctionSchema: "pgtofu_owned_drop", FunctionName: "touch_record",
	}}

	return &current, &base
}
