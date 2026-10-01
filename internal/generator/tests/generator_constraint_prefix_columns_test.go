package generator_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestGenerateTableWithConstraintPrefixColumns(t *testing.T) {
	t.Parallel()

	current := parseGeneratorSchema(t, `CREATE TABLE app.checkpoints (id UUID PRIMARY KEY);`)
	desired := parseGeneratorSchema(t, `
        CREATE TABLE app.checkpoints (id UUID PRIMARY KEY);
        CREATE TABLE app.review_reservations (
            checkpoint_id UUID NOT NULL,
            review_lane TEXT NOT NULL CHECK (review_lane <> ''),
            unique_key TEXT NOT NULL,
            exclude_reason TEXT,
            PRIMARY KEY (checkpoint_id, review_lane),
            FOREIGN KEY (checkpoint_id) REFERENCES app.checkpoints(id) ON DELETE CASCADE
        );`)

	diff, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)
	result, err := generator.New(testOptions()).Generate(diff)
	require.NoError(t, err)
	require.Len(t, result.Migrations, 1)

	upSQL := result.Migrations[0].UpFile.Content
	require.Contains(t, upSQL, "checkpoint_id UUID NOT NULL")
	require.Contains(t, upSQL, "unique_key TEXT NOT NULL")
	require.Contains(t, upSQL, "exclude_reason TEXT")
	require.Contains(t, upSQL, "PRIMARY KEY (checkpoint_id, review_lane)")
	require.Contains(
		t,
		upSQL,
		"FOREIGN KEY (checkpoint_id) REFERENCES app.checkpoints (id) ON DELETE CASCADE",
	)
}

func parseGeneratorSchema(t *testing.T, sql string) *schema.Database {
	t.Helper()

	db := &schema.Database{}
	require.NoError(t, parser.New().ParseSQL(sql, db))

	return db
}
