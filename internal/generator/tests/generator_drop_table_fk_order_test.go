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

func TestDroppedTablesFollowForeignKeyOrder(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		childSchema string
	}{
		{name: "same schema", childSchema: "catalog"},
		{name: "cross schema", childSchema: "reporting"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			current := droppedForeignKeyTables("catalog", test.childSchema)
			result, err := differ.New(differ.DefaultOptions()).Compare(current, &schema.Database{})
			require.NoError(t, err)

			child := test.childSchema + ".children"
			for _, change := range result.Changes {
				if change.Type == differ.ChangeTypeDropTable && change.ObjectName == child {
					require.Contains(t, change.RollbackDependsOn, "catalog.parents")
				}
			}

			generated, err := generator.New(testOptions()).Generate(result)
			require.NoError(t, err)

			var up, down strings.Builder
			for _, migration := range generated.Migrations {
				up.WriteString(migration.UpFile.Content)
			}

			for index := len(generated.Migrations) - 1; index >= 0; index-- {
				down.WriteString(generated.Migrations[index].DownFile.Content)
			}

			require.Contains(t, up.String(), "DROP TABLE IF EXISTS "+child)
			require.Contains(t, up.String(), "DROP TABLE IF EXISTS catalog.parents")
			require.Contains(t, down.String(), "CREATE TABLE catalog.parents")
			require.Contains(t, down.String(), "CREATE TABLE "+child)
			require.Less(t,
				strings.Index(up.String(), "DROP TABLE IF EXISTS "+child),
				strings.Index(up.String(), "DROP TABLE IF EXISTS catalog.parents"),
			)
			require.Less(t,
				strings.Index(down.String(), "CREATE TABLE catalog.parents"),
				strings.Index(down.String(), "CREATE TABLE "+child),
			)
		})
	}
}

//nolint:paralleltest // Uses a dedicated mutable database.
func TestDroppedForeignKeyTablesApplyAndRollback(t *testing.T) {
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

		_, cleanupErr := conn.Exec(cleanupCtx,
			"DROP SCHEMA IF EXISTS pgtofu_drop_child CASCADE; "+
				"DROP SCHEMA IF EXISTS pgtofu_drop_parent CASCADE")
		assert.NoError(t, cleanupErr)
		assert.NoError(t, conn.Close(cleanupCtx))
	})

	_, err = conn.Exec(ctx, `
		CREATE SCHEMA pgtofu_drop_parent;
		CREATE SCHEMA pgtofu_drop_child;
		CREATE TABLE pgtofu_drop_parent.parents (id UUID PRIMARY KEY);
		CREATE TABLE pgtofu_drop_child.children (
			id UUID PRIMARY KEY,
			parent_id UUID REFERENCES pgtofu_drop_parent.parents (id)
		);
	`)
	require.NoError(t, err)

	current := droppedForeignKeyTables("pgtofu_drop_parent", "pgtofu_drop_child")
	result, err := differ.New(differ.DefaultOptions()).Compare(current, &schema.Database{})
	require.NoError(t, err)
	generated, err := generator.New(testOptions()).Generate(result)
	require.NoError(t, err)

	for _, migration := range generated.Migrations {
		_, err = conn.Exec(ctx, migration.UpFile.Content)
		require.NoError(t, err)
	}

	for index := len(generated.Migrations) - 1; index >= 0; index-- {
		_, err = conn.Exec(ctx, generated.Migrations[index].DownFile.Content)
		require.NoError(t, err)
	}
}

func droppedForeignKeyTables(parentSchema, childSchema string) *schema.Database {
	return &schema.Database{Tables: []schema.Table{
		{
			Schema: parentSchema, Name: "parents",
			Columns: []schema.Column{{Name: "id", DataType: "uuid", Position: 1}},
			Constraints: []schema.Constraint{{
				Name: "parents_pkey", Type: "PRIMARY KEY", Columns: []string{"id"},
			}},
		},
		{
			Schema: childSchema, Name: "children",
			Columns: []schema.Column{
				{Name: "id", DataType: "uuid", Position: 1},
				{Name: "parent_id", DataType: "uuid", Position: 2},
			},
			Constraints: []schema.Constraint{
				{Name: "children_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
				{
					Name: "children_parent_id_fkey", Type: "FOREIGN KEY",
					Columns: []string{"parent_id"}, ReferencedSchema: parentSchema,
					ReferencedTable: "parents", ReferencedColumns: []string{"id"},
				},
			},
		},
	}}
}
