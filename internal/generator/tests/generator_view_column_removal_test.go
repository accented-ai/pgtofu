package generator_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestViewColumnCountChangesRebuildForUpAndDown(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		currentSQL string
		desiredSQL string
	}{
		{
			name:       "forward removal",
			currentSQL: "SELECT id, state FROM reporting.jobs",
			desiredSQL: "SELECT id FROM reporting.jobs",
		},
		{
			name:       "rollback removal",
			currentSQL: "SELECT id FROM reporting.jobs",
			desiredSQL: "SELECT id, state FROM reporting.jobs",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			current := &schema.Database{Views: []schema.View{{
				Schema: "reporting", Name: "job_summary", Definition: test.currentSQL,
			}}}
			desired := &schema.Database{Views: []schema.View{{
				Schema: "reporting", Name: "job_summary", Definition: test.desiredSQL,
			}}}

			result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
			require.NoError(t, err)
			require.NotEmpty(t, result.Changes)

			generated, err := generator.New(testOptions()).Generate(result)
			require.NoError(t, err)
			require.Len(t, generated.Migrations, 1)
			require.Contains(
				t,
				generated.Migrations[0].UpFile.Content,
				"DROP VIEW IF EXISTS reporting.job_summary",
			)
			require.Contains(
				t,
				generated.Migrations[0].UpFile.Content,
				"CREATE VIEW reporting.job_summary",
			)
			require.Contains(
				t,
				generated.Migrations[0].DownFile.Content,
				"DROP VIEW IF EXISTS reporting.job_summary",
			)
			require.Contains(
				t,
				generated.Migrations[0].DownFile.Content,
				"CREATE VIEW reporting.job_summary",
			)
		})
	}
}

func TestViewRecreationRetainsTransitiveDependents(t *testing.T) {
	t.Parallel()

	for _, batchSize := range []int{1, 50} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			t.Parallel()

			current := &schema.Database{
				Views: []schema.View{
					{
						Schema: "source_data", Name: "z_items",
						Definition: "SELECT id FROM source_data.items", Comment: "Original summary",
					},
					{
						Schema: "reporting", Name: "a_public_items",
						Definition: "SELECT id FROM source_data.z_items", Comment: "Public summary",
					},
					{
						Schema: "analytics", Name: "z_cached_items",
						Definition: "SELECT id FROM analytics.a_cache",
					},
				},
				MaterializedViews: []schema.MaterializedView{
					{
						Schema:     "analytics",
						Name:       "a_cache",
						Definition: "SELECT id FROM reporting.a_public_items",
						Comment:    "Cached summary",
						Indexes: []schema.Index{{
							Schema: "analytics", Name: "cache_id", TableName: "a_cache",
							Columns: []string{"id"}, IsUnique: true, Type: "btree",
						}},
					},
				},
			}
			desired := &schema.Database{
				Views:             append([]schema.View(nil), current.Views...),
				MaterializedViews: current.MaterializedViews,
			}
			desired.Views[0].Definition = "SELECT id, state FROM source_data.items"
			desired.Views[0].Comment = "Expanded summary"

			result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
			require.NoError(t, err)

			options := testOptions()
			options.MaxOperationsPerFile = batchSize
			generated, err := generator.New(options).Generate(result)
			require.NoError(t, err)
			require.Len(t, generated.Migrations, 1)

			for direction, sql := range map[string]string{
				"up":   generated.Migrations[0].UpFile.Content,
				"down": generated.Migrations[0].DownFile.Content,
			} {
				t.Run(direction, func(t *testing.T) {
					t.Parallel()

					previous := -1

					for _, statement := range []string{
						"DROP VIEW IF EXISTS analytics.z_cached_items",
						"DROP MATERIALIZED VIEW IF EXISTS analytics.a_cache",
						"DROP VIEW IF EXISTS reporting.a_public_items",
						"DROP VIEW IF EXISTS source_data.z_items",
						"CREATE VIEW source_data.z_items",
						"CREATE VIEW reporting.a_public_items",
						"CREATE MATERIALIZED VIEW analytics.a_cache",
						"CREATE VIEW analytics.z_cached_items",
					} {
						position := strings.Index(sql, statement)
						require.Greater(t, position, previous, statement)
						previous = position
					}

					require.Contains(t, sql, "COMMENT ON VIEW reporting.a_public_items")
					require.Contains(t, sql, "COMMENT ON MATERIALIZED VIEW analytics.a_cache")
					require.Contains(t, sql, "CREATE UNIQUE INDEX cache_id")
				})
			}
		})
	}
}

func TestAddedViewColumnCanOmitRollbackMigration(t *testing.T) {
	t.Parallel()

	current := &schema.Database{Views: []schema.View{{
		Schema: "reporting", Name: "job_summary",
		Definition: "SELECT id FROM reporting.jobs",
	}}}
	desired := &schema.Database{Views: []schema.View{{
		Schema: "reporting", Name: "job_summary",
		Definition: "SELECT id, state FROM reporting.jobs",
	}}}

	result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)

	options := testOptions()
	options.GenerateDownMigrations = false
	generated, err := generator.New(options).Generate(result)
	require.NoError(t, err)
	require.NotEmpty(t, generated.Migrations)
}

func TestViewRollbackReleasesNewDependencyBeforeDroppingIt(t *testing.T) {
	t.Parallel()

	current := foreignKeySchema(t, `CREATE TABLE source_data.records (id integer);
CREATE VIEW reporting.profile AS SELECT id FROM source_data.records;
COMMENT ON VIEW reporting.profile IS 'Original summary';`)
	desired := foreignKeySchema(t, `CREATE TABLE source_data.records (id integer, active boolean);
CREATE VIEW reporting.roster AS SELECT id, active FROM source_data.records;
CREATE VIEW reporting.profile AS SELECT id FROM reporting.roster;
COMMENT ON VIEW reporting.profile IS 'Expanded summary';`)

	for _, maxOperations := range []int{1, 50} {
		_, down := foreignKeyMigrationSQL(t, current, desired, maxOperations)
		requireSQLBefore(
			t,
			down,
			"DROP VIEW IF EXISTS reporting.profile",
			"DROP VIEW IF EXISTS reporting.roster",
		)
		requireSQLBefore(
			t,
			down,
			"DROP VIEW IF EXISTS reporting.roster",
			"DROP COLUMN IF EXISTS active",
		)
	}
}
