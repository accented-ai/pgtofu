package generator_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestParenthesizedJoinViewsRecreateInDependencyOrder(t *testing.T) {
	t.Parallel()

	current := foreignKeySchema(t, `
CREATE TABLE source_data.records (id integer, state text);
CREATE TABLE source_data.labels (id integer);
CREATE VIEW source_data.z_records AS SELECT id, state FROM source_data.records;
CREATE VIEW reporting.a_summary AS
WITH selected AS MATERIALIZED (
    SELECT record.id
    FROM ((source_data.z_records record
      JOIN source_data.records source ON source.id = record.id)
      JOIN source_data.labels label ON label.id = record.id)
)
SELECT id FROM selected;`)

	desired := &schema.Database{
		Schemas: current.Schemas,
		Tables:  current.Tables,
		Views:   append([]schema.View(nil), current.Views...),
	}
	for index := range desired.Views {
		if desired.Views[index].Name == "z_records" {
			desired.Views[index].Definition = "SELECT id FROM source_data.records"
		}
	}

	for _, batchSize := range []int{1, 50} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			t.Parallel()

			up, down := foreignKeyMigrationSQL(t, current, desired, batchSize)
			for _, sql := range []string{up, down} {
				require.Contains(t, sql, "CREATE VIEW reporting.a_summary")
				requireSQLBefore(t, sql,
					"DROP VIEW IF EXISTS reporting.a_summary",
					"DROP VIEW IF EXISTS source_data.z_records")
				requireSQLBefore(t, sql,
					"CREATE VIEW source_data.z_records",
					"CREATE VIEW reporting.a_summary")
			}
		})
	}
}
