package generator_test

import "testing"

func TestIndexChangesReleaseAndRestoreFunctionDependencies(t *testing.T) {
	t.Parallel()

	const (
		tableSQL   = `CREATE TABLE kb.items (surface text);`
		currentSQL = tableSQL + `
CREATE FUNCTION annotation.compact_key(value text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT lower(value) $$;
CREATE INDEX lookup_key ON kb.items (annotation.compact_key(surface));`
	)

	for _, desiredSQL := range []string{
		tableSQL,
		tableSQL + `
CREATE FUNCTION annotation.normalized_key(value text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT upper(value) $$;
CREATE INDEX lookup_key ON kb.items (annotation.normalized_key(surface));`,
	} {
		current, desired := foreignKeySchema(t, currentSQL), foreignKeySchema(t, desiredSQL)
		for _, maxOperations := range []int{1, 50} {
			up, down := foreignKeyMigrationSQL(t, current, desired, maxOperations)
			requireSQLBefore(
				t,
				up,
				"DROP INDEX IF EXISTS kb.lookup_key",
				"DROP FUNCTION IF EXISTS annotation.compact_key",
			)
			requireSQLBefore(
				t,
				down,
				"CREATE OR REPLACE FUNCTION annotation.COMPACT_KEY",
				"CREATE INDEX lookup_key",
			)
		}
	}
}
