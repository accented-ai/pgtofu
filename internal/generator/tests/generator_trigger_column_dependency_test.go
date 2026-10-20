package generator_test

import "testing"

func TestTriggerRecreationSurroundsReferencedColumnChanges(t *testing.T) {
	t.Parallel()

	const functionSQL = `CREATE FUNCTION catalog.observe() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;`

	for _, condition := range []bool{false, true} {
		beforeTrigger := `CREATE TRIGGER observe_change AFTER UPDATE OF legacy_value ON catalog.items
FOR EACH ROW EXECUTE FUNCTION catalog.observe();`
		afterTrigger := `CREATE TRIGGER observe_change AFTER UPDATE OF current_value ON catalog.items
FOR EACH ROW EXECUTE FUNCTION catalog.observe();`

		if condition {
			beforeTrigger = `CREATE TRIGGER observe_change AFTER UPDATE ON catalog.items
FOR EACH ROW WHEN (NEW.legacy_value IS NOT NULL) EXECUTE FUNCTION catalog.observe();`
			afterTrigger = `CREATE TRIGGER observe_change AFTER UPDATE ON catalog.items
FOR EACH ROW WHEN (NEW.current_value IS NOT NULL) EXECUTE FUNCTION catalog.observe();`
		}

		current := foreignKeySchema(
			t,
			`CREATE TABLE catalog.items (id integer, legacy_value text);`+
				functionSQL+beforeTrigger,
		)

		desired := foreignKeySchema(
			t,
			`CREATE TABLE catalog.items (id integer, current_value text);`+
				functionSQL+afterTrigger,
		)
		for _, maxOperations := range []int{1, 50} {
			up, down := foreignKeyMigrationSQL(t, current, desired, maxOperations)
			requireSQLBefore(
				t,
				up,
				"DROP TRIGGER IF EXISTS observe_change",
				"DROP COLUMN IF EXISTS legacy_value",
			)
			requireSQLBefore(t, up, "ADD COLUMN current_value", "CREATE TRIGGER observe_change")
			requireSQLBefore(
				t,
				down,
				"DROP TRIGGER IF EXISTS observe_change",
				"DROP COLUMN IF EXISTS current_value",
			)
			requireSQLBefore(t, down, "ADD COLUMN legacy_value", "CREATE TRIGGER observe_change")
		}
	}
}
