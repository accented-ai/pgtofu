package generator_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDroppedColumnRollbackRestoresComment(t *testing.T) {
	t.Parallel()

	current := foreignKeySchema(t, `CREATE TABLE catalog.items (id integer, history text);
COMMENT ON COLUMN catalog.items.history IS 'Retained history';`)
	desired := foreignKeySchema(t, `CREATE TABLE catalog.items (id integer);`)
	_, down := foreignKeyMigrationSQL(t, current, desired, 1)
	require.Contains(t, down, "COMMENT ON COLUMN catalog.items.history IS 'Retained history';")
}
