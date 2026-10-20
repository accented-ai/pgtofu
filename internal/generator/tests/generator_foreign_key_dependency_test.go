package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestForeignKeysFollowReferencedKeyChanges(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, parentSchema string
		parentKey          string
		foreignKey         string
		index              bool
		modifyIndex        bool
		newChild           bool
		modifyForeign      bool
	}{
		{name: "composite unique", parentSchema: "catalog", parentKey: "UNIQUE (id, scope)"},
		{name: "cross schema", parentSchema: "source_data", parentKey: "UNIQUE (id, scope)"},
		{name: "permuted unique columns", parentSchema: "catalog", parentKey: "UNIQUE (scope, id)"},
		{name: "unique index", parentSchema: "source_data", index: true},
		{name: "modified unique index", parentSchema: "source_data", index: true, modifyIndex: true},
		{
			name: "inline foreign key on new table", parentSchema: "catalog",
			parentKey: "UNIQUE (id, scope)", newChild: true,
		},
		{
			name: "modified foreign key", parentSchema: "catalog",
			parentKey: "UNIQUE (id, scope)", modifyForeign: true,
		},
		{
			name: "implicit primary key", parentSchema: "catalog",
			parentKey: "PRIMARY KEY (id)", foreignKey: "FOREIGN KEY (parent_id) REFERENCES catalog.z_parents",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			parent := test.parentSchema + ".z_parents"

			beforeParent := "CREATE TABLE " + parent + " (id integer, scope integer"
			if test.modifyForeign {
				beforeParent += ", CONSTRAINT old_key PRIMARY KEY (id)"
			}

			afterParent := beforeParent
			if !test.index {
				afterParent += ", CONSTRAINT parent_key " + test.parentKey
			}

			beforeChild := `CREATE TABLE catalog.a_children (
				id integer, parent_id integer, parent_scope integer`
			if test.modifyForeign {
				beforeChild += ", CONSTRAINT child_parent FOREIGN KEY (parent_id) REFERENCES " + parent + " (id)"
			}

			foreignKey := test.foreignKey
			if foreignKey == "" {
				foreignKey = "FOREIGN KEY (parent_id, parent_scope) REFERENCES " + parent + " (id, scope)"
			}

			afterChild := `CREATE TABLE catalog.a_children (
				id integer, parent_id integer, parent_scope integer,
				CONSTRAINT child_parent ` + foreignKey + ");"

			before := beforeParent + ");"
			if !test.newChild {
				before += beforeChild + ");"
			}

			if test.modifyIndex {
				before += "CREATE INDEX parent_key ON " + parent + " (id, scope);"
			}

			after := afterParent + ");" + afterChild
			provider := "ADD CONSTRAINT parent_key "
			dropProvider := "DROP CONSTRAINT IF EXISTS parent_key"

			if test.index {
				after += "CREATE UNIQUE INDEX parent_key ON " + parent + " (id, scope);"
				provider = "CREATE UNIQUE INDEX"
				dropProvider = "DROP INDEX IF EXISTS " + test.parentSchema + ".parent_key"
			}

			dependent := "ADD CONSTRAINT child_parent FOREIGN KEY"
			dropDependent := "DROP CONSTRAINT IF EXISTS child_parent"

			if test.newChild {
				dependent = "CREATE TABLE catalog.a_children"
				dropDependent = "DROP TABLE IF EXISTS catalog.a_children"
			}

			current, desired := foreignKeySchema(t, before), foreignKeySchema(t, after)
			for _, maxOperations := range []int{1, 50} {
				up, down := foreignKeyMigrationSQL(t, current, desired, maxOperations)
				requireSQLBefore(t, up, provider, dependent)
				requireSQLBefore(t, down, dropDependent, dropProvider)
				up, down = foreignKeyMigrationSQL(t, desired, current, maxOperations)
				requireSQLBefore(t, up, dropDependent, dropProvider)
				requireSQLBefore(t, down, provider, dependent)
			}
		})
	}
}

func TestMutualForeignKeysWaitForIndependentPrimaryKeys(t *testing.T) {
	t.Parallel()

	current := foreignKeySchema(t, `
		CREATE TABLE catalog.a (id integer, parent integer);
		CREATE TABLE catalog.b (id integer, parent integer);`)
	desired := foreignKeySchema(t, `
		CREATE TABLE catalog.a (id integer, parent integer,
			CONSTRAINT a_key PRIMARY KEY (id),
			CONSTRAINT a_parent FOREIGN KEY (parent) REFERENCES catalog.b (id));
		CREATE TABLE catalog.b (id integer, parent integer,
			CONSTRAINT b_key PRIMARY KEY (id),
			CONSTRAINT b_parent FOREIGN KEY (parent) REFERENCES catalog.a (id));`)
	up, down := foreignKeyMigrationSQL(t, current, desired, 1)
	requireSQLBefore(t, up, "ADD CONSTRAINT a_key", "ADD CONSTRAINT b_parent")
	requireSQLBefore(t, up, "ADD CONSTRAINT b_key", "ADD CONSTRAINT a_parent")
	requireSQLBefore(
		t,
		down,
		"DROP CONSTRAINT IF EXISTS a_parent",
		"DROP CONSTRAINT IF EXISTS b_key",
	)
	requireSQLBefore(
		t,
		down,
		"DROP CONSTRAINT IF EXISTS b_parent",
		"DROP CONSTRAINT IF EXISTS a_key",
	)
}

func TestSelfReferencingForeignKeyFollowsItsUniqueIndex(t *testing.T) {
	t.Parallel()

	for _, modifying := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "new index", true: "modified index"}[modifying],
			func(t *testing.T) {
				t.Parallel()

				before := `CREATE TABLE catalog.tree (id integer, parent_id integer);`
				if modifying {
					before += `CREATE INDEX tree_key ON catalog.tree (id);`
				}

				after := `CREATE TABLE catalog.tree (id integer, parent_id integer,
				CONSTRAINT tree_parent FOREIGN KEY (parent_id) REFERENCES catalog.tree (id));
				CREATE UNIQUE INDEX tree_key ON catalog.tree (id);`
				current, desired := foreignKeySchema(t, before), foreignKeySchema(t, after)
				up, down := foreignKeyMigrationSQL(t, current, desired, 1)
				requireSQLBefore(t, up, "CREATE UNIQUE INDEX", "ADD CONSTRAINT tree_parent")
				requireSQLBefore(
					t,
					down,
					"DROP CONSTRAINT IF EXISTS tree_parent",
					"DROP INDEX IF EXISTS",
				)
				up, down = foreignKeyMigrationSQL(t, desired, current, 1)
				requireSQLBefore(
					t,
					up,
					"DROP CONSTRAINT IF EXISTS tree_parent",
					"DROP INDEX IF EXISTS",
				)
				requireSQLBefore(t, down, "CREATE UNIQUE INDEX", "ADD CONSTRAINT tree_parent")
			},
		)
	}
}

func foreignKeySchema(t *testing.T, sql string) *schema.Database {
	t.Helper()

	p := parser.New()
	db := &schema.Database{}
	require.NoError(t, p.ParseSQL(sql, db))
	require.Empty(t, p.GetErrors())

	return db
}

func foreignKeyMigrationSQL(
	t *testing.T,
	current, desired *schema.Database,
	maxOperations int,
) (string, string) {
	t.Helper()

	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.MaxOperationsPerFile = maxOperations
	generated, err := generator.New(opts).Generate(result)
	require.NoError(t, err)

	var up, down strings.Builder
	for _, migration := range generated.Migrations {
		up.WriteString(migration.UpFile.Content)
	}

	for i := len(generated.Migrations) - 1; i >= 0; i-- {
		down.WriteString(generated.Migrations[i].DownFile.Content)
	}

	return up.String(), down.String()
}

func requireSQLBefore(t *testing.T, sql, provider, dependent string) {
	t.Helper()

	providerPosition, dependentPosition := strings.Index(
		sql,
		provider,
	), strings.Index(
		sql,
		dependent,
	)
	require.GreaterOrEqual(t, providerPosition, 0, provider)
	require.GreaterOrEqual(t, dependentPosition, 0, dependent)
	require.Less(t, providerPosition, dependentPosition)
}
