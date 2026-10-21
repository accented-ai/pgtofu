package generator_test

import (
	"context"
	"strings"
	"testing"
	"time"

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

func TestReferencedTableDropWaitsForForeignKeyRemoval(t *testing.T) {
	t.Parallel()

	current := foreignKeySchema(t, `
CREATE TABLE source_data.z_parents (id integer PRIMARY KEY);
CREATE TABLE catalog.a_children (
    id integer, parent_id integer,
    CONSTRAINT child_parent FOREIGN KEY (parent_id) REFERENCES source_data.z_parents (id)
);`)
	desired := foreignKeySchema(
		t,
		`CREATE TABLE catalog.a_children (id integer, parent_id integer);`,
	)

	for _, maxOperations := range []int{1, 50} {
		up, down := foreignKeyMigrationSQL(t, current, desired, maxOperations)
		requireSQLBefore(
			t,
			up,
			"DROP CONSTRAINT IF EXISTS child_parent",
			"DROP TABLE IF EXISTS source_data.z_parents",
		)
		requireSQLBefore(
			t,
			down,
			"CREATE TABLE source_data.z_parents",
			"ADD CONSTRAINT child_parent FOREIGN KEY",
		)
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

func TestForeignKeysSurviveReferencedKeyReplacement(t *testing.T) {
	t.Parallel()

	for _, self := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "related table", true: "self reference"}[self],
			func(t *testing.T) {
				t.Parallel()

				before := `CREATE TABLE catalog.parents (
				partition_key text, id integer, parent_id integer,
				CONSTRAINT parent_key PRIMARY KEY (id)`
				after := strings.Replace(
					before,
					"PRIMARY KEY (id)",
					"PRIMARY KEY (partition_key, id)",
					1,
				)
				foreign := `, CONSTRAINT child_parent FOREIGN KEY (parent_id) REFERENCES catalog.parents (id)`

				if !self {
					before += `); CREATE TABLE catalog.children (id integer PRIMARY KEY, partition_key text, parent_id integer`
					after += `); CREATE TABLE catalog.children (id integer PRIMARY KEY, partition_key text, parent_id integer`
				}

				before += foreign + ");"
				after += strings.ReplaceAll(foreign, "(parent_id)", "(partition_key, parent_id)")
				after = strings.ReplaceAll(
					after,
					"REFERENCES catalog.parents (id)",
					"REFERENCES catalog.parents (partition_key, id)",
				) + ");"

				current, desired := foreignKeySchema(t, before), foreignKeySchema(t, after)
				for _, maxOperations := range []int{1, 50} {
					for _, pair := range [][2]*schema.Database{{current, desired}, {desired, current}} {
						up, down := foreignKeyMigrationSQL(t, pair[0], pair[1], maxOperations)
						for _, sql := range []string{up, down} {
							requireSQLBefore(
								t,
								sql,
								"DROP CONSTRAINT IF EXISTS child_parent",
								"DROP CONSTRAINT IF EXISTS parent_key",
							)
							requireSQLBefore(
								t,
								sql,
								"ADD CONSTRAINT parent_key",
								"ADD CONSTRAINT child_parent",
							)
						}
					}
				}
			},
		)
	}
}

func TestUnchangedForeignKeysSurviveReferencedKeyReplacement(t *testing.T) {
	t.Parallel()

	for _, index := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "unique constraint", true: "unique index"}[index],
			func(t *testing.T) {
				t.Parallel()

				before := `CREATE TABLE catalog.parents (id integer, scope integer`
				if index {
					before += `); CREATE UNIQUE INDEX parent_key ON catalog.parents (id, scope);`
				} else {
					before += `, CONSTRAINT parent_key UNIQUE (id, scope));`
				}

				after := strings.ReplaceAll(before, "(id, scope)", "(scope, id)")
				child := `CREATE TABLE catalog.children (parent_id integer, parent_scope integer,
				CONSTRAINT child_parent FOREIGN KEY (parent_id, parent_scope) REFERENCES catalog.parents (id, scope));`

				current, desired := foreignKeySchema(
					t,
					before+child,
				), foreignKeySchema(
					t,
					after+child,
				)
				for _, pair := range [][2]*schema.Database{{current, desired}, {desired, current}} {
					up, down := foreignKeyMigrationSQL(t, pair[0], pair[1], 1)
					for _, sql := range []string{up, down} {
						provider := "ADD CONSTRAINT parent_key"
						if index {
							provider = "CREATE UNIQUE INDEX"
						}

						requireSQLBefore(t, sql, "DROP CONSTRAINT IF EXISTS child_parent", provider)
						requireSQLBefore(t, sql, provider, "ADD CONSTRAINT child_parent")
					}
				}
			},
		)
	}
}

func TestForeignKeyRecreationRetainsIgnoredConstraintName(t *testing.T) {
	t.Parallel()

	before := `CREATE TABLE catalog.parents (id integer, scope integer,
		CONSTRAINT parent_key UNIQUE (id, scope));
	CREATE TABLE catalog.children (parent_id integer, parent_scope integer,
		CONSTRAINT retained_name FOREIGN KEY (parent_id, parent_scope) REFERENCES catalog.parents (id, scope));`
	after := strings.ReplaceAll(before, "UNIQUE (id, scope)", "UNIQUE (scope, id)")
	after = strings.ReplaceAll(after, "retained_name", "ignored_name")
	options := differ.DefaultOptions()
	options.IgnoreConstraintNames = true
	result, err := differ.New(options).
		Compare(foreignKeySchema(t, before), foreignKeySchema(t, after))
	require.NoError(t, err)
	generated, err := generator.New(testOptions()).Generate(result)
	require.NoError(t, err)
	require.Len(t, generated.Migrations, 1)

	for _, file := range []*generator.MigrationFile{generated.Migrations[0].UpFile, generated.Migrations[0].DownFile} {
		require.Contains(t, file.Content, "ADD CONSTRAINT retained_name")
		require.NotContains(t, file.Content, "ignored_name")
	}
}

func TestForeignKeyRecreationWaitsForItsRemoval(t *testing.T) {
	t.Parallel()

	for _, index := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "unique constraint", true: "unique index"}[index],
			func(t *testing.T) {
				t.Parallel()

				before := `CREATE TABLE catalog.a_parents (id integer`
				if index {
					before += `); CREATE UNIQUE INDEX old_key ON catalog.a_parents (id);`
				} else {
					before += `, CONSTRAINT old_key UNIQUE (id));`
				}

				before += `CREATE TABLE catalog.z_children (parent_id integer,
				CONSTRAINT child_parent FOREIGN KEY (parent_id) REFERENCES catalog.a_parents (id));`
				after := strings.ReplaceAll(before, "old_key", "new_key")

				current, desired := foreignKeySchema(t, before), foreignKeySchema(t, after)
				for _, maxOperations := range []int{1, 50} {
					for _, pair := range [][2]*schema.Database{{current, desired}, {desired, current}} {
						up, down := foreignKeyMigrationSQL(t, pair[0], pair[1], maxOperations)
						for _, sql := range []string{up, down} {
							requireSQLBefore(
								t,
								sql,
								"DROP CONSTRAINT IF EXISTS child_parent",
								"ADD CONSTRAINT child_parent",
							)
						}

						result, err := differ.New(nil).Compare(pair[0], pair[1])
						require.NoError(t, err)

						options := testOptions()
						options.MaxOperationsPerFile = maxOperations
						generated, err := generator.New(options).Generate(result)
						require.NoError(t, err)

						for _, migration := range generated.Migrations {
							for _, file := range []*generator.MigrationFile{migration.UpFile, migration.DownFile} {
								require.Equal(
									t,
									strings.Contains(
										file.Content,
										"DROP CONSTRAINT IF EXISTS child_parent",
									),
									strings.Contains(file.Content, "ADD CONSTRAINT child_parent"),
									"foreign key recreation must stay in one transaction",
								)
							}
						}
					}
				}
			},
		)
	}
}

//nolint:paralleltest // Replays key replacements in a dedicated mutable schema.
func TestPostgresForeignKeyReplacementRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	conn, e := policyIntegrationDatabase(t, ctx, "key_replacement")
	before := `CREATE TABLE key_replacement.parents (
		partition_key text NOT NULL, id integer NOT NULL, parent_id integer,
		CONSTRAINT parent_key PRIMARY KEY (id),
		CONSTRAINT parent_self FOREIGN KEY (parent_id) REFERENCES key_replacement.parents (id));
	CREATE TABLE key_replacement.lookup_entries (
		partition_key text NOT NULL, lookup_key text NOT NULL, entry_number integer NOT NULL,
		CONSTRAINT lookup_entry_key UNIQUE (lookup_key, entry_number));
	CREATE TABLE key_replacement.children (
		partition_key text NOT NULL, id integer PRIMARY KEY, parent_id integer,
		target_lookup_key text, target_entry_number integer,
		CONSTRAINT child_parent FOREIGN KEY (parent_id) REFERENCES key_replacement.parents (id),
		CONSTRAINT child_lookup FOREIGN KEY (target_lookup_key, target_entry_number)
			REFERENCES key_replacement.lookup_entries (lookup_key, entry_number));`
	after := strings.NewReplacer(
		"CONSTRAINT parent_key PRIMARY KEY (id)",
		"CONSTRAINT parent_key PRIMARY KEY (partition_key, id)",
		"UNIQUE (lookup_key, entry_number)",
		"UNIQUE (partition_key, lookup_key, entry_number)",
		"FOREIGN KEY (parent_id)",
		"FOREIGN KEY (partition_key, parent_id)",
		"REFERENCES key_replacement.parents (id)",
		"REFERENCES key_replacement.parents (partition_key, id)",
		"FOREIGN KEY (target_lookup_key, target_entry_number)",
		"FOREIGN KEY (partition_key, target_lookup_key, target_entry_number)",
		"REFERENCES key_replacement.lookup_entries (lookup_key, entry_number)",
		"REFERENCES key_replacement.lookup_entries (partition_key, lookup_key, entry_number)",
	).Replace(before)
	_, err := conn.Exec(ctx, before+`
		INSERT INTO key_replacement.parents VALUES ('A', 1, 1);
		INSERT INTO key_replacement.lookup_entries VALUES ('A', 'lookup', 1);
		INSERT INTO key_replacement.children VALUES ('A', 1, 1, 'lookup', 1), ('A', 2, 1, NULL, NULL);`)
	require.NoError(t, err)
	current, desired := foreignKeySchema(t, before), foreignKeySchema(t, after)
	migration := policyMigration(t, current, desired)
	require.Len(
		t,
		migration.Migrations,
		1,
		"foreign key removal must not outlive its key replacement transaction",
	)

	for _, down := range []bool{false, true, false} {
		applyPolicyMigration(t, ctx, conn, migration, down)

		want := desired
		if down {
			want = current
		}

		assertPolicyConvergence(t, ctx, e, "key_replacement", want)

		var count int
		require.NoError(
			t,
			conn.QueryRow(ctx, "SELECT count(*) FROM key_replacement.children").Scan(&count),
		)
		require.Equal(t, 2, count)
	}

	_, err = conn.Exec(ctx, `INSERT INTO key_replacement.parents VALUES ('B', 1, NULL);
		INSERT INTO key_replacement.lookup_entries VALUES ('B', 'lookup', 1);
		INSERT INTO key_replacement.children VALUES ('B', 3, 1, 'lookup', 1);`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO key_replacement.children VALUES ('C', 4, 1, 'lookup', 1);`)
	require.ErrorContains(t, err, "violates foreign key constraint")
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
