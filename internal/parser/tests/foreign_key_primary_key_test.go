package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestForeignKeysResolveImplicitPrimaryKeyColumns(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, child string
		columns     []string
	}{
		{
			name: "inline reference",
			child: `CREATE TABLE catalog.children (
				parent_id integer REFERENCES catalog.parents
				ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED);`,
			columns: []string{"id"},
		},
		{
			name: "table reference",
			child: `CREATE TABLE catalog.children (parent_id integer,
				FOREIGN KEY (parent_id) REFERENCES catalog.parents
				ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED);`,
			columns: []string{"id"},
		},
		{
			name: "composite reference",
			child: `CREATE TABLE catalog.children (parent_id integer, parent_scope integer,
				FOREIGN KEY (parent_id, parent_scope) REFERENCES catalog.parents
				ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED);`,
			columns: []string{"id", "scope"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			p := parser.New()
			db := &schema.Database{}
			require.NoError(t, p.ParseSQL(test.child, db))
			require.Empty(t, p.GetErrors())
			require.Len(t, db.Tables[0].Constraints, 1)

			primary := "id"
			if len(test.columns) > 1 {
				primary += ", scope"
			}

			require.NoError(t, p.ParseSQL(`CREATE TABLE catalog.parents (
				id integer, scope integer, PRIMARY KEY (`+primary+`));`, db))
			require.Empty(t, p.GetErrors())

			child := db.GetTable("catalog", "children")
			require.NotNil(t, child)
			foreign := child.Constraints[0]
			require.True(t, foreign.IsForeignKey())
			require.Equal(t, "catalog.parents", foreign.QualifiedReferencedTable())
			require.Equal(t, test.columns, foreign.ReferencedColumns)
			require.Equal(t, "CASCADE", foreign.OnDelete)
			require.True(t, foreign.IsDeferrable)
			require.True(t, foreign.InitiallyDeferred)
		})
	}
}
