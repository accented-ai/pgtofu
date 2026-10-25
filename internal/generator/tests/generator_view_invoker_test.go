package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/postgresparse"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestGeneratedViewInvokerPolicy(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		before      string
		after       string
		upInvoker   bool
		downInvoker bool
		definition  bool
		columnType  bool
	}{
		{"create", "", "true", true, false, false, false},
		{"enable", "false", "true", true, false, false, false},
		{"disable", "true", "false", false, true, false, false},
		{"replace definition", "true", "true", true, true, true, false},
		{"recreate dependency", "true", "true", true, true, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			before := &schema.Database{}
			if test.before != "" {
				before = viewInvokerSchema(t, test.before)
			}

			after := viewInvokerSchema(t, test.after)
			if test.definition {
				for index := range after.Views {
					after.Views[index].Definition += " WHERE id >= 0"
				}
			}

			if test.columnType {
				after.Tables[0].Columns[0].DataType = "bigint"
			}

			generated := policyMigration(t, before, after)
			assertGeneratedViewInvoker(t, generated, false, test.upInvoker)

			if test.before != "" {
				assertGeneratedViewInvoker(t, generated, true, test.downInvoker)
			}
		})
	}
}

func TestDefaultViewInvokerPolicyIsEquivalent(t *testing.T) {
	t.Parallel()

	current := viewInvokerSchema(t, "false")
	desired := viewInvokerSchema(t, "")
	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)
	assert.Empty(t, result.Changes)
}

func viewInvokerSchema(t *testing.T, option string) *schema.Database {
	t.Helper()

	options := ""
	if option != "" {
		options = " WITH (security_invoker = " + option + ")"
	}

	source := `CREATE TABLE view_invoker.documents (id integer, organization_id text);
		CREATE VIEW view_invoker.visible` + options + ` AS
		SELECT id, organization_id FROM view_invoker.documents;
		CREATE VIEW view_invoker.selected` + options + ` AS
		SELECT id, organization_id FROM view_invoker.visible;`
	desired := &schema.Database{}
	p := parser.New()
	require.NoError(t, p.ParseSQL(source, desired))
	require.Empty(t, p.GetErrors())

	return desired
}

func assertGeneratedViewInvoker(
	t *testing.T,
	generated *generator.GenerateResult,
	down, invoker bool,
) {
	t.Helper()

	views := map[string]bool{}

	for _, migration := range generated.Migrations {
		file := migration.UpFile
		if down {
			file = migration.DownFile
		}

		tree, err := postgresparse.Parse(file.Content)
		require.NoError(t, err)

		for _, statement := range tree.GetStmts() {
			view := statement.GetStmt().GetViewStmt()
			if view == nil {
				continue
			}

			actual := false

			for _, node := range view.GetOptions() {
				option := node.GetDefElem()
				if option.GetDefname() == "security_invoker" {
					actual = strings.EqualFold(option.GetArg().GetString_().GetSval(), "true") ||
						option.GetArg().GetBoolean().GetBoolval()
				}
			}

			views[view.GetView().GetRelname()] = actual
		}
	}

	require.Equal(t, map[string]bool{"visible": invoker, "selected": invoker}, views)
}
