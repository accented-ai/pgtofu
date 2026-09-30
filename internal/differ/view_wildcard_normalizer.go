package differ

import (
	"maps"
	"sort"
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	wasmquery "github.com/wasilibs/go-pgquery"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/accented-ai/pgtofu/internal/schema"
)

// Catalog relations use qualified names; only scoped CTEs can be resolved
// without a schema name, since the connection's search path is unknown.
func postgresRelationColumns(db *schema.Database) map[string][]string {
	columns := make(map[string][]string)

	for _, table := range db.Tables {
		ordered := append([]schema.Column(nil), table.Columns...)
		sort.SliceStable(
			ordered,
			func(i, j int) bool { return ordered[i].Position < ordered[j].Position },
		)

		names := make([]string, 0, len(ordered))
		for _, column := range ordered {
			names = append(names, column.Name)
		}

		columns[table.QualifiedName()] = names
	}

	for _, view := range db.Views {
		columns[view.QualifiedName()] = postgresViewOutputNames(view.Definition)
	}

	for _, view := range db.MaterializedViews {
		columns[view.QualifiedName()] = postgresViewOutputNames(view.Definition)
	}

	return columns
}

func postgresViewOutputNames(definition string) []string {
	viewASTNormalizeMu.Lock()
	defer viewASTNormalizeMu.Unlock()

	tree, err := wasmquery.Parse(strings.TrimSpace(definition))
	if err != nil || len(tree.GetStmts()) != 1 {
		return nil
	}

	return postgresSelectOutputNames(tree.GetStmts()[0].GetStmt().GetSelectStmt())
}

func postgresSelectOutputNames(statement *pgquery.SelectStmt) []string {
	// Set operations inherit their output names from the leftmost SELECT.
	for statement != nil && len(statement.GetTargetList()) == 0 {
		statement = statement.GetLarg()
	}

	if statement == nil {
		return nil
	}

	columns := make([]string, 0, len(statement.GetTargetList()))
	for _, node := range statement.GetTargetList() {
		target := node.GetResTarget()
		if target == nil {
			return nil
		}

		name := target.GetName()
		if name == "" {
			name = postgresImplicitOutputName(target.GetVal())
		}

		if name == "" {
			return nil
		}

		columns = append(columns, name)
	}

	return columns
}

func postgresImplicitOutputName(node *pgquery.Node) string {
	if node == nil {
		return ""
	}

	if column := node.GetColumnRef(); column != nil {
		return postgresLastIdentifier(column.GetFields())
	}

	if function := node.GetFuncCall(); function != nil {
		return postgresLastIdentifier(function.GetFuncname())
	}

	if cast := node.GetTypeCast(); cast != nil {
		name := postgresImplicitOutputName(cast.GetArg())
		if name != "?column?" {
			return name
		}

		// A cast of an unnamed expression can take its name from the type.
		// Leave its output unresolved rather than guessing a catalog name.
		return ""
	}

	if expression := node.GetAExpr(); expression != nil {
		if expression.GetKind() == pgquery.A_Expr_Kind_AEXPR_NULLIF {
			return "nullif"
		}

		return "?column?"
	}

	if node.GetAConst() != nil || node.GetBoolExpr() != nil {
		return "?column?"
	}

	return ""
}

func postgresLastIdentifier(names []*pgquery.Node) string {
	if len(names) == 0 || names[len(names)-1].GetString_() == nil {
		return ""
	}

	return names[len(names)-1].GetString_().GetSval()
}

func expandPostgresRelationWildcards(message protoreflect.Message, columns map[string][]string) {
	if statement, ok := message.Interface().(*pgquery.SelectStmt); ok {
		if with := statement.GetWithClause(); with != nil {
			columns = postgresCTEColumns(with, columns)
		}

		expandPostgresSelectWildcard(statement, columns)
	}

	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind || field.Name() == "with_clause" {
			return true
		}

		if field.IsList() {
			list := value.List()
			for index := range list.Len() {
				expandPostgresRelationWildcards(list.Get(index).Message(), columns)
			}
		} else {
			expandPostgresRelationWildcards(value.Message(), columns)
		}

		return true
	})
}

func postgresCTEColumns(with *pgquery.WithClause, columns map[string][]string) map[string][]string {
	columns = maps.Clone(columns)
	if columns == nil {
		columns = make(map[string][]string)
	}

	// Recursive WITH names shadow outer CTEs before their queries run.
	if with.GetRecursive() {
		for _, node := range with.GetCtes() {
			if cte := node.GetCommonTableExpr(); cte != nil {
				columns[cte.GetCtename()] = nil
			}
		}
	}

	for _, node := range with.GetCtes() {
		cte := node.GetCommonTableExpr()
		if cte == nil || cte.GetCtequery() == nil {
			continue
		}

		expandPostgresRelationWildcards(cte.GetCtequery().ProtoReflect(), columns)
		columns[cte.GetCtename()] = postgresOutputAliases(
			postgresSelectOutputNames(cte.GetCtequery().GetSelectStmt()),
			cte.GetAliascolnames(),
		)
	}

	return columns
}

func expandPostgresSelectWildcard(statement *pgquery.SelectStmt, columns map[string][]string) {
	// Limit expansion to a single known relation, so joins and unresolved
	// projections keep their original structure for comparison.
	if len(statement.GetTargetList()) != 1 || len(statement.GetFromClause()) != 1 {
		return
	}

	relation := statement.GetFromClause()[0].GetRangeVar()
	if relation == nil {
		return
	}

	key := relation.GetRelname()
	if relation.GetSchemaname() != "" {
		key = relation.GetSchemaname() + "." + key
	}

	names := columns[key]
	if alias := relation.GetAlias(); alias != nil {
		names = postgresOutputAliases(names, alias.GetColnames())
	}

	if len(names) == 0 {
		return
	}

	target := statement.GetTargetList()[0].GetResTarget()
	if target == nil || target.GetName() != "" {
		return
	}

	column := target.GetVal().GetColumnRef()
	if column == nil || len(column.GetFields()) == 0 {
		return
	}

	fields := column.GetFields()
	if fields[len(fields)-1].GetAStar() == nil || len(fields) > 2 {
		return
	}

	qualifier := relation.GetRelname()
	if relation.GetAlias() != nil {
		qualifier = relation.GetAlias().GetAliasname()
	}

	if len(fields) == 2 && fields[0].GetString_().GetSval() != qualifier {
		return
	}

	targets := make([]*pgquery.Node, 0, len(names))
	for _, name := range names {
		targets = append(targets, &pgquery.Node{Node: &pgquery.Node_ResTarget{
			ResTarget: &pgquery.ResTarget{Val: &pgquery.Node{Node: &pgquery.Node_ColumnRef{
				ColumnRef: &pgquery.ColumnRef{Fields: []*pgquery.Node{
					{Node: &pgquery.Node_String_{String_: &pgquery.String{Sval: qualifier}}},
					{Node: &pgquery.Node_String_{String_: &pgquery.String{Sval: name}}},
				}},
			}}},
		}})
	}

	statement.TargetList = targets
}

func postgresOutputAliases(columns []string, aliases []*pgquery.Node) []string {
	if len(aliases) == 0 {
		return columns
	}

	if len(aliases) > len(columns) {
		return nil
	}

	columns = append([]string(nil), columns...)

	for index, node := range aliases {
		if node.GetString_() == nil {
			return nil
		}

		columns[index] = node.GetString_().GetSval()
	}

	return columns
}
