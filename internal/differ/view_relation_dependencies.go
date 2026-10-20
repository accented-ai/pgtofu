package differ

import (
	"maps"
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	wasmquery "github.com/wasilibs/go-pgquery"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func postgresRelationDependencies(definition string) ([]string, bool) {
	viewASTNormalizeMu.Lock()
	defer viewASTNormalizeMu.Unlock()

	tree, err := wasmquery.Parse(strings.TrimSpace(definition))
	if err != nil {
		return nil, false
	}

	var dependencies []string

	collectPostgresRelationDependencies(tree.ProtoReflect(), nil, &dependencies)

	return dedupeDependencies(dependencies), true
}

func collectPostgresRelationDependencies(
	message protoreflect.Message,
	ctes map[string]bool,
	dependencies *[]string,
) {
	if relation, ok := message.Interface().(*pgquery.RangeVar); ok {
		if relation.GetSchemaname() == "" && ctes[relation.GetRelname()] {
			return
		}

		name := relation.GetRelname()
		if relation.GetSchemaname() != "" {
			name = relation.GetSchemaname() + "." + name
		}

		*dependencies = append(*dependencies, name)

		return
	}

	statement, isSelect := message.Interface().(*pgquery.SelectStmt)
	if isSelect && statement.GetWithClause() != nil {
		ctes = collectPostgresCTERelationDependencies(statement.GetWithClause(), ctes, dependencies)
	}

	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind || (isSelect && field.Name() == "with_clause") {
			return true
		}

		if field.IsList() {
			list := value.List()
			for index := range list.Len() {
				collectPostgresRelationDependencies(list.Get(index).Message(), ctes, dependencies)
			}
		} else {
			collectPostgresRelationDependencies(value.Message(), ctes, dependencies)
		}

		return true
	})
}

func collectPostgresCTERelationDependencies(
	with *pgquery.WithClause,
	parent map[string]bool,
	dependencies *[]string,
) map[string]bool {
	ctes := maps.Clone(parent)
	if ctes == nil {
		ctes = make(map[string]bool)
	}

	if with.GetRecursive() {
		for _, node := range with.GetCtes() {
			if cte := node.GetCommonTableExpr(); cte != nil {
				ctes[cte.GetCtename()] = true
			}
		}
	}

	for _, node := range with.GetCtes() {
		cte := node.GetCommonTableExpr()
		if cte == nil || cte.GetCtequery() == nil {
			continue
		}

		collectPostgresRelationDependencies(cte.GetCtequery().ProtoReflect(), ctes, dependencies)
		ctes[cte.GetCtename()] = true
	}

	return ctes
}
