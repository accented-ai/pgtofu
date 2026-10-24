package differ

import (
	"fmt"
	"maps"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func normalizePostgresViewAliases(message protoreflect.Message) bool {
	// Function sources and ambiguous whole-row references keep their original
	// names until their namespaces can be resolved conservatively.
	if !postgresViewAliasSourcesSupported(message) {
		return false
	}

	normalizePostgresAliasReferences(message, nil, 0)

	return true
}

func postgresViewAliasSourcesSupported(message protoreflect.Message) bool {
	// A single name can resolve to either a column or a whole-row alias.
	if column, ok := message.Interface().(*pgquery.ColumnRef); ok &&
		len(column.GetFields()) == 1 && column.GetFields()[0].GetString_() != nil {
		return false
	}

	if statement, ok := message.Interface().(*pgquery.SelectStmt); ok {
		for _, source := range statement.GetFromClause() {
			if !postgresAliasSourceSupported(source) {
				return false
			}
		}
	}

	supported := true

	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind {
			return true
		}

		if field.IsList() {
			list := value.List()
			for index := range list.Len() {
				if !postgresViewAliasSourcesSupported(list.Get(index).Message()) {
					supported = false
					return false
				}
			}
		} else {
			supported = postgresViewAliasSourcesSupported(value.Message())
		}

		return supported
	})

	return supported
}

func postgresAliasSourceSupported(source *pgquery.Node) bool {
	if source.GetRangeVar() != nil || source.GetRangeSubselect() != nil {
		return true
	}

	if join := source.GetJoinExpr(); join != nil {
		return postgresAliasSourceSupported(join.GetLarg()) &&
			postgresAliasSourceSupported(join.GetRarg())
	}

	return false
}

func normalizePostgresAliasReferences(
	message protoreflect.Message,
	aliases map[string]string,
	depth int,
) {
	if statement, ok := message.Interface().(*pgquery.SelectStmt); ok {
		normalizePostgresSelectAliases(statement, aliases, depth)
		return
	}

	if column, ok := message.Interface().(*pgquery.ColumnRef); ok && len(column.GetFields()) == 2 {
		if qualifier := column.GetFields()[0].GetString_(); qualifier != nil {
			if binding := aliases[qualifier.GetSval()]; binding != "" {
				qualifier.Sval = binding
			}
		}
	}

	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind {
			return true
		}

		if field.IsList() {
			list := value.List()
			for index := range list.Len() {
				normalizePostgresAliasReferences(list.Get(index).Message(), aliases, depth)
			}
		} else {
			normalizePostgresAliasReferences(value.Message(), aliases, depth)
		}

		return true
	})
}

func normalizePostgresSelectAliases(
	statement *pgquery.SelectStmt,
	parent map[string]string,
	depth int,
) {
	if with := statement.GetWithClause(); with != nil {
		for _, node := range with.GetCtes() {
			if cte := node.GetCommonTableExpr(); cte != nil && cte.GetCtequery() != nil {
				normalizePostgresAliasReferences(cte.GetCtequery().ProtoReflect(), parent, depth+1)
			}
		}
	}

	aliases := clonePostgresAliases(parent)
	nextAlias := 0

	for _, source := range statement.GetFromClause() {
		maps.Copy(aliases, normalizePostgresFromAliases(source, parent, aliases, depth, &nextAlias))
	}

	message := statement.ProtoReflect()
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Name() == "from_clause" || field.Name() == "with_clause" ||
			field.Kind() != protoreflect.MessageKind {
			return true
		}

		if field.IsList() {
			list := value.List()
			for index := range list.Len() {
				normalizePostgresAliasReferences(list.Get(index).Message(), aliases, depth+1)
			}
		} else {
			normalizePostgresAliasReferences(value.Message(), aliases, depth+1)
		}

		return true
	})
}

func normalizePostgresFromAliases(
	source *pgquery.Node,
	parent map[string]string,
	visible map[string]string,
	depth int,
	nextAlias *int,
) map[string]string {
	if relation := source.GetRangeVar(); relation != nil {
		return postgresRelationAliasBinding(
			relation.GetAlias(),
			relation.GetRelname(),
			depth,
			nextAlias,
		)
	}

	if derived := source.GetRangeSubselect(); derived != nil {
		inherited := parent
		if derived.GetLateral() {
			inherited = visible
		}

		if query := derived.GetSubquery(); query != nil {
			normalizePostgresAliasReferences(query.ProtoReflect(), inherited, depth+1)
		}

		return postgresRelationAliasBinding(derived.GetAlias(), "", depth, nextAlias)
	}

	join := source.GetJoinExpr()
	bindings := normalizePostgresFromAliases(join.GetLarg(), parent, visible, depth, nextAlias)
	rightVisible := clonePostgresAliases(visible)
	maps.Copy(rightVisible, bindings)
	right := normalizePostgresFromAliases(join.GetRarg(), parent, rightVisible, depth, nextAlias)
	maps.Copy(bindings, right)

	if join.GetQuals() != nil {
		// JOIN conditions see their operands and enclosing queries, not earlier
		// comma-separated FROM items visible to a LATERAL right operand.
		joinAliases := clonePostgresAliases(parent)
		maps.Copy(joinAliases, bindings)
		normalizePostgresAliasReferences(join.GetQuals().ProtoReflect(), joinAliases, depth+1)
	}

	if alias := join.GetJoinUsingAlias(); alias != nil {
		maps.Copy(bindings, postgresRelationAliasBinding(alias, "", depth, nextAlias))
	}

	if alias := join.GetAlias(); alias != nil {
		return postgresRelationAliasBinding(alias, "", depth, nextAlias)
	}

	return bindings
}

func postgresRelationAliasBinding(
	alias *pgquery.Alias,
	relation string,
	depth int,
	nextAlias *int,
) map[string]string {
	if alias == nil {
		// An unaliased local relation shadows a correlated outer alias.
		return map[string]string{relation: ""}
	}

	name := alias.GetAliasname()

	// NUL cannot occur in a SQL identifier, so these labels cannot collide with
	// an actual alias, relation name, or correlated reference.
	alias.Aliasname = fmt.Sprintf("\x00relation:%d:%d", depth, *nextAlias)
	*nextAlias++

	return map[string]string{name: alias.GetAliasname()}
}

func clonePostgresAliases(aliases map[string]string) map[string]string {
	cloned := maps.Clone(aliases)
	if cloned == nil {
		cloned = make(map[string]string)
	}

	return cloned
}
