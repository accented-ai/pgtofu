package generator

import (
	pgquery "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func normalizeViewJoinConditionOrder(tree *pgquery.ParseResult) {
	var visit func(protoreflect.Message)

	visit = func(message protoreflect.Message) {
		if join, ok := message.Interface().(*pgquery.JoinExpr); ok {
			normalizeViewJoin(join)
		}

		message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if field.IsList() && field.Kind() == protoreflect.MessageKind {
				list := value.List()
				for index := range list.Len() {
					visit(list.Get(index).Message())
				}

				return true
			}

			if field.Kind() == protoreflect.MessageKind && message.Has(field) {
				visit(value.Message())
			}

			return true
		})
	}

	visit(tree.ProtoReflect())
}

func normalizeViewJoin(join *pgquery.JoinExpr) {
	leftAliases := viewRelationAliases(join.GetLarg())

	rightAliases := viewRelationAliases(join.GetRarg())
	if len(leftAliases) == 0 || len(rightAliases) == 0 {
		return
	}

	normalizeViewJoinQualifiers(join.GetQuals(), leftAliases, rightAliases)
}

func normalizeViewJoinQualifiers(
	node *pgquery.Node,
	leftAliases map[string]struct{},
	rightAliases map[string]struct{},
) {
	if node == nil {
		return
	}

	if boolean := node.GetBoolExpr(); boolean != nil {
		for _, argument := range boolean.GetArgs() {
			normalizeViewJoinQualifiers(argument, leftAliases, rightAliases)
		}

		return
	}

	expression := node.GetAExpr()
	if expression == nil {
		return
	}

	reversedOperator, ok := reversibleViewJoinOperator(expression)
	if !ok {
		return
	}

	leftRelation := viewColumnRelation(expression.GetLexpr())

	rightRelation := viewColumnRelation(expression.GetRexpr())
	if !viewAliasSetContains(rightAliases, leftRelation) ||
		!viewAliasSetContains(leftAliases, rightRelation) {
		return
	}

	leftExpression := expression.GetLexpr()
	rightExpression := expression.GetRexpr()

	expression.Lexpr, expression.Rexpr = rightExpression, leftExpression
	if reversedOperator != "" {
		expression.Name[0].GetString_().Sval = reversedOperator
	}
}

func reversibleViewJoinOperator(expression *pgquery.A_Expr) (string, bool) {
	switch expression.GetKind() {
	case pgquery.A_Expr_Kind_AEXPR_DISTINCT,
		pgquery.A_Expr_Kind_AEXPR_NOT_DISTINCT:
		return "", true
	case pgquery.A_Expr_Kind_AEXPR_OP:
		if len(expression.GetName()) != 1 || expression.GetName()[0].GetString_() == nil {
			return "", false
		}

		switch expression.GetName()[0].GetString_().GetSval() {
		case "=":
			return "=", true
		case "<>":
			return "<>", true
		case "!=":
			return "!=", true
		case "<":
			return ">", true
		case ">":
			return "<", true
		case "<=":
			return ">=", true
		case ">=":
			return "<=", true
		default:
			return "", false
		}
	default:
		return "", false
	}
}

func viewRelationAliases(node *pgquery.Node) map[string]struct{} {
	aliases := make(map[string]struct{})
	collectViewRelationAliases(node, aliases)

	return aliases
}

func collectViewRelationAliases(node *pgquery.Node, aliases map[string]struct{}) {
	if node == nil {
		return
	}

	switch {
	case node.GetRangeVar() != nil:
		relation := node.GetRangeVar()
		if !addViewRelationAlias(aliases, relation.GetAlias()) {
			aliases[relation.GetRelname()] = struct{}{}
		}
	case node.GetJoinExpr() != nil:
		join := node.GetJoinExpr()
		if addViewRelationAlias(aliases, join.GetAlias()) {
			return
		}

		collectViewRelationAliases(join.GetLarg(), aliases)
		collectViewRelationAliases(join.GetRarg(), aliases)
	case node.GetRangeSubselect() != nil:
		addViewRelationAlias(aliases, node.GetRangeSubselect().GetAlias())
	case node.GetRangeFunction() != nil:
		addViewRelationAlias(aliases, node.GetRangeFunction().GetAlias())
	case node.GetRangeTableFunc() != nil:
		addViewRelationAlias(aliases, node.GetRangeTableFunc().GetAlias())
	case node.GetJsonTable() != nil:
		addViewRelationAlias(aliases, node.GetJsonTable().GetAlias())
	case node.GetRangeTableSample() != nil:
		collectViewRelationAliases(node.GetRangeTableSample().GetRelation(), aliases)
	}
}

func addViewRelationAlias(aliases map[string]struct{}, alias *pgquery.Alias) bool {
	if alias == nil || alias.GetAliasname() == "" {
		return false
	}

	aliases[alias.GetAliasname()] = struct{}{}

	return true
}

func viewColumnRelation(node *pgquery.Node) string {
	column := node.GetColumnRef()
	if column == nil || len(column.GetFields()) < 2 {
		return ""
	}

	fields := column.GetFields()
	relation := fields[len(fields)-2].GetString_()

	name := fields[len(fields)-1].GetString_()
	if relation == nil || name == nil {
		return ""
	}

	return relation.GetSval()
}

func viewAliasSetContains(aliases map[string]struct{}, alias string) bool {
	if alias == "" {
		return false
	}

	_, ok := aliases[alias]

	return ok
}
