package differ

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/accented-ai/pgtofu/internal/postgresparse"
)

func policyExpressionsEqual(current, desired string) bool {
	currentAST, currentOK := canonicalPolicyExpression(current)

	desiredAST, desiredOK := canonicalPolicyExpression(desired)
	if currentOK && desiredOK {
		return currentAST == desiredAST
	}

	return strings.TrimSpace(current) == strings.TrimSpace(desired)
}

func canonicalPolicyExpression(expression string) (string, bool) {
	if strings.TrimSpace(expression) == "" {
		return "", true
	}

	tree, err := postgresparse.Parse("SELECT " + expression)
	if err != nil || len(tree.GetStmts()) != 1 {
		return "", false
	}

	statement := tree.GetStmts()[0].GetStmt().GetSelectStmt()
	if statement == nil || len(statement.GetTargetList()) != 1 {
		return "", false
	}

	return canonicalPolicyNode(statement.GetTargetList()[0].GetResTarget().GetVal()), true
}

func canonicalPolicyNode(node *pgquery.Node) string {
	if node == nil {
		return "null"
	}

	// PostgreSQL adds text casts to string literals during expression analysis.
	// Keep other casts: they can change which rows a policy admits.
	if cast := node.GetTypeCast(); cast != nil && isPolicyTextType(cast.GetTypeName()) &&
		len(cast.GetTypeName().GetTypmods()) == 0 &&
		len(cast.GetTypeName().GetArrayBounds()) == 0 {
		if constant := cast.GetArg().GetAConst(); constant != nil && constant.GetSval() != nil {
			return canonicalPolicyNode(cast.GetArg())
		}
	}

	if expression := node.GetAExpr(); expression != nil {
		return canonicalPolicyOperator(expression)
	}

	if expression := node.GetBoolExpr(); expression != nil {
		arguments := make([]string, 0, len(expression.GetArgs()))
		for _, argument := range expression.GetArgs() {
			arguments = append(arguments, canonicalPolicyNode(argument))
		}

		return policyBoolean(expression.GetBoolop().String(), arguments...)
	}

	if call := node.GetFuncCall(); call != nil {
		names := call.GetFuncname()
		if len(names) == 2 && (names[0].GetString_().GetSval() == "public" ||
			names[0].GetString_().GetSval() == "pg_catalog") {
			names = names[1:]
		}

		parts := make([]string, 0, len(names))
		for _, name := range names {
			parts = append(parts, canonicalPolicyNode(name))
		}

		return canonicalPolicyFields(call.ProtoReflect(), map[string]string{
			"funcname": "[" + strings.Join(parts, ",") + "]",
		})
	}

	return canonicalPolicyFields(node.ProtoReflect(), nil)
}

func isPolicyTextType(typeName *pgquery.TypeName) bool {
	names := typeName.GetNames()
	if len(names) == 1 {
		return names[0].GetString_().GetSval() == "text"
	}

	return len(names) == 2 && names[0].GetString_().GetSval() == "pg_catalog" &&
		names[1].GetString_().GetSval() == "text"
}

func canonicalPolicyOperator(expression *pgquery.A_Expr) string {
	operator := normalizePostgresOperator(postgresOperatorName(expression.GetName()))

	names := expression.GetName()
	if len(names) > 1 && names[0].GetString_().GetSval() != "pg_catalog" {
		operator = names[0].GetString_().GetSval() + "." + operator
	}

	left, right := expression.GetLexpr(), expression.GetRexpr()
	switch expression.GetKind() {
	case pgquery.A_Expr_Kind_AEXPR_IN:
		return policyMembership(left, right.GetList().GetItems(), operator != "=")
	case pgquery.A_Expr_Kind_AEXPR_OP_ANY:
		if operator == "=" && right.GetAArrayExpr() != nil {
			return policyMembership(left, right.GetAArrayExpr().GetElements(), false)
		}
	case pgquery.A_Expr_Kind_AEXPR_OP_ALL:
		if operator == "!=" && right.GetAArrayExpr() != nil {
			return policyMembership(left, right.GetAArrayExpr().GetElements(), true)
		}
	case pgquery.A_Expr_Kind_AEXPR_BETWEEN, pgquery.A_Expr_Kind_AEXPR_NOT_BETWEEN:
		bounds := right.GetList().GetItems()
		if len(bounds) == 2 {
			if expression.GetKind() == pgquery.A_Expr_Kind_AEXPR_NOT_BETWEEN {
				return policyBoolean("OR_EXPR", policyComparison("<", left, bounds[0]),
					policyComparison(">", left, bounds[1]))
			}

			return policyBoolean("AND_EXPR", policyComparison(">=", left, bounds[0]),
				policyComparison("<=", left, bounds[1]))
		}
	case pgquery.A_Expr_Kind_AEXPR_OP, pgquery.A_Expr_Kind_AEXPR_LIKE,
		pgquery.A_Expr_Kind_AEXPR_ILIKE:
		return policyComparison(operator, left, right)
	default:
	}

	return canonicalPolicyFields(expression.ProtoReflect(), nil)
}

func policyComparison(operator string, left, right *pgquery.Node) string {
	return "compare(" + strconv.Quote(operator) + "," +
		canonicalPolicyNode(left) + "," + canonicalPolicyNode(right) + ")"
}

func policyBoolean(operator string, arguments ...string) string {
	return "boolean(" + operator + ",[" + strings.Join(arguments, ",") + "])"
}

func policyMembership(left *pgquery.Node, values []*pgquery.Node, negated bool) string {
	if len(values) == 1 {
		operator := "="
		if negated {
			operator = "!="
		}

		return policyComparison(operator, left, values[0])
	}

	elements := make([]string, 0, len(values))
	for _, value := range values {
		elements = append(elements, canonicalPolicyNode(value))
	}

	return "membership(" + strconv.FormatBool(negated) + "," +
		canonicalPolicyNode(left) + ",[" + strings.Join(elements, ",") + "])"
}

func canonicalPolicyFields(message protoreflect.Message, overrides map[string]string) string {
	fields := make([]string, 0, message.Descriptor().Fields().Len())
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		name := string(field.Name())
		if isPostgresLocationField(name) {
			return true
		}

		canonical, overridden := overrides[name]
		if !overridden {
			canonical = canonicalPolicyField(field, value)
		}

		fields = append(fields, name+":"+canonical)

		return true
	})
	sort.Strings(fields)

	return string(message.Descriptor().FullName()) + "{" + strings.Join(fields, ",") + "}"
}

func canonicalPolicyField(field protoreflect.FieldDescriptor, value protoreflect.Value) string {
	if field.IsList() {
		list := value.List()

		items := make([]string, 0, list.Len())
		for i := range list.Len() {
			items = append(items, canonicalPolicyValue(field, list.Get(i)))
		}

		return "[" + strings.Join(items, ",") + "]"
	}

	return canonicalPolicyValue(field, value)
}

func canonicalPolicyValue(field protoreflect.FieldDescriptor, value protoreflect.Value) string {
	if field.Kind() == protoreflect.MessageKind {
		message := value.Message()
		if node, ok := message.Interface().(*pgquery.Node); ok {
			return canonicalPolicyNode(node)
		}

		return canonicalPolicyFields(message, nil)
	}

	if field.Kind() == protoreflect.StringKind {
		return strconv.Quote(value.String())
	}

	return fmt.Sprint(value.Interface())
}
