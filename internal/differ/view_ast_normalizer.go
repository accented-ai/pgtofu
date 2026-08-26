package differ

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	wasmquery "github.com/wasilibs/go-pgquery"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The WASM PostgreSQL parser retains one runtime per concurrent caller. Schema
// comparison is sequential in normal use, while tests compare views in parallel.
var viewASTNormalizeMu sync.Mutex //nolint:gochecknoglobals

func (vn *viewNormalizer) definitionsEqual(current, desired string) bool {
	var (
		currentAST, currentOK = canonicalPostgresView(current)
		desiredAST, desiredOK = canonicalPostgresView(desired)
	)

	if currentOK && desiredOK && currentAST == desiredAST {
		return true
	}

	return vn.normalizeDefinition(current) == vn.normalizeDefinition(desired)
}

func canonicalPostgresView(definition string) (string, bool) {
	definition = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(definition), ";"))
	if definition == "" {
		return "", false
	}

	viewASTNormalizeMu.Lock()
	defer viewASTNormalizeMu.Unlock()

	tree, err := wasmquery.Parse(definition)
	if err != nil {
		return "", false
	}

	return canonicalPostgresMessage(tree.ProtoReflect()), true
}

func canonicalPostgresMessage(message protoreflect.Message) string {
	if node, ok := message.Interface().(*pgquery.Node); ok {
		return canonicalPostgresNode(node)
	}

	return canonicalPostgresMessageFields(message)
}

func canonicalPostgresMessageFields(message protoreflect.Message) string {
	fields := make([]string, 0, message.Descriptor().Fields().Len())

	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		name := string(field.Name())
		if isPostgresLocationField(name) ||
			isImplicitPostgresCaseDefault(message, field, value) {
			return true
		}

		fields = append(fields, name+":"+canonicalPostgresField(field, value))

		return true
	})
	sort.Strings(fields)

	return string(message.Descriptor().FullName()) + "{" + strings.Join(fields, ",") + "}"
}

func canonicalPostgresNode(node *pgquery.Node) string {
	if node == nil {
		return "null"
	}

	if cast := node.GetTypeCast(); cast != nil && isIgnorableViewTypeCastNode(cast) {
		return canonicalPostgresNode(cast.GetArg())
	}

	if expression := node.GetAExpr(); expression != nil {
		return canonicalPostgresAExpr(expression)
	}

	if expression := node.GetBoolExpr(); expression != nil {
		return canonicalPostgresBoolExpr(expression)
	}

	return canonicalPostgresMessageFields(node.ProtoReflect())
}

func canonicalPostgresField(
	field protoreflect.FieldDescriptor,
	value protoreflect.Value,
) string {
	if field.IsList() {
		list := value.List()
		items := make([]string, 0, list.Len())

		for index := range list.Len() {
			items = append(items, canonicalPostgresValue(field, list.Get(index)))
		}

		return "[" + strings.Join(items, ",") + "]"
	}

	return canonicalPostgresValue(field, value)
}

func canonicalPostgresValue(
	field protoreflect.FieldDescriptor,
	value protoreflect.Value,
) string {
	switch field.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return canonicalPostgresMessage(value.Message())
	case protoreflect.EnumKind:
		enumValue := field.Enum().Values().ByNumber(value.Enum())
		if enumValue == nil {
			return strconv.FormatInt(int64(value.Enum()), 10)
		}

		return strconv.Quote(string(enumValue.Name()))
	case protoreflect.BoolKind:
		return strconv.FormatBool(value.Bool())
	case protoreflect.StringKind:
		return strconv.Quote(value.String())
	case protoreflect.BytesKind:
		return strconv.Quote(string(value.Bytes()))
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return strconv.FormatFloat(value.Float(), 'g', -1, 64)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(value.Int(), 10)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(value.Uint(), 10)
	default:
		return fmt.Sprint(value.Interface())
	}
}

func canonicalPostgresAExpr(expression *pgquery.A_Expr) string {
	operator := postgresOperatorName(expression.GetName())

	switch expression.GetKind() {
	case pgquery.A_Expr_Kind_AEXPR_IN:
		return canonicalPostgresMembership(
			expression.GetLexpr(),
			postgresArrayElements(expression.GetRexpr()),
			operator == "<>",
		)
	case pgquery.A_Expr_Kind_AEXPR_OP_ANY:
		elements := postgresArrayElements(expression.GetRexpr())
		if operator == "=" && elements != nil {
			return canonicalPostgresMembership(expression.GetLexpr(), elements, false)
		}

		return canonicalPostgresQuantifiedComparison(expression, "any")
	case pgquery.A_Expr_Kind_AEXPR_OP_ALL:
		elements := postgresArrayElements(expression.GetRexpr())
		if operator == "<>" && elements != nil {
			return canonicalPostgresMembership(expression.GetLexpr(), elements, true)
		}

		return canonicalPostgresQuantifiedComparison(expression, "all")
	case pgquery.A_Expr_Kind_AEXPR_BETWEEN:
		return canonicalPostgresBetween(expression, false, false)
	case pgquery.A_Expr_Kind_AEXPR_NOT_BETWEEN:
		return canonicalPostgresBetween(expression, true, false)
	case pgquery.A_Expr_Kind_AEXPR_BETWEEN_SYM:
		return canonicalPostgresBetween(expression, false, true)
	case pgquery.A_Expr_Kind_AEXPR_NOT_BETWEEN_SYM:
		return canonicalPostgresBetween(expression, true, true)
	case pgquery.A_Expr_Kind_AEXPR_DISTINCT:
		return canonicalPostgresComparison(
			"is distinct from",
			expression.GetLexpr(),
			expression.GetRexpr(),
		)
	case pgquery.A_Expr_Kind_AEXPR_NOT_DISTINCT:
		return canonicalPostgresComparison(
			"is not distinct from",
			expression.GetLexpr(),
			expression.GetRexpr(),
		)
	case pgquery.A_Expr_Kind_AEXPR_SIMILAR:
		return canonicalPostgresSimilar(expression, operator)
	case pgquery.A_Expr_Kind_AEXPR_OP,
		pgquery.A_Expr_Kind_AEXPR_LIKE,
		pgquery.A_Expr_Kind_AEXPR_ILIKE:
		if isPostgresSimilarEscapeCall(expression.GetRexpr()) &&
			(operator == "~" || operator == "!~") {
			return canonicalPostgresSimilar(expression, operator)
		}

		return canonicalPostgresComparison(
			normalizePostgresOperator(operator),
			expression.GetLexpr(),
			expression.GetRexpr(),
		)
	default:
		return canonicalPostgresMessageFields(expression.ProtoReflect())
	}
}

func canonicalPostgresBoolExpr(expression *pgquery.BoolExpr) string {
	if expression.GetBoolop() == pgquery.BoolExprType_NOT_EXPR && len(expression.GetArgs()) == 1 {
		argument := expression.GetArgs()[0]
		if distinct := argument.GetAExpr(); distinct != nil &&
			distinct.GetKind() == pgquery.A_Expr_Kind_AEXPR_DISTINCT {
			return canonicalPostgresComparison(
				"is not distinct from",
				distinct.GetLexpr(),
				distinct.GetRexpr(),
			)
		}
	}

	arguments := make([]string, 0, len(expression.GetArgs()))
	for _, argument := range expression.GetArgs() {
		arguments = append(arguments, canonicalPostgresNode(argument))
	}

	return canonicalPostgresBoolean(
		strings.TrimSuffix(strings.ToLower(expression.GetBoolop().String()), "_expr"),
		arguments...,
	)
}

func canonicalPostgresMembership(
	left *pgquery.Node,
	elements []*pgquery.Node,
	negated bool,
) string {
	if len(elements) == 1 {
		operator := "="
		if negated {
			operator = "!="
		}

		return canonicalPostgresComparison(operator, left, elements[0])
	}

	values := make([]string, 0, len(elements))
	for _, element := range elements {
		values = append(values, canonicalPostgresNode(element))
	}

	operator := "in"
	if negated {
		operator = "not in"
	}

	return "membership(" + strconv.Quote(operator) + "," +
		canonicalPostgresNode(left) + ",[" + strings.Join(values, ",") + "])"
}

func canonicalPostgresQuantifiedComparison(
	expression *pgquery.A_Expr,
	quantifier string,
) string {
	return "quantified(" +
		strconv.Quote(normalizePostgresOperator(postgresOperatorName(expression.GetName()))) + "," +
		strconv.Quote(quantifier) + "," +
		canonicalPostgresNode(expression.GetLexpr()) + "," +
		canonicalPostgresNode(expression.GetRexpr()) + ")"
}

func canonicalPostgresBetween(expression *pgquery.A_Expr, negated, symmetric bool) string {
	bounds := postgresArrayElements(expression.GetRexpr())
	if len(bounds) != 2 {
		return canonicalPostgresMessageFields(expression.ProtoReflect())
	}

	forward := canonicalPostgresBoundPair(expression.GetLexpr(), bounds[0], bounds[1], negated)
	if !symmetric {
		return forward
	}

	reverse := canonicalPostgresBoundPair(expression.GetLexpr(), bounds[1], bounds[0], negated)

	outerOperator := "or"
	if negated {
		outerOperator = "and"
	}

	return canonicalPostgresBoolean(outerOperator, forward, reverse)
}

func canonicalPostgresBoundPair(value, lower, upper *pgquery.Node, negated bool) string {
	if negated {
		return canonicalPostgresBoolean(
			"or",
			canonicalPostgresComparison("<", value, lower),
			canonicalPostgresComparison(">", value, upper),
		)
	}

	return canonicalPostgresBoolean(
		"and",
		canonicalPostgresComparison(">=", value, lower),
		canonicalPostgresComparison("<=", value, upper),
	)
}

func canonicalPostgresBoolean(operator string, arguments ...string) string {
	return "boolean(" + strconv.Quote(operator) + ",[" + strings.Join(arguments, ",") + "])"
}

func canonicalPostgresComparison(operator string, left, right *pgquery.Node) string {
	return "comparison(" + strconv.Quote(operator) + "," +
		canonicalPostgresNode(left) + "," + canonicalPostgresNode(right) + ")"
}

func canonicalPostgresSimilar(expression *pgquery.A_Expr, operator string) string {
	call := expression.GetRexpr().GetFuncCall()

	arguments := make([]string, 0)
	if call != nil {
		arguments = make([]string, 0, len(call.GetArgs()))
		for _, argument := range call.GetArgs() {
			arguments = append(arguments, canonicalPostgresNode(argument))
		}
	}

	return "similar(" + strconv.FormatBool(operator == "!~") + "," +
		canonicalPostgresNode(expression.GetLexpr()) + ",[" +
		strings.Join(arguments, ",") + "])"
}

func postgresArrayElements(node *pgquery.Node) []*pgquery.Node {
	node = unwrapIgnorableViewTypeCast(node)
	if node == nil {
		return nil
	}

	if list := node.GetList(); list != nil {
		return list.GetItems()
	}

	if array := node.GetAArrayExpr(); array != nil {
		return array.GetElements()
	}

	return nil
}

func unwrapIgnorableViewTypeCast(node *pgquery.Node) *pgquery.Node {
	for node != nil {
		cast := node.GetTypeCast()
		if cast == nil || !isIgnorableViewTypeCastNode(cast) {
			return node
		}

		node = cast.GetArg()
	}

	return nil
}

func isIgnorableViewTypeCastNode(cast *pgquery.TypeCast) bool {
	if cast == nil {
		return false
	}

	if isIgnorableViewTypeCast(cast.GetTypeName()) {
		return true
	}

	return strings.EqualFold(
		postgresNodeListLastString(cast.GetTypeName().GetNames()),
		"unknown",
	) && isPostgresNullNode(cast.GetArg())
}

func isImplicitPostgresCaseDefault(
	message protoreflect.Message,
	field protoreflect.FieldDescriptor,
	value protoreflect.Value,
) bool {
	if field.Name() != "defresult" {
		return false
	}

	if _, ok := message.Interface().(*pgquery.CaseExpr); !ok {
		return false
	}

	node, ok := value.Message().Interface().(*pgquery.Node)

	return ok && isPostgresNullNode(node)
}

func isPostgresNullNode(node *pgquery.Node) bool {
	node = unwrapIgnorableViewTypeCast(node)
	if node == nil {
		return false
	}

	constant := node.GetAConst()

	return constant != nil && constant.GetIsnull()
}

func isIgnorableViewTypeCast(typeName *pgquery.TypeName) bool {
	if typeName == nil {
		return false
	}

	name := postgresNodeListLastString(typeName.GetNames())
	switch strings.ToLower(name) {
	case "numeric", "text", "int8", "bigint", "int4", "integer", "int2", "smallint",
		"float4", "real", "float8", "varchar", "bpchar", "timestamp", "timestamptz",
		"interval", "bool", "boolean", "json", "jsonb":
		return true
	default:
		return false
	}
}

func isPostgresSimilarEscapeCall(node *pgquery.Node) bool {
	if node == nil || node.GetFuncCall() == nil {
		return false
	}

	return strings.EqualFold(
		postgresNodeListLastString(node.GetFuncCall().GetFuncname()),
		"similar_to_escape",
	)
}

func postgresOperatorName(nodes []*pgquery.Node) string {
	return strings.ToLower(postgresNodeListLastString(nodes))
}

func postgresNodeListLastString(nodes []*pgquery.Node) string {
	if len(nodes) == 0 || nodes[len(nodes)-1].GetString_() == nil {
		return ""
	}

	return nodes[len(nodes)-1].GetString_().GetSval()
}

func normalizePostgresOperator(operator string) string {
	switch operator {
	case "<>", "!=":
		return "!="
	case "~~":
		return "like"
	case "!~~":
		return "not like"
	case "~~*":
		return "ilike"
	case "!~~*":
		return "not ilike"
	default:
		return operator
	}
}

func isPostgresLocationField(name string) bool {
	return strings.HasSuffix(name, "location") || name == "stmt_len"
}
