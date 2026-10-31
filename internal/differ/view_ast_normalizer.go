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

type postgresViewNormalization uint8

const (
	legacyPostgresView postgresViewNormalization = iota
	sourcePostgresView
	catalogPostgresView
)

type postgresASTNormalizer struct {
	mode postgresViewNormalization
}

func (vn *viewNormalizer) definitionsEqual(current, desired string) bool {
	var (
		currentAST, currentOK, currentScoped = canonicalPostgresView(current, vn.currentColumns)
		desiredAST, desiredOK, desiredScoped = canonicalPostgresView(desired, vn.desiredColumns)
	)

	if currentOK && desiredOK {
		if currentAST == desiredAST {
			return true
		}

		// The text fallback can erase binding differences in qualified columns.
		if currentScoped && desiredScoped {
			return false
		}
	}

	return vn.normalizeDefinition(current) == vn.normalizeDefinition(desired)
}

func canonicalPostgresView(definition string, columns map[string][]string) (string, bool, bool) {
	return canonicalPostgresViewMode(definition, columns, legacyPostgresView)
}

func canonicalPostgresViewMode(
	definition string,
	columns map[string][]string,
	mode postgresViewNormalization,
) (string, bool, bool) {
	definition = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(definition), ";"))
	if definition == "" {
		return "", false, false
	}

	tree, err := postgresparse.Parse(definition)
	if err != nil {
		return "", false, false
	}

	expandPostgresRelationWildcards(tree.ProtoReflect(), columns)
	scopedAliases := normalizePostgresViewAliases(tree.ProtoReflect())

	return (postgresASTNormalizer{mode: mode}).message(tree.ProtoReflect()), true, scopedAliases
}

func canonicalPostgresExpression(expression string) (string, bool) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return "", false
	}

	tree, err := postgresparse.Parse("SELECT " + expression)
	if err != nil || len(tree.GetStmts()) != 1 {
		return "", false
	}

	statement := tree.GetStmts()[0].GetStmt().GetSelectStmt()
	if statement == nil || len(statement.GetTargetList()) != 1 {
		return "", false
	}

	target := statement.GetTargetList()[0].GetResTarget()
	if target == nil || target.GetVal() == nil {
		return "", false
	}

	return canonicalPostgresNode(target.GetVal()), true
}

func canonicalPostgresNode(node *pgquery.Node) string {
	return (postgresASTNormalizer{mode: legacyPostgresView}).node(node)
}

func (n postgresASTNormalizer) message(message protoreflect.Message) string {
	if node, ok := message.Interface().(*pgquery.Node); ok {
		return n.node(node)
	}

	return n.messageFields(message)
}

func (n postgresASTNormalizer) messageFields(message protoreflect.Message) string {
	fields := make([]string, 0, message.Descriptor().Fields().Len())

	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		name := string(field.Name())
		if isPostgresLocationField(name) ||
			isPostgresAnonymousTargetName(message, name) ||
			n.isImplicitCaseDefault(message, field, value) ||
			isDefaultPostgresAscendingSort(message, field, value) {
			return true
		}

		fields = append(fields, name+":"+n.field(field, value))

		return true
	})
	sort.Strings(fields)

	return string(message.Descriptor().FullName()) + "{" + strings.Join(fields, ",") + "}"
}

func isPostgresAnonymousTargetName(message protoreflect.Message, field string) bool {
	target, ok := message.Interface().(*pgquery.ResTarget)

	return ok && field == "name" && target.GetName() == "?column?" &&
		postgresImplicitOutputName(target.GetVal()) == "?column?"
}

func (n postgresASTNormalizer) node(node *pgquery.Node) string {
	if node == nil {
		return "null"
	}

	if cast := node.GetTypeCast(); cast != nil && n.isIgnorableTypeCast(cast) {
		return n.node(cast.GetArg())
	}

	if expression := node.GetAExpr(); expression != nil {
		if n.mode != legacyPostgresView && len(expression.GetName()) > 1 {
			return n.messageFields(expression.ProtoReflect())
		}

		return n.aExpr(expression)
	}

	if expression := node.GetBoolExpr(); expression != nil {
		return n.boolExpr(expression)
	}

	return n.messageFields(node.ProtoReflect())
}

func (n postgresASTNormalizer) field(
	field protoreflect.FieldDescriptor,
	value protoreflect.Value,
) string {
	if field.IsList() {
		list := value.List()
		items := make([]string, 0, list.Len())

		for index := range list.Len() {
			items = append(items, n.value(field, list.Get(index)))
		}

		return "[" + strings.Join(items, ",") + "]"
	}

	return n.value(field, value)
}

func (n postgresASTNormalizer) value(
	field protoreflect.FieldDescriptor,
	value protoreflect.Value,
) string {
	switch field.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return n.message(value.Message())
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

func (n postgresASTNormalizer) aExpr(expression *pgquery.A_Expr) string {
	operator := postgresOperatorName(expression.GetName())

	switch expression.GetKind() {
	case pgquery.A_Expr_Kind_AEXPR_IN:
		return n.membership(
			expression.GetLexpr(),
			n.arrayElements(expression.GetRexpr()),
			operator == "<>",
		)
	case pgquery.A_Expr_Kind_AEXPR_OP_ANY:
		elements := n.arrayElements(expression.GetRexpr())
		if operator == "=" && elements != nil {
			return n.membership(expression.GetLexpr(), elements, false)
		}

		return n.quantifiedComparison(expression, "any")
	case pgquery.A_Expr_Kind_AEXPR_OP_ALL:
		elements := n.arrayElements(expression.GetRexpr())
		if operator == "<>" && elements != nil {
			return n.membership(expression.GetLexpr(), elements, true)
		}

		return n.quantifiedComparison(expression, "all")
	case pgquery.A_Expr_Kind_AEXPR_BETWEEN:
		return n.between(expression, false, false)
	case pgquery.A_Expr_Kind_AEXPR_NOT_BETWEEN:
		return n.between(expression, true, false)
	case pgquery.A_Expr_Kind_AEXPR_BETWEEN_SYM:
		return n.between(expression, false, true)
	case pgquery.A_Expr_Kind_AEXPR_NOT_BETWEEN_SYM:
		return n.between(expression, true, true)
	case pgquery.A_Expr_Kind_AEXPR_DISTINCT:
		if expanded, ok := n.rowDistinct(expression, false); ok {
			return expanded
		}

		return n.comparison(
			"is distinct from",
			expression.GetLexpr(),
			expression.GetRexpr(),
		)
	case pgquery.A_Expr_Kind_AEXPR_NOT_DISTINCT:
		if expanded, ok := n.rowDistinct(expression, true); ok {
			return expanded
		}

		return n.comparison(
			"is not distinct from",
			expression.GetLexpr(),
			expression.GetRexpr(),
		)
	case pgquery.A_Expr_Kind_AEXPR_SIMILAR:
		if n.mode != legacyPostgresView && !n.isSimilarEscapeCall(expression.GetRexpr()) {
			return n.messageFields(expression.ProtoReflect())
		}

		return n.similar(expression, operator)
	case pgquery.A_Expr_Kind_AEXPR_OP,
		pgquery.A_Expr_Kind_AEXPR_LIKE,
		pgquery.A_Expr_Kind_AEXPR_ILIKE:
		if n.isSimilarEscapeCall(expression.GetRexpr()) &&
			(operator == "~" || operator == "!~") {
			return n.similar(expression, operator)
		}

		return n.comparison(
			normalizePostgresOperator(operator),
			expression.GetLexpr(),
			expression.GetRexpr(),
		)
	default:
		return n.messageFields(expression.ProtoReflect())
	}
}

func (n postgresASTNormalizer) rowDistinct(
	expression *pgquery.A_Expr,
	negated bool,
) (string, bool) {
	left := n.rowElements(expression.GetLexpr())

	right := n.rowElements(expression.GetRexpr())
	if len(left) == 0 || len(left) != len(right) {
		return "", false
	}

	operator := "is distinct from"
	booleanOperator := "or"

	if negated {
		operator = "is not distinct from"
		booleanOperator = "and"
	}

	comparisons := make([]string, 0, len(left))
	for index := range left {
		comparisons = append(
			comparisons,
			n.comparison(operator, left[index], right[index]),
		)
	}

	return canonicalPostgresBoolean(booleanOperator, comparisons...), true
}

func (n postgresASTNormalizer) rowElements(node *pgquery.Node) []*pgquery.Node {
	node = n.unwrapTypeCast(node)
	if node == nil || node.GetRowExpr() == nil {
		return nil
	}

	return node.GetRowExpr().GetArgs()
}

func (n postgresASTNormalizer) boolExpr(expression *pgquery.BoolExpr) string {
	if expression.GetBoolop() == pgquery.BoolExprType_NOT_EXPR && len(expression.GetArgs()) == 1 {
		argument := expression.GetArgs()[0]
		if distinct := argument.GetAExpr(); distinct != nil &&
			distinct.GetKind() == pgquery.A_Expr_Kind_AEXPR_DISTINCT {
			return n.comparison(
				"is not distinct from",
				distinct.GetLexpr(),
				distinct.GetRexpr(),
			)
		}
	}

	arguments := make([]string, 0, len(expression.GetArgs()))
	for _, argument := range expression.GetArgs() {
		arguments = append(arguments, n.node(argument))
	}

	return canonicalPostgresBoolean(
		strings.TrimSuffix(strings.ToLower(expression.GetBoolop().String()), "_expr"),
		arguments...,
	)
}

func (n postgresASTNormalizer) membership(
	left *pgquery.Node,
	elements []*pgquery.Node,
	negated bool,
) string {
	if len(elements) == 1 {
		operator := "="
		if negated {
			operator = "!="
		}

		return n.comparison(operator, left, elements[0])
	}

	values := make([]string, 0, len(elements))
	for _, element := range elements {
		values = append(values, n.node(element))
	}

	operator := "in"
	if negated {
		operator = "not in"
	}

	return "membership(" + strconv.Quote(operator) + "," +
		n.node(left) + ",[" + strings.Join(values, ",") + "])"
}

func (n postgresASTNormalizer) quantifiedComparison(
	expression *pgquery.A_Expr,
	quantifier string,
) string {
	return "quantified(" +
		strconv.Quote(normalizePostgresOperator(postgresOperatorName(expression.GetName()))) + "," +
		strconv.Quote(quantifier) + "," +
		n.node(expression.GetLexpr()) + "," +
		n.node(expression.GetRexpr()) + ")"
}

func (n postgresASTNormalizer) between(
	expression *pgquery.A_Expr,
	negated, symmetric bool,
) string {
	bounds := n.arrayElements(expression.GetRexpr())
	if len(bounds) != 2 {
		return n.messageFields(expression.ProtoReflect())
	}

	forward := n.boundPair(expression.GetLexpr(), bounds[0], bounds[1], negated)
	if !symmetric {
		return forward
	}

	reverse := n.boundPair(expression.GetLexpr(), bounds[1], bounds[0], negated)

	outerOperator := "or"
	if negated {
		outerOperator = "and"
	}

	return canonicalPostgresBoolean(outerOperator, forward, reverse)
}

func (n postgresASTNormalizer) boundPair(
	value, lower, upper *pgquery.Node,
	negated bool,
) string {
	if negated {
		return canonicalPostgresBoolean(
			"or",
			n.comparison("<", value, lower),
			n.comparison(">", value, upper),
		)
	}

	return canonicalPostgresBoolean(
		"and",
		n.comparison(">=", value, lower),
		n.comparison("<=", value, upper),
	)
}

func canonicalPostgresBoolean(operator string, arguments ...string) string {
	return "boolean(" + strconv.Quote(operator) + ",[" + strings.Join(arguments, ",") + "])"
}

func (n postgresASTNormalizer) comparison(operator string, left, right *pgquery.Node) string {
	leftValue := n.node(left)
	rightValue := n.node(right)

	if reversedOperator, ok := reversedPostgresComparisonOperator(operator); ok &&
		isQualifiedPostgresColumnReference(left) &&
		isQualifiedPostgresColumnReference(right) &&
		rightValue < leftValue {
		operator = reversedOperator
		leftValue, rightValue = rightValue, leftValue
	}

	return "comparison(" + strconv.Quote(operator) + "," +
		leftValue + "," + rightValue + ")"
}

func isQualifiedPostgresColumnReference(node *pgquery.Node) bool {
	column := node.GetColumnRef()
	if column == nil || len(column.GetFields()) < 2 {
		return false
	}

	fields := column.GetFields()

	return fields[len(fields)-2].GetString_() != nil &&
		fields[len(fields)-1].GetString_() != nil
}

func reversedPostgresComparisonOperator(operator string) (string, bool) {
	switch operator {
	case "=", "!=", "is distinct from", "is not distinct from":
		return operator, true
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
}

func (n postgresASTNormalizer) similar(expression *pgquery.A_Expr, operator string) string {
	call := expression.GetRexpr().GetFuncCall()

	arguments := make([]string, 0)
	if call != nil {
		arguments = make([]string, 0, len(call.GetArgs()))
		for _, argument := range call.GetArgs() {
			arguments = append(arguments, n.node(argument))
		}
	}

	return "similar(" + strconv.FormatBool(operator == "!~") + "," +
		n.node(expression.GetLexpr()) + ",[" +
		strings.Join(arguments, ",") + "])"
}

func (n postgresASTNormalizer) arrayElements(node *pgquery.Node) []*pgquery.Node {
	node = n.unwrapTypeCast(node)
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

func (n postgresASTNormalizer) unwrapTypeCast(node *pgquery.Node) *pgquery.Node {
	for node != nil {
		cast := node.GetTypeCast()
		if cast == nil || !n.isIgnorableTypeCast(cast) {
			return node
		}

		node = cast.GetArg()
	}

	return nil
}

func (n postgresASTNormalizer) isIgnorableTypeCast(cast *pgquery.TypeCast) bool {
	if cast == nil {
		return false
	}

	if n.mode != legacyPostgresView {
		return n.isCatalogLiteralCast(cast)
	}

	if isIgnorableViewTypeCast(cast.GetTypeName()) {
		return true
	}

	return strings.EqualFold(
		postgresNodeListLastString(cast.GetTypeName().GetNames()),
		"unknown",
	) && n.isNull(cast.GetArg())
}

func (n postgresASTNormalizer) isCatalogLiteralCast(cast *pgquery.TypeCast) bool {
	typeName := cast.GetTypeName()
	if !isPlainPostgresType(typeName) {
		return false
	}

	constant := cast.GetArg().GetAConst()
	if constant == nil {
		return false
	}

	return n.isBuiltinName(typeName.GetNames(), "text") && constant.GetSval() != nil ||
		n.isBuiltinName(typeName.GetNames(), "unknown") && constant.GetIsnull()
}

func isPlainPostgresType(typeName *pgquery.TypeName) bool {
	return typeName != nil && len(typeName.GetTypmods()) == 0 &&
		len(typeName.GetArrayBounds()) == 0 && typeName.GetTypemod() < 0 &&
		!typeName.GetSetof() && !typeName.GetPctType() && typeName.GetTypeOid() == 0
}

func (n postgresASTNormalizer) isCatalogNullDefault(node *pgquery.Node) bool {
	cast := node.GetTypeCast()
	if n.mode != catalogPostgresView || cast == nil || !isPlainPostgresType(cast.GetTypeName()) ||
		cast.GetArg().GetAConst() == nil || !cast.GetArg().GetAConst().GetIsnull() {
		return false
	}

	name := postgresNodeListLastString(cast.GetTypeName().GetNames())
	if !n.isBuiltinName(cast.GetTypeName().GetNames(), name) {
		return false
	}

	switch name {
	case "numeric", "text", "int8", "int4", "int2", "float4", "float8", "varchar", "bpchar",
		"timestamp", "timestamptz", "interval", "bool", "json", "jsonb":
		return true
	default:
		return false
	}
}

func (n postgresASTNormalizer) isBuiltinName(names []*pgquery.Node, name string) bool {
	if len(names) == 1 {
		return n.mode == catalogPostgresView && postgresNodeListLastString(names) == name
	}

	return len(names) == 2 && names[0].GetString_().GetSval() == "pg_catalog" &&
		postgresNodeListLastString(names) == name
}

func (n postgresASTNormalizer) isImplicitCaseDefault(
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

	// A catalog CASE default has the resolved result type; result-arm casts stay intact.
	return ok && (n.isNull(node) || n.isCatalogNullDefault(node))
}

func isDefaultPostgresAscendingSort(
	message protoreflect.Message,
	field protoreflect.FieldDescriptor,
	value protoreflect.Value,
) bool {
	if field.Name() != "sortby_dir" {
		return false
	}

	if _, ok := message.Interface().(*pgquery.SortBy); !ok {
		return false
	}

	direction := pgquery.SortByDir(value.Enum())

	return direction == pgquery.SortByDir_SORTBY_DEFAULT ||
		direction == pgquery.SortByDir_SORTBY_ASC
}

func (n postgresASTNormalizer) isNull(node *pgquery.Node) bool {
	node = n.unwrapTypeCast(node)
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

func (n postgresASTNormalizer) isSimilarEscapeCall(node *pgquery.Node) bool {
	if node == nil || node.GetFuncCall() == nil {
		return false
	}

	if n.mode != legacyPostgresView {
		call := node.GetFuncCall()

		return n.isBuiltinName(call.GetFuncname(), "similar_to_escape") &&
			len(call.GetArgs()) >= 1 && len(call.GetArgs()) <= 2 &&
			len(call.GetAggOrder()) == 0 && call.GetAggFilter() == nil && call.GetOver() == nil &&
			!call.GetAggWithinGroup() && !call.GetAggStar() && !call.GetAggDistinct() &&
			!call.GetFuncVariadic()
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
