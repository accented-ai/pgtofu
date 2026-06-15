package differ

import (
	"fmt"
	"regexp"
	"strings"
)

func normalizeExpression(expr string) string {
	expr = strings.TrimSpace(expr)

	expr = strings.TrimPrefix(expr, "CHECK ")
	expr = strings.TrimPrefix(expr, "CHECK(")
	expr = strings.TrimPrefix(expr, "CHECK (")
	expr = strings.Join(strings.Fields(expr), " ")
	expr = strings.ToLower(expr)
	expr = normalizeLikeOperators(expr)
	expr = normalizeQuotedIdentifiers(expr)

	for strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		inner := expr[1 : len(expr)-1]
		if countParenDepth(inner) >= 0 {
			expr = strings.TrimSpace(inner)
		} else {
			break
		}
	}

	expr = removeTypeCasts(expr)
	expr = normalizeInClauses(expr)
	expr = normalizeBetweenExpressions(expr)

	return expr
}

func normalizeQuotedIdentifiers(expr string) string {
	var result strings.Builder

	for i := 0; i < len(expr); {
		switch expr[i] {
		case '\'':
			i = copySingleQuotedString(&result, expr, i)

		case '"':
			end, ident, ok := readQuotedIdentifier(expr, i)
			if !ok {
				result.WriteString(expr[i:])
				return result.String()
			}

			if isSimpleIdentifierText(ident) {
				result.WriteString(ident)
			} else {
				result.WriteString(expr[i:end])
			}

			i = end

		default:
			result.WriteByte(expr[i])
			i++
		}
	}

	return result.String()
}

func copySingleQuotedString(result *strings.Builder, expr string, start int) int {
	result.WriteByte(expr[start])

	for i := start + 1; i < len(expr); i++ {
		result.WriteByte(expr[i])

		if expr[i] != '\'' {
			continue
		}

		if i+1 < len(expr) && expr[i+1] == '\'' {
			i++
			result.WriteByte(expr[i])

			continue
		}

		return i + 1
	}

	return len(expr)
}

func readQuotedIdentifier(expr string, start int) (int, string, bool) {
	var ident strings.Builder

	for i := start + 1; i < len(expr); i++ {
		if expr[i] != '"' {
			ident.WriteByte(expr[i])
			continue
		}

		if i+1 < len(expr) && expr[i+1] == '"' {
			ident.WriteByte('"')

			i++

			continue
		}

		return i + 1, ident.String(), true
	}

	return start, "", false
}

func isSimpleIdentifierText(ident string) bool {
	if ident == "" {
		return false
	}

	if !isLowerIdentifierStart(ident[0]) {
		return false
	}

	for i := 1; i < len(ident); i++ {
		if !isLowerIdentifierPart(ident[i]) {
			return false
		}
	}

	return true
}

func isLowerIdentifierStart(ch byte) bool {
	return ch == '_' || (ch >= 'a' && ch <= 'z')
}

func isLowerIdentifierPart(ch byte) bool {
	return isLowerIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}

func countParenDepth(s string) int {
	depth := 0

	for _, ch := range s {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return depth
			}
		}
	}

	return depth
}

func removeTypeCasts(expr string) string {
	typeCasts := []string{
		"::double precision", "::real", "::numeric", "::integer", "::bigint",
		"::smallint", "::text[]", "::text", "::varchar", "::character varying",
		"::boolean", "::timestamp", "::timestamptz", "::date", "::time",
	}
	for _, cast := range typeCasts {
		expr = strings.ReplaceAll(expr, cast, "")
	}

	expr = removeLiteralParens(expr)

	expr = strings.ReplaceAll(expr, "( ", "(")
	expr = strings.ReplaceAll(expr, " )", ")")

	return expr
}

func removeLiteralParens(expr string) string {
	numPattern := regexp.MustCompile(`\((-?\d+(?:\.\d+)?)\)`)
	expr = numPattern.ReplaceAllString(expr, "$1")
	strPattern := regexp.MustCompile(`\(('[^']*')\)`)
	expr = strPattern.ReplaceAllString(expr, "$1")
	identPattern := regexp.MustCompile(`\((\w+)\)`)
	expr = identPattern.ReplaceAllString(expr, "$1")
	expr = removeComparisonParens(expr)

	return expr
}

func removeComparisonParensOnce(expr string) string {
	start, end := findInnermostParens(expr)
	if start == -1 {
		return expr
	}

	inner := expr[start+1 : end]
	if isSimpleComparison(inner) || canRemoveArithmeticParens(inner, expr, end) {
		return expr[:start] + inner + expr[end+1:]
	}

	return expr[:start] + "\x00" + expr[start+1:end] + "\x01" + expr[end+1:]
}

func canRemoveArithmeticParens(inner, fullExpr string, closePos int) bool {
	inner = strings.TrimSpace(inner)
	if inner == "" {
		return false
	}

	lower := strings.ToLower(inner)
	if containsLogicalKeyword(lower) {
		return false
	}

	if containsComparisonOperator(inner) ||
		strings.Contains(inner, "<=") ||
		strings.Contains(inner, ">=") ||
		strings.Contains(inner, "<>") ||
		strings.Contains(inner, "!=") {
		return false
	}

	innerPrec := getLowestArithmeticPrecedence(inner)
	if innerPrec == 0 {
		return false
	}

	outerOp := getOperatorAfterParen(fullExpr, closePos)
	if outerOp == 0 {
		return true
	}

	outerPrec := getArithmeticPrecedence(outerOp)
	if outerPrec == 0 {
		return true
	}

	return innerPrec >= outerPrec
}

func getLowestArithmeticPrecedence(expr string) int {
	lowest := 0

	for i := range len(expr) {
		ch := expr[i]
		prec := getArithmeticPrecedence(ch)

		if prec > 0 && (lowest == 0 || prec < lowest) {
			lowest = prec
		}
	}

	return lowest
}

func getArithmeticPrecedence(ch byte) int {
	switch ch {
	case '+', '-':
		return 1
	case '*', '/', '%':
		return 2
	default:
		return 0
	}
}

func getOperatorAfterParen(expr string, closePos int) byte {
	for i := closePos + 1; i < len(expr); i++ {
		ch := expr[i]
		if ch == ' ' || ch == '\t' || ch == '\n' {
			continue
		}

		if ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == '%' {
			return ch
		}

		return 0
	}

	return 0
}

func findInnermostParens(expr string) (int, int) {
	lastOpen := -1

	for i, ch := range expr {
		switch ch {
		case '(':
			lastOpen = i
		case ')':
			if lastOpen != -1 {
				return lastOpen, i
			}
		}
	}

	return -1, -1
}

func removeComparisonParens(expr string) string {
	prev := ""
	for prev != expr {
		prev = expr
		expr = removeComparisonParensOnce(expr)
	}

	expr = strings.ReplaceAll(expr, "\x00", "(")
	expr = strings.ReplaceAll(expr, "\x01", ")")

	return expr
}

func isSimpleComparison(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return false
	}

	lower := strings.ToLower(expr)

	if containsLogicalKeyword(lower) {
		return false
	}

	hasComparison := strings.Contains(expr, "<=") ||
		strings.Contains(expr, ">=") ||
		strings.Contains(expr, "<>") ||
		strings.Contains(expr, "!=") ||
		strings.Contains(lower, " is null") ||
		strings.Contains(lower, " is not null") ||
		strings.Contains(lower, " like ") ||
		strings.Contains(lower, " ilike ") ||
		strings.Contains(lower, " similar to ") ||
		containsComparisonOperator(expr)

	return hasComparison
}

func containsLogicalKeyword(expr string) bool {
	keywords := []string{" and ", " or "}
	for _, kw := range keywords {
		if strings.Contains(expr, kw) {
			return true
		}
	}

	if strings.HasPrefix(expr, "and ") || strings.HasPrefix(expr, "or ") {
		return true
	}

	if strings.HasSuffix(expr, " and") || strings.HasSuffix(expr, " or") {
		return true
	}

	return false
}

func containsComparisonOperator(expr string) bool {
	for i := range len(expr) {
		ch := expr[i]
		if ch != '<' && ch != '>' && ch != '=' {
			continue
		}

		prevIsOp := i > 0 &&
			(expr[i-1] == '<' || expr[i-1] == '>' || expr[i-1] == '!' || expr[i-1] == '=')
		if prevIsOp {
			continue
		}

		nextIsOp := i+1 < len(expr) &&
			(expr[i+1] == '<' || expr[i+1] == '>' || expr[i+1] == '=')
		if nextIsOp {
			continue
		}

		return true
	}

	return false
}

func normalizeInClauses(expr string) string {
	expr = normalizeArrayConstructorToIn(expr)
	expr = normalizeArrayLiteralToIn(expr)
	expr = normalizeInClauseSpacing(expr)
	expr = normalizeSingleElementIn(expr)

	return expr
}

func normalizeSingleElementIn(expr string) string {
	withParens := regexp.MustCompile(`(\w+)\s+in\s*\(([^,)]+)\)`)
	expr = withParens.ReplaceAllString(expr, "$1 = $2")

	withoutParens := regexp.MustCompile(`(\w+)\s+in\s+('[^']*')`)
	expr = withoutParens.ReplaceAllString(expr, "$1 = $2")

	return expr
}

func normalizeArrayConstructorToIn(expr string) string {
	arrayComparisonPattern := regexp.MustCompile(
		`\s*(=|<>)\s*(any|all)\s*\(\s*\(?array\s*\[(.*?)\]\s*\)?\s*\)`,
	)

	for {
		match := findArrayConstructorComparison(expr, arrayComparisonPattern)
		if match == nil {
			return expr
		}

		operator := expr[match.operatorStart:match.operatorEnd]
		quantifier := expr[match.quantifierStart:match.quantifierEnd]

		replacementOperator := "not in"
		if operator == "=" && quantifier == "any" {
			replacementOperator = "in"
		}

		lhsStart := findArrayComparisonLHSStart(expr, match.operatorStart)
		lhs := normalizeArrayComparisonLHS(expr[lhsStart:match.operatorStart])
		values := normalizeArrayComparisonValues(
			expr[match.valuesStart:match.valuesEnd],
		)

		expr = expr[:lhsStart] +
			fmt.Sprintf("%s %s (%s)", lhs, replacementOperator, values) +
			expr[match.end:]
	}
}

type arrayConstructorComparisonMatch struct {
	operatorStart   int
	operatorEnd     int
	quantifierStart int
	quantifierEnd   int
	valuesStart     int
	valuesEnd       int
	end             int
}

func findArrayConstructorComparison(
	expr string,
	pattern *regexp.Regexp,
) *arrayConstructorComparisonMatch {
	searchStart := 0

	for searchStart < len(expr) {
		loc := pattern.FindStringSubmatchIndex(expr[searchStart:])
		if loc == nil {
			return nil
		}

		match := &arrayConstructorComparisonMatch{
			operatorStart:   searchStart + loc[2],
			operatorEnd:     searchStart + loc[3],
			quantifierStart: searchStart + loc[4],
			quantifierEnd:   searchStart + loc[5],
			valuesStart:     searchStart + loc[6],
			valuesEnd:       searchStart + loc[7],
			end:             searchStart + loc[1],
		}

		operator := expr[match.operatorStart:match.operatorEnd]

		quantifier := expr[match.quantifierStart:match.quantifierEnd]
		if (operator == "=" && quantifier == "any") ||
			(operator == "<>" && quantifier == "all") {
			return match
		}

		searchStart += loc[1]
	}

	return nil
}

func findArrayComparisonLHSStart(expr string, operatorStart int) int {
	depth := 0

	for i := operatorStart - 1; i >= 0; i-- {
		switch expr[i] {
		case ')':
			depth++
			continue
		case '(':
			if depth == 0 {
				return i + 1
			}

			depth--

			continue
		}

		if depth != 0 {
			continue
		}

		if isArrayComparisonBoundaryWordEndingAt(expr, i) {
			return skipLeadingSpaces(expr, i+1)
		}
	}

	return 0
}

func isArrayComparisonBoundaryWordEndingAt(expr string, end int) bool {
	for _, word := range []string{"and", "or", "when", "then", "else", "where"} {
		if isLogicalWordEndingAt(expr, end, word) {
			return true
		}
	}

	return false
}

func isLogicalWordEndingAt(expr string, end int, word string) bool {
	start := end - len(word) + 1
	if start < 0 || expr[start:end+1] != word {
		return false
	}

	before := start - 1
	after := end + 1

	return isExpressionBoundary(expr, before) && isExpressionBoundary(expr, after)
}

func isExpressionBoundary(expr string, idx int) bool {
	if idx < 0 || idx >= len(expr) {
		return true
	}

	ch := expr[idx]

	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' ||
		ch == '(' || ch == ')'
}

func skipLeadingSpaces(expr string, start int) int {
	for start < len(expr) {
		switch expr[start] {
		case ' ', '\t', '\n', '\r':
			start++
		default:
			return start
		}
	}

	return start
}

func normalizeArrayComparisonLHS(lhs string) string {
	lhs = strings.TrimSpace(lhs)

	for strings.HasPrefix(lhs, "(") && strings.HasSuffix(lhs, ")") {
		inner := strings.TrimSpace(lhs[1 : len(lhs)-1])
		if countParenDepth(inner) != 0 {
			break
		}

		lhs = inner
	}

	for strings.HasPrefix(lhs, "(") && countParenDepth(lhs) > 0 {
		lhs = strings.TrimSpace(lhs[1:])
	}

	return strings.Trim(lhs, `"`)
}

func normalizeArrayComparisonValues(values string) string {
	values = strings.ReplaceAll(values, "::text", "")
	values = strings.ReplaceAll(values, "::integer", "")
	values = strings.ReplaceAll(values, "::bigint", "")
	values = strings.ReplaceAll(values, "::character varying", "")

	parts := splitExpressionList(values)
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	return strings.Join(parts, ", ")
}

func splitExpressionList(values string) []string {
	parts := []string{}

	var current strings.Builder

	depth := 0
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(values); i++ {
		ch := values[i]

		switch {
		case inSingleQuote:
			current.WriteByte(ch)

			if ch == '\'' {
				if i+1 < len(values) && values[i+1] == '\'' {
					i++
					current.WriteByte(values[i])
				} else {
					inSingleQuote = false
				}
			}

		case inDoubleQuote:
			current.WriteByte(ch)

			if ch == '"' {
				if i+1 < len(values) && values[i+1] == '"' {
					i++
					current.WriteByte(values[i])
				} else {
					inDoubleQuote = false
				}
			}

		case ch == '\'':
			inSingleQuote = true

			current.WriteByte(ch)

		case ch == '"':
			inDoubleQuote = true

			current.WriteByte(ch)

		case ch == '(':
			depth++

			current.WriteByte(ch)

		case ch == ')':
			if depth > 0 {
				depth--
			}

			current.WriteByte(ch)

		case ch == ',' && depth == 0:
			parts = append(parts, current.String())
			current.Reset()

		default:
			current.WriteByte(ch)
		}
	}

	parts = append(parts, current.String())

	return parts
}

func normalizeArrayLiteralToIn(expr string) string {
	arrayLiteralPattern := regexp.MustCompile(
		`(?:\((\w+|"[^"]+")\)|(\w+|"[^"]+"))` +
			`\s*=\s*any\s*\(?\s*'\{([^}]*)\}'` +
			`(?:::(?:text|character varying|integer|bigint))?(?:\[\])?\s*\)?`,
	)

	arrayLiteralAllPattern := regexp.MustCompile(
		`(?:\((\w+|"[^"]+")\)|(\w+|"[^"]+"))` +
			`\s*<>\s*all\s*\(?\s*'\{([^}]*)\}'` +
			`(?:::(?:text|character varying|integer|bigint))?(?:\[\])?\s*\)?`,
	)

	expr = arrayLiteralAllPattern.ReplaceAllStringFunc(expr, func(match string) string {
		submatches := arrayLiteralAllPattern.FindStringSubmatch(match)
		if len(submatches) == 4 {
			col := submatches[1]
			if col == "" {
				col = submatches[2]
			}

			col = strings.Trim(col, `"`)
			rawValues := submatches[3]

			parts := strings.Split(rawValues, ",")
			quotedParts := make([]string, len(parts))

			for i, part := range parts {
				part = strings.TrimSpace(part)
				if !strings.HasPrefix(part, "'") {
					part = "'" + part + "'"
				}

				quotedParts[i] = part
			}

			return fmt.Sprintf("%s not in (%s)", col, strings.Join(quotedParts, ", "))
		}

		return match
	})

	expr = arrayLiteralPattern.ReplaceAllStringFunc(expr, func(match string) string {
		submatches := arrayLiteralPattern.FindStringSubmatch(match)
		if len(submatches) == 4 {
			col := submatches[1]
			if col == "" {
				col = submatches[2]
			}

			col = strings.Trim(col, `"`)
			rawValues := submatches[3]

			parts := strings.Split(rawValues, ",")
			quotedParts := make([]string, len(parts))

			for i, part := range parts {
				part = strings.TrimSpace(part)
				if !strings.HasPrefix(part, "'") {
					part = "'" + part + "'"
				}

				quotedParts[i] = part
			}

			return fmt.Sprintf("%s in (%s)", col, strings.Join(quotedParts, ", "))
		}

		return match
	})

	return expr
}

func normalizeInClauseSpacing(expr string) string {
	inPattern := regexp.MustCompile(`(\w+)\s+in\s*\(([^)]+)\)`)

	return inPattern.ReplaceAllStringFunc(expr, func(match string) string {
		submatches := inPattern.FindStringSubmatch(match)
		if len(submatches) == 3 {
			col := submatches[1]
			values := submatches[2]

			parts := strings.Split(values, ",")
			normalizedParts := make([]string, len(parts))

			for i, part := range parts {
				normalizedParts[i] = strings.TrimSpace(part)
			}

			return fmt.Sprintf("%s in (%s)", col, strings.Join(normalizedParts, ", "))
		}

		return match
	})
}

func normalizeBetweenExpressions(expr string) string {
	if strings.Contains(expr, " between ") {
		expr = expandBetween(expr)
		expr = removeComparisonParens(expr)
		expr = stripTopLevelAndParens(expr)
	}

	expr = normalizeOperators(expr)
	expr = strings.Join(strings.Fields(expr), " ")

	return expr
}

func stripTopLevelAndParens(expr string) string {
	if !strings.HasPrefix(expr, "(") || !strings.HasSuffix(expr, ")") {
		return expr
	}

	inner := expr[1 : len(expr)-1]
	if countParenDepth(inner) < 0 {
		return expr
	}

	lower := strings.ToLower(inner)
	if !strings.Contains(lower, " or ") {
		return inner
	}

	return expr
}

func expandBetween(expr string) string {
	betweenPattern := regexp.MustCompile(
		`(\w+)\s+between\s+(\S+)\s+and\s+(\S+)`,
	)

	return betweenPattern.ReplaceAllString(expr, "(($1 >= $2) and ($1 <= $3))")
}

func normalizeOperators(expr string) string {
	expr = strings.ReplaceAll(expr, ">=", "§GE§")
	expr = strings.ReplaceAll(expr, "<=", "§LE§")
	expr = strings.ReplaceAll(expr, "!=", "§NE§")
	expr = strings.ReplaceAll(expr, "<>", "§NE§")
	expr = strings.ReplaceAll(expr, ">", "§GT§")
	expr = strings.ReplaceAll(expr, "<", "§LT§")
	expr = strings.ReplaceAll(expr, "=", "§EQ§")

	expr = strings.ReplaceAll(expr, "§GE§", " >= ")
	expr = strings.ReplaceAll(expr, "§LE§", " <= ")
	expr = strings.ReplaceAll(expr, "§NE§", " != ")
	expr = strings.ReplaceAll(expr, "§GT§", " > ")
	expr = strings.ReplaceAll(expr, "§LT§", " < ")
	expr = strings.ReplaceAll(expr, "§EQ§", " = ")

	return expr
}

// normalizeLikeOperators rewrites PostgreSQL's ~~/~~* operators to LIKE/ILIKE
// keywords so a parsed `LIKE` matches the `~~` from pg_get_constraintdef.
// Longest operators first so `~~` does not clobber `!~~*`.
func normalizeLikeOperators(expr string) string {
	replacements := []struct{ op, keyword string }{
		{"!~~*", " not ilike "},
		{"~~*", " ilike "},
		{"!~~", " not like "},
		{"~~", " like "},
	}
	for _, r := range replacements {
		expr = strings.ReplaceAll(expr, r.op, r.keyword)
	}

	return strings.Join(strings.Fields(expr), " ")
}

func normalizeComment(comment string) string {
	if comment == "" {
		return ""
	}

	comment = strings.ReplaceAll(comment, "\r\n", "")
	comment = strings.ReplaceAll(comment, "\n", "")
	comment = strings.ReplaceAll(comment, "\r", "")
	comment = regexp.MustCompile(`\s+`).ReplaceAllString(comment, " ")

	return strings.TrimSpace(comment)
}
