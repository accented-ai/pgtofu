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

	return expr
}

func normalizeArrayConstructorToIn(expr string) string {
	anyPattern := regexp.MustCompile(
		`\(?(\w+|"[^"]+")\)?\s*=\s*any\s*\(\s*\(?array\s*\[(.*?)\]\s*\)?\s*\)`,
	)

	return anyPattern.ReplaceAllStringFunc(expr, func(match string) string {
		submatches := anyPattern.FindStringSubmatch(match)
		if len(submatches) == 3 {
			col := submatches[1]
			col = strings.Trim(col, `"`)
			values := submatches[2]
			values = strings.ReplaceAll(values, "::text", "")
			values = strings.ReplaceAll(values, "::integer", "")
			values = strings.ReplaceAll(values, "::bigint", "")
			values = strings.ReplaceAll(values, "::character varying", "")

			return fmt.Sprintf("%s in (%s)", col, values)
		}

		return match
	})
}

func normalizeArrayLiteralToIn(expr string) string {
	arrayLiteralPattern := regexp.MustCompile(
		`\(?(\w+|"[^"]+")\)?\s*=\s*any\s*\(?\s*'\{([^}]*)\}'(?:::(?:text|character varying|integer|bigint))?(?:\[\])?\s*\)?`,
	)

	return arrayLiteralPattern.ReplaceAllStringFunc(expr, func(match string) string {
		submatches := arrayLiteralPattern.FindStringSubmatch(match)
		if len(submatches) == 3 {
			col := submatches[1]
			col = strings.Trim(col, `"`)
			rawValues := submatches[2]

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
	}

	expr = normalizeOperators(expr)
	expr = strings.Join(strings.Fields(expr), " ")

	return expr
}

func expandBetween(expr string) string {
	parts := strings.Split(expr, " between ")
	if len(parts) != 2 {
		return expr
	}

	col := strings.TrimSpace(parts[0])
	rest := strings.TrimSpace(parts[1])

	andParts := strings.Split(rest, " and ")
	if len(andParts) != 2 {
		return expr
	}

	lower := strings.TrimSpace(andParts[0])
	upper := strings.TrimSpace(andParts[1])

	return fmt.Sprintf("(%s >= %s) and (%s <= %s)", col, lower, col, upper)
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
