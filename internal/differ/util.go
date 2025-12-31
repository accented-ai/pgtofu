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
		"::smallint", "::text", "::varchar", "::character varying", "::boolean",
		"::timestamp", "::timestamptz", "::date", "::time",
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

	return expr
}

func normalizeInClauses(expr string) string {
	anyPattern := regexp.MustCompile(`(\w+)\s*=\s*any\s*\(\s*array\s*\[(.*?)\]\s*\)`)
	expr = anyPattern.ReplaceAllStringFunc(expr, func(match string) string {
		submatches := anyPattern.FindStringSubmatch(match)
		if len(submatches) == 3 {
			col := submatches[1]
			values := submatches[2]
			values = strings.ReplaceAll(values, "::text", "")
			values = strings.ReplaceAll(values, "::integer", "")
			values = strings.ReplaceAll(values, "::bigint", "")

			return fmt.Sprintf("%s in (%s)", col, values)
		}

		return match
	})

	return expr
}

func normalizeBetweenExpressions(expr string) string {
	if strings.Contains(expr, " between ") {
		expr = expandBetween(expr)
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
