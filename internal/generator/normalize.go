package generator

import (
	"regexp"
	"strings"

	"github.com/accented-ai/pgtofu/internal/parser"
)

func NormalizeSQL(sql string) string {
	if sql == "" {
		return ""
	}

	result := sql
	result = normalizeBooleans(result)
	result = uppercaseBuiltinCastTypes(result)
	result = normalizeFunctionNames(result)
	result = normalizeQuantifiedComparisonSpacing(result)

	return result
}

func NormalizeCheckConstraint(def string) string {
	if def == "" {
		return ""
	}

	result := def
	result = normalizeAnyArrayToIn(result)
	result = uppercaseBuiltinCastTypes(result)
	result = normalizeBooleans(result)
	result = normalizeFunctionNames(result)
	result = normalizeQuantifiedComparisonSpacing(result)

	return result
}

func NormalizeWhereClause(where string) string {
	if where == "" {
		return ""
	}

	result := where
	result = normalizeBooleans(result)
	result = uppercaseBuiltinCastTypes(result)
	result = normalizeQuantifiedComparisonSpacing(result)

	return result
}

func NormalizeDefaultValue(defaultVal string) string {
	if defaultVal == "" {
		return ""
	}

	result := defaultVal

	if strings.EqualFold(result, "true") {
		return "TRUE"
	}

	if strings.EqualFold(result, "false") {
		return "FALSE"
	}

	result = uppercaseBuiltinCastTypes(result)
	result = normalizeFunctionNames(result)

	return result
}

func NormalizeDataType(dataType string) string {
	upper := strings.ToUpper(strings.TrimSpace(dataType))

	isArray := strings.HasSuffix(upper, "[]")
	if isArray {
		upper = strings.TrimSuffix(upper, "[]")
	}

	typeAliases := map[string]string{
		"TIMESTAMP WITH TIME ZONE":    "TIMESTAMPTZ",
		"TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
		"TIME WITH TIME ZONE":         "TIMETZ",
		"TIME WITHOUT TIME ZONE":      "TIME",
		"CHARACTER VARYING":           "VARCHAR",
		"CHARACTER":                   "CHAR",
	}

	if alias, ok := typeAliases[upper]; ok {
		upper = alias
	}

	if isArray {
		upper += "[]"
	}

	return upper
}

func uppercaseBuiltinCastTypes(expression string) string {
	tokens, err := parser.NewLexer(expression).Tokenize()
	if err != nil {
		return expression
	}

	replacements := make(map[int]string)
	markBuiltinCastTypes(tokens, replacements)

	if len(replacements) == 0 {
		return expression
	}

	var output strings.Builder

	position := 0

	for index, token := range tokens {
		replacement, ok := replacements[index]
		if !ok {
			continue
		}

		output.WriteString(expression[position:token.Start])
		output.WriteString(replacement)

		position = token.End
	}

	output.WriteString(expression[position:])

	return output.String()
}

func normalizeBooleans(s string) string {
	truePattern := regexp.MustCompile(`\btrue\b`)
	falsePattern := regexp.MustCompile(`\bfalse\b`)

	return mapOutsideStringLiterals(s, func(segment string) string {
		segment = truePattern.ReplaceAllString(segment, "TRUE")
		segment = falsePattern.ReplaceAllString(segment, "FALSE")

		return segment
	})
}

func mapOutsideStringLiterals(s string, fn func(string) string) string {
	var out, run strings.Builder

	flush := func() {
		if run.Len() > 0 {
			out.WriteString(fn(run.String()))
			run.Reset()
		}
	}

	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '\'' && c != '"' {
			run.WriteByte(c)

			continue
		}

		flush()
		out.WriteByte(c)

		for i++; i < len(s); i++ {
			out.WriteByte(s[i])

			if s[i] != c {
				continue
			}

			if i+1 < len(s) && s[i+1] == c {
				out.WriteByte(s[i+1])
				i++

				continue
			}

			break
		}
	}

	flush()

	return out.String()
}

func normalizeFunctionNames(s string) string {
	functions := []string{
		"uuid_generate_v4",
		"uuid_generate_v1",
		"gen_random_uuid",
		"now",
		"current_timestamp",
		"current_date",
		"current_time",
		"current_user",
		"session_user",
		"localtime",
		"localtimestamp",
		"nextval",
		"setval",
		"currval",
		"lastval",
		"coalesce",
		"nullif",
		"greatest",
		"least",
		"btrim",
		"array_agg",
		"string_agg",
		"json_agg",
		"jsonb_agg",
		"json_typeof",
		"jsonb_typeof",
		"cardinality",
		"count",
		"sum",
		"avg",
		"min",
		"max",
	}

	escapedFunctions := make([]string, len(functions))
	for i, fn := range functions {
		escapedFunctions[i] = regexp.QuoteMeta(fn)
	}

	pattern := regexp.MustCompile(
		`(?i)\b(?:` + strings.Join(escapedFunctions, "|") + `)\s*\(`,
	)

	return mapOutsideStringLiterals(s, func(segment string) string {
		return pattern.ReplaceAllStringFunc(segment, func(match string) string {
			// Find where the paren is and uppercase everything before it
			parenIdx := strings.Index(match, "(")
			if parenIdx == -1 {
				return match
			}

			return strings.ToUpper(strings.TrimSpace(match[:parenIdx])) + "("
		})
	})
}

func normalizeQuantifiedComparisonSpacing(s string) string {
	quantifierPattern := regexp.MustCompile(`(?i)\b(?:any|all|some)\s+\(`)

	return mapOutsideStringLiterals(s, func(segment string) string {
		return quantifierPattern.ReplaceAllStringFunc(segment, func(match string) string {
			parenIndex := strings.LastIndex(match, "(")
			if parenIndex == -1 {
				return match
			}

			return strings.ToUpper(strings.TrimSpace(match[:parenIndex])) + "("
		})
	})
}

func normalizeAnyArrayToIn(s string) string {
	// Pattern: (column = ANY (ARRAY['a', 'b', 'c']))
	// Should become: column IN ('a', 'b', 'c')
	//
	// This is complex because we need to:
	// 1. Find the pattern
	// 2. Extract the values from ARRAY[...]
	// 3. Rebuild as IN (...)
	result := s

	anyArrayPattern := regexp.MustCompile(
		`\(\s*([^=]+?)\s*=\s*ANY\s*\(\s*ARRAY\s*\[(.*?)\]\s*\)\s*\)`,
	)

	result = anyArrayPattern.ReplaceAllStringFunc(result, func(match string) string {
		submatches := anyArrayPattern.FindStringSubmatch(match)
		if len(submatches) < 3 {
			return match
		}

		column := strings.TrimSpace(submatches[1])
		values := submatches[2]

		cleanValues := normalizeArrayValues(values)

		return "(" + column + " IN (" + cleanValues + "))"
	})

	return result
}

func normalizeArrayValues(values string) string {
	var result []string

	var current strings.Builder

	inQuote := false
	depth := 0

	for i := 0; i < len(values); i++ { //nolint:intrange // i is modified in loop body
		c := values[i]

		switch c {
		case '\'':
			handleQuoteChar(&inQuote, &i, values, &current, c)

		case '(':
			if !inQuote {
				depth++
			}

			current.WriteByte(c)

		case ')':
			if !inQuote {
				depth--
			}

			current.WriteByte(c)

		case ',':
			if !inQuote && depth == 0 {
				val := strings.TrimSpace(current.String())
				if val != "" {
					result = append(result, val)
				}

				current.Reset()
			} else {
				current.WriteByte(c)
			}

		default:
			current.WriteByte(c)
		}
	}

	val := strings.TrimSpace(current.String())
	if val != "" {
		result = append(result, val)
	}

	return strings.Join(result, ", ")
}

func handleQuoteChar(inQuote *bool, i *int, values string, current *strings.Builder, c byte) {
	switch {
	case !*inQuote:
		*inQuote = true
	case *i+1 < len(values) && values[*i+1] == '\'':
		current.WriteByte(c)
		current.WriteByte(c)

		*i++

		return
	default:
		*inQuote = false
	}

	current.WriteByte(c)
}

func FormatCheckConstraintMultiline(column string, values []string) string {
	if len(values) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("CHECK (")
	sb.WriteString(column)
	sb.WriteString(" IN (\n")

	for i, v := range values {
		sb.WriteString(sqlIndent)
		sb.WriteString(formatSQLStringLiteral(v))

		if i < len(values)-1 {
			sb.WriteString(",")
		}

		sb.WriteString("\n")
	}

	sb.WriteString("))")

	return sb.String()
}
