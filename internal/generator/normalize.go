package generator

import (
	"regexp"
	"strings"
)

func NormalizeSQL(sql string) string {
	if sql == "" {
		return ""
	}

	result := sql
	result = normalizeBooleans(result)
	result = normalizeTypeCasts(result)
	result = normalizeFunctionNames(result)

	return result
}

func NormalizeCheckConstraint(def string) string {
	if def == "" {
		return ""
	}

	result := def
	result = normalizeAnyArrayToIn(result)
	result = normalizeTypeCasts(result)
	result = normalizeBooleans(result)
	result = normalizeFunctionNames(result)

	return result
}

func NormalizeWhereClause(where string) string {
	if where == "" {
		return ""
	}

	result := where
	result = normalizeBooleans(result)
	result = normalizeTypeCasts(result)

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

	result = normalizeTypeCasts(result)
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

func normalizeBooleans(s string) string {
	result := s

	truePattern := regexp.MustCompile(`\btrue\b`)
	falsePattern := regexp.MustCompile(`\bfalse\b`)

	result = truePattern.ReplaceAllString(result, "TRUE")
	result = falsePattern.ReplaceAllString(result, "FALSE")

	return result
}

func normalizeTypeCasts(s string) string {
	result := s

	stringCastPattern := regexp.MustCompile(`('(?:[^']*|'')*')::[a-zA-Z_][a-zA-Z0-9_\s]*(?:\[\])?`)
	result = stringCastPattern.ReplaceAllString(result, "$1")

	return result
}

func normalizeFunctionNames(s string) string {
	result := s

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
		"array_agg",
		"string_agg",
		"json_agg",
		"jsonb_agg",
		"count",
		"sum",
		"avg",
		"min",
		"max",
	}

	for _, fn := range functions {
		pattern := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(fn) + `\s*\(`)

		result = pattern.ReplaceAllStringFunc(result, func(match string) string {
			// Find where the paren is and uppercase everything before it
			parenIdx := strings.Index(match, "(")
			if parenIdx == -1 {
				return match
			}

			return strings.ToUpper(strings.TrimSpace(match[:parenIdx])) + "("
		})
	}

	return result
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
					result = append(result, normalizeArrayValue(val))
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
		result = append(result, normalizeArrayValue(val))
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

func normalizeArrayValue(val string) string {
	castPattern := regexp.MustCompile(`^('(?:[^']*|'')*')::[a-zA-Z_][a-zA-Z0-9_\s]*(?:\[\])?$`)
	if matches := castPattern.FindStringSubmatch(val); matches != nil {
		return matches[1]
	}

	return val
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
