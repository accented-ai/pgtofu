package parser

import (
	"strings"
)

var sqlKeywords = map[string]bool{ //nolint:gochecknoglobals
	"SELECT": true, "FROM": true, "WHERE": true, "INSERT": true,
	"UPDATE": true, "DELETE": true, "CREATE": true, "ALTER": true,
	"DROP": true, "TABLE": true, "INDEX": true, "VIEW": true,
	"FUNCTION": true, "TRIGGER": true, "AS": true, "ON": true,
	"AND": true, "OR": true, "NOT": true, "NULL": true, "TRUE": true,
	"FALSE": true, "IS": true, "IN": true, "EXISTS": true, "BETWEEN": true,
	"LIKE": true, "ILIKE": true, "CASE": true, "WHEN": true, "THEN": true,
	"ELSE": true, "END": true, "JOIN": true, "LEFT": true, "RIGHT": true,
	"INNER": true, "OUTER": true, "FULL": true, "CROSS": true, "NATURAL": true,
	"USING": true, "GROUP": true, "HAVING": true, "ORDER": true, "BY": true,
	"LIMIT": true, "OFFSET": true, "DISTINCT": true, "UNION": true,
	"INTERSECT": true, "EXCEPT": true, "ALL": true, "ANY": true, "SOME": true,
}

func NormalizeSQL(sql string) string {
	sql = stripComments(sql)
	sql = normalizeWhitespace(sql)
	sql = normalizeKeywords(sql)
	sql = strings.TrimSuffix(strings.TrimSpace(sql), ";")

	return sql
}

func normalizeKeywords(sql string) string {
	var result strings.Builder

	inString := false
	inQuoted := false

	var strChar rune

	words := strings.Fields(sql)
	for i, word := range words {
		if i > 0 {
			result.WriteString(" ")
		}

		for _, ch := range word {
			if !inString && !inQuoted && (ch == '\'' || ch == '"') {
				if ch == '\'' {
					inString = true
				} else {
					inQuoted = true
				}

				strChar = ch
			} else if (inString && ch == '\'' && strChar == '\'') ||
				(inQuoted && ch == '"' && strChar == '"') {
				inString = false
				inQuoted = false
			}
		}

		if !inString && !inQuoted {
			upper := strings.ToUpper(word)
			if sqlKeywords[upper] {
				result.WriteString(upper)
			} else {
				result.WriteString(word)
			}
		} else {
			result.WriteString(word)
		}
	}

	return result.String()
}

func CompareSQL(sql1, sql2 string) bool {
	return NormalizeSQL(sql1) == NormalizeSQL(sql2)
}
