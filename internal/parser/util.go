package parser

import (
	"regexp"
	"strings"
)

func extractParens(s string) string {
	s = strings.TrimSpace(s)

	startIdx := strings.Index(s, "(")
	if startIdx == -1 {
		return ""
	}

	depth := 0

	for i := startIdx; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return s[startIdx+1 : i]
			}
		}
	}

	return ""
}

func splitByComma(s string) []string {
	var (
		parts   []string
		current strings.Builder
		depth   int
		inStr   bool
		strChar rune
	)

	for i, ch := range s {
		switch {
		case !inStr && (ch == '\'' || ch == '"'):
			inStr = true
			strChar = ch
			current.WriteRune(ch)

		case inStr && ch == strChar:
			if i+1 < len(s) && rune(s[i+1]) == strChar {
				current.WriteRune(ch)
				current.WriteRune(ch)
			} else {
				inStr = false

				current.WriteRune(ch)
			}

		case !inStr && ch == '(':
			depth++

			current.WriteRune(ch)

		case !inStr && ch == ')':
			depth--

			current.WriteRune(ch)

		case !inStr && ch == ',' && depth == 0:
			parts = append(parts, strings.TrimSpace(current.String()))
			current.Reset()

		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}

	return parts
}

func hasKeyword(s, keyword string) bool {
	pattern := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(keyword) + `\b`)
	return pattern.MatchString(s)
}

func extractAfterKeyword(s, keyword string) string {
	if keyword == "ON DELETE" || keyword == "ON UPDATE" {
		return extractActionAfterKeyword(s, keyword)
	}

	pattern := regexp.MustCompile(
		`(?i)\b` + regexp.QuoteMeta(
			keyword,
		) + `\s+(.+?)(?:\s+(?:NOT|NULL|CHECK|REFERENCES|PRIMARY|FOREIGN|UNIQUE|DEFAULT|CONSTRAINT)|$)`,
	)
	if matches := pattern.FindStringSubmatch(s); len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}

	return ""
}

func extractActionAfterKeyword(s, keyword string) string {
	pattern := regexp.MustCompile(
		`(?i)\b` + regexp.QuoteMeta(
			keyword,
		) + `\s+(CASCADE|RESTRICT|NO\s+ACTION|SET\s+NULL|SET\s+DEFAULT)`,
	)
	if matches := pattern.FindStringSubmatch(s); len(matches) > 1 {
		action := strings.ToUpper(strings.Join(strings.Fields(matches[1]), " "))
		return action
	}

	return ""
}

func stripComments(s string) string {
	s = regexp.MustCompile(`--[^\n]*`).ReplaceAllString(s, "")
	s = regexp.MustCompile(`/\*.*?\*/`).ReplaceAllString(s, "")

	return s
}

func normalizeWhitespace(s string) string {
	operators := []string{"!=", "<>", "<=", ">=", "=", "<", ">"}
	for _, op := range operators {
		// Add space before and after operator if not already present
		// Pattern: (non-space)(operator)(non-space) -> $1 $2 $3
		pattern := regexp.MustCompile(`([^\s])` + regexp.QuoteMeta(op) + `([^\s])`)
		s = pattern.ReplaceAllString(s, `$1 `+op+` $2`)
	}

	// Then collapse multiple spaces into single spaces
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, " ")

	return strings.TrimSpace(s)
}
