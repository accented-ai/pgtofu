package differ

import (
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strings"

	"github.com/accented-ai/pgtofu/internal/parser"
)

type viewNormalizer struct {
	typeCastPattern *regexp.Regexp
	intervalPattern *regexp.Regexp
}

func NewViewNormalizer() *viewNormalizer {
	return &viewNormalizer{
		typeCastPattern: regexp.MustCompile(
			`::(?:numeric|text|bigint|integer|smallint|real|double precision|character varying|varchar|timestamp|timestamptz|interval|boolean|jsonb|json)`, //nolint:lll
		),
		intervalPattern: regexp.MustCompile(`\binterval\s+('[0-9:]+')`),
	}
}

func (vn *viewNormalizer) normalizeDefinition(definition string) string {
	def := vn.NormalizeText(definition)

	if parsed := parseAndNormalizeView(def); parsed != "" {
		return parsed
	}

	return def
}

func (vn *viewNormalizer) NormalizeText(definition string) string {
	def := strings.TrimSpace(definition)
	def = normalizeWhitespace(def)
	def = strings.ToLower(def)
	def = strings.TrimSuffix(def, ";")

	def = vn.removeTypeCasts(def)
	def = vn.normalizeBooleans(def)
	def = vn.normalizeIntervals(def)
	def = vn.removeRedundantParens(def)

	return def
}

func (vn *viewNormalizer) removeTypeCasts(s string) string {
	return vn.typeCastPattern.ReplaceAllString(s, "")
}

func (vn *viewNormalizer) normalizeBooleans(s string) string {
	replacements := map[string]string{
		"= true":  "= 't'",
		"= false": "= 'f'",
		" true ":  " 't' ",
		" false ": " 'f' ",
	}

	for old, new := range replacements {
		s = strings.ReplaceAll(s, old, new)
	}

	return s
}

func (vn *viewNormalizer) normalizeIntervals(s string) string {
	dayIntervals := map[string]string{
		"interval '1 day'":   "'1 day'",
		"interval '2 days'":  "'2 days'",
		"interval '3 days'":  "'3 days'",
		"interval '7 days'":  "'7 days'",
		"interval '30 days'": "'30 days'",
		"interval '1 year'":  "'1 year'",
	}

	for pattern, replacement := range dayIntervals {
		s = strings.ReplaceAll(s, pattern, replacement)
	}

	intervalLiteralPattern := regexp.MustCompile(
		`(?:interval\s+)?'([^']+)'`,
	)

	s = intervalLiteralPattern.ReplaceAllStringFunc(s, func(match string) string {
		start := strings.Index(match, "'")

		end := strings.LastIndex(match, "'")
		if start == -1 || end == -1 || start == end {
			return match
		}

		content := match[start+1 : end]

		normalized := normalizeIntervalValue(content)
		if normalized != "" {
			return "'" + normalized + "'"
		}

		return match
	})

	return s
}

func normalizeIntervalValue(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))

	if isHHMMSSFormat(value) {
		return value
	}

	parts := strings.Fields(value)
	if len(parts) != 2 {
		return ""
	}

	var num int
	if _, err := fmt.Sscanf(parts[0], "%d", &num); err != nil {
		return ""
	}

	unit := strings.TrimSuffix(parts[1], "s")

	switch unit {
	case "second":
		return fmt.Sprintf("%02d:%02d:%02d", 0, 0, num)
	case "minute":
		return fmt.Sprintf("%02d:%02d:%02d", 0, num, 0)
	case "hour":
		return fmt.Sprintf("%02d:%02d:%02d", num, 0, 0)
	default:
		return ""
	}
}

func isHHMMSSFormat(s string) bool {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return false
	}

	for _, part := range parts {
		if len(part) != 2 {
			return false
		}

		for _, c := range part {
			if c < '0' || c > '9' {
				return false
			}
		}
	}

	return true
}

func (vn *viewNormalizer) removeRedundantParens(s string) string {
	return s
}

func normalizeWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func normalizeCheckOption(opt string) string {
	opt = strings.ToLower(strings.TrimSpace(opt))
	if opt == "none" {
		return ""
	}

	return opt
}

func NormalizeViewDefinition(definition string) string {
	vn := NewViewNormalizer()
	return vn.normalizeDefinition(definition)
}

func parseAndNormalizeView(def string) string {
	lexer := parser.NewLexer(def)

	tokens, err := lexer.Tokenize()
	if err != nil {
		return ""
	}

	normalizer := &sqlNormalizer{
		tokens: tokens,
	}

	normalized, err := normalizer.normalize()
	if err != nil {
		return ""
	}

	return normalized
}

type sqlNormalizer struct {
	tokens []parser.Token
	pos    int
}

func (n *sqlNormalizer) normalize() (string, error) {
	stmt := n.parseSelectStatement()

	jsonData, err := json.Marshal(stmt)
	if err != nil {
		return "", fmt.Errorf("failed to marshal statement to JSON: %w", err)
	}

	return string(jsonData), nil
}

func (n *sqlNormalizer) parseSelectStatement() map[string]any {
	stmt := make(map[string]any)
	aliases := make(map[string]any)

	var fromTable string

	for n.pos < len(n.tokens) {
		tok := n.current()
		if tok.Type == parser.TokenEOF {
			break
		}

		if n.matchKeyword("WITH") {
			n.advance()
			ctes := n.parseWithClause()
			stmt["with"] = ctes

			continue
		}

		if n.matchKeyword("SELECT") {
			n.advance()
			selectList, selectAliases := n.parseSelectList()
			stmt["select"] = selectList

			maps.Copy(aliases, selectAliases)

			continue
		}

		if n.matchKeyword("FROM") {
			n.advance()
			fromClause, table := n.parseFromClause()
			stmt["from"] = fromClause
			fromTable = table

			continue
		}

		if n.matchKeyword("WHERE") {
			n.advance()
			whereClause := n.parseExpression(false)
			stmt["where"] = whereClause

			continue
		}

		if n.matchKeyword("GROUP") {
			n.advance()

			if n.matchKeyword("BY") {
				n.advance()
				groupBy := n.parseGroupBy(aliases)
				stmt["group_by"] = groupBy
			}

			continue
		}

		if n.matchKeyword("HAVING") {
			n.advance()
			havingClause := n.parseExpression(false)
			stmt["having"] = havingClause

			continue
		}

		if n.matchKeyword("ORDER") {
			n.advance()

			if n.matchKeyword("BY") {
				n.advance()
				orderBy := n.parseOrderBy(aliases)
				stmt["order_by"] = orderBy
			}

			continue
		}

		if n.matchKeyword("LIMIT") {
			n.advance()
			limit := n.parseExpression(false)
			stmt["limit"] = limit

			continue
		}

		if n.matchKeyword("OFFSET") {
			n.advance()
			offset := n.parseExpression(false)
			stmt["offset"] = offset

			continue
		}

		n.advance()
	}

	if fromTable != "" {
		n.stripQualifiersFromStmt(stmt, fromTable)
	}

	return stmt
}

func (n *sqlNormalizer) parseWithClause() []map[string]any {
	var ctes []map[string]any

	for n.current().Type != parser.TokenEOF {
		if n.matchKeyword("SELECT") {
			break
		}

		cteName := ""
		if n.current().Type == parser.TokenIdentifier ||
			n.current().Type == parser.TokenQuotedIdentifier {
			cteName = strings.ToLower(n.current().Literal)
			n.advance()
		}

		if n.matchKeyword("AS") {
			n.advance()
		}

		if n.current().Type == parser.TokenLParen {
			n.advance()

			depth := 1
			start := n.pos

			for depth > 0 && n.pos < len(n.tokens) {
				if n.current().Type == parser.TokenLParen {
					depth++
				} else if n.current().Type == parser.TokenRParen {
					depth--
				}

				if depth > 0 {
					n.advance()
				}
			}

			cteTokens := n.tokens[start:n.pos]
			if len(cteTokens) > 0 {
				subNormalizer := &sqlNormalizer{
					tokens: cteTokens,
					pos:    0,
				}

				cteStmt := subNormalizer.parseSelectStatement()
				ctes = append(ctes, map[string]any{
					"name": cteName,
					"stmt": cteStmt,
				})
			}

			n.advance()
		}

		if n.current().Type == parser.TokenComma {
			n.advance()
			continue
		}

		if n.matchKeyword("SELECT") {
			break
		}

		if n.current().Type == parser.TokenEOF {
			break
		}

		n.advance()
	}

	return ctes
}

func (n *sqlNormalizer) parseSelectList() ([]map[string]any, map[string]any) {
	var items []map[string]any

	aliases := make(map[string]any)

	for n.current().Type != parser.TokenEOF {
		if n.matchKeyword("FROM") || n.matchKeyword("WHERE") ||
			n.matchKeyword("GROUP") || n.matchKeyword("ORDER") ||
			n.matchKeyword("LIMIT") || n.matchKeyword("OFFSET") {
			break
		}

		expr := n.parseExpression(true)
		alias := ""

		if n.matchKeyword("AS") {
			n.advance()

			if n.current().Type == parser.TokenIdentifier ||
				n.current().Type == parser.TokenQuotedIdentifier {
				alias = strings.ToLower(n.current().Literal)
				n.advance()
			}
		} else if n.current().Type == parser.TokenIdentifier || n.current().Type == parser.TokenQuotedIdentifier {
			if !n.matchKeyword("FROM") && !n.matchKeyword("WHERE") &&
				!n.matchKeyword("GROUP") && !n.matchKeyword("ORDER") &&
				!n.matchKeyword("LIMIT") && !n.matchKeyword("OFFSET") &&
				n.current().Type != parser.TokenComma {
				alias = strings.ToLower(n.current().Literal)
				n.advance()
			}
		}

		item := map[string]any{"expr": expr}
		if alias != "" {
			item["alias"] = alias
			aliases[alias] = expr
		}

		items = append(items, item)

		if n.current().Type == parser.TokenComma {
			n.advance()
			continue
		}

		break
	}

	return items, aliases
}

func (n *sqlNormalizer) parseFromClause() ([]map[string]any, string) { //nolint:cyclop,gocognit,gocyclo
	var (
		tables       []map[string]any
		primaryTable string
	)

	if n.current().Type == parser.TokenLParen {
		n.advance()
	}

	for n.current().Type != parser.TokenEOF {
		if n.matchKeyword("WHERE") || n.matchKeyword("GROUP") ||
			n.matchKeyword("ORDER") || n.matchKeyword("HAVING") ||
			n.matchKeyword("LIMIT") || n.matchKeyword("OFFSET") {
			break
		}

		if n.matchKeyword("LEFT") || n.matchKeyword("RIGHT") ||
			n.matchKeyword("INNER") || n.matchKeyword("OUTER") ||
			n.matchKeyword("CROSS") {
			n.advance()

			if n.matchKeyword("OUTER") || n.matchKeyword("JOIN") {
				n.advance()
			}

			continue
		}

		if n.matchKeyword("JOIN") {
			n.advance()
			continue
		}

		if n.matchKeyword("ON") { //nolint:nestif
			n.advance()

			parenDepth := 0

			for n.pos < len(n.tokens) {
				tok := n.current()
				if tok.Type == parser.TokenLParen {
					parenDepth++
				} else if tok.Type == parser.TokenRParen {
					if parenDepth == 0 {
						break
					}

					parenDepth--
				}

				if parenDepth == 0 {
					if n.matchKeyword("WHERE") || n.matchKeyword("GROUP") ||
						n.matchKeyword("ORDER") || n.matchKeyword("HAVING") ||
						n.matchKeyword("LIMIT") || n.matchKeyword("OFFSET") ||
						n.matchKeyword("LEFT") || n.matchKeyword("RIGHT") ||
						n.matchKeyword("INNER") || n.matchKeyword("JOIN") {
						break
					}
				}

				n.advance()
			}

			continue
		}

		if n.current().Type == parser.TokenRParen {
			n.advance()
			continue
		}

		if n.current().Type == parser.TokenIdentifier || //nolint:nestif
			n.current().Type == parser.TokenQuotedIdentifier {
			tableName := strings.ToLower(n.current().Literal)
			n.advance()

			if n.current().Type == parser.TokenDot {
				n.advance()

				if n.current().Type == parser.TokenIdentifier ||
					n.current().Type == parser.TokenQuotedIdentifier {
					tableName = tableName + "." + strings.ToLower(n.current().Literal)
					n.advance()
				}
			}

			tableAlias := ""

			if n.matchKeyword("AS") {
				n.advance()
			}

			if n.current().Type == parser.TokenIdentifier ||
				n.current().Type == parser.TokenQuotedIdentifier {
				if !n.matchKeyword("WHERE") && !n.matchKeyword("GROUP") &&
					!n.matchKeyword("ORDER") && !n.matchKeyword("HAVING") &&
					!n.matchKeyword("LEFT") && !n.matchKeyword("RIGHT") &&
					!n.matchKeyword("INNER") && !n.matchKeyword("CROSS") &&
					!n.matchKeyword("JOIN") && !n.matchKeyword("LIMIT") &&
					!n.matchKeyword("OFFSET") {
					tableAlias = strings.ToLower(n.current().Literal)
					n.advance()
				}
			}

			if primaryTable == "" {
				primaryTable = tableName
			}

			table := map[string]any{"name": tableName}
			if tableAlias != "" {
				table["alias"] = tableAlias
			}

			tables = append(tables, table)

			continue
		}

		if n.current().Type == parser.TokenComma {
			n.advance()
			continue
		}

		n.advance()
	}

	return tables, primaryTable
}

func (n *sqlNormalizer) parseExpression(stopAtComma bool) map[string]any {
	expr := make(map[string]any)

	var parts []string

	parenDepth := 0
	caseDepth := 0

	for n.pos < len(n.tokens) {
		tok := n.current()

		if tok.Type == parser.TokenEOF {
			break
		}

		if n.matchKeyword("CASE") {
			caseDepth++
		} else if n.matchKeyword("END") && caseDepth > 0 {
			caseDepth--
		}

		if tok.Type == parser.TokenLParen {
			parenDepth++
		} else if tok.Type == parser.TokenRParen {
			if parenDepth == 0 {
				break
			}

			parenDepth--
		}

		if parenDepth == 0 && caseDepth == 0 { //nolint:nestif
			if stopAtComma && tok.Type == parser.TokenComma {
				break
			}

			if n.matchKeyword("FROM") || n.matchKeyword("WHERE") ||
				n.matchKeyword("GROUP") || n.matchKeyword("ORDER") ||
				n.matchKeyword("HAVING") || n.matchKeyword("LIMIT") ||
				n.matchKeyword("OFFSET") {
				break
			}

			if tok.Type == parser.TokenKeyword {
				keyword := strings.ToUpper(tok.Literal)
				if keyword == "AS" {
					break
				}
			}
		}

		literal := n.normalizeTokenLiteral(tok)
		parts = append(parts, literal)

		n.advance()
	}

	if len(parts) > 0 {
		normalized := strings.Join(parts, " ")
		normalized = n.normalizeParentheses(normalized)
		expr["value"] = normalized
	}

	return expr
}

func (n *sqlNormalizer) normalizeTokenLiteral(tok parser.Token) string {
	switch tok.Type {
	case parser.TokenIdentifier, parser.TokenKeyword:
		return strings.ToLower(tok.Literal)
	case parser.TokenQuotedIdentifier:
		return strings.ToLower(strings.Trim(tok.Literal, `"`))
	default:
		return tok.Literal
	}
}

func (n *sqlNormalizer) normalizeParentheses(s string) string {
	s = strings.TrimSpace(s)

	for {
		trimmed := s
		if strings.HasPrefix(s, "( ") && strings.HasSuffix(s, " )") { //nolint:nestif
			inner := strings.TrimSpace(s[2 : len(s)-2])

			parenDepth := 0
			valid := true

			for _, r := range inner {
				if r == '(' {
					parenDepth++
				} else if r == ')' {
					parenDepth--
					if parenDepth < 0 {
						valid = false
						break
					}
				}
			}

			if valid && parenDepth == 0 {
				s = inner
			} else {
				break
			}
		} else {
			break
		}

		if trimmed == s {
			break
		}
	}

	for range 10 {
		normalized := n.removeRedundantInnerParentheses(s)
		if normalized == s {
			break
		}

		s = normalized
	}

	return s
}

func (n *sqlNormalizer) removeRedundantInnerParentheses(s string) string {
	result := strings.Builder{}
	i := 0

	for i < len(s) {
		if i+1 < len(s) && s[i] == '(' && s[i+1] == ' ' { //nolint:nestif
			parenDepth := 1
			j := i + 2

			for j < len(s) && parenDepth > 0 {
				switch s[j] {
				case '(':
					parenDepth++
				case ')':
					parenDepth--
				}

				if parenDepth > 0 {
					j++
				}
			}

			if parenDepth == 0 && j < len(s) {
				inner := s[i+2 : j]

				if n.canRemoveParentheses(inner) {
					result.WriteString(inner)

					i = j + 1
					if i < len(s) && s[i] == ' ' {
						i++
					}

					continue
				}
			}
		}

		result.WriteByte(s[i])
		i++
	}

	return result.String()
}

func (n *sqlNormalizer) canRemoveParentheses(s string) bool {
	if n.isSimpleExpression(s) {
		return true
	}

	s = strings.TrimSpace(s)
	lowerS := strings.ToLower(s)

	if strings.HasPrefix(lowerS, "count ") ||
		strings.HasPrefix(lowerS, "max ") ||
		strings.HasPrefix(lowerS, "min ") ||
		strings.HasPrefix(lowerS, "sum ") ||
		strings.HasPrefix(lowerS, "avg ") {
		return true
	}

	if strings.Contains(lowerS, " is not null") || strings.Contains(lowerS, " is null") {
		return true
	}

	keywords := []string{" and ", " or "}
	for _, kw := range keywords {
		if strings.Contains(lowerS, kw) {
			parenDepth := 0

			for _, r := range s {
				switch r {
				case '(':
					parenDepth++
				case ')':
					parenDepth--
				}
			}

			if parenDepth == 0 {
				return true
			}
		}
	}

	parenDepth := 0
	topLevelOps := 0

	for _, r := range s {
		switch r {
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		}

		if parenDepth == 0 && (r == '*' || r == '/') {
			topLevelOps++
		}
	}

	if topLevelOps <= 2 && parenDepth == 0 {
		return true
	}

	return false
}

func (n *sqlNormalizer) isSimpleExpression(s string) bool {
	s = strings.TrimSpace(s)

	keywords := []string{" and ", " or ", " case ", " when ", " then ", " else ", " end "}

	lowerS := strings.ToLower(s)
	for _, kw := range keywords {
		if strings.Contains(lowerS, kw) {
			return false
		}
	}

	parenDepth := 0
	hasTopLevelOperator := false
	topLevelOperators := 0

	i := 0
	for i < len(s) {
		r := rune(s[i])
		switch r {
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		}

		if parenDepth == 0 {
			if r == '+' || r == '-' || r == '*' || r == '/' ||
				r == '=' || r == '<' || r == '>' {
				hasTopLevelOperator = true
				topLevelOperators++
			}
		}

		i++
	}

	if hasTopLevelOperator && topLevelOperators <= 3 {
		return true
	}

	return false
}

func (n *sqlNormalizer) parseGroupBy(aliases map[string]any) []map[string]any {
	var items []map[string]any

	for n.current().Type != parser.TokenEOF {
		if n.matchKeyword("HAVING") || n.matchKeyword("ORDER") ||
			n.matchKeyword("LIMIT") || n.matchKeyword("OFFSET") {
			break
		}

		expr := n.parseExpression(true)
		if exprValue, ok := expr["value"].(string); ok {
			normalized := strings.TrimSpace(exprValue)
			if aliasExpr, found := aliases[normalized]; found {
				expr = aliasExpr.(map[string]any) //nolint:forcetypeassert
			}
		}

		items = append(items, expr)

		if n.current().Type == parser.TokenComma {
			n.advance()
			continue
		}

		break
	}

	return items
}

func (n *sqlNormalizer) parseOrderBy(aliases map[string]any) []map[string]any {
	var items []map[string]any

	for n.current().Type != parser.TokenEOF {
		if n.matchKeyword("LIMIT") || n.matchKeyword("OFFSET") {
			break
		}

		expr := n.parseExpression(true)
		if exprValue, ok := expr["value"].(string); ok {
			normalized := strings.TrimSpace(exprValue)
			if aliasExpr, found := aliases[normalized]; found {
				expr = aliasExpr.(map[string]any) //nolint:forcetypeassert
			}
		}

		direction := "ASC"
		if n.matchKeyword("ASC") || n.matchKeyword("DESC") {
			direction = strings.ToUpper(n.current().Literal)
			n.advance()
		}

		item := map[string]any{
			"expr":      expr,
			"direction": direction,
		}

		items = append(items, item)

		if n.current().Type == parser.TokenComma {
			n.advance()
			continue
		}

		break
	}

	return items
}

func (n *sqlNormalizer) stripQualifiersFromStmt(stmt map[string]any, fromTable string) {
	tableParts := strings.Split(fromTable, ".")
	tableNames := make(map[string]bool)

	if len(tableParts) > 1 {
		tableNames[tableParts[0]] = true
		tableNames[tableParts[len(tableParts)-1]] = true
	}

	tableNames[fromTable] = true

	if fromClause, ok := stmt["from"].([]map[string]any); ok {
		for i, table := range fromClause {
			if name, ok := table["name"].(string); ok {
				parts := strings.Split(name, ".")
				if len(parts) > 1 {
					tableNames[parts[0]] = true
					fromClause[i]["name"] = parts[len(parts)-1]
				}

				tableNames[parts[len(parts)-1]] = true
				tableNames[name] = true
			}

			if alias, ok := table["alias"].(string); ok {
				tableNames[alias] = true
			}
		}
	}

	n.stripQualifiers(stmt, tableNames)
}

func (n *sqlNormalizer) stripQualifiers(node any, tableNames map[string]bool) {
	switch v := node.(type) {
	case map[string]any:
		if value, ok := v["value"].(string); ok {
			v["value"] = n.stripQualifierFromString(value, tableNames)
		}

		for _, child := range v {
			n.stripQualifiers(child, tableNames)
		}
	case []any:
		for _, child := range v {
			n.stripQualifiers(child, tableNames)
		}
	case []map[string]any:
		for _, child := range v {
			n.stripQualifiers(child, tableNames)
		}
	}
}

func (n *sqlNormalizer) stripQualifierFromString(s string, tableNames map[string]bool) string {
	words := strings.Fields(s)
	collapsed := n.collapseQualifiedNames(words, tableNames)
	collapsed = n.stripRemainingQualifiers(collapsed, tableNames)

	return strings.Join(collapsed, " ")
}

func (n *sqlNormalizer) collapseQualifiedNames(
	words []string,
	tableNames map[string]bool,
) []string {
	var collapsed []string

	for i := 0; i < len(words); i++ {
		if !n.isDotPattern(words, i) {
			collapsed = append(collapsed, words[i])
			continue
		}

		if tableNames[words[i]] {
			i += 2
			if i < len(words) {
				collapsed = append(collapsed, words[i])
			}
		} else {
			collapsed = append(collapsed, words[i]+"."+words[i+2])
			i += 2
		}
	}

	return collapsed
}

func (n *sqlNormalizer) isDotPattern(words []string, i int) bool {
	return i+2 < len(words) && words[i+1] == "."
}

func (n *sqlNormalizer) stripRemainingQualifiers(
	words []string,
	tableNames map[string]bool,
) []string {
	for i, word := range words {
		if !strings.Contains(word, ".") {
			continue
		}

		parts := strings.Split(word, ".")
		if len(parts) == 2 && tableNames[parts[0]] {
			words[i] = parts[1]
		}
	}

	return words
}

func (n *sqlNormalizer) current() parser.Token {
	if n.pos >= len(n.tokens) {
		return parser.Token{Type: parser.TokenEOF}
	}

	return n.tokens[n.pos]
}

func (n *sqlNormalizer) advance() {
	if n.pos < len(n.tokens) {
		n.pos++
	}
}

func (n *sqlNormalizer) matchKeyword(keyword string) bool {
	tok := n.current()
	if tok.Type != parser.TokenKeyword {
		return false
	}

	return strings.EqualFold(tok.Literal, keyword)
}
