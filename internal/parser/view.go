package parser

import (
	"regexp"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type viewStatement struct {
	schemaName   string
	viewName     string
	definition   string
	withClause   string
	withData     bool
	materialized bool
}

func (p *Parser) parseCreateView(stmt string, db *schema.Database) error {
	parsed, err := p.parseViewStatement(stmt, false)
	if err != nil || parsed == nil {
		return err
	}

	view := schema.View{
		Schema:     parsed.schemaName,
		Name:       parsed.viewName,
		Definition: parsed.definition,
	}

	for i, existing := range db.Views {
		if existing.Schema == parsed.schemaName && existing.Name == parsed.viewName {
			db.Views[i] = view
			return nil
		}
	}

	db.Views = append(db.Views, view)

	return nil
}

func (p *Parser) parseCreateMaterializedView(stmt string, db *schema.Database) error {
	parsed, err := p.parseViewStatement(stmt, true)
	if err != nil || parsed == nil {
		return err
	}

	definition := parsed.definition
	withClause := strings.ToUpper(parsed.withClause)

	isContinuousAgg := strings.Contains(withClause, "TIMESCALEDB.CONTINUOUS")
	if !isContinuousAgg {
		isContinuousAgg = strings.Contains(strings.ToUpper(stmt), "TIMESCALEDB.CONTINUOUS")
	}

	if isContinuousAgg {
		htSchema, htName := extractHypertableFromQuery(definition)

		cagg := schema.ContinuousAggregate{
			Schema:           parsed.schemaName,
			ViewName:         parsed.viewName,
			HypertableSchema: htSchema,
			HypertableName:   htName,
			Query:            definition,
			WithData:         parsed.withData,
			Materialized:     true,
			Finalized:        strings.Contains(strings.ToUpper(stmt), "FINALIZED"),
		}

		for i, existing := range db.ContinuousAggregates {
			if existing.Schema == parsed.schemaName && existing.ViewName == parsed.viewName {
				db.ContinuousAggregates[i] = cagg
				return nil
			}
		}

		db.ContinuousAggregates = append(db.ContinuousAggregates, cagg)
	} else {
		mv := schema.MaterializedView{
			Schema:     parsed.schemaName,
			Name:       parsed.viewName,
			Definition: definition,
			WithData:   parsed.withData,
		}

		for i, existing := range db.MaterializedViews {
			if existing.Schema == parsed.schemaName && existing.Name == parsed.viewName {
				db.MaterializedViews[i] = mv
				return nil
			}
		}

		db.MaterializedViews = append(db.MaterializedViews, mv)
	}

	return nil
}

func (p *Parser) parseViewStatement( //nolint:cyclop,gocognit,gocyclo,maintidx
	stmt string,
	materialized bool,
) (*viewStatement, error) {
	tokens, err := NewLexer(stmt).Tokenize()
	if err != nil {
		return nil, WrapParseError(err, "tokenizing view statement")
	}

	if len(tokens) == 0 {
		return nil, NewParseError("empty view statement")
	}

	idx := nextNonCommentIndex(tokens, 0)
	if idx >= len(tokens) || upperLiteral(tokens, idx) != "CREATE" {
		return nil, NewParseError("expected CREATE keyword")
	}

	idx = nextNonCommentIndex(tokens, idx+1)

	if idx < len(tokens) && upperLiteral(tokens, idx) == "OR" {
		replaceIdx := nextNonCommentIndex(tokens, idx+1)
		if replaceIdx >= len(tokens) || upperLiteral(tokens, replaceIdx) != "REPLACE" {
			return nil, NewParseError("expected REPLACE keyword")
		}

		idx = nextNonCommentIndex(tokens, replaceIdx+1)
	}

	isMaterialized := false
	if idx < len(tokens) && upperLiteral(tokens, idx) == "MATERIALIZED" {
		isMaterialized = true
		idx = nextNonCommentIndex(tokens, idx+1)
	}

	if isMaterialized != materialized {
		return nil, NewParseError("unexpected MATERIALIZED clause")
	}

	if idx >= len(tokens) || upperLiteral(tokens, idx) != "VIEW" {
		return nil, NewParseError("expected VIEW keyword")
	}

	idx = nextNonCommentIndex(tokens, idx+1)

	if idx < len(tokens) && upperLiteral(tokens, idx) == "IF" {
		notIdx := nextNonCommentIndex(tokens, idx+1)

		existsIdx := nextNonCommentIndex(tokens, notIdx+1)
		if notIdx >= len(tokens) || upperLiteral(tokens, notIdx) != "NOT" ||
			existsIdx >= len(tokens) || upperLiteral(tokens, existsIdx) != "EXISTS" {
			return nil, NewParseError("malformed IF NOT EXISTS clause")
		}

		idx = nextNonCommentIndex(tokens, existsIdx+1)
	}

	if idx >= len(tokens) {
		return nil, NewParseError("missing view name")
	}

	nameStartIdx := idx
	nameStart := tokens[nameStartIdx].Start
	cur := nameStartIdx
	expectIdentifier := true
	nameEnd := nameStart

	for cur < len(tokens) {
		tok := tokens[cur]
		if tok.Type == TokenComment {
			cur++
			continue
		}

		if expectIdentifier {
			if tok.Type != TokenIdentifier && tok.Type != TokenQuotedIdentifier {
				break
			}

			nameEnd = tok.End
			cur++
			expectIdentifier = false

			continue
		}

		if tok.Type != TokenDot {
			break
		}

		nameEnd = tok.End
		cur++
		expectIdentifier = true
	}

	if expectIdentifier {
		return nil, NewParseError("invalid view name")
	}

	nameLiteral := strings.TrimSpace(stmt[nameStart:nameEnd])
	if nameLiteral == "" {
		return nil, NewParseError("empty view name")
	}

	idx = cur

	idx = nextNonCommentIndex(tokens, idx)
	if idx < len(tokens) && tokens[idx].Type == TokenLParen {
		_, afterColsIdx, err := extractParenthesizedLiteral(stmt, tokens, idx)
		if err != nil {
			return nil, err
		}

		idx = afterColsIdx
	}

	idx = nextNonCommentIndex(tokens, idx)
	withClause := ""

	if idx < len(tokens) && upperLiteral(tokens, idx) == "WITH" {
		nextIdx := nextNonCommentIndex(tokens, idx+1)
		if nextIdx < len(tokens) && tokens[nextIdx].Type == TokenLParen {
			clause, afterWithIdx, err := extractParenthesizedLiteral(stmt, tokens, nextIdx)
			if err != nil {
				return nil, err
			}

			withClause = clause
			idx = nextNonCommentIndex(tokens, afterWithIdx)
		}
	}

	asIdx := findKeyword(tokens, "AS", idx)
	if asIdx == -1 {
		return nil, NewParseError("missing AS clause")
	}

	defStartIdx := nextNonCommentIndex(tokens, asIdx+1)
	if defStartIdx >= len(tokens) {
		return nil, NewParseError("missing view definition")
	}

	defStart := tokens[defStartIdx].Start
	defEnd := len(stmt)

	if semiIdx := findToken(tokens, TokenSemicolon, defStartIdx); semiIdx != -1 {
		defEnd = tokens[semiIdx].Start
	}

	withData := true

	if materialized { //nolint:nestif
		lastIdx := prevNonCommentIndex(tokens, len(tokens)-1)
		if lastIdx != -1 && tokens[lastIdx].Type == TokenSemicolon {
			lastIdx = prevNonCommentIndex(tokens, lastIdx-1)
		}

		if lastIdx != -1 && upperLiteral(tokens, lastIdx) == "DATA" {
			dataIdx := lastIdx
			prevIdx := prevNonCommentIndex(tokens, dataIdx-1)
			hasNo := false

			if prevIdx != -1 && upperLiteral(tokens, prevIdx) == "NO" {
				hasNo = true
				prevIdx = prevNonCommentIndex(tokens, prevIdx-1)
			}

			if prevIdx != -1 && upperLiteral(tokens, prevIdx) == "WITH" {
				defEnd = tokens[prevIdx].Start
				withData = !hasNo
			}
		}
	}

	definition := strings.TrimSpace(stmt[defStart:defEnd])
	definition = strings.TrimSuffix(definition, ";")
	definition = strings.TrimSpace(definition)
	definition = stripInlineComments(definition)

	schemaName, viewName := p.splitSchemaTable(nameLiteral)

	return &viewStatement{
		schemaName:   schemaName,
		viewName:     viewName,
		definition:   definition,
		withClause:   withClause,
		withData:     withData,
		materialized: materialized,
	}, nil
}

func stripInlineComments(sql string) string {
	lines := strings.Split(sql, "\n")

	var result []string

	for _, line := range lines {
		if idx := strings.Index(line, "--"); idx >= 0 {
			inString := false

			for i := range idx {
				if line[i] == '\'' {
					inString = !inString
				}
			}

			if !inString {
				line = line[:idx]
			}
		}

		if trimmed := strings.TrimSpace(line); trimmed != "" {
			result = append(result, line)
		}
	}

	return strings.Join(result, "\n")
}

func extractHypertableFromQuery(query string) (schemaName, tableName string) {
	normalized := normalizeWhitespace(query)
	normalized = strings.ToUpper(normalized)

	fromPattern := regexp.MustCompile(`\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]*|"[^"]*"(?:\."[^"]*")?)`)
	matches := fromPattern.FindStringSubmatch(normalized)

	if len(matches) < 2 {
		return schema.DefaultSchema, ""
	}

	tableName = strings.TrimSpace(matches[1])

	if idx := strings.IndexAny(tableName, " ,;"); idx > 0 {
		tableName = tableName[:idx]
	}

	if strings.Contains(tableName, ".") {
		parts := strings.SplitN(tableName, ".", 2)

		return strings.ToLower(
				strings.Trim(parts[0], `"`),
			), strings.ToLower(
				strings.Trim(parts[1], `"`),
			)
	}

	return schema.DefaultSchema, strings.ToLower(strings.Trim(tableName, `"`))
}
