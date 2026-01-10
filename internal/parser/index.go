package parser

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type indexStatement struct {
	isUnique    bool
	indexName   string
	tableSchema string
	tableName   string
	indexType   string
	columnExprs []string
	includeCols []string
	whereClause string
	definition  string
}

func (p *Parser) parseCreateIndex(stmt string, db *schema.Database) error {
	parsed, err := p.parseIndexStatement(stmt)
	if err != nil || parsed == nil {
		return err
	}

	idx := schema.Index{
		Schema:         parsed.tableSchema,
		Name:           parsed.indexName,
		TableName:      parsed.tableName,
		Columns:        parsed.columnExprs,
		IsUnique:       parsed.isUnique,
		IsPrimary:      false,
		Type:           parsed.indexType,
		Where:          parsed.whereClause,
		IncludeColumns: parsed.includeCols,
		Definition:     parsed.definition,
	}

	if table := db.GetTable(parsed.tableSchema, parsed.tableName); table != nil {
		for i, existing := range table.Indexes {
			if existing.Name == parsed.indexName {
				table.Indexes[i] = idx
				return nil
			}
		}

		table.Indexes = append(table.Indexes, idx)

		return nil
	}

	if mv := db.GetMaterializedView(parsed.tableSchema, parsed.tableName); mv != nil {
		for i, existing := range mv.Indexes {
			if existing.Name == parsed.indexName {
				mv.Indexes[i] = idx
				return nil
			}
		}

		mv.Indexes = append(mv.Indexes, idx)

		return nil
	}

	if ca := db.GetContinuousAggregate(parsed.tableSchema, parsed.tableName); ca != nil {
		for i, existing := range ca.Indexes {
			if existing.Name == parsed.indexName {
				ca.Indexes[i] = idx
				return nil
			}
		}

		ca.Indexes = append(ca.Indexes, idx)

		return nil
	}

	p.addWarning(
		0,
		fmt.Sprintf(
			"table %s.%s not found for index %s",
			parsed.tableSchema,
			parsed.tableName,
			parsed.indexName,
		),
	)

	return nil
}

func (p *Parser) parseIndexStatement( //nolint:cyclop,gocognit,gocyclo
	stmt string,
) (*indexStatement, error) {
	tokens, err := NewLexer(stmt).Tokenize()
	if err != nil {
		return nil, WrapParseError(err, "tokenizing index statement")
	}

	if len(tokens) == 0 {
		return nil, NewParseError("empty index statement")
	}

	idx := nextNonCommentIndex(tokens, 0)
	if idx >= len(tokens) || upperLiteral(tokens, idx) != "CREATE" {
		return nil, NewParseError("expected CREATE keyword")
	}

	idx = nextNonCommentIndex(tokens, idx+1)

	isUnique := false
	if idx < len(tokens) && upperLiteral(tokens, idx) == "UNIQUE" {
		isUnique = true
		idx = nextNonCommentIndex(tokens, idx+1)
	}

	if idx >= len(tokens) || upperLiteral(tokens, idx) != "INDEX" {
		return nil, NewParseError("expected INDEX keyword")
	}

	idx = nextNonCommentIndex(tokens, idx+1)

	if idx < len(tokens) && upperLiteral(tokens, idx) == "CONCURRENTLY" {
		idx = nextNonCommentIndex(tokens, idx+1)
	}

	if idx < len(tokens) && upperLiteral(tokens, idx) == "IF" {
		if nextNonCommentIndex(tokens, idx+1) >= len(tokens) ||
			upperLiteral(tokens, idx+1) != "NOT" ||
			nextNonCommentIndex(tokens, idx+2) >= len(tokens) ||
			upperLiteral(tokens, idx+2) != "EXISTS" {
			return nil, NewParseError("malformed IF NOT EXISTS clause")
		}

		idx = nextNonCommentIndex(tokens, idx+3)
	}

	if idx >= len(tokens) {
		return nil, NewParseError("missing index name")
	}

	nameToken := tokens[idx]
	if nameToken.Type != TokenIdentifier && nameToken.Type != TokenQuotedIdentifier {
		return nil, NewParseError("invalid index name")
	}

	indexName := p.normalizeIdent(nameToken.Literal)
	idx = nextNonCommentIndex(tokens, idx+1)

	if idx >= len(tokens) || upperLiteral(tokens, idx) != "ON" {
		return nil, NewParseError("expected ON clause")
	}

	tableStart := nextNonCommentIndex(tokens, idx+1)
	if tableStart >= len(tokens) {
		return nil, NewParseError("missing index target table")
	}

	tableLiteral, nextIdx := collectLiteralUntil(
		tokens,
		stmt,
		tableStart,
		"USING",
		"WHERE",
		"INCLUDE",
		"TABLESPACE",
		"WITH",
		"(",
	)

	tableLiteral = strings.TrimSpace(tableLiteral)
	if tableLiteral == "" {
		return nil, NewParseError("missing table reference")
	}

	if strings.HasPrefix(strings.ToUpper(tableLiteral), "ONLY ") {
		tableLiteral = strings.TrimSpace(tableLiteral[4:])
	}

	tableSchema, tableName := p.splitSchemaTable(tableLiteral)

	idx = nextIdx

	indexType := "btree"

	if idx < len(tokens) && upperLiteral(tokens, idx) == "USING" {
		typeIdx := nextNonCommentIndex(tokens, idx+1)
		if typeIdx >= len(tokens) {
			return nil, NewParseError("missing index type")
		}

		indexTypeLiteral := strings.TrimSpace(tokens[typeIdx].Literal)
		indexType = strings.ToLower(indexTypeLiteral)
		idx = nextNonCommentIndex(tokens, typeIdx+1)
	}

	if idx >= len(tokens) || tokens[idx].Type != TokenLParen {
		return nil, NewParseError("missing index column list")
	}

	columnLiteral, nextIdx, err := extractParenthesizedLiteral(stmt, tokens, idx)
	if err != nil {
		return nil, err
	}

	columns := parseIndexColumnsLiteral(p, columnLiteral)
	if len(columns) == 0 {
		return nil, NewParseError("no index columns")
	}

	idx = nextIdx

	includeColumns := []string{}
	whereClause := ""

	for idx < len(tokens) {
		word := upperLiteral(tokens, idx)

		switch word {
		case "INCLUDE":
			parentIdx := nextNonCommentIndex(tokens, idx+1)
			if parentIdx >= len(tokens) || tokens[parentIdx].Type != TokenLParen {
				return nil, NewParseError("missing INCLUDE column list")
			}

			includeLiteral, nextIncludeIdx, err := extractParenthesizedLiteral(
				stmt,
				tokens,
				parentIdx,
			)
			if err != nil {
				return nil, err
			}

			includeColumns = parseIncludeColumnsLiteral(p, includeLiteral)
			idx = nextIncludeIdx

		case "WHERE":
			expr, nextWhereIdx := collectLiteralUntil(
				tokens,
				stmt,
				idx+1,
				"INCLUDE",
				"TABLESPACE",
				"WITH",
			)
			whereClause = strings.TrimSpace(expr)
			idx = nextWhereIdx

		case "":
			idx++
		default:
			idx++
		}
	}

	return &indexStatement{
		isUnique:    isUnique,
		indexName:   indexName,
		tableSchema: tableSchema,
		tableName:   tableName,
		indexType:   indexType,
		columnExprs: columns,
		includeCols: includeColumns,
		whereClause: strings.TrimSpace(whereClause),
		definition:  stmt,
	}, nil
}

func parseIndexColumnsLiteral(p *Parser, literal string) []string {
	parts := splitByComma(literal)
	columns := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			columns = append(columns, p.normalizeIndexColumn(part))
		}
	}

	return columns
}

func parseIncludeColumnsLiteral(p *Parser, literal string) []string {
	if literal == "" {
		return nil
	}

	parts := splitByComma(literal)

	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			columns = append(columns, p.normalizeIdent(trimmed))
		}
	}

	return columns
}

func (p *Parser) normalizeIndexColumn(colExpr string) string {
	colExpr = strings.TrimSpace(colExpr)

	if strings.Contains(colExpr, "(") || strings.Contains(colExpr, ")") {
		return colExpr
	}

	parts := strings.Fields(colExpr)
	if len(parts) == 0 {
		return colExpr
	}

	return p.normalizeIdent(parts[0])
}
