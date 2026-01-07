package parser

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type commentObjectType int

const (
	commentObjectUnknown commentObjectType = iota
	commentObjectTable
	commentObjectColumn
	commentObjectView
	commentObjectMaterializedView
	commentObjectFunction
	commentObjectExtension
	commentObjectTypeAlias
)

type commentStatement struct {
	objectType   commentObjectType
	schemaName   string
	objectName   string
	columnName   string
	functionArgs []string
	commentText  string
	isNull       bool
}

func (p *Parser) parseComment(stmt string, db *schema.Database) error { //nolint:cyclop
	parsed, err := p.parseCommentStatement(stmt)
	if err != nil {
		return err
	}

	if parsed == nil {
		return nil
	}

	commentValue := ""
	if !parsed.isNull {
		commentValue = parsed.commentText
	}

	switch parsed.objectType {
	case commentObjectTable:
		table := db.GetTable(parsed.schemaName, parsed.objectName)
		if table != nil {
			table.Comment = commentValue
			return nil
		}

		p.addWarning(
			0,
			fmt.Sprintf("table %s.%s not found for comment", parsed.schemaName, parsed.objectName),
		)

	case commentObjectColumn:
		table := db.GetTable(parsed.schemaName, parsed.objectName)
		if table == nil {
			p.addWarning(
				0,
				fmt.Sprintf(
					"table %s.%s not found for column comment",
					parsed.schemaName,
					parsed.objectName,
				),
			)

			return nil
		}

		if col := table.GetColumn(parsed.columnName); col != nil {
			col.Comment = commentValue
		} else {
			p.addWarning(
				0,
				fmt.Sprintf(
					"column %s not found in table %s.%s",
					parsed.columnName,
					parsed.schemaName,
					parsed.objectName,
				),
			)
		}

	case commentObjectView:
		if view := db.GetView(parsed.schemaName, parsed.objectName); view != nil {
			view.Comment = commentValue
			return nil
		}

		if cagg := db.GetContinuousAggregate(parsed.schemaName, parsed.objectName); cagg != nil {
			cagg.Comment = commentValue
			return nil
		}

		p.addWarning(
			0,
			fmt.Sprintf("view %s.%s not found for comment", parsed.schemaName, parsed.objectName),
		)

	case commentObjectMaterializedView:
		for i := range db.MaterializedViews {
			mv := &db.MaterializedViews[i]
			if mv.Schema == parsed.schemaName && mv.Name == parsed.objectName {
				mv.Comment = commentValue
				return nil
			}
		}

		if cagg := db.GetContinuousAggregate(parsed.schemaName, parsed.objectName); cagg != nil {
			cagg.Comment = commentValue
			return nil
		}

		p.addWarning(
			0,
			fmt.Sprintf(
				"materialized view %s.%s not found for comment",
				parsed.schemaName,
				parsed.objectName,
			),
		)

	case commentObjectFunction:
		fn := db.GetFunction(parsed.schemaName, parsed.objectName, parsed.functionArgs)
		if fn != nil {
			fn.Comment = commentValue
			return nil
		}

		p.addWarning(
			0,
			fmt.Sprintf(
				"function %s.%s not found for comment",
				parsed.schemaName,
				parsed.objectName,
			),
		)

	case commentObjectExtension:
		for i := range db.Extensions {
			if strings.EqualFold(db.Extensions[i].Name, parsed.objectName) {
				db.Extensions[i].Comment = commentValue
				return nil
			}
		}

		p.addWarning(0, fmt.Sprintf("extension %s not found for comment", parsed.objectName))

	case commentObjectTypeAlias:
		for i := range db.CustomTypes {
			ct := &db.CustomTypes[i]
			if ct.Schema == parsed.schemaName && ct.Name == parsed.objectName {
				ct.Comment = commentValue
				return nil
			}
		}

		p.addWarning(
			0,
			fmt.Sprintf("type %s.%s not found for comment", parsed.schemaName, parsed.objectName),
		)

	default:
		p.addWarning(0, "unsupported COMMENT ON statement")
	}

	return nil
}

func (p *Parser) parseCommentStatement(stmt string) (*commentStatement, error) {
	tokens, err := NewLexer(stmt).Tokenize()
	if err != nil {
		return nil, WrapParseError(err, "tokenizing comment statement")
	}

	idx := nextNonCommentIndex(tokens, 0)
	if idx >= len(tokens) || upperLiteral(tokens, idx) != "COMMENT" {
		return nil, NewParseError("expected COMMENT keyword")
	}

	idx = nextNonCommentIndex(tokens, idx+1)
	if idx >= len(tokens) || upperLiteral(tokens, idx) != "ON" {
		return nil, NewParseError("expected ON keyword")
	}

	objIdx := nextNonCommentIndex(tokens, idx+1)
	if objIdx >= len(tokens) {
		return nil, NewParseError("missing comment target")
	}

	statement := &commentStatement{}

	switch upperLiteral(tokens, objIdx) {
	case "TABLE":
		statement.objectType = commentObjectTable
		nameStart := nextNonCommentIndex(tokens, objIdx+1)

		return p.populateTableComment(statement, stmt, tokens, nameStart)

	case "COLUMN":
		statement.objectType = commentObjectColumn
		nameStart := nextNonCommentIndex(tokens, objIdx+1)

		return p.populateColumnComment(statement, stmt, tokens, nameStart)

	case "VIEW":
		statement.objectType = commentObjectView
		nameStart := nextNonCommentIndex(tokens, objIdx+1)

		return p.populateTableLikeComment(statement, stmt, tokens, nameStart)

	case "MATERIALIZED":
		viewIdx := nextNonCommentIndex(tokens, objIdx+1)
		if viewIdx >= len(tokens) || upperLiteral(tokens, viewIdx) != "VIEW" {
			return nil, NewParseError("expected VIEW keyword")
		}

		statement.objectType = commentObjectMaterializedView
		nameStart := nextNonCommentIndex(tokens, viewIdx+1)

		return p.populateTableLikeComment(statement, stmt, tokens, nameStart)

	case "FUNCTION":
		statement.objectType = commentObjectFunction
		nameStart := nextNonCommentIndex(tokens, objIdx+1)

		return p.populateFunctionComment(statement, stmt, tokens, nameStart)

	case "EXTENSION":
		statement.objectType = commentObjectExtension
		nameStart := nextNonCommentIndex(tokens, objIdx+1)

		return p.populateExtensionComment(statement, stmt, tokens, nameStart)

	case "TYPE":
		statement.objectType = commentObjectTypeAlias
		nameStart := nextNonCommentIndex(tokens, objIdx+1)

		return p.populateTableLikeComment(statement, stmt, tokens, nameStart)

	case "INDEX":
		p.addWarning(0, "index comments not yet supported")
		return nil, nil //nolint:nilnil

	default:
		return nil, NewParseError("unsupported COMMENT ON target")
	}
}

func (p *Parser) populateTableComment(
	statement *commentStatement,
	stmt string,
	tokens []Token,
	startIdx int,
) (*commentStatement, error) {
	return p.populateTableLikeComment(statement, stmt, tokens, startIdx)
}

func (p *Parser) populateTableLikeComment(
	statement *commentStatement,
	stmt string,
	tokens []Token,
	startIdx int,
) (*commentStatement, error) {
	isIdx := findKeyword(tokens, "IS", startIdx)
	if isIdx == -1 {
		return nil, NewParseError("missing IS keyword")
	}

	startIdx = nextNonCommentIndex(tokens, startIdx)
	if startIdx >= isIdx {
		return nil, NewParseError("missing comment target name")
	}

	literal := strings.TrimSpace(stmt[tokens[startIdx].Start:tokens[isIdx].Start])
	if literal == "" {
		return nil, NewParseError("empty comment target")
	}

	schemaName, objectName := p.splitSchemaTable(literal)
	statement.schemaName = schemaName
	statement.objectName = objectName

	commentText, isNull, err := parseCommentText(tokens, isIdx)
	if err != nil {
		return nil, err
	}

	statement.commentText = commentText
	statement.isNull = isNull

	return statement, nil
}

func (p *Parser) populateColumnComment(
	statement *commentStatement,
	stmt string,
	tokens []Token,
	startIdx int,
) (*commentStatement, error) {
	isIdx := findKeyword(tokens, "IS", startIdx)
	if isIdx == -1 {
		return nil, NewParseError("missing IS keyword")
	}

	startIdx = nextNonCommentIndex(tokens, startIdx)
	if startIdx >= isIdx {
		return nil, NewParseError("missing column reference")
	}

	literal := strings.TrimSpace(stmt[tokens[startIdx].Start:tokens[isIdx].Start])
	if literal == "" {
		return nil, NewParseError("empty column reference")
	}

	schemaName, tableName, columnName, err := parseColumnReferenceLiteral(p, literal)
	if err != nil {
		return nil, WrapParseError(err, "parsing column reference")
	}

	statement.schemaName = schemaName
	statement.objectName = tableName
	statement.columnName = columnName

	commentText, isNull, err := parseCommentText(tokens, isIdx)
	if err != nil {
		return nil, err
	}

	statement.commentText = commentText
	statement.isNull = isNull

	return statement, nil
}

func (p *Parser) populateFunctionComment(
	statement *commentStatement,
	stmt string,
	tokens []Token,
	startIdx int,
) (*commentStatement, error) {
	isIdx := findKeyword(tokens, "IS", startIdx)
	if isIdx == -1 {
		return nil, NewParseError("missing IS keyword")
	}

	startIdx = nextNonCommentIndex(tokens, startIdx)
	if startIdx >= isIdx {
		return nil, NewParseError("missing function reference")
	}

	literal := strings.TrimSpace(stmt[tokens[startIdx].Start:tokens[isIdx].Start])
	if literal == "" {
		return nil, NewParseError("empty function reference")
	}

	schemaName, funcName, args, err := parseFunctionSignatureLiteral(p, literal)
	if err != nil {
		return nil, WrapParseError(err, "parsing function signature")
	}

	statement.schemaName = schemaName
	statement.objectName = funcName
	statement.functionArgs = args

	commentText, isNull, err := parseCommentText(tokens, isIdx)
	if err != nil {
		return nil, err
	}

	statement.commentText = commentText
	statement.isNull = isNull

	return statement, nil
}

func (p *Parser) populateExtensionComment(
	statement *commentStatement,
	stmt string,
	tokens []Token,
	startIdx int,
) (*commentStatement, error) {
	isIdx := findKeyword(tokens, "IS", startIdx)
	if isIdx == -1 {
		return nil, NewParseError("missing IS keyword")
	}

	startIdx = nextNonCommentIndex(tokens, startIdx)
	if startIdx >= isIdx {
		return nil, NewParseError("missing extension name")
	}

	literal := strings.TrimSpace(stmt[tokens[startIdx].Start:tokens[isIdx].Start])
	if literal == "" {
		return nil, NewParseError("empty extension name")
	}

	statement.objectName = strings.ToLower(p.normalizeIdent(literal))

	commentText, isNull, err := parseCommentText(tokens, isIdx)
	if err != nil {
		return nil, err
	}

	statement.commentText = commentText
	statement.isNull = isNull

	return statement, nil
}

func parseCommentText(tokens []Token, isIdx int) (string, bool, error) {
	commentIdx := nextNonCommentIndex(tokens, isIdx+1)
	if commentIdx >= len(tokens) {
		return "", false, NewParseError("missing comment text")
	}

	if upperLiteral(tokens, commentIdx) == "NULL" {
		return "", true, nil
	}

	var builder strings.Builder

	for i := commentIdx; i < len(tokens); i++ {
		token := tokens[i]
		if token.Type == TokenComment {
			continue
		}

		if token.Type == TokenString {
			part, err := decodeStringLiteral(token.Literal)
			if err != nil {
				return "", false, err
			}

			builder.WriteString(part)

			continue
		}

		if token.Type == TokenSemicolon || token.Type == TokenEOF {
			break
		}

		break
	}

	if builder.Len() == 0 {
		return "", false, NewParseError("cannot extract comment text")
	}

	return builder.String(), false, nil
}

func parseColumnReferenceLiteral(p *Parser, literal string) (string, string, string, error) {
	refTokens, err := NewLexer(literal).Tokenize()
	if err != nil {
		return "", "", "", err
	}

	var segments []string

	for _, token := range refTokens {
		switch token.Type {
		case TokenIdentifier, TokenQuotedIdentifier:
			segments = append(segments, strings.TrimSpace(token.Literal))
		case TokenEOF, TokenComment:
			continue
		case TokenDot:
			continue
		default:
			return "", "", "", NewParseError("invalid column reference")
		}
	}

	if len(segments) < 2 {
		return "", "", "", NewParseError("invalid column reference")
	}

	columnName := p.normalizeIdent(segments[len(segments)-1])
	tableLiteral := strings.Join(segments[:len(segments)-1], ".")
	schemaName, tableName := p.splitSchemaTable(tableLiteral)

	return schemaName, tableName, columnName, nil
}

func parseFunctionSignatureLiteral(
	p *Parser,
	literal string,
) (string, string, []string, error) {
	literal = strings.TrimSpace(literal)
	if literal == "" {
		return "", "", nil, NewParseError("empty function reference")
	}

	openIdx := strings.Index(literal, "(")

	closeIdx := strings.LastIndex(literal, ")")
	if openIdx == -1 || closeIdx == -1 || closeIdx < openIdx {
		return "", "", nil, NewParseError("missing function argument list")
	}

	namePart := strings.TrimSpace(literal[:openIdx])
	argsPart := strings.TrimSpace(literal[openIdx+1 : closeIdx])

	schemaName, funcName := p.splitSchemaTable(namePart)

	var args []string

	if argsPart != "" {
		for _, arg := range splitByComma(argsPart) {
			args = append(args, strings.TrimSpace(arg))
		}
	}

	return schemaName, funcName, args, nil
}

func decodeStringLiteral(literal string) (string, error) {
	literal = strings.TrimSpace(literal)
	if len(literal) < 2 {
		return "", NewParseError("invalid string literal")
	}

	if literal[0] == '\'' && literal[len(literal)-1] == '\'' {
		content := literal[1 : len(literal)-1]
		content = strings.ReplaceAll(content, "''", "'")

		return content, nil
	}

	if literal[0] == '$' {
		tagEnd := strings.Index(literal[1:], "$")
		if tagEnd == -1 {
			return "", NewParseError("invalid dollar-quoted string")
		}

		tagLen := tagEnd + 2
		if len(literal) < 2*tagLen {
			return "", NewParseError("invalid dollar-quoted string")
		}

		return literal[tagLen : len(literal)-tagLen], nil
	}

	return "", NewParseError("unsupported string literal format")
}
