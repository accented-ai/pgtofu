package parser

import (
	"regexp"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type functionStatement struct {
	schemaName  string
	funcName    string
	argLiteral  string
	argTypes    []string
	argNames    []string
	argModes    []string
	returnType  string
	language    string
	volatility  string
	body        string
	isStrict    bool
	securityDef bool
	definition  string
}

func (p *Parser) parseCreateFunction(stmt string, db *schema.Database) error {
	parsed, err := p.parseFunctionStatement(stmt)
	if err != nil || parsed == nil {
		return err
	}

	fn := schema.Function{
		Schema:            parsed.schemaName,
		Name:              parsed.funcName,
		ArgumentTypes:     parsed.argTypes,
		ArgumentNames:     parsed.argNames,
		ArgumentModes:     parsed.argModes,
		ReturnType:        parsed.returnType,
		Language:          parsed.language,
		Body:              parsed.body,
		Volatility:        parsed.volatility,
		Definition:        parsed.definition,
		IsStrict:          parsed.isStrict,
		IsSecurityDefiner: parsed.securityDef,
	}

	for i, existing := range db.Functions {
		if existing.Schema == parsed.schemaName && existing.Name == parsed.funcName &&
			equalStringSlices(existing.ArgumentTypes, parsed.argTypes) {
			db.Functions[i] = fn
			return nil
		}
	}

	db.Functions = append(db.Functions, fn)

	return nil
}

func (p *Parser) parseFunctionStatement(stmt string) (*functionStatement, error) {
	tokens, err := NewLexer(stmt).Tokenize()
	if err != nil {
		return nil, WrapParseError(err, "tokenizing function statement")
	}

	if len(tokens) == 0 {
		return nil, NewParseError("empty function statement")
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

	if idx >= len(tokens) || upperLiteral(tokens, idx) != "FUNCTION" {
		return nil, NewParseError("expected FUNCTION keyword")
	}

	nameIdx := nextNonCommentIndex(tokens, idx+1)
	if nameIdx >= len(tokens) {
		return nil, NewParseError("missing function name")
	}

	nameStart := tokens[nameIdx].Start

	openIdx := findToken(tokens, TokenLParen, nameIdx)
	if openIdx == -1 {
		return nil, NewParseError("missing function argument list")
	}

	nameLiteral := strings.TrimSpace(stmt[nameStart:tokens[openIdx].Start])
	if nameLiteral == "" {
		return nil, NewParseError("empty function name")
	}

	schemaName, funcName, err := p.parseFunctionName(nameLiteral)
	if err != nil {
		return nil, err
	}

	argsLiteral, nextIdx, err := extractParenthesizedLiteral(stmt, tokens, openIdx)
	if err != nil {
		return nil, err
	}

	argNames, argTypes, argModes := parseFunctionArguments(argsLiteral)

	returnType := "void"

	retIdx := findKeyword(tokens, "RETURNS", nextIdx)
	if retIdx != -1 {
		retLiteral, afterRet := collectLiteralUntil(
			tokens,
			stmt,
			retIdx+1,
			"LANGUAGE",
			"IMMUTABLE",
			"STABLE",
			"VOLATILE",
			"STRICT",
			"COST",
			"ROWS",
			"SET",
			"SECURITY",
			"PARALLEL",
			"AS",
		)
		if retLiteral != "" {
			returnType = strings.TrimSpace(retLiteral)
		}

		nextIdx = afterRet
	}

	language := "sql"

	langIdx := findKeyword(tokens, "LANGUAGE", nextIdx)
	if langIdx != -1 {
		valIdx := nextNonCommentIndex(tokens, langIdx+1)
		if valIdx < len(tokens) {
			language = strings.ToLower(strings.TrimSpace(tokens[valIdx].Literal))
		}
	}

	body := p.extractFunctionBody(stmt)

	volatility := schema.VolatilityVolatile

	switch {
	case findKeyword(tokens, "IMMUTABLE", 0) != -1:
		volatility = schema.VolatilityImmutable
	case findKeyword(tokens, "STABLE", 0) != -1:
		volatility = schema.VolatilityStable
	}

	isStrict := findKeyword(tokens, "STRICT", 0) != -1 ||
		strings.Contains(strings.ToUpper(stmt), "RETURNS NULL ON NULL INPUT")

	securityDef := strings.Contains(strings.ToUpper(stmt), "SECURITY DEFINER")

	return &functionStatement{
		schemaName:  schemaName,
		funcName:    funcName,
		argLiteral:  argsLiteral,
		argTypes:    argTypes,
		argNames:    argNames,
		argModes:    argModes,
		returnType:  returnType,
		language:    language,
		volatility:  volatility,
		body:        body,
		isStrict:    isStrict,
		securityDef: securityDef,
		definition:  stmt,
	}, nil
}

func (p *Parser) parseFunctionName(literal string) (string, string, error) {
	literal = strings.TrimSpace(literal)
	if literal == "" {
		return "", "", NewParseError("empty function name")
	}

	schemaName, funcName := p.splitSchemaTable(literal)
	if funcName == "" {
		return "", "", NewParseError("empty function identifier")
	}

	return schemaName, funcName, nil
}

func parseFunctionArguments(argsLiteral string) ([]string, []string, []string) {
	if strings.TrimSpace(argsLiteral) == "" {
		return nil, nil, nil
	}

	var (
		argTypes []string
		argNames []string
		argModes []string
	)

	for _, arg := range splitByComma(argsLiteral) {
		arg = strings.TrimSpace(arg)
		if arg == "" {
			continue
		}

		parts := strings.Fields(arg)
		if len(parts) == 0 {
			continue
		}

		mode := "IN"
		offset := 0

		first := strings.ToUpper(parts[0])
		if first == "IN" || first == "OUT" || first == "INOUT" ||
			first == "VARIADIC" || first == "TABLE" {
			mode = first
			offset = 1
		}

		if len(parts) > offset+1 {
			argNames = append(argNames, parts[offset])
			argTypes = append(argTypes, strings.Join(parts[offset+1:], " "))
		} else if len(parts) > offset {
			argNames = append(argNames, "")
			argTypes = append(argTypes, strings.Join(parts[offset:], " "))
		}

		argModes = append(argModes, mode)
	}

	return argNames, argTypes, argModes
}

func (p *Parser) extractFunctionBody(stmt string) string {
	dollarPattern := regexp.MustCompile(`\$([a-zA-Z_][a-zA-Z0-9_]*)?\$`)
	matches := dollarPattern.FindAllStringIndex(stmt, -1)

	if len(matches) >= 2 {
		start := matches[0][1]

		end := matches[len(matches)-1][0]
		if start < end {
			return strings.TrimSpace(stmt[start:end])
		}
	}

	singleQuotePattern := regexp.MustCompile(`(?i)AS\s+'((?:[^']|'')*)'`)
	if m := singleQuotePattern.FindStringSubmatch(stmt); len(m) > 1 {
		return strings.ReplaceAll(m[1], "''", "'")
	}

	return ""
}

type triggerStatement struct {
	name           string
	timing         string
	events         []string
	tableSchema    string
	tableName      string
	forEachRow     bool
	whenCondition  string
	functionSchema string
	functionName   string
	definition     string
}

func (p *Parser) parseCreateTrigger(stmt string, db *schema.Database) error {
	parsed, err := p.parseTriggerStatement(stmt)
	if err != nil || parsed == nil {
		return err
	}

	trigger := schema.Trigger{
		Schema:         parsed.tableSchema,
		Name:           parsed.name,
		TableName:      parsed.tableName,
		Timing:         parsed.timing,
		Events:         parsed.events,
		ForEachRow:     parsed.forEachRow,
		WhenCondition:  parsed.whenCondition,
		FunctionSchema: parsed.functionSchema,
		FunctionName:   parsed.functionName,
		Definition:     parsed.definition,
	}

	for i, existing := range db.Triggers {
		if existing.Schema == parsed.tableSchema &&
			existing.TableName == parsed.tableName &&
			existing.Name == parsed.name {
			db.Triggers[i] = trigger
			return nil
		}
	}

	db.Triggers = append(db.Triggers, trigger)

	return nil
}

func (p *Parser) parseTriggerStatement( //nolint:cyclop,gocognit,gocyclo,maintidx
	stmt string,
) (*triggerStatement, error) {
	tokens, err := NewLexer(stmt).Tokenize()
	if err != nil {
		return nil, WrapParseError(err, "tokenizing trigger statement")
	}

	if len(tokens) == 0 {
		return nil, NewParseError("empty trigger statement")
	}

	idx := nextNonCommentIndex(tokens, 0)
	if idx >= len(tokens) || upperLiteral(tokens, idx) != "CREATE" {
		return nil, NewParseError("expected CREATE keyword")
	}

	idx = nextNonCommentIndex(tokens, idx+1)
	if idx < len(tokens) && upperLiteral(tokens, idx) == "CONSTRAINT" {
		idx = nextNonCommentIndex(tokens, idx+1)
	}

	if idx >= len(tokens) || upperLiteral(tokens, idx) != "TRIGGER" {
		return nil, NewParseError("expected TRIGGER keyword")
	}

	nameIdx := nextNonCommentIndex(tokens, idx+1)
	if nameIdx >= len(tokens) {
		return nil, NewParseError("missing trigger name")
	}

	nameToken := tokens[nameIdx]
	if nameToken.Type != TokenIdentifier && nameToken.Type != TokenQuotedIdentifier {
		return nil, NewParseError("invalid trigger name")
	}

	triggerName := p.normalizeIdent(nameToken.Literal)

	idx = nextNonCommentIndex(tokens, nameIdx+1)
	if idx >= len(tokens) {
		return nil, NewParseError("missing trigger timing")
	}

	var timing string

	word := upperLiteral(tokens, idx)
	switch word {
	case "BEFORE", "AFTER":
		timing = word
		idx = nextNonCommentIndex(tokens, idx+1)
	case "INSTEAD":
		ofIdx := nextNonCommentIndex(tokens, idx+1)
		if ofIdx >= len(tokens) || upperLiteral(tokens, ofIdx) != "OF" {
			return nil, NewParseError("expected INSTEAD OF")
		}

		timing = "INSTEAD OF"
		idx = nextNonCommentIndex(tokens, ofIdx+1)
	default:
		return nil, NewParseError("invalid trigger timing")
	}

	if idx >= len(tokens) {
		return nil, NewParseError("missing trigger events")
	}

	eventTokens := []string{}

	for idx < len(tokens) {
		word = upperLiteral(tokens, idx)
		if word == "ON" {
			break
		}

		if word != "" {
			eventTokens = append(eventTokens, word)
		}

		idx = nextNonCommentIndex(tokens, idx+1)
	}

	if idx >= len(tokens) || upperLiteral(tokens, idx) != "ON" {
		return nil, NewParseError("missing ON clause")
	}

	events := make([]string, 0, len(eventTokens))
	for _, tok := range eventTokens {
		if tok == "OR" {
			continue
		}

		if tok != "" {
			events = append(events, tok)
		}
	}

	if len(events) == 0 {
		return nil, NewParseError("missing trigger events")
	}

	tableStart := nextNonCommentIndex(tokens, idx+1)
	if tableStart >= len(tokens) {
		return nil, NewParseError("missing trigger table")
	}

	tableLiteral, afterTableIdx := collectLiteralUntil(
		tokens,
		stmt,
		tableStart,
		"FOR",
		"WHEN",
		"EXECUTE",
	)

	tableLiteral = strings.TrimSpace(tableLiteral)
	if tableLiteral == "" {
		return nil, NewParseError("missing trigger table")
	}

	tableSchema, tableName := p.splitSchemaTable(tableLiteral)

	forEachRow := false

	forIdx := findKeyword(tokens, "FOR", afterTableIdx)
	if forIdx != -1 {
		eachIdx := nextNonCommentIndex(tokens, forIdx+1)
		if eachIdx < len(tokens) && upperLiteral(tokens, eachIdx) == "EACH" {
			rowIdx := nextNonCommentIndex(tokens, eachIdx+1)
			if rowIdx < len(tokens) && upperLiteral(tokens, rowIdx) == "ROW" {
				forEachRow = true
			}
		}
	}

	whenCondition := ""

	whenIdx := findKeyword(tokens, "WHEN", afterTableIdx)
	if whenIdx != -1 {
		parenIdx := nextNonCommentIndex(tokens, whenIdx+1)
		if parenIdx < len(tokens) && tokens[parenIdx].Type == TokenLParen {
			condition, _, err := extractParenthesizedLiteral(stmt, tokens, parenIdx)
			if err != nil {
				return nil, err
			}

			whenCondition = strings.TrimSpace(condition)
		}
	}

	execIdx := findKeyword(tokens, "EXECUTE", afterTableIdx)
	if execIdx == -1 {
		return nil, NewParseError("missing EXECUTE clause")
	}

	callIdx := nextNonCommentIndex(tokens, execIdx+1)
	if callIdx >= len(tokens) {
		return nil, NewParseError("missing EXECUTE target")
	}

	callWord := upperLiteral(tokens, callIdx)
	if callWord != "FUNCTION" && callWord != "PROCEDURE" {
		return nil, NewParseError("expected FUNCTION or PROCEDURE")
	}

	callStart := nextNonCommentIndex(tokens, callIdx+1)
	if callStart >= len(tokens) {
		return nil, NewParseError("missing EXECUTE target reference")
	}

	callLiteral, _ := collectLiteralUntil(
		tokens,
		stmt,
		callStart,
	)

	callLiteral = strings.TrimSpace(callLiteral)

	funcSchema, funcName, err := parseTriggerFunctionReference(p, callLiteral)
	if err != nil {
		return nil, err
	}

	return &triggerStatement{
		name:           triggerName,
		timing:         timing,
		events:         events,
		tableSchema:    tableSchema,
		tableName:      tableName,
		forEachRow:     forEachRow,
		whenCondition:  whenCondition,
		functionSchema: funcSchema,
		functionName:   funcName,
		definition:     stmt,
	}, nil
}

func parseTriggerFunctionReference(p *Parser, literal string) (string, string, error) {
	literal = strings.TrimSpace(literal)
	if literal == "" {
		return "", "", NewParseError("invalid EXECUTE target")
	}

	openIdx := strings.Index(literal, "(")

	namePart := literal
	if openIdx != -1 {
		namePart = strings.TrimSpace(literal[:openIdx])
	}

	schemaName, funcName := p.splitSchemaTable(namePart)
	if funcName == "" {
		return "", "", NewParseError("invalid EXECUTE target")
	}

	return schemaName, funcName, nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
