package parser

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

var tableNameRe = regexp.MustCompile(
	`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?|"[^"]*"(?:\."[^"]*")?)`, //nolint:lll
)

func (p *Parser) parseCreateTable(stmt string, db *schema.Database) error {
	stmtUpper := strings.ToUpper(stmt)

	if strings.Contains(stmtUpper, "PARTITION OF") {
		return p.parsePartitionOfTokens(stmt, db)
	}

	matches := tableNameRe.FindStringSubmatch(stmt)
	if len(matches) < 2 {
		return errors.New("cannot extract table name")
	}

	schemaName, tableName := p.splitSchemaTable(matches[1])

	content := extractParens(stmt)
	if content == "" {
		return errors.New("no table definition found")
	}

	columns, constraints := p.parseTableContent(content)

	partitionStrategy := p.parsePartitionBy(stmt)

	table := schema.Table{
		Schema:            schemaName,
		Name:              tableName,
		Columns:           columns,
		Constraints:       constraints,
		Indexes:           []schema.Index{},
		PartitionStrategy: partitionStrategy,
	}

	p.finalizeTableConstraints(&table)

	for i, existing := range db.Tables {
		if existing.Schema == schemaName && existing.Name == tableName {
			db.Tables[i] = table
			return nil
		}
	}

	db.Tables = append(db.Tables, table)

	return nil
}

func (p *Parser) parseTableContent(content string) ([]schema.Column, []schema.Constraint) {
	var (
		columns     []schema.Column
		constraints []schema.Constraint
		position    = 1
	)

	content = stripComments(content)
	parts := splitTableDefinition(content)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if isConstraint(part) {
			if c, err := p.parseConstraint(part); err == nil {
				constraints = append(constraints, c)
			} else {
				p.addWarning(0, fmt.Sprintf("parsing constraint: %v", err))
			}
		} else {
			col, inline, err := p.parseColumn(part, position)
			if err != nil {
				p.addWarning(0, fmt.Sprintf("parsing column: %v", err))
				continue
			}

			columns = append(columns, col)
			constraints = append(constraints, inline...)
			position++
		}
	}

	return columns, constraints
}

func splitTableDefinition(content string) []string {
	tokens, err := NewLexer(content).Tokenize()
	if err != nil || len(tokens) == 0 {
		return splitByComma(content)
	}

	if tokens[len(tokens)-1].Type == TokenEOF {
		tokens = tokens[:len(tokens)-1]
	}

	var (
		parts []string
		start int
		depth int
	)

	for _, token := range tokens {
		switch token.Type {
		case TokenLParen:
			depth++
		case TokenRParen:
			if depth > 0 {
				depth--
			}
		case TokenComma:
			if depth == 0 {
				segment := strings.TrimSpace(content[start:token.Start])
				if segment != "" {
					parts = append(parts, segment)
				}

				start = token.End
			}
		}
	}

	if start < len(content) {
		segment := strings.TrimSpace(content[start:])
		if segment != "" {
			parts = append(parts, segment)
		}
	}

	if len(parts) == 0 {
		return splitByComma(content)
	}

	return parts
}

func tokenizeColumnDefinition(def string) ([]Token, error) {
	tokens, err := NewLexer(def).Tokenize()
	if err != nil {
		return nil, err
	}

	return tokens, nil
}

func isColumnConstraintBoundary(tokens []Token, idx int) bool {
	lit := upperLiteral(tokens, idx)

	switch lit {
	case "CONSTRAINT", "DEFAULT", "REFERENCES", "CHECK", "UNIQUE", "GENERATED", "COLLATE":
		return true
	case "PRIMARY":
		return upperLiteral(tokens, idx+1) == "KEY"
	case "NOT":
		return upperLiteral(tokens, idx+1) == "NULL"
	case "FOREIGN":
		return upperLiteral(tokens, idx+1) == "KEY"
	}

	return false
}

func upperLiteral(tokens []Token, idx int) string {
	if idx < 0 || idx >= len(tokens) {
		return ""
	}

	return strings.ToUpper(strings.TrimSpace(tokens[idx].Literal))
}

func nextNonCommentIndex(tokens []Token, idx int) int {
	for idx < len(tokens) && tokens[idx].Type == TokenComment {
		idx++
	}

	return idx
}

func collectLiteralUntil(tokens []Token, stmt string, idx int, stopWords ...string) (string, int) {
	stopSet := make(map[string]struct{}, len(stopWords))
	for _, word := range stopWords {
		stopSet[strings.ToUpper(word)] = struct{}{}
	}

	idx = nextNonCommentIndex(tokens, idx)

	start := -1
	end := -1
	current := idx

	for current < len(tokens) {
		token := tokens[current]
		if token.Type == TokenComment {
			current++
			continue
		}

		if start == -1 {
			start = token.Start
		}

		if token.Type == TokenSemicolon {
			end = token.Start
			break
		}

		if token.Type == TokenLParen {
			if _, ok := stopSet["("]; ok {
				end = token.Start
				break
			}
		}

		if token.Type == TokenKeyword || token.Type == TokenIdentifier {
			if _, ok := stopSet[upperLiteral(tokens, current)]; ok {
				end = token.Start
				break
			}
		}

		end = token.End
		current++
	}

	if start == -1 {
		return "", current
	}

	if end == -1 {
		end = len(stmt)
	}

	literal := strings.TrimSpace(stmt[start:end])

	return literal, current
}

func findKeyword(tokens []Token, word string, start int) int {
	word = strings.ToUpper(word)

	for i := start; i < len(tokens); i++ {
		if tokens[i].Type == TokenComment {
			continue
		}

		if upperLiteral(tokens, i) == word {
			return i
		}
	}

	return -1
}

func findToken(tokens []Token, tType TokenType, start int) int {
	for i := start; i < len(tokens); i++ {
		if tokens[i].Type == tType {
			return i
		}
	}

	return -1
}

func prevNonCommentIndex(tokens []Token, idx int) int {
	for idx >= 0 {
		if tokens[idx].Type != TokenComment && tokens[idx].Type != TokenEOF {
			return idx
		}

		idx--
	}

	return -1
}

func tokenizeConstraintDefinition(def string) ([]Token, error) {
	return NewLexer(def).Tokenize()
}

type constraintParser struct {
	parser *Parser
	def    string
	tokens []Token
	pos    int
}

func newConstraintParser(p *Parser, def string, tokens []Token) *constraintParser {
	return &constraintParser{
		parser: p,
		def:    def,
		tokens: tokens,
	}
}

func (cp *constraintParser) skipComments() {
	for cp.pos < len(cp.tokens) && cp.tokens[cp.pos].Type == TokenComment {
		cp.pos++
	}
}

func (cp *constraintParser) peekWord() string {
	tokenIdx := cp.pos
	cp.skipComments()

	return upperLiteral(cp.tokens, tokenIdx)
}

func (cp *constraintParser) consumeToken() (Token, error) {
	cp.skipComments()

	if cp.pos >= len(cp.tokens) {
		return Token{}, NewParseError("unexpected end of constraint")
	}

	token := cp.tokens[cp.pos]
	cp.pos++

	return token, nil
}

func (cp *constraintParser) consumeWord(expected string) error {
	token, err := cp.consumeToken()
	if err != nil {
		return err
	}

	actual := strings.ToUpper(strings.TrimSpace(token.Literal))
	if actual != expected {
		return NewParseError(fmt.Sprintf("expected %s, got %s", expected, actual))
	}

	return nil
}

func (cp *constraintParser) consumeIdentifier() (string, error) {
	token, err := cp.consumeToken()
	if err != nil {
		return "", err
	}

	switch token.Type {
	case TokenIdentifier, TokenQuotedIdentifier:
		return cp.parser.normalizeIdent(token.Literal), nil
	default:
		return "", NewParseError("expected identifier, got " + token.Literal)
	}
}

func (cp *constraintParser) consumeConstraintName() (string, error) {
	if cp.peekWord() != "CONSTRAINT" {
		return "", nil
	}

	if err := cp.consumeWord("CONSTRAINT"); err != nil {
		return "", err
	}

	name, err := cp.consumeIdentifier()
	if err != nil {
		return "", WrapParseError(err, "reading constraint name")
	}

	return name, nil
}

func (cp *constraintParser) consumeParenthesized() (string, error) {
	cp.skipComments()

	if cp.pos >= len(cp.tokens) || cp.tokens[cp.pos].Type != TokenLParen {
		return "", NewParseError("expected '('")
	}

	open := cp.tokens[cp.pos]
	start := open.End

	cp.pos++
	depth := 1

	for cp.pos < len(cp.tokens) {
		token := cp.tokens[cp.pos]

		switch token.Type {
		case TokenLParen:
			depth++
		case TokenRParen:
			depth--
			if depth == 0 {
				end := token.Start
				cp.pos++

				return strings.TrimSpace(cp.def[start:end]), nil
			}
		}

		cp.pos++
	}

	return "", NewParseError("unterminated parentheses")
}

func (cp *constraintParser) consumeReferenceTable() (string, error) {
	cp.skipComments()

	if cp.pos >= len(cp.tokens) {
		return "", NewParseError("expected referenced table")
	}

	start := cp.tokens[cp.pos].Start

	for cp.pos < len(cp.tokens) && cp.tokens[cp.pos].Type != TokenLParen {
		cp.pos++
	}

	if cp.pos >= len(cp.tokens) {
		return "", NewParseError("missing referenced column list")
	}

	return strings.TrimSpace(cp.def[start:cp.tokens[cp.pos].Start]), nil
}

func (cp *constraintParser) remaining() string {
	cp.skipComments()

	if cp.pos >= len(cp.tokens) {
		return ""
	}

	return strings.TrimSpace(cp.def[cp.tokens[cp.pos].Start:])
}

func (cp *constraintParser) parsePrimaryKey(name string) (schema.Constraint, error) {
	if err := cp.consumeWord("PRIMARY"); err != nil {
		return schema.Constraint{}, err
	}

	if err := cp.consumeWord("KEY"); err != nil {
		return schema.Constraint{}, err
	}

	columnList, err := cp.consumeParenthesized()
	if err != nil {
		return schema.Constraint{}, err
	}

	columns := normalizeIdentifierList(cp.parser, columnList)
	definition := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(columns, ", "))

	remaining := cp.remaining()
	isDeferrable := hasKeyword(remaining, "DEFERRABLE")
	initiallyDeferred := hasKeyword(remaining, "INITIALLY DEFERRED")

	return schema.Constraint{
		Name:              name,
		Type:              schema.ConstraintPrimaryKey,
		Columns:           columns,
		Definition:        definition,
		IsDeferrable:      isDeferrable,
		InitiallyDeferred: initiallyDeferred,
	}, nil
}

func (cp *constraintParser) parseUnique(name string) (schema.Constraint, error) {
	if err := cp.consumeWord("UNIQUE"); err != nil {
		return schema.Constraint{}, err
	}

	columnList, err := cp.consumeParenthesized()
	if err != nil {
		return schema.Constraint{}, err
	}

	columns := normalizeIdentifierList(cp.parser, columnList)
	definition := fmt.Sprintf("UNIQUE (%s)", strings.Join(columns, ", "))

	remaining := cp.remaining()
	isDeferrable := hasKeyword(remaining, "DEFERRABLE")
	initiallyDeferred := hasKeyword(remaining, "INITIALLY DEFERRED") ||
		(hasKeyword(remaining, "DEFERRABLE") && hasKeyword(remaining, "DEFERRED"))

	return schema.Constraint{
		Name:              name,
		Type:              schema.ConstraintUnique,
		Columns:           columns,
		Definition:        definition,
		IsDeferrable:      isDeferrable,
		InitiallyDeferred: initiallyDeferred,
	}, nil
}

func (cp *constraintParser) parseCheck(name string) (schema.Constraint, error) {
	if err := cp.consumeWord("CHECK"); err != nil {
		return schema.Constraint{}, err
	}

	expr, err := cp.consumeParenthesized()
	if err != nil {
		return schema.Constraint{}, err
	}

	definition := fmt.Sprintf("CHECK (%s)", expr)

	return schema.Constraint{
		Name:            name,
		Type:            schema.ConstraintCheck,
		Definition:      definition,
		CheckExpression: definition,
	}, nil
}

func (cp *constraintParser) parseForeignKey(name string) (schema.Constraint, error) {
	if err := cp.consumeWord("FOREIGN"); err != nil {
		return schema.Constraint{}, err
	}

	if err := cp.consumeWord("KEY"); err != nil {
		return schema.Constraint{}, err
	}

	srcList, err := cp.consumeParenthesized()
	if err != nil {
		return schema.Constraint{}, err
	}

	if err := cp.consumeWord("REFERENCES"); err != nil {
		return schema.Constraint{}, err
	}

	refTable, err := cp.consumeReferenceTable()
	if err != nil {
		return schema.Constraint{}, err
	}

	refCols, err := cp.consumeParenthesized()
	if err != nil {
		return schema.Constraint{}, err
	}

	remaining := cp.remaining()

	onDelete := extractAfterKeyword(remaining, "ON DELETE")
	if onDelete == "" {
		onDelete = schema.NoAction
	}

	onUpdate := extractAfterKeyword(remaining, "ON UPDATE")
	if onUpdate == "" {
		onUpdate = schema.NoAction
	}

	isDeferrable := hasKeyword(remaining, "DEFERRABLE")
	initiallyDeferred := hasKeyword(remaining, "INITIALLY DEFERRED")

	srcColumns := normalizeIdentifierList(cp.parser, srcList)
	refColumns := normalizeIdentifierList(cp.parser, refCols)

	definition := fmt.Sprintf(
		"FOREIGN KEY (%s) REFERENCES %s(%s)",
		strings.Join(srcColumns, ", "),
		refTable,
		strings.Join(refColumns, ", "),
	)

	return schema.Constraint{
		Name:              name,
		Type:              schema.ConstraintForeignKey,
		Columns:           srcColumns,
		Definition:        definition,
		ReferencedTable:   refTable,
		ReferencedColumns: refColumns,
		OnDelete:          onDelete,
		OnUpdate:          onUpdate,
		IsDeferrable:      isDeferrable,
		InitiallyDeferred: initiallyDeferred,
	}, nil
}

func (p *Parser) parseColumn( //nolint:cyclop,gocyclo
	def string,
	position int,
) (schema.Column, []schema.Constraint, error) {
	tokens, err := tokenizeColumnDefinition(def)
	if err != nil {
		return schema.Column{}, nil, err
	}

	if len(tokens) == 0 || tokens[0].Type == TokenEOF {
		return schema.Column{}, nil, errors.New("invalid column definition")
	}

	columnName := p.normalizeIdent(tokens[0].Literal)
	if columnName == "" {
		return schema.Column{}, nil, ErrMissingIdentifier
	}

	typeStartIdx := 1
	for typeStartIdx < len(tokens) && tokens[typeStartIdx].Type == TokenComment {
		typeStartIdx++
	}

	if typeStartIdx >= len(tokens) || tokens[typeStartIdx].Type == TokenEOF {
		return schema.Column{}, nil, errors.New("cannot extract data type")
	}

	typeStart := tokens[typeStartIdx].Start
	typeEnd := len(def)
	constraintStartIdx := len(tokens)
	depth := 0

	for i := typeStartIdx; i < len(tokens); i++ {
		token := tokens[i]

		switch token.Type {
		case TokenLParen:
			depth++
		case TokenRParen:
			if depth > 0 {
				depth--
			}
		}

		if depth == 0 && token.Type != TokenEOF && isColumnConstraintBoundary(tokens, i) {
			typeEnd = token.Start
			constraintStartIdx = i

			break
		}
	}

	if typeEnd < typeStart {
		typeEnd = len(def)
	}

	dataType := strings.TrimSpace(def[typeStart:typeEnd])
	if dataType == "" {
		return schema.Column{}, nil, errors.New("cannot extract data type")
	}

	isArray := strings.HasSuffix(dataType, "[]")
	baseType, precision, scale, maxLength := parseTypeParams(dataType)

	rest := strings.TrimSpace(def[typeEnd:])
	constraintTokens := filterConstraintTokens(tokens[constraintStartIdx:])
	upperWords := constraintWords(constraintTokens)
	defaultVal := extractDefault(rest)

	baseTypeUpper := strings.ToUpper(baseType)
	switch baseTypeUpper {
	case "SERIAL":
		baseType = "INTEGER"

		if defaultVal == "" {
			defaultVal = "__SERIAL__"
		}
	case "BIGSERIAL":
		baseType = "BIGINT"

		if defaultVal == "" {
			defaultVal = "__BIGSERIAL__"
		}
	case "SMALLSERIAL":
		baseType = "SMALLINT"

		if defaultVal == "" {
			defaultVal = "__SMALLSERIAL__"
		}
	default:
		baseType = strings.ToUpper(baseType)
	}

	column := schema.Column{
		Name:       columnName,
		DataType:   baseType,
		IsNullable: isColumnNullable(upperWords),
		Default:    defaultVal,
		Position:   position,
		Precision:  precision,
		Scale:      scale,
		MaxLength:  maxLength,
		IsArray:    isArray,
	}

	var inline []schema.Constraint

	if containsSequence(upperWords, "PRIMARY", "KEY") {
		inline = append(inline, schema.Constraint{
			Type:       schema.ConstraintPrimaryKey,
			Columns:    []string{columnName},
			Definition: fmt.Sprintf("PRIMARY KEY (%s)", columnName),
		})
	}

	if containsWord(upperWords, "UNIQUE") && !containsSequence(upperWords, "PRIMARY", "KEY") {
		inline = append(inline, schema.Constraint{
			Type:       schema.ConstraintUnique,
			Columns:    []string{columnName},
			Definition: fmt.Sprintf("UNIQUE (%s)", columnName),
		})
	}

	if containsWord(upperWords, "REFERENCES") {
		if fk, err := p.parseInlineForeignKey(columnName, rest); err == nil {
			inline = append(inline, fk)
		}
	}

	if containsWord(upperWords, "CHECK") {
		if check, err := p.parseInlineCheck(columnName, rest); err == nil {
			inline = append(inline, check)
		}
	}

	return column, inline, nil
}

func filterConstraintTokens(tokens []Token) []Token {
	if len(tokens) == 0 {
		return nil
	}

	var filtered []Token //nolint:prealloc

	for _, token := range tokens {
		if token.Type == TokenEOF || token.Type == TokenComment {
			continue
		}

		filtered = append(filtered, token)
	}

	return filtered
}

func constraintWords(tokens []Token) []string {
	if len(tokens) == 0 {
		return nil
	}

	var words []string

	for _, token := range tokens {
		switch token.Type {
		case TokenKeyword, TokenIdentifier:
			word := strings.ToUpper(strings.TrimSpace(token.Literal))
			if word != "" {
				words = append(words, word)
			}
		}
	}

	return words
}

func containsWord(words []string, target string) bool {
	target = strings.ToUpper(target)
	return slices.Contains(words, target)
}

func containsSequence(words []string, sequence ...string) bool {
	if len(sequence) == 0 || len(words) < len(sequence) {
		return false
	}

	for i := 0; i <= len(words)-len(sequence); i++ {
		match := true

		for j, seq := range sequence {
			if words[i+j] != strings.ToUpper(seq) {
				match = false
				break
			}
		}

		if match {
			return true
		}
	}

	return false
}

func isColumnNullable(words []string) bool {
	if len(words) == 0 {
		return true
	}

	if containsSequence(words, "NOT", "NULL") {
		return false
	}

	if containsSequence(words, "PRIMARY", "KEY") {
		return false
	}

	return true
}

func normalizeIdentifierList(p *Parser, list string) []string {
	if list == "" {
		return nil
	}

	parts := splitByComma(list)
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		result = append(result, p.normalizeIdent(part))
	}

	return result
}

func isConstraint(def string) bool {
	upper := strings.ToUpper(strings.TrimSpace(def))

	return strings.HasPrefix(upper, "CONSTRAINT ") ||
		strings.HasPrefix(upper, "PRIMARY KEY") ||
		strings.HasPrefix(upper, "FOREIGN KEY") ||
		strings.HasPrefix(upper, "UNIQUE") ||
		strings.HasPrefix(upper, "CHECK") ||
		strings.HasPrefix(upper, "EXCLUDE")
}

func (p *Parser) parseConstraint(def string) (schema.Constraint, error) {
	def = strings.TrimSpace(def)

	tokens, err := tokenizeConstraintDefinition(def)
	if err != nil {
		return schema.Constraint{}, WrapParseError(err, "tokenizing constraint")
	}

	parser := newConstraintParser(p, def, tokens)

	name, err := parser.consumeConstraintName()
	if err != nil {
		return schema.Constraint{}, WrapParseError(err, "reading constraint name")
	}

	switch parser.peekWord() {
	case "PRIMARY":
		return parser.parsePrimaryKey(name)
	case "FOREIGN":
		return parser.parseForeignKey(name)
	case "UNIQUE":
		return parser.parseUnique(name)
	case "CHECK":
		return parser.parseCheck(name)
	case "EXCLUDE":
		return schema.Constraint{
			Name:       name,
			Type:       schema.ConstraintExclude,
			Definition: def,
		}, nil
	default:
		return schema.Constraint{}, NewParseError("unknown constraint type")
	}
}

func extractParenthesizedLiteral(stmt string, tokens []Token, idx int) (string, int, error) {
	if idx >= len(tokens) || tokens[idx].Type != TokenLParen {
		return "", idx, NewParseError("expected '('")
	}

	start := tokens[idx].End
	depth := 1

	for i := idx + 1; i < len(tokens); i++ {
		token := tokens[i]
		if token.Type == TokenComment {
			continue
		}

		switch token.Type {
		case TokenLParen:
			depth++
		case TokenRParen:
			depth--
			if depth == 0 {
				end := token.Start
				return strings.TrimSpace(stmt[start:end]), i + 1, nil
			}
		}
	}

	return "", len(tokens), NewParseError("unterminated parentheses")
}

func (p *Parser) parseInlineForeignKey(colName, def string) (schema.Constraint, error) {
	tokens, err := tokenizeConstraintDefinition(def)
	if err != nil {
		return schema.Constraint{}, err
	}

	refIdx := findKeyword(tokens, "REFERENCES", 0)
	if refIdx == -1 {
		return schema.Constraint{}, NewParseError("invalid REFERENCES syntax")
	}

	tableIdx := nextNonCommentIndex(tokens, refIdx+1)
	if tableIdx >= len(tokens) {
		return schema.Constraint{}, NewParseError("missing referenced table")
	}

	literalStart := tokens[tableIdx].Start
	literalEnd := literalStart
	closeIdx := -1

	for i := tableIdx; i < len(tokens); i++ {
		token := tokens[i]
		if token.Type == TokenComment {
			continue
		}

		if token.Type == TokenLParen {
			closeIdx = i
			break
		}

		literalEnd = token.End
	}

	if closeIdx == -1 {
		return schema.Constraint{}, NewParseError("missing referenced column list")
	}

	refTableLiteral := strings.TrimSpace(def[literalStart:literalEnd])
	if refTableLiteral == "" {
		return schema.Constraint{}, NewParseError("missing referenced table")
	}

	refColumnsLiteral, afterColsIdx, err := extractParenthesizedLiteral(def, tokens, closeIdx)
	if err != nil {
		return schema.Constraint{}, err
	}

	refColumns := normalizeIdentifierList(p, refColumnsLiteral)
	if len(refColumns) == 0 {
		return schema.Constraint{}, NewParseError("missing referenced columns")
	}

	remaining := ""

	nextIdx := nextNonCommentIndex(tokens, afterColsIdx)
	if nextIdx < len(tokens) {
		remaining = strings.TrimSpace(def[tokens[nextIdx].Start:])
	}

	onDelete := extractAfterKeyword(remaining, "ON DELETE")
	if onDelete == "" {
		onDelete = schema.NoAction
	}

	onUpdate := extractAfterKeyword(remaining, "ON UPDATE")
	if onUpdate == "" {
		onUpdate = schema.NoAction
	}

	isDeferrable := hasKeyword(remaining, "DEFERRABLE")
	initiallyDeferred := hasKeyword(remaining, "INITIALLY DEFERRED")

	definition := fmt.Sprintf(
		"FOREIGN KEY (%s) REFERENCES %s(%s)",
		colName,
		refTableLiteral,
		strings.Join(refColumns, ", "),
	)

	return schema.Constraint{
		Type:              schema.ConstraintForeignKey,
		Columns:           []string{colName},
		ReferencedTable:   refTableLiteral,
		ReferencedColumns: refColumns,
		OnDelete:          onDelete,
		OnUpdate:          onUpdate,
		IsDeferrable:      isDeferrable,
		InitiallyDeferred: initiallyDeferred,
		Definition:        definition,
	}, nil
}

func (p *Parser) parseInlineCheck(colName, def string) (schema.Constraint, error) {
	tokens, err := tokenizeConstraintDefinition(def)
	if err != nil {
		return schema.Constraint{}, err
	}

	checkIdx := findKeyword(tokens, "CHECK", 0)
	if checkIdx == -1 {
		return schema.Constraint{}, NewParseError("invalid CHECK syntax")
	}

	parenIdx := nextNonCommentIndex(tokens, checkIdx+1)
	if parenIdx >= len(tokens) || tokens[parenIdx].Type != TokenLParen {
		return schema.Constraint{}, NewParseError("invalid CHECK syntax")
	}

	expr, _, err := extractParenthesizedLiteral(def, tokens, parenIdx)
	if err != nil {
		return schema.Constraint{}, err
	}

	expr = strings.TrimSpace(expr)
	if expr == "" {
		return schema.Constraint{}, NewParseError("invalid CHECK syntax")
	}

	definition := fmt.Sprintf("CHECK (%s)", expr)

	return schema.Constraint{
		Type:            schema.ConstraintCheck,
		Columns:         []string{colName},
		Definition:      definition,
		CheckExpression: definition,
	}, nil
}

func parseTypeParams(dataType string) (base string, precision, scale, maxLength *int) {
	dataType = strings.TrimSpace(dataType)

	if strings.HasSuffix(dataType, "[]") {
		base = strings.TrimSuffix(dataType, "[]")
		return base, nil, nil, nil
	}

	if !strings.Contains(dataType, "(") {
		return dataType, nil, nil, nil
	}

	openIdx := strings.Index(dataType, "(")
	closeIdx := strings.LastIndex(dataType, ")")

	if openIdx == -1 || closeIdx == -1 || closeIdx < openIdx {
		return dataType, nil, nil, nil
	}

	base = strings.TrimSpace(dataType[:openIdx])
	params := strings.TrimSpace(dataType[openIdx+1 : closeIdx])

	parts := strings.Split(params, ",")

	if len(parts) == 1 { //nolint:nestif
		var val int
		if _, err := fmt.Sscanf(strings.TrimSpace(parts[0]), "%d", &val); err == nil {
			baseUpper := strings.ToUpper(base)
			if strings.Contains(baseUpper, "CHAR") || baseUpper == "VARCHAR" ||
				baseUpper == "CHAR" {
				maxLength = &val
			} else {
				precision = &val
			}
		}
	} else if len(parts) == 2 {
		var prec, sc int
		if _, err := fmt.Sscanf(strings.TrimSpace(parts[0]), "%d", &prec); err == nil {
			precision = &prec
		}

		if _, err := fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &sc); err == nil {
			scale = &sc
		}
	}

	return base, precision, scale, maxLength
}

func extractDefault(def string) string {
	tokens, err := tokenizeConstraintDefinition(def)
	if err != nil || len(tokens) == 0 {
		return ""
	}

	if tokens[len(tokens)-1].Type == TokenEOF {
		tokens = tokens[:len(tokens)-1]
	}

	defaultIdx := findKeyword(tokens, "DEFAULT", 0)
	if defaultIdx == -1 {
		return ""
	}

	expr, _ := collectLiteralUntil(
		tokens,
		def,
		defaultIdx+1,
		"NOT",
		"NULL",
		"PRIMARY",
		"FOREIGN",
		"CHECK",
		"REFERENCES",
		"UNIQUE",
		"CONSTRAINT",
		"GENERATED",
		"COLLATE",
	)

	return strings.TrimSpace(expr)
}

func (p *Parser) finalizeTableConstraints(table *schema.Table) {
	for i := range table.Columns {
		col := &table.Columns[i]
		if col.Default == "__SERIAL__" || col.Default == "__BIGSERIAL__" ||
			col.Default == "__SMALLSERIAL__" {
			sequenceName := fmt.Sprintf("%s_%s_seq", table.Name, col.Name)
			if table.Schema != "" && table.Schema != schema.DefaultSchema {
				sequenceName = fmt.Sprintf("%s.%s", table.Schema, sequenceName)
			}

			col.Default = fmt.Sprintf("nextval('%s'::regclass)", sequenceName)
		}
	}

	for i := range table.Constraints {
		constraint := &table.Constraints[i]

		if constraint.Name == "" {
			constraint.Name = p.generateConstraintName(table.Name, constraint)
		}

		if constraint.Type == schema.ConstraintPrimaryKey ||
			constraint.Type == schema.ConstraintUnique {
			indexName := constraint.Name
			if constraint.Type == schema.ConstraintPrimaryKey {
				table.Indexes = append(table.Indexes, schema.Index{
					Schema:    table.Schema,
					TableName: table.Name,
					Name:      indexName,
					Columns:   constraint.Columns,
					Type:      "btree",
					IsUnique:  true,
					IsPrimary: true,
					Definition: fmt.Sprintf(
						"CREATE UNIQUE INDEX %s ON %s USING btree (%s)",
						indexName,
						table.QualifiedName(),
						strings.Join(constraint.Columns, ", "),
					),
				})
			} else {
				table.Indexes = append(table.Indexes, schema.Index{
					Schema:    table.Schema,
					TableName: table.Name,
					Name:      indexName,
					Columns:   constraint.Columns,
					Type:      "btree",
					IsUnique:  true,
					IsPrimary: false,
					Definition: fmt.Sprintf(
						"CREATE UNIQUE INDEX %s ON %s USING btree (%s)",
						indexName,
						table.QualifiedName(),
						strings.Join(constraint.Columns, ", "),
					),
				})
			}
		}

		if constraint.Type == schema.ConstraintForeignKey && constraint.ReferencedTable != "" {
			refSchema, refTable := p.splitSchemaTable(constraint.ReferencedTable)
			if refSchema == "" {
				refSchema = table.Schema
			}

			constraint.ReferencedSchema = refSchema
			constraint.ReferencedTable = refTable
		}
	}
}

func (p *Parser) generateConstraintName(tableName string, constraint *schema.Constraint) string {
	var name string

	switch constraint.Type {
	case schema.ConstraintPrimaryKey:
		name = tableName + "_pkey"
	case schema.ConstraintUnique:
		if len(constraint.Columns) == 1 {
			name = tableName + "_" + constraint.Columns[0] + "_key"
		} else {
			name = tableName + "_" + strings.Join(constraint.Columns, "_") + "_key"
		}
	case schema.ConstraintForeignKey:
		if len(constraint.Columns) == 1 {
			name = tableName + "_" + constraint.Columns[0] + "_fkey"
		} else {
			name = tableName + "_" + strings.Join(constraint.Columns, "_") + "_fkey"
		}
	case schema.ConstraintCheck:
		if len(constraint.Columns) == 1 {
			name = tableName + "_" + constraint.Columns[0] + "_check"
		} else {
			name = tableName + "_check"
		}
	case schema.ConstraintExclude:
		name = tableName + "_exclude"
	default:
		name = tableName + "_constraint"
	}

	if len(name) > 63 {
		name = name[:63]
	}

	return name
}

func (p *Parser) parseAlterTable(stmt string, db *schema.Database) error {
	if !hasKeyword(strings.ToUpper(stmt), "TIMESCALEDB.COMPRESS") {
		return nil
	}

	pattern := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s+SET`)

	matches := pattern.FindStringSubmatch(stmt)
	if len(matches) < 2 {
		return errors.New("cannot extract table name")
	}

	schemaName, tableName := p.splitSchemaTable(matches[1])

	var ht *schema.Hypertable

	for i := range db.Hypertables {
		if db.Hypertables[i].Schema == schemaName && db.Hypertables[i].TableName == tableName {
			ht = &db.Hypertables[i]
			break
		}
	}

	if ht == nil {
		db.Hypertables = append(db.Hypertables, schema.Hypertable{
			Schema:    schemaName,
			TableName: tableName,
		})
		ht = &db.Hypertables[len(db.Hypertables)-1]
	}

	ht.CompressionEnabled = true
	if ht.CompressionSettings == nil {
		ht.CompressionSettings = &schema.CompressionSettings{}
	}

	if segmentRe := regexp.MustCompile(`(?i)timescaledb\.compress_segmentby\s*=\s*'([^']*)'`); segmentRe.MatchString(
		stmt,
	) {
		if m := segmentRe.FindStringSubmatch(stmt); len(m) > 1 {
			for seg := range strings.SplitSeq(m[1], ",") {
				ht.CompressionSettings.SegmentByColumns = append(
					ht.CompressionSettings.SegmentByColumns,
					strings.TrimSpace(seg),
				)
			}
		}
	}

	orderRe := regexp.MustCompile(`(?i)timescaledb\.compress_orderby\s*=\s*'([^']*)'`)
	if orderRe.MatchString(stmt) { //nolint:nestif
		if m := orderRe.FindStringSubmatch(stmt); len(m) > 1 {
			for order := range strings.SplitSeq(m[1], ",") {
				order = strings.TrimSpace(order)

				parts := strings.Fields(order)
				if len(parts) > 0 {
					col := schema.OrderByColumn{
						Column:    parts[0],
						Direction: "ASC",
					}
					if len(parts) > 1 && strings.ToUpper(parts[1]) == "DESC" {
						col.Direction = "DESC"
					}

					ht.CompressionSettings.OrderByColumns = append(
						ht.CompressionSettings.OrderByColumns,
						col,
					)
				}
			}
		}
	}

	return nil
}

func (p *Parser) parsePartitionBy(stmt string) *schema.PartitionStrategy {
	strategy, err := p.parsePartitionByTokens(stmt)
	if err != nil {
		return nil
	}

	if strategy != nil {
		return strategy
	}

	return nil
}

func (p *Parser) parsePartitionByTokens(stmt string) (*schema.PartitionStrategy, error) {
	tokens, err := NewLexer(stmt).Tokenize()
	if err != nil {
		return nil, err
	}

	for idx := 0; idx < len(tokens); idx++ {
		idx = nextNonCommentIndex(tokens, idx)
		if idx >= len(tokens) {
			break
		}

		if upperLiteral(tokens, idx) != "PARTITION" {
			continue
		}

		byIdx := nextNonCommentIndex(tokens, idx+1)
		if byIdx >= len(tokens) || upperLiteral(tokens, byIdx) != "BY" {
			continue
		}

		typeIdx := nextNonCommentIndex(tokens, byIdx+1)
		if typeIdx >= len(tokens) {
			return nil, NewParseError("missing partition strategy")
		}

		partitionType := upperLiteral(tokens, typeIdx)
		if partitionType != "HASH" && partitionType != "RANGE" && partitionType != "LIST" {
			continue
		}

		openIdx := nextNonCommentIndex(tokens, typeIdx+1)
		if openIdx >= len(tokens) || tokens[openIdx].Type != TokenLParen {
			return nil, NewParseError("missing partition column list")
		}

		start := tokens[openIdx].End
		depth := 1
		end := len(stmt)
		closeFound := false

		for k := openIdx + 1; k < len(tokens); k++ {
			token := tokens[k]
			if token.Type == TokenComment {
				continue
			}

			switch token.Type {
			case TokenLParen:
				depth++
			case TokenRParen:
				depth--
				if depth == 0 {
					end = token.Start
					closeFound = true
				}
			}

			if closeFound {
				break
			}
		}

		if !closeFound {
			return nil, NewParseError("unterminated partition column list")
		}

		columnList := strings.TrimSpace(stmt[start:end])
		columns := normalizeIdentifierList(p, columnList)

		return &schema.PartitionStrategy{
			Type:    partitionType,
			Columns: columns,
		}, nil
	}

	return nil, nil //nolint:nilnil
}

func (p *Parser) parsePartitionOfTokens(stmt string, db *schema.Database) error {
	tokens, err := NewLexer(stmt).Tokenize()
	if err != nil {
		return err
	}

	createIdx := findKeyword(tokens, "CREATE", 0)
	if createIdx == -1 {
		return errors.New("missing CREATE keyword")
	}

	tableIdx := nextNonCommentIndex(tokens, createIdx+1)
	if tableIdx >= len(tokens) || upperLiteral(tokens, tableIdx) != "TABLE" {
		return errors.New("missing TABLE keyword")
	}

	tableLiteral, afterTableIdx := collectLiteralUntil(tokens, stmt, tableIdx+1, "PARTITION")
	if tableLiteral == "" {
		return errors.New("cannot extract partition table name")
	}

	_, partitionName := p.splitSchemaTable(tableLiteral)

	partitionIdx := nextNonCommentIndex(tokens, afterTableIdx)
	if partitionIdx >= len(tokens) || upperLiteral(tokens, partitionIdx) != "PARTITION" {
		return errors.New("missing PARTITION keyword")
	}

	ofIdx := nextNonCommentIndex(tokens, partitionIdx+1)
	if ofIdx >= len(tokens) || upperLiteral(tokens, ofIdx) != "OF" {
		return errors.New("missing OF keyword")
	}

	parentLiteral, afterParentIdx := collectLiteralUntil(
		tokens,
		stmt,
		ofIdx+1,
		"FOR",
		"USING",
		"WITH",
		"TABLESPACE",
	)
	if parentLiteral == "" {
		return errors.New("cannot extract parent table name")
	}

	parentSchema, parentName := p.splitSchemaTable(parentLiteral)

	partitionDef := ""

	forIdx := findKeyword(tokens, "FOR", afterParentIdx)
	if forIdx != -1 {
		valuesIdx := nextNonCommentIndex(tokens, forIdx+1)
		if valuesIdx < len(tokens) && upperLiteral(tokens, valuesIdx) == "VALUES" {
			start := tokens[forIdx].Start

			end := len(stmt)
			if semicolonIdx := findToken(tokens, TokenSemicolon, forIdx); semicolonIdx != -1 {
				end = tokens[semicolonIdx].Start
			}

			partitionDef = strings.TrimSpace(stmt[start:end])
		}
	}

	parentTable := db.GetTable(parentSchema, parentName)
	if parentTable == nil {
		ctx := p.ensureContext()
		ctx.deferred = append(ctx.deferred, deferredPartition{
			parentSchema:  parentSchema,
			parentName:    parentName,
			partitionName: partitionName,
			definition:    partitionDef,
		})

		return nil
	}

	if parentTable.PartitionStrategy == nil {
		parentTable.PartitionStrategy = &schema.PartitionStrategy{}
	}

	parentTable.PartitionStrategy.Partitions = append(
		parentTable.PartitionStrategy.Partitions,
		schema.Partition{
			Name:       partitionName,
			Definition: partitionDef,
		},
	)

	return nil
}

func (p *Parser) parseDoBlock(stmt string, _ *schema.Database) error {
	stmtUpper := strings.ToUpper(stmt)

	if strings.Contains(stmtUpper, "CREATE TABLE") && strings.Contains(stmtUpper, "PARTITION") {
		p.addWarning(
			0,
			"DO block contains partition creation logic. "+
				"Consider using declarative PARTITION BY syntax with explicit CREATE TABLE ... PARTITION OF statements instead.",
		)
	}

	return nil
}
