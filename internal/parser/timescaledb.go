package parser

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type timescaleCall struct {
	name       string
	positional []string
	named      map[string]string
}

func (p *Parser) parseCreateHypertable(stmt string, db *schema.Database) error {
	call, err := parseTimescaleCall(stmt)
	if err != nil {
		return err
	}

	if call.name != "create_hypertable" {
		return NewParseError("unexpected TimescaleDB function")
	}

	if len(call.positional) < 2 {
		return NewParseError("create_hypertable requires at least 2 arguments")
	}

	tableSchema, tableName := p.splitSchemaTable(unquote(call.positional[0]))

	var (
		timeColumn        string
		partitionInterval string
	)

	second := strings.TrimSpace(call.positional[1])
	switch {
	case strings.HasPrefix(strings.ToUpper(second), "BY_RANGE"):
		column, interval, err := parseTimescaleByRange(second)
		if err != nil {
			return err
		}

		timeColumn = column
		partitionInterval = interval
	case strings.HasPrefix(strings.ToUpper(second), "BY_HASH"):
		column, err := parseTimescaleByHash(second)
		if err != nil {
			return err
		}

		timeColumn = column
	default:
		timeColumn = unquote(second)

		if len(call.positional) > 2 {
			partitionInterval = extractIntervalValue(call.positional[2])
		}
	}

	if partitionInterval == "" {
		if val, ok := call.named["chunk_time_interval"]; ok {
			partitionInterval = extractIntervalValue(val)
		}
	}

	var ht *schema.Hypertable

	for i := range db.Hypertables {
		if db.Hypertables[i].Schema == tableSchema && db.Hypertables[i].TableName == tableName {
			ht = &db.Hypertables[i]
			break
		}
	}

	if ht == nil {
		db.Hypertables = append(db.Hypertables, schema.Hypertable{
			Schema:    tableSchema,
			TableName: tableName,
		})
		ht = &db.Hypertables[len(db.Hypertables)-1]
	}

	ht.TimeColumnName = timeColumn
	if partitionInterval != "" {
		ht.ChunkTimeInterval = partitionInterval
	}

	return nil
}

func (p *Parser) parseCompressionPolicy(stmt string, db *schema.Database) error {
	call, err := parseTimescaleCall(stmt)
	if err != nil {
		return err
	}

	if call.name != "add_compression_policy" {
		return NewParseError("unexpected TimescaleDB function")
	}

	if len(call.positional) < 1 {
		return NewParseError("add_compression_policy requires hypertable name")
	}

	tableSchema, tableName := p.splitSchemaTable(unquote(call.positional[0]))

	compressAfter := ""
	if len(call.positional) > 1 {
		compressAfter = call.positional[1]
	}

	if compressAfter == "" {
		if val, ok := call.named["compress_after"]; ok {
			compressAfter = val
		}
	}

	compressAfter = extractIntervalValue(compressAfter)

	var ht *schema.Hypertable

	for i := range db.Hypertables {
		if db.Hypertables[i].Schema == tableSchema && db.Hypertables[i].TableName == tableName {
			ht = &db.Hypertables[i]
			break
		}
	}

	if ht == nil {
		return fmt.Errorf("hypertable %s.%s not found", tableSchema, tableName)
	}

	ht.CompressionEnabled = true
	if ht.CompressionSettings == nil {
		ht.CompressionSettings = &schema.CompressionSettings{}
	}

	ht.CompressionSettings.ChunkTimeInterval = compressAfter

	return nil
}

func (p *Parser) parseRetentionPolicy(stmt string, db *schema.Database) error {
	call, err := parseTimescaleCall(stmt)
	if err != nil {
		return err
	}

	if call.name != "add_retention_policy" {
		return NewParseError("unexpected TimescaleDB function")
	}

	if len(call.positional) < 1 {
		return NewParseError("add_retention_policy requires hypertable name")
	}

	tableSchema, tableName := p.splitSchemaTable(unquote(call.positional[0]))

	dropAfter := ""
	if len(call.positional) > 1 {
		dropAfter = call.positional[1]
	}

	if dropAfter == "" {
		if val, ok := call.named["drop_after"]; ok {
			dropAfter = val
		}
	}

	dropAfter = extractIntervalValue(dropAfter)

	var ht *schema.Hypertable

	for i := range db.Hypertables {
		if db.Hypertables[i].Schema == tableSchema && db.Hypertables[i].TableName == tableName {
			ht = &db.Hypertables[i]
			break
		}
	}

	if ht == nil {
		return fmt.Errorf("hypertable %s.%s not found", tableSchema, tableName)
	}

	ht.RetentionPolicy = &schema.RetentionPolicy{
		DropAfter: dropAfter,
	}

	return nil
}

func (p *Parser) parseContinuousAggregatePolicy(stmt string, db *schema.Database) error {
	call, err := parseTimescaleCall(stmt)
	if err != nil {
		return err
	}

	if call.name != "add_continuous_aggregate_policy" {
		return NewParseError("unexpected TimescaleDB function")
	}

	if len(call.positional) < 1 {
		return NewParseError("add_continuous_aggregate_policy requires view name")
	}

	caggSchema, caggName := p.splitSchemaTable(unquote(call.positional[0]))

	startOffset := ""
	if val, ok := call.named["start_offset"]; ok {
		startOffset = val
	} else if len(call.positional) > 1 {
		startOffset = call.positional[1]
	}

	endOffset := ""
	if val, ok := call.named["end_offset"]; ok {
		endOffset = val
	} else if len(call.positional) > 2 {
		endOffset = call.positional[2]
	}

	scheduleInterval := ""
	if val, ok := call.named["schedule_interval"]; ok {
		scheduleInterval = val
	} else if len(call.positional) > 3 {
		scheduleInterval = call.positional[3]
	}

	var cagg *schema.ContinuousAggregate

	for i := range db.ContinuousAggregates {
		if db.ContinuousAggregates[i].Schema == caggSchema &&
			db.ContinuousAggregates[i].ViewName == caggName {
			cagg = &db.ContinuousAggregates[i]
			break
		}
	}

	if cagg == nil {
		return fmt.Errorf("continuous aggregate %s.%s not found", caggSchema, caggName)
	}

	cagg.RefreshPolicy = &schema.RefreshPolicy{
		StartOffset:      extractIntervalValue(startOffset),
		EndOffset:        extractIntervalValue(endOffset),
		ScheduleInterval: extractIntervalValue(scheduleInterval),
	}

	return nil
}

func parseTimescaleCall(stmt string) (*timescaleCall, error) {
	tokens, err := NewLexer(stmt).Tokenize()
	if err != nil {
		return nil, WrapParseError(err, "tokenizing TimescaleDB statement")
	}

	if len(tokens) == 0 {
		return nil, NewParseError("empty TimescaleDB statement")
	}

	idx := nextNonCommentIndex(tokens, 0)
	if idx >= len(tokens) || upperLiteral(tokens, idx) != "SELECT" {
		return nil, NewParseError("TimescaleDB functions must be invoked via SELECT")
	}

	call, err := parseCallTokens(tokens, stmt, idx+1)
	if err != nil {
		return nil, err
	}

	return call, nil
}

func parseTimescaleByRange(arg string) (string, string, error) {
	call, err := parseInlineCall(arg, "BY_RANGE")
	if err != nil {
		return "", "", err
	}

	if len(call.positional) == 0 {
		return "", "", NewParseError("by_range requires a column name")
	}

	column := unquote(strings.TrimSpace(call.positional[0]))

	interval := ""
	if len(call.positional) > 1 {
		interval = extractIntervalValue(call.positional[1])
	}

	if interval == "" {
		if val, ok := call.named["interval"]; ok {
			interval = extractIntervalValue(val)
		} else if val, ok := call.named["chunk_time_interval"]; ok {
			interval = extractIntervalValue(val)
		}
	}

	return column, interval, nil
}

func parseTimescaleByHash(arg string) (string, error) {
	call, err := parseInlineCall(arg, "BY_HASH")
	if err != nil {
		return "", err
	}

	if len(call.positional) == 0 {
		return "", NewParseError("by_hash requires a column name")
	}

	return unquote(strings.TrimSpace(call.positional[0])), nil
}

func parseInlineCall(literal, expected string) (*timescaleCall, error) {
	tokens, err := NewLexer(literal).Tokenize()
	if err != nil {
		return nil, WrapParseError(err, "tokenizing "+strings.ToLower(expected))
	}

	call, err := parseCallTokens(tokens, literal, 0)
	if err != nil {
		return nil, err
	}

	if call.name != strings.ToLower(expected) {
		return nil, NewParseError(fmt.Sprintf("expected %s invocation", strings.ToLower(expected)))
	}

	return call, nil
}

func parseCallTokens(tokens []Token, stmt string, startIdx int) (*timescaleCall, error) {
	idx := nextNonCommentIndex(tokens, startIdx)
	if idx >= len(tokens) {
		return nil, NewParseError("missing function name")
	}

	nameStart := tokens[idx].Start
	cur := idx
	nameEnd := nameStart

	for cur < len(tokens) {
		tok := tokens[cur]
		switch tok.Type {
		case TokenIdentifier, TokenQuotedIdentifier:
			nameEnd = tok.End

			cur++
			if cur < len(tokens) && tokens[cur].Type == TokenDot {
				nameEnd = tokens[cur].End
				cur++
			}
		default:
			goto nameCollected
		}
	}

nameCollected:
	if nameEnd <= nameStart {
		return nil, NewParseError("invalid function name")
	}

	funcNameLiteral := strings.TrimSpace(stmt[nameStart:nameEnd])
	funcNameLiteral = strings.ReplaceAll(funcNameLiteral, `"`, "")
	funcNameLiteral = strings.ToLower(funcNameLiteral)

	openIdx := nextNonCommentIndex(tokens, cur)
	if openIdx >= len(tokens) || tokens[openIdx].Type != TokenLParen {
		return nil, NewParseError("missing function arguments")
	}

	argsLiteral, _, err := extractParenthesizedLiteral(stmt, tokens, openIdx)
	if err != nil {
		return nil, err
	}

	arguments := splitArgumentsLiteral(argsLiteral)
	positional := make([]string, 0, len(arguments))
	named := make(map[string]string, len(arguments))

	for _, arg := range arguments {
		if idx := strings.Index(arg, "=>"); idx != -1 {
			key := strings.TrimSpace(arg[:idx])
			value := strings.TrimSpace(arg[idx+2:])
			key = strings.ToLower(strings.Trim(strings.TrimSpace(key), `"`))
			named[key] = value
		} else if trimmed := strings.TrimSpace(arg); trimmed != "" {
			positional = append(positional, trimmed)
		}
	}

	return &timescaleCall{
		name:       funcNameLiteral,
		positional: positional,
		named:      named,
	}, nil
}

func splitArgumentsLiteral(literal string) []string {
	literal = strings.TrimSpace(literal)
	if literal == "" {
		return nil
	}

	tokens, err := NewLexer(literal).Tokenize()
	if err != nil || len(tokens) == 0 {
		return []string{literal}
	}

	if tokens[len(tokens)-1].Type == TokenEOF {
		tokens = tokens[:len(tokens)-1]
	}

	var (
		parts        []string
		start        = 0
		depthParen   = 0
		depthBracket = 0
	)

	for _, token := range tokens {
		switch token.Type {
		case TokenLParen:
			depthParen++
		case TokenRParen:
			if depthParen > 0 {
				depthParen--
			}
		case TokenLBracket:
			depthBracket++
		case TokenRBracket:
			if depthBracket > 0 {
				depthBracket--
			}
		case TokenComma:
			if depthParen == 0 && depthBracket == 0 {
				segment := strings.TrimSpace(literal[start:token.Start])
				if segment != "" {
					parts = append(parts, segment)
				}

				start = token.End
			}
		}
	}

	if start < len(literal) {
		segment := strings.TrimSpace(literal[start:])
		if segment != "" {
			parts = append(parts, segment)
		}
	}

	return parts
}

func extractIntervalValue(s string) string {
	s = strings.TrimSpace(s)

	intervalPattern := regexp.MustCompile(`(?i)INTERVAL\s+'([^']+)'`)
	if matches := intervalPattern.FindStringSubmatch(s); len(matches) > 1 {
		return matches[1]
	}

	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		return s[1 : len(s)-1]
	}

	return s
}
