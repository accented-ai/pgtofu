package generator

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/MeKo-Christian/go-sqlfmt/pkg/sqlfmt/core"
	"github.com/MeKo-Christian/go-sqlfmt/pkg/sqlfmt/dialects"
	pgquery "github.com/pganalyze/pg_query_go/v6"
	wasmquery "github.com/wasilibs/go-pgquery"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/accented-ai/pgtofu/internal/parser"
)

// The WASM parser keeps one runtime per concurrent caller. View generation is
// sequential in production, and serializing test callers prevents parallel
// builders from retaining several large runtimes in the parser pool.
var viewQueryFormatMu sync.Mutex //nolint:gochecknoglobals

type viewQueryNames struct {
	relationAliases map[string]int
	functions       map[string]int
}

func formatViewQuery(query string) (string, error) {
	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))

	if query == "" {
		return "", errors.New("view query cannot be empty")
	}

	viewQueryFormatMu.Lock()
	defer viewQueryFormatMu.Unlock()

	tree, err := wasmquery.Parse(query)
	if err != nil {
		return "", fmt.Errorf("parse view query: %w", err)
	}

	canonical, err := wasmquery.Deparse(tree)
	if err != nil {
		return "", fmt.Errorf("deparse view query: %w", err)
	}

	names := collectViewQueryNames(tree)

	normalized, err := normalizeViewQueryStyle(canonical, names)
	if err != nil {
		return "", fmt.Errorf("normalize view query style: %w", err)
	}

	formatterConfig := &dialects.Config{
		Indent:              "    ",
		KeywordCase:         dialects.KeywordCaseUppercase,
		MaxLineLength:       generatedSQLLineLength,
		JoinIndentStyle:     core.JoinIndentRootLevel,
		TokenizerConfig:     dialects.NewPostgreSQLTokenizerConfig(),
		LinesBetweenQueries: 1,
	}
	formatter := dialects.NewPostgreSQLFormatter(formatterConfig)
	configureViewFormatterLayout(formatterConfig.TokenizerConfig)

	formatted := strings.TrimSpace(formatter.Format(normalized))
	formatted = strings.TrimSuffix(formatted, ";")
	formatted = strings.TrimSpace(formatted)

	formatted, err = formatMultilineCaseConditions(formatted)
	if err != nil {
		return "", fmt.Errorf("format multiline CASE condition: %w", err)
	}

	formatted, err = formatViewQueryLayout(formatted)
	if err != nil {
		return "", fmt.Errorf("format view query layout: %w", err)
	}

	roundTripTree, err := wasmquery.Parse(formatted)
	if err != nil {
		return "", fmt.Errorf("parse formatted view query: %w", err)
	}

	roundTripCanonical, err := wasmquery.Deparse(roundTripTree)
	if err != nil {
		return "", fmt.Errorf("deparse formatted view query: %w", err)
	}

	if canonical != roundTripCanonical {
		return "", errors.New("formatted view query changed PostgreSQL parse tree")
	}

	return formatted, nil
}

func collectViewQueryNames(tree *pgquery.ParseResult) viewQueryNames {
	names := viewQueryNames{
		relationAliases: make(map[string]int),
		functions:       make(map[string]int),
	}

	var visit func(protoreflect.Message)

	visit = func(message protoreflect.Message) {
		switch value := message.Interface().(type) {
		case *pgquery.RangeVar:
			addViewAlias(names.relationAliases, value.GetAlias())
		case *pgquery.JoinExpr:
			addViewAlias(names.relationAliases, value.GetAlias())
			addViewAlias(names.relationAliases, value.GetJoinUsingAlias())
		case *pgquery.RangeSubselect:
			addViewAlias(names.relationAliases, value.GetAlias())
		case *pgquery.RangeFunction:
			addViewAlias(names.relationAliases, value.GetAlias())
		case *pgquery.RangeTableFunc:
			addViewAlias(names.relationAliases, value.GetAlias())
		case *pgquery.JsonTable:
			addViewAlias(names.relationAliases, value.GetAlias())
		case *pgquery.FuncCall:
			if functionName := lastViewFunctionName(value.GetFuncname()); functionName != "" {
				names.functions[functionName]++
			}
		}

		message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if field.IsList() && field.Kind() == protoreflect.MessageKind {
				list := value.List()
				for i := range list.Len() {
					visit(list.Get(i).Message())
				}

				return true
			}

			if field.Kind() == protoreflect.MessageKind && message.Has(field) {
				visit(value.Message())
			}

			return true
		})
	}

	visit(tree.ProtoReflect())

	return names
}

func addViewAlias(aliases map[string]int, alias *pgquery.Alias) {
	if alias == nil || alias.GetAliasname() == "" {
		return
	}

	aliases[alias.GetAliasname()]++
}

func lastViewFunctionName(parts []*pgquery.Node) string {
	if len(parts) == 0 {
		return ""
	}

	name := parts[len(parts)-1].GetString_()
	if name == nil {
		return ""
	}

	return name.GetSval()
}

func normalizeViewQueryStyle(query string, names viewQueryNames) (string, error) {
	tokens, err := parser.NewLexer(query).Tokenize()
	if err != nil {
		return "", fmt.Errorf("tokenize view query: %w", err)
	}

	aliasIndexes := findImplicitViewAliasIndexes(tokens, names.relationAliases)
	functionIndexes := findViewFunctionIndexes(tokens, names.functions, aliasIndexes)
	replacements := make(map[int]string)
	insertions := make(map[int][]string)

	for index := range aliasIndexes {
		insertions[index] = append(insertions[index], "AS ")
	}

	for index := range functionIndexes {
		replacements[index] = strings.ToUpper(tokens[index].Literal)
	}

	markViewLiterals(tokens, replacements)

	for i := range tokens {
		if !strings.EqualFold(tokens[i].Literal, "JOIN") || hasExplicitJoinType(tokens, i) {
			continue
		}

		insertions[i] = append(insertions[i], "INNER ")
	}

	markViewCastTypes(tokens, replacements)

	var (
		output   strings.Builder
		position int
	)

	for i, token := range tokens {
		if token.Type == parser.TokenEOF {
			break
		}

		output.WriteString(query[position:token.Start])

		for _, insertion := range insertions[i] {
			output.WriteString(insertion)
		}

		if replacement, ok := replacements[i]; ok {
			output.WriteString(replacement)
		} else {
			output.WriteString(query[token.Start:token.End])
		}

		position = token.End
	}

	output.WriteString(query[position:])

	return output.String(), nil
}

func markViewLiterals(tokens []parser.Token, replacements map[int]string) {
	for i, token := range tokens {
		if token.Type == parser.TokenQuotedIdentifier || token.Type == parser.TokenString {
			continue
		}

		switch strings.ToUpper(token.Literal) {
		case "TRUE", "FALSE", "NULL":
			replacements[i] = strings.ToUpper(token.Literal)
		}
	}
}

type viewCaseFrame struct {
	whenOffset int
}

type viewCaseCondition struct {
	whenOffset int
	thenOffset int
}

func formatMultilineCaseConditions(query string) (string, error) {
	tokens, err := parser.NewLexer(query).Tokenize()
	if err != nil {
		return "", fmt.Errorf("tokenize formatted view query: %w", err)
	}

	conditions := collectMultilineCaseConditions(tokens)
	if len(conditions) == 0 {
		return query, nil
	}

	lines := strings.Split(query, "\n")
	lineStarts := viewQueryLineStarts(query)
	indentAdds := make([]int, len(lines))
	conditionByLine := make(map[int]viewCaseCondition, len(conditions))

	for _, condition := range conditions {
		var (
			whenLine = viewQueryLineAt(lineStarts, condition.whenOffset)
			thenLine = viewQueryLineAt(lineStarts, condition.thenOffset)
		)

		if whenLine == thenLine || whenLine >= len(lines) || thenLine >= len(lines) {
			continue
		}

		var (
			whenColumn = condition.whenOffset - lineStarts[whenLine]
			beforeWhen = lines[whenLine][:whenColumn]
			afterWhen  = strings.TrimSpace(lines[whenLine][whenColumn+len("WHEN"):])
		)

		if strings.TrimSpace(beforeWhen) != "" || afterWhen == "" {
			continue
		}

		conditionByLine[whenLine] = condition
		for line := whenLine + 1; line <= thenLine; line++ {
			indentAdds[line]++
		}
	}

	var output strings.Builder

	for lineIndex, line := range lines {
		if lineIndex > 0 {
			output.WriteByte('\n')
		}

		extraIndent := strings.Repeat("    ", indentAdds[lineIndex])

		condition, ok := conditionByLine[lineIndex]
		if !ok {
			output.WriteString(extraIndent)
			output.WriteString(line)

			continue
		}

		whenColumn := condition.whenOffset - lineStarts[lineIndex]
		lineIndent := line[:whenColumn]
		expression := strings.TrimSpace(line[whenColumn+len("WHEN"):])

		output.WriteString(extraIndent)
		output.WriteString(lineIndent)
		output.WriteString("WHEN\n")
		output.WriteString(extraIndent)
		output.WriteString(lineIndent)
		output.WriteString("    ")
		output.WriteString(expression)
	}

	return output.String(), nil
}

func collectMultilineCaseConditions(tokens []parser.Token) []viewCaseCondition {
	frames := make([]viewCaseFrame, 0)
	conditions := make([]viewCaseCondition, 0)

	for _, token := range tokens {
		switch strings.ToUpper(token.Literal) {
		case "CASE":
			frames = append(frames, viewCaseFrame{whenOffset: -1})
		case "WHEN":
			if len(frames) > 0 {
				frames[len(frames)-1].whenOffset = token.Start
			}
		case "THEN":
			if len(frames) > 0 && frames[len(frames)-1].whenOffset >= 0 {
				conditions = append(conditions, viewCaseCondition{
					whenOffset: frames[len(frames)-1].whenOffset,
					thenOffset: token.Start,
				})
				frames[len(frames)-1].whenOffset = -1
			}
		case "END":
			if len(frames) > 0 {
				frames = frames[:len(frames)-1]
			}
		}
	}

	return conditions
}

func viewQueryLineStarts(query string) []int {
	starts := []int{0}

	for index, value := range []byte(query) {
		if value == '\n' {
			starts = append(starts, index+1)
		}
	}

	return starts
}

func viewQueryLineAt(starts []int, offset int) int {
	line := 0

	for line+1 < len(starts) && starts[line+1] <= offset {
		line++
	}

	return line
}

func findImplicitViewAliasIndexes(tokens []parser.Token, aliases map[string]int) map[int]bool {
	indexes := make(map[int]bool)

	for i, token := range tokens {
		if !isViewIdentifierToken(token) {
			continue
		}

		name := viewIdentifierName(token)
		if aliases[name] == 0 || !isImplicitViewAlias(tokens, i) {
			continue
		}

		indexes[i] = true
	}

	return indexes
}

func findViewFunctionIndexes(
	tokens []parser.Token,
	functions map[string]int,
	aliasIndexes map[int]bool,
) map[int]bool {
	indexes := make(map[int]bool)

	for i, token := range tokens {
		if aliasIndexes[i] || token.Type == parser.TokenQuotedIdentifier ||
			!isViewIdentifierToken(token) {
			continue
		}

		name := strings.ToLower(token.Literal)
		if functions[name] == 0 || nextViewTokenType(tokens, i) != parser.TokenLParen {
			continue
		}

		indexes[i] = true
	}

	return indexes
}

func isImplicitViewAlias(tokens []parser.Token, index int) bool {
	previous := previousViewToken(tokens, index)
	if previous == nil || strings.EqualFold(previous.Literal, "AS") ||
		previous.Type == parser.TokenDot {
		return false
	}

	switch previous.Type {
	case parser.TokenIdentifier,
		parser.TokenQuotedIdentifier,
		parser.TokenRParen,
		parser.TokenRBracket:
	case parser.TokenOperator:
		if previous.Literal != "*" {
			return false
		}
	default:
		return false
	}

	next := nextViewToken(tokens, index)
	if next == nil {
		return true
	}

	switch next.Type {
	case parser.TokenEOF,
		parser.TokenComma,
		parser.TokenRParen,
		parser.TokenSemicolon,
		parser.TokenLParen:
		return true
	}

	switch strings.ToUpper(next.Literal) {
	case "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL", "JOIN",
		"ON", "USING", "WHERE", "GROUP", "HAVING", "WINDOW", "ORDER",
		"LIMIT", "OFFSET", "FETCH", "FOR", "UNION", "INTERSECT", "EXCEPT",
		"TABLESAMPLE":
		return true
	default:
		return false
	}
}

func hasExplicitJoinType(tokens []parser.Token, index int) bool {
	previous := previousViewToken(tokens, index)
	if previous == nil {
		return false
	}

	switch strings.ToUpper(previous.Literal) {
	case "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "OUTER":
		return true
	default:
		return false
	}
}

func markViewCastTypes(tokens []parser.Token, replacements map[int]string) {
	for i := range tokens {
		if tokens[i].Type == parser.TokenColon && i+1 < len(tokens) &&
			tokens[i+1].Type == parser.TokenColon {
			markViewTypeAt(tokens, i+2, replacements)
		}

		if !strings.EqualFold(tokens[i].Literal, "CAST") ||
			nextViewTokenType(tokens, i) != parser.TokenLParen {
			continue
		}

		openIndex := nextViewTokenIndex(tokens, i)
		depth := 0

		for j := openIndex; j < len(tokens); j++ {
			switch tokens[j].Type {
			case parser.TokenLParen:
				depth++
			case parser.TokenRParen:
				depth--
				if depth == 0 {
					j = len(tokens)
				}
			default:
				if depth == 1 && strings.EqualFold(tokens[j].Literal, "AS") {
					markViewTypeAt(tokens, j+1, replacements)
					j = len(tokens)
				}
			}
		}
	}
}

func markViewTypeAt(tokens []parser.Token, start int, replacements map[int]string) {
	if start >= len(tokens) || !isViewIdentifierToken(tokens[start]) {
		return
	}

	nameIndexes := []int{start}
	position := start + 1

	for position+1 < len(tokens) && tokens[position].Type == parser.TokenDot &&
		isViewIdentifierToken(tokens[position+1]) {
		nameIndexes = append(nameIndexes, position+1)
		position += 2
	}

	baseIndex := nameIndexes[len(nameIndexes)-1]
	if tokens[baseIndex].Type == parser.TokenQuotedIdentifier ||
		!isBuiltinViewType(tokens[baseIndex].Literal) {
		return
	}

	replacements[baseIndex] = strings.ToUpper(tokens[baseIndex].Literal)

	if position < len(tokens) && tokens[position].Type == parser.TokenLParen {
		position = tokenAfterMatchingParen(tokens, position)
	}

	for position < len(tokens) && isViewTypeQualifier(tokens[position].Literal) {
		if tokens[position].Type != parser.TokenQuotedIdentifier {
			replacements[position] = strings.ToUpper(tokens[position].Literal)
		}

		position++
	}
}

func tokenAfterMatchingParen(tokens []parser.Token, openIndex int) int {
	depth := 0

	for i := openIndex; i < len(tokens); i++ {
		switch tokens[i].Type {
		case parser.TokenLParen:
			depth++
		case parser.TokenRParen:
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}

	return len(tokens)
}

func isBuiltinViewType(value string) bool {
	switch strings.ToLower(value) {
	case "anyarray", "anycompatible", "anycompatiblearray", "anycompatiblemultirange",
		"anycompatiblenonarray", "anycompatiblerange", "anyelement", "anyenum",
		"anymultirange", "anynonarray", "anyrange", "bigint", "bigserial", "bit",
		"bool", "boolean", "box", "bpchar", "bytea", "char", "character", "cid",
		"cidr", "circle", "cstring", "date", "decimal", "double", "float4",
		"float8", "inet", "int", "int2", "int4", "int8", "integer", "internal",
		"interval", "json", "jsonb", "line", "lseg", "macaddr", "macaddr8",
		"money", "name", "numeric", "oid", "path", "pg_lsn", "point", "polygon",
		"real", "record", "regclass", "regcollation", "regconfig", "regdictionary",
		"regnamespace", "regoper", "regoperator", "regproc", "regprocedure", "regrole",
		"regtype", "serial", "serial2", "serial4", "serial8", "smallint",
		"smallserial", "text", "time", "timestamp", "timetz", "timestamptz",
		"trigger", "tsquery", "tsvector", "txid_snapshot", "uuid", "varbit",
		"varchar", "void", "xid", "xid8", "xml":
		return true
	default:
		return false
	}
}

func isViewTypeQualifier(value string) bool {
	switch strings.ToLower(value) {
	case "varying", "precision", "with", "without", "time", "zone", "year",
		"month", "day", "hour", "minute", "second", "to":
		return true
	default:
		return false
	}
}

func isViewIdentifierToken(token parser.Token) bool {
	return token.Type == parser.TokenIdentifier ||
		token.Type == parser.TokenQuotedIdentifier ||
		token.Type == parser.TokenKeyword
}

func viewIdentifierName(token parser.Token) string {
	if token.Type != parser.TokenQuotedIdentifier {
		return strings.ToLower(token.Literal)
	}

	name := strings.TrimSuffix(strings.TrimPrefix(token.Literal, `"`), `"`)

	return strings.ReplaceAll(name, `""`, `"`)
}

func previousViewToken(tokens []parser.Token, index int) *parser.Token {
	index--
	if index < 0 {
		return nil
	}

	return &tokens[index]
}

func nextViewToken(tokens []parser.Token, index int) *parser.Token {
	index = nextViewTokenIndex(tokens, index)
	if index >= len(tokens) {
		return nil
	}

	return &tokens[index]
}

func nextViewTokenType(tokens []parser.Token, index int) parser.TokenType {
	next := nextViewToken(tokens, index)
	if next == nil {
		return parser.TokenEOF
	}

	return next.Type
}

func nextViewTokenIndex(tokens []parser.Token, index int) int {
	index++
	for index < len(tokens) && tokens[index].Type == parser.TokenComment {
		index++
	}

	return index
}
