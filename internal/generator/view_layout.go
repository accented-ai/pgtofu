package generator

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/MeKo-Christian/go-sqlfmt/pkg/sqlfmt/dialects"

	"github.com/accented-ai/pgtofu/internal/parser"
)

const (
	viewLayoutIndent      = "    "
	viewCompactLineLength = 80
)

type viewJoinLayout struct {
	line        int
	endLine     int
	onEndColumn int
}

type viewJoinConditionState struct {
	parenDepth   int
	bracketDepth int
	caseDepth    int
}

type viewSourceLayout struct {
	keywordLine   int
	relationLine  int
	qualifierLine int
	compactLine   string
}

type viewTextReplacement struct {
	start       int
	end         int
	replacement string
}

type viewInlineJoinLayout struct {
	onColumn        int
	onEndColumn     int
	qualifierIndent string
}

func formatViewQueryLayout(query string) (string, error) {
	formatted, err := formatViewOffsets(query)
	if err != nil {
		return "", err
	}

	formatted, err = normalizeViewJoinClauses(formatted)
	if err != nil {
		return "", err
	}

	formatted, err = formatViewJoinConditions(formatted)
	if err != nil {
		return "", err
	}

	formatted, err = compactViewSources(formatted)
	if err != nil {
		return "", err
	}

	formatted, err = formatViewSingleTargets(formatted)
	if err != nil {
		return "", err
	}

	formatted, err = formatViewArrayConstructors(formatted)
	if err != nil {
		return "", err
	}

	return formatted, nil
}

func formatViewOffsets(query string) (string, error) {
	tokens, err := parser.NewLexer(query).Tokenize()
	if err != nil {
		return "", fmt.Errorf("tokenize OFFSET layout: %w", err)
	}

	lineStarts := viewQueryLineStarts(query)
	lines := strings.Split(query, "\n")
	depths := viewTokenParenDepths(tokens)
	replacements := make([]viewTextReplacement, 0)

	for index, token := range tokens {
		if !strings.EqualFold(token.Literal, "OFFSET") {
			continue
		}

		line := viewQueryLineAt(lineStarts, token.Start)
		if line >= len(lines) ||
			strings.TrimSpace(lines[line][:token.Start-lineStarts[line]]) == "" {
			continue
		}

		selectIndex := previousViewSelectIndex(tokens, depths, index)
		if selectIndex < 0 {
			continue
		}

		selectLine := viewQueryLineAt(lineStarts, tokens[selectIndex].Start)
		indent := leadingViewWhitespace(lines[selectLine])
		start := token.Start

		for start > lineStarts[line] && (query[start-1] == ' ' || query[start-1] == '\t') {
			start--
		}

		replacements = append(replacements, viewTextReplacement{
			start:       start,
			end:         token.Start,
			replacement: "\n" + indent,
		})
	}

	return applyViewTextReplacements(query, replacements), nil
}

func normalizeViewJoinClauses(query string) (string, error) {
	aligned, err := alignViewJoinClauses(query)
	if err != nil {
		return "", err
	}

	return splitInlineViewJoinQualifiers(aligned)
}

func alignViewJoinClauses(query string) (string, error) {
	tokens, err := parser.NewLexer(query).Tokenize()
	if err != nil {
		return "", fmt.Errorf("tokenize JOIN alignment: %w", err)
	}

	lineStarts := viewQueryLineStarts(query)
	lines := strings.Split(query, "\n")
	depths := viewTokenParenDepths(tokens)

	for index, token := range tokens {
		if !strings.EqualFold(token.Literal, "JOIN") {
			continue
		}

		line := viewQueryLineAt(lineStarts, token.Start)
		if line >= len(lines) || !viewJoinStartsLine(lines[line], token.Start-lineStarts[line]) {
			continue
		}

		selectIndex := previousViewSelectIndex(tokens, depths, index)
		if selectIndex < 0 {
			continue
		}

		selectLine := viewQueryLineAt(lineStarts, tokens[selectIndex].Start)
		lines[line] = leadingViewWhitespace(lines[selectLine]) +
			strings.TrimLeft(lines[line], " \t")
	}

	return strings.Join(lines, "\n"), nil
}

func splitInlineViewJoinQualifiers(query string) (string, error) {
	tokens, err := parser.NewLexer(query).Tokenize()
	if err != nil {
		return "", fmt.Errorf("tokenize inline JOIN qualifiers: %w", err)
	}

	lineStarts := viewQueryLineStarts(query)
	lines := strings.Split(query, "\n")
	depths := viewTokenParenDepths(tokens)
	layouts := make(map[int]viewInlineJoinLayout)

	for index, token := range tokens {
		if !strings.EqualFold(token.Literal, "ON") {
			continue
		}

		line := viewQueryLineAt(lineStarts, token.Start)
		if line >= len(lines) ||
			strings.TrimSpace(lines[line][:token.Start-lineStarts[line]]) == "" {
			continue
		}

		joinIndex := previousViewJoinIndex(tokens, depths, index)
		if joinIndex < 0 {
			continue
		}

		joinLine := viewQueryLineAt(lineStarts, tokens[joinIndex].Start)

		_, hasBooleanConnector := viewJoinConditionEnd(tokens, index)
		if joinLine == line && !hasBooleanConnector {
			continue
		}

		layouts[line] = viewInlineJoinLayout{
			onColumn:        token.Start - lineStarts[line],
			onEndColumn:     token.End - lineStarts[line],
			qualifierIndent: leadingViewWhitespace(lines[joinLine]) + viewLayoutIndent,
		}
	}

	if len(layouts) == 0 {
		return query, nil
	}

	result := make([]string, 0, len(lines)+len(layouts))

	for lineIndex, line := range lines {
		layout, ok := layouts[lineIndex]
		if !ok {
			result = append(result, line)

			continue
		}

		result = append(result, strings.TrimRight(line[:layout.onColumn], " \t"))

		qualifier := layout.qualifierIndent + "ON"
		if condition := strings.TrimSpace(line[layout.onEndColumn:]); condition != "" {
			qualifier += " " + condition
		}

		result = append(result, qualifier)
	}

	return strings.Join(result, "\n"), nil
}

func viewTokenParenDepths(tokens []parser.Token) []int {
	depths := make([]int, len(tokens))
	depth := 0

	for index, token := range tokens {
		if token.Type == parser.TokenRParen && depth > 0 {
			depth--
		}

		depths[index] = depth

		if token.Type == parser.TokenLParen {
			depth++
		}
	}

	return depths
}

func previousViewSelectIndex(tokens []parser.Token, depths []int, index int) int {
	for candidate := index - 1; candidate >= 0; candidate-- {
		if depths[candidate] == depths[index] &&
			strings.EqualFold(tokens[candidate].Literal, "SELECT") {
			return candidate
		}
	}

	return -1
}

func previousViewJoinIndex(tokens []parser.Token, depths []int, index int) int {
	for candidate := index - 1; candidate >= 0; candidate-- {
		if depths[candidate] != depths[index] {
			continue
		}

		upper := strings.ToUpper(tokens[candidate].Literal)
		if upper == "JOIN" {
			return candidate
		}

		switch upper {
		case "ON", "WHERE", "SELECT", "FROM", "GROUP", "HAVING", "WINDOW",
			"ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT":
			return -1
		}
	}

	return -1
}

func viewJoinStartsLine(line string, joinColumn int) bool {
	if joinColumn < 0 || joinColumn > len(line) {
		return false
	}

	for _, field := range strings.Fields(strings.ToUpper(line[:joinColumn])) {
		switch field {
		case "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "NATURAL":
		default:
			return false
		}
	}

	return true
}

func leadingViewWhitespace(line string) string {
	return line[:len(line)-len(strings.TrimLeft(line, " \t"))]
}

func applyViewTextReplacements(query string, replacements []viewTextReplacement) string {
	if len(replacements) == 0 {
		return query
	}

	var output strings.Builder

	position := 0
	for _, replacement := range replacements {
		output.WriteString(query[position:replacement.start])
		output.WriteString(replacement.replacement)
		position = replacement.end
	}

	output.WriteString(query[position:])

	return output.String()
}

func formatViewArrayConstructors(query string) (string, error) {
	tokens, err := parser.NewLexer(query).Tokenize()
	if err != nil {
		return "", fmt.Errorf("tokenize ARRAY layout: %w", err)
	}

	lineStarts := viewQueryLineStarts(query)

	layouts := collectViewArrayLayouts(query, tokens, lineStarts)
	if len(layouts) == 0 {
		return query, nil
	}

	var output strings.Builder

	position := 0
	for _, layout := range layouts {
		if layout.start < position {
			continue
		}

		output.WriteString(query[position:layout.start])
		output.WriteString(layout.replacement)
		position = layout.end
	}

	output.WriteString(query[position:])

	return output.String(), nil
}

func collectViewArrayLayouts(
	query string,
	tokens []parser.Token,
	lineStarts []int,
) []viewTextReplacement {
	layouts := make([]viewTextReplacement, 0)

	for index, token := range tokens {
		if !strings.EqualFold(token.Literal, "ARRAY") || index+1 >= len(tokens) ||
			tokens[index+1].Type != parser.TokenLBracket {
			continue
		}

		openIndex := index + 1

		closeIndex := matchingViewArrayBracket(tokens, openIndex)
		if closeIndex < 0 || viewQueryLineAt(lineStarts, tokens[openIndex].Start) ==
			viewQueryLineAt(lineStarts, tokens[closeIndex].Start) {
			continue
		}

		elements, ok := simpleViewArrayElements(query, tokens, openIndex, closeIndex)
		if !ok {
			continue
		}

		baseIndent := viewArrayBaseIndent(query, token.Start, lineStarts)
		itemIndent := baseIndent + viewLayoutIndent

		var replacement strings.Builder

		replacement.WriteString("ARRAY [\n")

		for elementIndex, element := range elements {
			replacement.WriteString(itemIndent)
			replacement.WriteString(element)

			if elementIndex < len(elements)-1 {
				replacement.WriteByte(',')
			}

			replacement.WriteByte('\n')
		}

		replacement.WriteString(baseIndent)
		replacement.WriteByte(']')

		layouts = append(layouts, viewTextReplacement{
			start:       token.Start,
			end:         tokens[closeIndex].End,
			replacement: replacement.String(),
		})
	}

	return layouts
}

func matchingViewArrayBracket(tokens []parser.Token, openIndex int) int {
	depth := 0

	for index := openIndex; index < len(tokens); index++ {
		switch tokens[index].Type {
		case parser.TokenLBracket:
			depth++
		case parser.TokenRBracket:
			depth--
			if depth == 0 {
				return index
			}
		}
	}

	return -1
}

func simpleViewArrayElements(
	query string,
	tokens []parser.Token,
	openIndex int,
	closeIndex int,
) ([]string, bool) {
	var parenDepth, bracketDepth int

	elementStart := tokens[openIndex].End
	elements := make([]string, 0)

	for index := openIndex + 1; index < closeIndex; index++ {
		token := tokens[index]

		if token.Type == parser.TokenComma && parenDepth == 0 && bracketDepth == 0 {
			element, ok := simpleViewArrayElement(query[elementStart:token.Start])
			if !ok {
				return nil, false
			}

			elements = append(elements, element)
			elementStart = token.End

			continue
		}

		switch token.Type {
		case parser.TokenLParen:
			parenDepth++
		case parser.TokenRParen:
			parenDepth--
		case parser.TokenLBracket:
			bracketDepth++
		case parser.TokenRBracket:
			bracketDepth--
		}

		if parenDepth < 0 || bracketDepth < 0 {
			return nil, false
		}
	}

	if parenDepth != 0 || bracketDepth != 0 {
		return nil, false
	}

	element, ok := simpleViewArrayElement(query[elementStart:tokens[closeIndex].Start])
	if !ok {
		return nil, false
	}

	elements = append(elements, element)

	return elements, len(elements) > 1
}

func simpleViewArrayElement(element string) (string, bool) {
	element = strings.TrimSpace(element)

	return element, element != "" && !strings.ContainsAny(element, "\r\n")
}

func viewArrayBaseIndent(query string, offset int, lineStarts []int) string {
	line := viewQueryLineAt(lineStarts, offset)
	prefix := query[lineStarts[line]:offset]

	if strings.TrimSpace(prefix) == "" {
		return prefix
	}

	return strings.Repeat(" ", utf8.RuneCountInString(prefix))
}

func compactViewSources(query string) (string, error) {
	tokens, err := parser.NewLexer(query).Tokenize()
	if err != nil {
		return "", fmt.Errorf("tokenize source layout: %w", err)
	}

	lineStarts := viewQueryLineStarts(query)
	lines := strings.Split(query, "\n")

	layouts := collectViewSourceLayouts(tokens, lineStarts, lines)
	if len(layouts) == 0 {
		return query, nil
	}

	replacements := make(map[int]string, len(layouts))
	removeLines := make(map[int]bool, len(layouts)*2)

	for _, layout := range layouts {
		replacements[layout.keywordLine] = layout.compactLine
		removeLines[layout.relationLine] = true

		if layout.qualifierLine >= 0 {
			removeLines[layout.qualifierLine] = true
		}
	}

	result := make([]string, 0, len(lines)-len(removeLines))
	for lineIndex, line := range lines {
		if removeLines[lineIndex] {
			continue
		}

		if replacement, ok := replacements[lineIndex]; ok {
			line = replacement
		}

		result = append(result, line)
	}

	return strings.Join(result, "\n"), nil
}

func collectViewSourceLayouts(
	tokens []parser.Token,
	lineStarts []int,
	lines []string,
) []viewSourceLayout {
	layouts := make([]viewSourceLayout, 0)

	for tokenIndex, token := range tokens {
		keywordLine := viewQueryLineAt(lineStarts, token.Start)
		if keywordLine >= len(lines) ||
			!isCompactableViewSourceKeyword(token, lines[keywordLine]) {
			continue
		}

		relationIndex := nextViewLayoutToken(tokens, tokenIndex+1)
		if relationIndex >= len(tokens) || tokens[relationIndex].Type == parser.TokenEOF {
			continue
		}

		relationLine := viewQueryLineAt(lineStarts, tokens[relationIndex].Start)
		if relationLine != keywordLine+1 || relationLine >= len(lines) {
			continue
		}

		relationEnd, ok := compactableViewRelationLine(
			tokens,
			relationIndex,
			relationLine,
			lineStarts,
		)
		if !ok {
			continue
		}

		compactLine := strings.TrimRight(lines[keywordLine], " \t") +
			" " + strings.TrimSpace(lines[relationLine])
		if !viewLineFitsCompactLimit(compactLine) {
			continue
		}

		layout := viewSourceLayout{
			keywordLine:   keywordLine,
			relationLine:  relationLine,
			qualifierLine: -1,
			compactLine:   compactLine,
		}

		if strings.EqualFold(token.Literal, "JOIN") {
			qualifierIndex := nextViewLayoutToken(tokens, relationEnd+1)

			qualifier, qualifierLine, ok := compactableViewJoinQualifier(
				tokens,
				qualifierIndex,
				relationLine,
				lineStarts,
				lines,
			)
			if ok && viewLineFitsCompactLimit(compactLine+" "+qualifier) {
				layout.compactLine += " " + qualifier
				layout.qualifierLine = qualifierLine
			}
		}

		layouts = append(layouts, layout)
	}

	return layouts
}

func isCompactableViewSourceKeyword(token parser.Token, line string) bool {
	trimmed := strings.TrimSpace(line)
	if strings.EqualFold(token.Literal, "FROM") {
		return strings.EqualFold(trimmed, "FROM")
	}

	if !strings.EqualFold(token.Literal, "JOIN") {
		return false
	}

	fields := strings.Fields(strings.ToUpper(trimmed))
	if len(fields) == 0 || fields[len(fields)-1] != "JOIN" {
		return false
	}

	for _, field := range fields[:len(fields)-1] {
		switch field {
		case "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "NATURAL":
		default:
			return false
		}
	}

	return true
}

func compactableViewRelationLine(
	tokens []parser.Token,
	startIndex int,
	relationLine int,
	lineStarts []int,
) (int, bool) {
	var (
		parenDepth   int
		bracketDepth int
		lastIndex    = -1
	)

	for index := startIndex; index < len(tokens); index++ {
		token := tokens[index]
		if token.Type == parser.TokenEOF ||
			viewQueryLineAt(lineStarts, token.Start) != relationLine {
			break
		}

		if token.Type == parser.TokenComment || token.Type == parser.TokenSemicolon {
			return 0, false
		}

		if index == startIndex && token.Type == parser.TokenLParen {
			return 0, false
		}

		if (strings.EqualFold(token.Literal, "SELECT") ||
			strings.EqualFold(token.Literal, "WITH")) && index != startIndex {
			return 0, false
		}

		if token.Type == parser.TokenComma && parenDepth == 0 && bracketDepth == 0 {
			return 0, false
		}

		switch token.Type {
		case parser.TokenLParen:
			parenDepth++
		case parser.TokenRParen:
			parenDepth--
		case parser.TokenLBracket:
			bracketDepth++
		case parser.TokenRBracket:
			bracketDepth--
		}

		if parenDepth < 0 || bracketDepth < 0 {
			return 0, false
		}

		lastIndex = index
	}

	return lastIndex, lastIndex >= startIndex && parenDepth == 0 && bracketDepth == 0
}

func compactableViewJoinQualifier(
	tokens []parser.Token,
	qualifierIndex int,
	relationLine int,
	lineStarts []int,
	lines []string,
) (string, int, bool) {
	if qualifierIndex >= len(tokens) || tokens[qualifierIndex].Type == parser.TokenEOF {
		return "", 0, false
	}

	qualifierLine := viewQueryLineAt(lineStarts, tokens[qualifierIndex].Start)
	if qualifierLine != relationLine+1 || qualifierLine >= len(lines) {
		return "", 0, false
	}

	qualifierColumn := tokens[qualifierIndex].Start - lineStarts[qualifierLine]
	qualifierPrefix := lines[qualifierLine][:qualifierColumn]

	if strings.TrimSpace(qualifierPrefix) != "" {
		return "", 0, false
	}

	qualifier := strings.TrimSpace(lines[qualifierLine])
	if strings.EqualFold(tokens[qualifierIndex].Literal, "ON") {
		if strings.EqualFold(qualifier, "ON") {
			return "", 0, false
		}

		endIndex, hasBooleanConnector := viewJoinConditionEnd(tokens, qualifierIndex)
		if hasBooleanConnector || endIndex <= qualifierIndex ||
			viewQueryLineAt(lineStarts, tokens[endIndex].Start) != qualifierLine {
			return "", 0, false
		}

		return qualifier, qualifierLine, true
	}

	if strings.EqualFold(tokens[qualifierIndex].Literal, "USING") &&
		viewLineTokensBalanced(tokens, qualifierIndex, qualifierLine, lineStarts) {
		return qualifier, qualifierLine, true
	}

	return "", 0, false
}

func viewLineTokensBalanced(
	tokens []parser.Token,
	startIndex int,
	line int,
	lineStarts []int,
) bool {
	var parenDepth, bracketDepth int

	for index := startIndex; index < len(tokens); index++ {
		token := tokens[index]
		if token.Type == parser.TokenEOF || viewQueryLineAt(lineStarts, token.Start) != line {
			break
		}

		if token.Type == parser.TokenComment || token.Type == parser.TokenSemicolon {
			return false
		}

		switch token.Type {
		case parser.TokenLParen:
			parenDepth++
		case parser.TokenRParen:
			parenDepth--
		case parser.TokenLBracket:
			bracketDepth++
		case parser.TokenRBracket:
			bracketDepth--
		}

		if parenDepth < 0 || bracketDepth < 0 {
			return false
		}
	}

	return parenDepth == 0 && bracketDepth == 0
}

func viewLineFitsCompactLimit(line string) bool {
	return utf8.RuneCountInString(line) <= viewCompactLineLength
}

func configureViewFormatterLayout(config *dialects.TokenizerConfig) {
	config.ReservedWords = append(
		append([]string(nil), config.ReservedWords...),
		"IS NOT DISTINCT FROM",
		"IS DISTINCT FROM",
	)

	withoutCTEs := make([]string, 0, len(config.ReservedTopLevelWords))

	for _, keyword := range config.ReservedTopLevelWords {
		if strings.EqualFold(keyword, "WITH") || strings.EqualFold(keyword, "WITH RECURSIVE") {
			continue
		}

		withoutCTEs = append(withoutCTEs, keyword)
	}

	config.ReservedTopLevelWords = withoutCTEs
	config.ReservedTopLevelWordsNoIndent = append(
		append([]string(nil), config.ReservedTopLevelWordsNoIndent...),
		"WITH RECURSIVE",
		"WITH",
	)
}

func formatViewJoinConditions(query string) (string, error) {
	tokens, err := parser.NewLexer(query).Tokenize()
	if err != nil {
		return "", fmt.Errorf("tokenize JOIN layout: %w", err)
	}

	lineStarts := viewQueryLineStarts(query)
	lines := strings.Split(query, "\n")

	layouts := collectViewJoinLayouts(tokens, lineStarts, lines)
	if len(layouts) == 0 {
		return query, nil
	}

	indentAdds := make([]int, len(lines))
	layoutByLine := make(map[int]viewJoinLayout, len(layouts))

	for _, layout := range layouts {
		layoutByLine[layout.line] = layout
		for line := layout.line + 1; line <= layout.endLine && line < len(lines); line++ {
			indentAdds[line]++
		}
	}

	var output strings.Builder

	for lineIndex, line := range lines {
		if lineIndex > 0 {
			output.WriteByte('\n')
		}

		extraIndent := strings.Repeat(viewLayoutIndent, indentAdds[lineIndex])
		layout, ok := layoutByLine[lineIndex]

		if !ok {
			output.WriteString(extraIndent)
			output.WriteString(line)

			continue
		}

		condition := strings.TrimSpace(line[layout.onEndColumn:])
		onLine := strings.TrimRight(line[:layout.onEndColumn], " ")
		baseIndent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]

		output.WriteString(extraIndent)
		output.WriteString(onLine)
		output.WriteByte('\n')
		output.WriteString(extraIndent)
		output.WriteString(baseIndent)
		output.WriteString(viewLayoutIndent)
		output.WriteString(condition)
	}

	return output.String(), nil
}

func collectViewJoinLayouts(
	tokens []parser.Token,
	lineStarts []int,
	lines []string,
) []viewJoinLayout {
	layouts := make([]viewJoinLayout, 0)

	for i, token := range tokens {
		if !strings.EqualFold(token.Literal, "ON") {
			continue
		}

		line := viewQueryLineAt(lineStarts, token.Start)
		if line >= len(lines) ||
			strings.TrimSpace(lines[line][:token.Start-lineStarts[line]]) != "" {
			continue
		}

		nextIndex := nextViewLayoutToken(tokens, i+1)
		if nextIndex < len(tokens) && strings.EqualFold(tokens[nextIndex].Literal, "CONFLICT") {
			continue
		}

		onEndColumn := token.End - lineStarts[line]
		if onEndColumn >= len(lines[line]) || strings.TrimSpace(lines[line][onEndColumn:]) == "" {
			continue
		}

		endIndex, hasBooleanConnector := viewJoinConditionEnd(tokens, i)
		if !hasBooleanConnector || endIndex <= i {
			continue
		}

		layouts = append(layouts, viewJoinLayout{
			line:        line,
			endLine:     viewQueryLineAt(lineStarts, tokens[endIndex].Start),
			onEndColumn: onEndColumn,
		})
	}

	return layouts
}

func viewJoinConditionEnd(tokens []parser.Token, onIndex int) (int, bool) {
	var (
		state        viewJoinConditionState
		lastIndex    = onIndex
		hasConnector bool
	)

	for i := nextViewLayoutToken(tokens, onIndex+1); i < len(tokens); i++ {
		token := tokens[i]
		upper := strings.ToUpper(token.Literal)

		if token.Type == parser.TokenComment {
			continue
		}

		if state.atTopLevel() && isViewJoinConditionBoundary(tokens, i, upper) {
			break
		}

		if state.consume(token, upper) {
			hasConnector = true
		}

		lastIndex = i
	}

	return lastIndex, hasConnector
}

func (state *viewJoinConditionState) atTopLevel() bool {
	return state.parenDepth == 0 && state.bracketDepth == 0 && state.caseDepth == 0
}

func (state *viewJoinConditionState) consume(token parser.Token, upper string) bool {
	state.consumeDelimiter(token)

	if state.parenDepth != 0 || state.bracketDepth != 0 {
		return false
	}

	switch upper {
	case "CASE":
		state.caseDepth++
	case "END":
		if state.caseDepth > 0 {
			state.caseDepth--
		}
	case "AND":
		if state.caseDepth != 0 {
			return false
		}

		return true
	case "OR":
		return state.caseDepth == 0
	}

	return false
}

func (state *viewJoinConditionState) consumeDelimiter(token parser.Token) {
	switch token.Type {
	case parser.TokenLParen:
		state.parenDepth++
	case parser.TokenRParen:
		if state.parenDepth > 0 {
			state.parenDepth--
		}
	case parser.TokenLBracket:
		state.bracketDepth++
	case parser.TokenRBracket:
		if state.bracketDepth > 0 {
			state.bracketDepth--
		}
	}
}

func isViewJoinConditionBoundary(tokens []parser.Token, index int, upper string) bool {
	token := tokens[index]
	if token.Type == parser.TokenEOF || token.Type == parser.TokenSemicolon ||
		token.Type == parser.TokenComma || token.Type == parser.TokenRParen {
		return true
	}

	if (upper == "LEFT" || upper == "RIGHT") &&
		nextViewTokenType(tokens, index) == parser.TokenLParen {
		return false
	}

	switch upper {
	case "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL", "JOIN",
		"WHERE", "GROUP", "HAVING", "WINDOW", "ORDER", "LIMIT", "OFFSET",
		"FETCH", "FOR", "UNION", "INTERSECT", "EXCEPT":
		return true
	default:
		return false
	}
}

func formatViewSingleTargets(query string) (string, error) {
	tokens, err := parser.NewLexer(query).Tokenize()
	if err != nil {
		return "", fmt.Errorf("tokenize SELECT layout: %w", err)
	}

	lineStarts := viewQueryLineStarts(query)
	lines := strings.Split(query, "\n")
	removeLines := make(map[int]bool)
	mergeTargets := make(map[int]string)

	for i, token := range tokens {
		if !strings.EqualFold(token.Literal, "SELECT") {
			continue
		}

		selectLine := viewQueryLineAt(lineStarts, token.Start)
		if selectLine >= len(lines) ||
			!strings.EqualFold(strings.TrimSpace(lines[selectLine]), "SELECT") {
			continue
		}

		firstIndex, lastIndex, single := viewSelectTargetRange(tokens, i)
		if !single {
			continue
		}

		firstLine := viewQueryLineAt(lineStarts, tokens[firstIndex].Start)
		lastLine := viewQueryLineAt(lineStarts, tokens[lastIndex].Start)

		if firstLine != lastLine || firstLine != selectLine+1 || firstLine >= len(lines) {
			continue
		}

		target := strings.TrimSpace(lines[firstLine])
		if target == "" || len(lines[selectLine])+1+len(target) > generatedSQLLineLength {
			continue
		}

		mergeTargets[selectLine] = target
		removeLines[firstLine] = true
	}

	if len(mergeTargets) == 0 {
		return query, nil
	}

	result := make([]string, 0, len(lines)-len(removeLines))
	for i, line := range lines {
		if removeLines[i] {
			continue
		}

		if target, ok := mergeTargets[i]; ok {
			line += " " + target
		}

		result = append(result, line)
	}

	return strings.Join(result, "\n"), nil
}

func viewSelectTargetRange(tokens []parser.Token, selectIndex int) (int, int, bool) {
	firstIndex := nextViewLayoutToken(tokens, selectIndex+1)
	if firstIndex >= len(tokens) {
		return 0, 0, false
	}

	var (
		parenDepth   int
		bracketDepth int
		lastIndex    = -1
	)

	for i := firstIndex; i < len(tokens); i++ {
		token := tokens[i]
		if token.Type == parser.TokenComment {
			continue
		}

		if parenDepth == 0 && bracketDepth == 0 {
			if token.Type == parser.TokenComma {
				return 0, 0, false
			}

			if isViewSelectTargetBoundary(token) {
				break
			}
		}

		switch token.Type {
		case parser.TokenLParen:
			parenDepth++
		case parser.TokenRParen:
			if parenDepth == 0 {
				return firstIndex, lastIndex, lastIndex >= firstIndex
			}

			parenDepth--
		case parser.TokenLBracket:
			bracketDepth++
		case parser.TokenRBracket:
			if bracketDepth > 0 {
				bracketDepth--
			}
		}

		lastIndex = i
	}

	return firstIndex, lastIndex, lastIndex >= firstIndex
}

func isViewSelectTargetBoundary(token parser.Token) bool {
	if token.Type == parser.TokenEOF || token.Type == parser.TokenSemicolon ||
		token.Type == parser.TokenRParen {
		return true
	}

	switch strings.ToUpper(token.Literal) {
	case "FROM", "INTO", "WHERE", "GROUP", "HAVING", "WINDOW", "ORDER",
		"LIMIT", "OFFSET", "FETCH", "FOR", "UNION", "INTERSECT", "EXCEPT":
		return true
	default:
		return false
	}
}

func nextViewLayoutToken(tokens []parser.Token, position int) int {
	for position < len(tokens) && tokens[position].Type == parser.TokenComment {
		position++
	}

	return position
}
