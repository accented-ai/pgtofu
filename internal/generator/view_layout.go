package generator

import (
	"fmt"
	"strings"

	"github.com/MeKo-Christian/go-sqlfmt/pkg/sqlfmt/dialects"

	"github.com/accented-ai/pgtofu/internal/parser"
)

const viewLayoutIndent = "    "

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

func formatViewQueryLayout(query string) (string, error) {
	formatted, err := formatViewJoinConditions(query)
	if err != nil {
		return "", err
	}

	formatted, err = formatViewSingleTargets(formatted)
	if err != nil {
		return "", err
	}

	return formatted, nil
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
