package generator

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/parser"
)

func compactViewValuesSources(query string) (string, error) {
	tokens, err := parser.NewLexer(query).Tokenize()
	if err != nil {
		return "", fmt.Errorf("tokenize VALUES source layout: %w", err)
	}

	lineStarts := viewQueryLineStarts(query)
	lines := strings.Split(query, "\n")
	depths := viewTokenParenDepths(tokens)
	replacements := make([]viewTextReplacement, 0)

	for index, token := range tokens {
		if len(replacements) > 0 && token.Start < replacements[len(replacements)-1].end {
			continue
		}

		line := viewQueryLineAt(lineStarts, token.Start)
		if !isCompactableViewSourceKeyword(token, lines[line]) {
			continue
		}

		endIndex := viewValuesSourceEnd(tokens, depths, index+1, lineStarts)
		if endIndex < 0 {
			continue
		}

		compact, ok := compactViewValuesText(query, tokens[index+1:endIndex+1])
		if !ok || !viewLineFitsCompactLimit(lines[line]+" "+compact) {
			continue
		}

		replacements = append(replacements, viewTextReplacement{
			start:       token.End,
			end:         tokens[endIndex].End,
			replacement: " " + compact,
		})
	}

	return applyViewTextReplacements(query, replacements), nil
}

func viewValuesSourceEnd(tokens []parser.Token, depths []int, start int, lineStarts []int) int {
	if start+1 >= len(tokens) || tokens[start].Type != parser.TokenLParen ||
		!strings.EqualFold(tokens[start+1].Literal, "VALUES") {
		return -1
	}

	for index := start + 2; index < len(tokens); index++ {
		if tokens[index].Type != parser.TokenRParen || depths[index] != depths[start] {
			continue
		}

		// Include the relation alias and column aliases on the closing line.
		line := viewQueryLineAt(lineStarts, tokens[index].Start)
		for index+1 < len(tokens) && tokens[index+1].Type != parser.TokenEOF &&
			viewQueryLineAt(lineStarts, tokens[index+1].Start) == line {
			index++
		}

		return index
	}

	return -1
}

func compactViewValuesText(query string, tokens []parser.Token) (string, bool) {
	var compact strings.Builder

	for index, token := range tokens {
		text := query[token.Start:token.End]
		if token.Type == parser.TokenComment || strings.ContainsAny(text, "\r\n") {
			return "", false
		}

		if index > 0 {
			previous := tokens[index-1]

			gap := query[previous.End:token.Start]
			if strings.ContainsAny(gap, "\r\n") {
				gap = " "
				if previous.Type == parser.TokenLParen || previous.Type == parser.TokenLBracket ||
					token.Type == parser.TokenRParen || token.Type == parser.TokenRBracket ||
					token.Type == parser.TokenComma {
					gap = ""
				}
			}

			compact.WriteString(gap)
		}

		compact.WriteString(text)
	}

	return compact.String(), true
}
