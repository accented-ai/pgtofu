package parser

import "strings"

func splitStatements(sql string) ([]Statement, error) {
	tokens, err := NewLexer(sql).Tokenize()
	if err != nil {
		return nil, err
	}

	return splitTokensIntoStatements(sql, tokens), nil
}

func splitTokensIntoStatements(sql string, tokens []Token) []Statement {
	var ( //nolint:prealloc
		statements    []Statement
		currentTokens []Token
		start         int
	)

	emit := func(end int, includeSemicolon bool) {
		segment := strings.TrimSpace(sql[start:end])
		if segment == "" || isCommentOnly(segment) {
			currentTokens = currentTokens[:0]
			start = end

			return
		}

		tokenCutoff := len(currentTokens)
		if includeSemicolon && tokenCutoff > 0 {
			tokenCutoff--
		}

		stmtTokens := cloneTokens(currentTokens[:tokenCutoff])
		statements = append(statements, Statement{
			Type:   determineStatementType(stmtTokens, segment),
			SQL:    segment,
			Tokens: stmtTokens,
			Line:   statementLine(stmtTokens),
		})

		currentTokens = currentTokens[:0]
		start = end
	}

	for _, token := range tokens {
		if token.Type == TokenEOF {
			break
		}

		currentTokens = append(currentTokens, token)

		if token.Type == TokenSemicolon {
			emit(token.Start, true)
			start = token.End
		}
	}

	if trimmed := strings.TrimSpace(sql[start:]); trimmed != "" && !isCommentOnly(trimmed) {
		emit(len(sql), false)
	}

	return statements
}

func cloneTokens(tokens []Token) []Token {
	if len(tokens) == 0 {
		return nil
	}

	result := make([]Token, len(tokens))
	copy(result, tokens)

	return result
}

func statementLine(tokens []Token) int {
	for _, token := range tokens {
		if token.Type == TokenComment {
			continue
		}

		if token.Line > 0 {
			return token.Line
		}
	}

	return 0
}

func isCommentOnly(stmt string) bool {
	for line := range strings.SplitSeq(stmt, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "/*") {
			continue
		}

		return false
	}

	return true
}

func stripLeadingComments(stmt string) string {
	lines := strings.Split(stmt, "\n")

	var result []string //nolint:prealloc

	foundSQL := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !foundSQL && (trimmed == "" || strings.HasPrefix(trimmed, "--")) {
			continue
		}

		foundSQL = true

		result = append(result, line)
	}

	return strings.Join(result, "\n")
}
