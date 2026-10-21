package differ

import (
	"strings"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/postgresparse"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func columnsHaveSameDefault(current, desired *schema.Column) bool {
	if current.Default == "" || desired.Default == "" {
		return current.Default == desired.Default
	}

	currentBound, desiredBound := current.BoundDefault(), desired.BoundDefault()
	if currentBound != "" && desiredBound != "" {
		return boundDefaultsEqual(currentBound, desiredBound)
	}

	if currentBound != "" && defaultHasQualifiedName(desired.Default) {
		return boundDefaultsEqual(currentBound, desired.Default)
	}

	if AreDefaultsEqual(current.Default, desired.Default) {
		return true
	}

	return currentBound != "" && boundDefaultsEqual(currentBound, desired.Default)
}

func defaultHasQualifiedName(expression string) bool {
	tokens, err := parser.NewLexer(expression).Tokenize()
	if err != nil {
		return true
	}

	previous := parser.TokenIllegal

	for _, token := range tokens {
		if token.Type == parser.TokenComment {
			continue
		}

		if token.Type == parser.TokenDot && (previous == parser.TokenIdentifier ||
			previous == parser.TokenQuotedIdentifier || previous == parser.TokenKeyword) {
			return true
		}

		previous = token.Type
	}

	return false
}

func boundDefaultsEqual(current, desired string) bool {
	left, leftOK := canonicalBoundDefault(current)

	right, rightOK := canonicalBoundDefault(desired)
	if !leftOK || !rightOK {
		return strings.TrimSpace(current) == strings.TrimSpace(desired)
	}

	return left == right
}

func canonicalBoundDefault(expression string) (string, bool) {
	tree, err := postgresparse.Parse("SELECT " + expression)
	if err != nil || len(tree.GetStmts()) != 1 {
		return "", false
	}

	sql, err := postgresparse.Deparse(tree)

	return sql, err == nil
}
