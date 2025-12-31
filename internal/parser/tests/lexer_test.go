package parser_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/parser"
)

func TestLexerSimpleTokens(t *testing.T) {
	t.Parallel()

	input := "CREATE TABLE public.users (id SERIAL PRIMARY KEY, name TEXT);"

	tokens, err := parser.NewLexer(input).Tokenize()
	require.NoError(t, err)

	require.Greater(t, len(tokens), 5)
	require.Equal(t, parser.TokenKeyword, tokens[0].Type)
	require.Equal(t, "CREATE", strings.ToUpper(tokens[0].Literal))
	require.Equal(t, parser.TokenKeyword, tokens[1].Type)
	require.Equal(t, "TABLE", strings.ToUpper(tokens[1].Literal))
	require.Equal(t, parser.TokenIdentifier, tokens[2].Type)
	require.Equal(t, "public", strings.ToLower(tokens[2].Literal))
	require.Equal(t, parser.TokenDot, tokens[3].Type)
	require.Equal(t, parser.TokenIdentifier, tokens[4].Type)
	require.Equal(t, "users", strings.ToLower(tokens[4].Literal))
	require.Equal(t, parser.TokenSemicolon, tokens[len(tokens)-2].Type)
	require.Equal(t, parser.TokenEOF, tokens[len(tokens)-1].Type)
}

func TestLexerStringLiteral(t *testing.T) {
	t.Parallel()

	input := "INSERT INTO logs VALUES ('O''Reilly', 42);"

	tokens, err := parser.NewLexer(input).Tokenize()
	require.NoError(t, err)

	found := false

	for _, tok := range tokens {
		if tok.Type == parser.TokenString && tok.Literal == "'O''Reilly'" {
			found = true
			break
		}
	}

	require.True(t, found, "expected string literal token not found")
}

func TestLexerDollarQuotedString(t *testing.T) {
	t.Parallel()

	input := "SELECT $$multi\nline$$, $tag$nested$tag$;"

	tokens, err := parser.NewLexer(input).Tokenize()
	require.NoError(t, err)

	literals := make(map[string]bool)

	for _, tok := range tokens {
		if tok.Type == parser.TokenString {
			literals[tok.Literal] = true
		}
	}

	require.Contains(t, literals, "$$multi\nline$$")
	require.Contains(t, literals, "$tag$nested$tag$")
}

func TestLexerComments(t *testing.T) {
	t.Parallel()

	input := "-- comment\nSELECT 1; /* block */"

	tokens, err := parser.NewLexer(input).Tokenize()
	require.NoError(t, err)

	require.Equal(t, parser.TokenComment, tokens[0].Type)
	require.Equal(t, parser.TokenKeyword, tokens[1].Type)
	require.Equal(t, parser.TokenNumber, tokens[2].Type)
	require.Equal(t, parser.TokenSemicolon, tokens[3].Type)
	require.Equal(t, parser.TokenComment, tokens[4].Type)
	require.Equal(t, parser.TokenEOF, tokens[5].Type)
}

func TestLexerErrors(t *testing.T) {
	t.Parallel()

	_, err := parser.NewLexer("SELECT 'unterminated").Tokenize()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unterminated string literal")

	_, err = parser.NewLexer("SELECT $$unterminated").Tokenize()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unterminated dollar-quoted string")

	_, err = parser.NewLexer("/* unterminated").Tokenize()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unterminated comment")
}
