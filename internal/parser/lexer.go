package parser

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type TokenType int

const (
	TokenIllegal TokenType = iota
	TokenEOF
	TokenKeyword
	TokenIdentifier
	TokenQuotedIdentifier
	TokenString
	TokenNumber
	TokenOperator
	TokenLParen
	TokenRParen
	TokenComma
	TokenSemicolon
	TokenDot
	TokenColon
	TokenComment
	TokenLBracket
	TokenRBracket
)

type Token struct {
	Type    TokenType
	Literal string
	Start   int
	End     int
	Line    int
	Column  int
}

type Lexer struct {
	input  string
	pos    int
	line   int
	column int
}

var (
	errInvalidDollarTag        = errors.New("invalid dollar-quote tag")
	errUnterminatedDollarQuote = errors.New("unterminated dollar-quoted string")
	errUnterminatedString      = errors.New("unterminated string literal")
	errUnterminatedComment     = errors.New("unterminated comment")
)

var keywordSet = map[string]struct{}{ //nolint:gochecknoglobals
	"ADD":          {},
	"ALTER":        {},
	"ALL":          {},
	"AND":          {},
	"ANY":          {},
	"AS":           {},
	"BETWEEN":      {},
	"BY":           {},
	"CASE":         {},
	"COMMENT":      {},
	"CONTINUOUS":   {},
	"CREATE":       {},
	"CROSS":        {},
	"DEFAULT":      {},
	"DELETE":       {},
	"DISTINCT":     {},
	"DO":           {},
	"DROP":         {},
	"ELSE":         {},
	"END":          {},
	"EXCEPT":       {},
	"EXISTS":       {},
	"EXTENSION":    {},
	"FALSE":        {},
	"FROM":         {},
	"FULL":         {},
	"FUNCTION":     {},
	"GROUP":        {},
	"HAVING":       {},
	"ILIKE":        {},
	"IN":           {},
	"INDEX":        {},
	"INNER":        {},
	"INSERT":       {},
	"INTERSECT":    {},
	"IS":           {},
	"JOIN":         {},
	"KEY":          {},
	"LEFT":         {},
	"LIKE":         {},
	"LIMIT":        {},
	"MATERIALIZED": {},
	"NATURAL":      {},
	"NOT":          {},
	"NULL":         {},
	"OFFSET":       {},
	"ON":           {},
	"OR":           {},
	"ORDER":        {},
	"OUTER":        {},
	"POLICY":       {},
	"PRIMARY":      {},
	"REPLACE":      {},
	"RETENTION":    {},
	"RIGHT":        {},
	"SCHEMA":       {},
	"SELECT":       {},
	"SEQUENCE":     {},
	"SOME":         {},
	"TABLE":        {},
	"THEN":         {},
	"TRIGGER":      {},
	"TRUE":         {},
	"TYPE":         {},
	"UNION":        {},
	"UNIQUE":       {},
	"UPDATE":       {},
	"USING":        {},
	"VIEW":         {},
	"WHEN":         {},
	"WHERE":        {},
}

func NewLexer(input string) *Lexer {
	return &Lexer{
		input:  input,
		line:   1,
		column: 1,
	}
}

func (l *Lexer) Tokenize() ([]Token, error) {
	var tokens []Token

	for {
		token, err := l.nextToken()
		if err != nil {
			return nil, err
		}

		tokens = append(tokens, token)

		if token.Type == TokenEOF {
			break
		}
	}

	return tokens, nil
}

func (l *Lexer) nextToken() (Token, error) { //nolint:cyclop,gocyclo
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{
			Type:   TokenEOF,
			Start:  l.pos,
			End:    l.pos,
			Line:   l.line,
			Column: l.column,
		}, nil
	}

	startPos := l.pos
	startLine := l.line
	startCol := l.column
	ch := l.peek()

	switch {
	case ch == '\'':
		return l.readString(startPos, startLine, startCol)
	case ch == '"':
		return l.readQuotedIdentifier(startPos, startLine, startCol)
	case ch == '$':
		token, err := l.readDollarQuotedString(startPos, startLine, startCol)
		if err != nil {
			if errors.Is(err, errInvalidDollarTag) {
				return l.makeSingleCharToken(TokenOperator, startPos, startLine, startCol, "$"), nil
			}

			return Token{}, err
		}

		return token, nil
	case ch == '-' && l.peekAheadString("--"):
		return l.readLineComment(startPos, startLine, startCol), nil
	case ch == '/' && l.peekAheadString("/*"):
		return l.readBlockComment(startPos, startLine, startCol)
	case isIdentifierStart(ch):
		return l.readIdentifier(startPos, startLine, startCol), nil
	case unicode.IsDigit(ch):
		return l.readNumber(startPos, startLine, startCol), nil
	case ch == ';':
		return l.consumeSingleCharacter(TokenSemicolon, startPos, startLine, startCol), nil
	case ch == ',':
		return l.consumeSingleCharacter(TokenComma, startPos, startLine, startCol), nil
	case ch == '(':
		return l.consumeSingleCharacter(TokenLParen, startPos, startLine, startCol), nil
	case ch == ')':
		return l.consumeSingleCharacter(TokenRParen, startPos, startLine, startCol), nil
	case ch == '[':
		return l.consumeSingleCharacter(TokenLBracket, startPos, startLine, startCol), nil
	case ch == ']':
		return l.consumeSingleCharacter(TokenRBracket, startPos, startLine, startCol), nil
	case ch == '.':
		return l.consumeSingleCharacter(TokenDot, startPos, startLine, startCol), nil
	case ch == ':':
		return l.consumeSingleCharacter(TokenColon, startPos, startLine, startCol), nil
	case ch == '<' && l.peekAheadString("<>"):
		return l.consumeMultiCharacter(TokenOperator, startPos, startLine, startCol, 2), nil
	case ch == '!' && l.peekAheadString("!="):
		return l.consumeMultiCharacter(TokenOperator, startPos, startLine, startCol, 2), nil
	case ch == '<' && l.peekAheadString("<="):
		return l.consumeMultiCharacter(TokenOperator, startPos, startLine, startCol, 2), nil
	case ch == '>' && l.peekAheadString(">="):
		return l.consumeMultiCharacter(TokenOperator, startPos, startLine, startCol, 2), nil
	case ch == '<':
		return l.consumeSingleCharacter(TokenOperator, startPos, startLine, startCol), nil
	case ch == '>':
		return l.consumeSingleCharacter(TokenOperator, startPos, startLine, startCol), nil
	case ch == '=':
		return l.consumeSingleCharacter(TokenOperator, startPos, startLine, startCol), nil
	case ch == '+':
		return l.consumeSingleCharacter(TokenOperator, startPos, startLine, startCol), nil
	case ch == '*':
		return l.consumeSingleCharacter(TokenOperator, startPos, startLine, startCol), nil
	case ch == '-':
		return l.consumeSingleCharacter(TokenOperator, startPos, startLine, startCol), nil
	case ch == '/':
		return l.consumeSingleCharacter(TokenOperator, startPos, startLine, startCol), nil
	default:
		l.advance()

		return Token{
			Type:    TokenIllegal,
			Literal: string(ch),
			Start:   startPos,
			End:     l.pos,
			Line:    startLine,
			Column:  startCol,
		}, fmt.Errorf("unexpected character %q at line %d column %d", ch, startLine, startCol)
	}
}

func (l *Lexer) readIdentifier(startPos, startLine, startCol int) Token {
	l.advance()

	for {
		ch := l.peek()
		if !isIdentifierPart(ch) {
			break
		}

		l.advance()
	}

	literal := l.input[startPos:l.pos]
	upper := strings.ToUpper(literal)

	tokenType := TokenIdentifier
	if _, ok := keywordSet[upper]; ok {
		tokenType = TokenKeyword
	}

	return Token{
		Type:    tokenType,
		Literal: literal,
		Start:   startPos,
		End:     l.pos,
		Line:    startLine,
		Column:  startCol,
	}
}

func (l *Lexer) readNumber(startPos, startLine, startCol int) Token {
	hasDot := false

	for {
		ch := l.peek()

		if unicode.IsDigit(ch) {
			l.advance()
			continue
		}

		if ch == '.' && !hasDot {
			hasDot = true

			l.advance()

			continue
		}

		break
	}

	return Token{
		Type:    TokenNumber,
		Literal: l.input[startPos:l.pos],
		Start:   startPos,
		End:     l.pos,
		Line:    startLine,
		Column:  startCol,
	}
}

func (l *Lexer) readString(startPos, startLine, startCol int) (Token, error) {
	l.advance() // consume opening quote

	for {
		ch := l.peek()
		if ch == 0 {
			return Token{}, errUnterminatedString
		}

		l.advance()

		if ch == '\'' {
			if l.peek() == '\'' {
				l.advance()
				continue
			}

			break
		}
	}

	return Token{
		Type:    TokenString,
		Literal: l.input[startPos:l.pos],
		Start:   startPos,
		End:     l.pos,
		Line:    startLine,
		Column:  startCol,
	}, nil
}

func (l *Lexer) readQuotedIdentifier(startPos, startLine, startCol int) (Token, error) {
	l.advance() // consume opening quote

	for {
		ch := l.peek()
		if ch == 0 {
			return Token{}, errUnterminatedString
		}

		l.advance()

		if ch == '"' {
			if l.peek() == '"' {
				l.advance()
				continue
			}

			break
		}
	}

	return Token{
		Type:    TokenQuotedIdentifier,
		Literal: l.input[startPos:l.pos],
		Start:   startPos,
		End:     l.pos,
		Line:    startLine,
		Column:  startCol,
	}, nil
}

func (l *Lexer) readDollarQuotedString(startPos, startLine, startCol int) (Token, error) {
	origPos, origLine, origCol := l.pos, l.line, l.column

	tag, err := l.consumeDollarTag()
	if err != nil {
		l.pos = origPos
		l.line = origLine
		l.column = origCol

		return Token{}, err
	}

	for {
		if l.pos >= len(l.input) {
			return Token{}, errUnterminatedDollarQuote
		}

		if strings.HasPrefix(l.input[l.pos:], tag) {
			l.advanceN(len(tag))

			return Token{
				Type:    TokenString,
				Literal: l.input[startPos:l.pos],
				Start:   startPos,
				End:     l.pos,
				Line:    startLine,
				Column:  startCol,
			}, nil
		}

		l.advance()
	}
}

func (l *Lexer) readLineComment(startPos, startLine, startCol int) Token {
	l.advance() // first '-'
	l.advance() // second '-'

	for {
		ch := l.peek()
		if ch == 0 {
			break
		}

		l.advance()

		if ch == '\n' {
			break
		}
	}

	return Token{
		Type:    TokenComment,
		Literal: l.input[startPos:l.pos],
		Start:   startPos,
		End:     l.pos,
		Line:    startLine,
		Column:  startCol,
	}
}

func (l *Lexer) readBlockComment(startPos, startLine, startCol int) (Token, error) {
	l.advance() // '/'
	l.advance() // '*'

	depth := 1

	for depth > 0 {
		ch := l.peek()
		if ch == 0 {
			return Token{}, errUnterminatedComment
		}

		if ch == '/' && l.peekAheadString("/*") {
			l.advance()
			l.advance()

			depth++

			continue
		}

		if ch == '*' && l.peekAheadString("*/") {
			l.advance()
			l.advance()

			depth--

			continue
		}

		l.advance()
	}

	return Token{
		Type:    TokenComment,
		Literal: l.input[startPos:l.pos],
		Start:   startPos,
		End:     l.pos,
		Line:    startLine,
		Column:  startCol,
	}, nil
}

func (l *Lexer) consumeDollarTag() (string, error) {
	if l.peek() != '$' {
		return "", errInvalidDollarTag
	}

	var builder strings.Builder

	r, _ := l.advance()
	builder.WriteRune(r)

	for {
		ch := l.peek()
		if ch == 0 {
			return "", errUnterminatedDollarQuote
		}

		if ch == '$' {
			r, _ = l.advance()
			builder.WriteRune(r)

			break
		}

		if !isDollarTagChar(ch) {
			return "", errInvalidDollarTag
		}

		r, _ = l.advance()
		builder.WriteRune(r)
	}

	return builder.String(), nil
}

func (l *Lexer) consumeSingleCharacter(
	tokenType TokenType,
	startPos, startLine, startCol int,
) Token {
	l.advance()

	return Token{
		Type:    tokenType,
		Literal: l.input[startPos:l.pos],
		Start:   startPos,
		End:     l.pos,
		Line:    startLine,
		Column:  startCol,
	}
}

func (l *Lexer) consumeMultiCharacter(
	tokenType TokenType, //nolint:unparam
	startPos, startLine, startCol, length int, //nolint:unparam
) Token {
	l.advanceN(length)

	return Token{
		Type:    tokenType,
		Literal: l.input[startPos:l.pos],
		Start:   startPos,
		End:     l.pos,
		Line:    startLine,
		Column:  startCol,
	}
}

func (l *Lexer) makeSingleCharToken(
	tokenType TokenType,
	startPos, startLine, startCol int,
	literal string,
) Token {
	l.advance()

	return Token{
		Type:    tokenType,
		Literal: literal,
		Start:   startPos,
		End:     l.pos,
		Line:    startLine,
		Column:  startCol,
	}
}

func (l *Lexer) skipWhitespace() {
	for {
		ch := l.peek()
		if ch == 0 || !unicode.IsSpace(ch) {
			return
		}

		l.advance()
	}
}

func (l *Lexer) advance() (rune, int) {
	if l.pos >= len(l.input) {
		return 0, 0
	}

	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += w

	if r == '\n' {
		l.line++
		l.column = 1
	} else {
		l.column++
	}

	return r, w
}

func (l *Lexer) advanceN(n int) {
	for consumed := 0; consumed < n; {
		_, w := l.advance()
		if w == 0 {
			return
		}

		consumed += w
	}
}

func (l *Lexer) peek() rune {
	if l.pos >= len(l.input) {
		return 0
	}

	r, _ := utf8.DecodeRuneInString(l.input[l.pos:])

	return r
}

func (l *Lexer) peekAheadString(s string) bool {
	if len(s) == 0 {
		return true
	}

	if l.pos+len(s) > len(l.input) {
		return false
	}

	return strings.HasPrefix(l.input[l.pos:], s)
}

func isIdentifierStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || r >= 0x80
}

func isIdentifierPart(r rune) bool {
	return isIdentifierStart(r) || unicode.IsDigit(r)
}

func isDollarTagChar(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
