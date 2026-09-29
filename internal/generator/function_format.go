package generator

import (
	"strings"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func formatFunctionArgumentSignature(fn *schema.Function) string {
	return "(" + formatFunctionArgumentList(fn) + ")"
}

func formatFunctionArgumentList(fn *schema.Function) string {
	if fn == nil {
		return ""
	}

	if len(fn.ArgumentNames) == 0 {
		return strings.Join(formatFunctionDataTypes(fn.ArgumentTypes), ", ")
	}

	parts := make([]string, 0, len(fn.ArgumentTypes))
	for i, argType := range fn.ArgumentTypes {
		var part string
		if i < len(fn.ArgumentModes) && fn.ArgumentModes[i] != "IN" {
			part = fn.ArgumentModes[i] + " "
		}

		if i < len(fn.ArgumentNames) && fn.ArgumentNames[i] != "" {
			part += fn.ArgumentNames[i] + " "
		}

		part += formatFunctionDataType(argType)
		parts = append(parts, part)
	}

	return strings.Join(parts, ", ")
}

func formatFunctionDataTypes(dataTypes []string) []string {
	formatted := make([]string, len(dataTypes))
	for i, dataType := range dataTypes {
		formatted[i] = formatFunctionDataType(dataType)
	}

	return formatted
}

func formatFunctionReturnType(returnType string) string {
	returnType = strings.TrimSpace(returnType)

	formatted := formatFunctionDataType(returnType)
	if len(returnType) < len("TABLE") || !strings.EqualFold(returnType[:len("TABLE")], "TABLE") {
		return formatted
	}

	tokens, err := parser.NewLexer(returnType).Tokenize()
	if err != nil || len(tokens) < 3 ||
		!strings.EqualFold(tokens[0].Literal, "TABLE") ||
		tokens[1].Type != parser.TokenLParen {
		return formatted
	}

	output := []byte(formatted)
	depth := 0
	wantColumnName := false

	for _, token := range tokens[1:] {
		switch token.Type {
		case parser.TokenLParen:
			depth++
			if depth == 1 {
				wantColumnName = true
			}
		case parser.TokenRParen:
			depth--
		case parser.TokenComma:
			if depth == 1 {
				wantColumnName = true
			}
		case parser.TokenComment:
			continue
		default:
			if depth == 1 && wantColumnName {
				if token.Type == parser.TokenIdentifier || token.Type == parser.TokenKeyword {
					for i := token.Start; i < token.End; i++ {
						output[i] = lowerASCIIByte(output[i])
					}
				}

				wantColumnName = false
			}
		}
	}

	return string(output)
}

func formatFunctionDataType(dataType string) string {
	dataType = strings.TrimSpace(dataType)
	if dataType == "" {
		return ""
	}

	var out strings.Builder

	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(dataType); i++ {
		ch := dataType[i]

		if inSingleQuote {
			out.WriteByte(ch)

			if ch == '\'' {
				if i+1 < len(dataType) && dataType[i+1] == '\'' {
					out.WriteByte(dataType[i+1])
					i++

					continue
				}

				inSingleQuote = false
			}

			continue
		}

		if inDoubleQuote {
			out.WriteByte(ch)

			if ch == '"' {
				if i+1 < len(dataType) && dataType[i+1] == '"' {
					out.WriteByte(dataType[i+1])
					i++

					continue
				}

				inDoubleQuote = false
			}

			continue
		}

		switch ch {
		case '\'':
			inSingleQuote = true

			out.WriteByte(ch)
		case '"':
			inDoubleQuote = true

			out.WriteByte(ch)
		default:
			out.WriteByte(upperASCIIByte(ch))
		}
	}

	return out.String()
}

func upperASCIIByte(ch byte) byte {
	if ch >= 'a' && ch <= 'z' {
		return ch - 'a' + 'A'
	}

	return ch
}

func lowerASCIIByte(ch byte) byte {
	if ch >= 'A' && ch <= 'Z' {
		return ch - 'A' + 'a'
	}

	return ch
}
