package parser

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

var (
	quotedIdentRe = regexp.MustCompile(`^"([^"]*)"$`)
	schemaTableRe = regexp.MustCompile(
		`^(?:([a-zA-Z_][a-zA-Z0-9_]*|"[^"]*")\.)?([a-zA-Z_][a-zA-Z0-9_]*|"[^"]*")$`,
	)
	identifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

type IdentifierNormalizer struct {
	caseSensitive bool
}

func NewIdentifierNormalizer(caseSensitive bool) *IdentifierNormalizer {
	return &IdentifierNormalizer{caseSensitive: caseSensitive}
}

func (n *IdentifierNormalizer) Normalize(ident string) string {
	ident = strings.TrimSpace(ident)

	if matches := quotedIdentRe.FindStringSubmatch(ident); matches != nil {
		if n.caseSensitive {
			return matches[1]
		}

		return strings.ToLower(matches[1])
	}

	if n.caseSensitive {
		return ident
	}

	return strings.ToLower(ident)
}

func (n *IdentifierNormalizer) SplitQualified(qualified string) (schemaName, objectName string) {
	qualified = strings.TrimSpace(qualified)

	matches := schemaTableRe.FindStringSubmatch(qualified)
	if matches == nil {
		return schema.DefaultSchema, n.Normalize(qualified)
	}

	schemaPart := matches[1]
	objectPart := matches[2]

	if schemaPart == "" {
		schemaName = schema.DefaultSchema
	} else {
		schemaName = n.Normalize(schemaPart)
	}

	return schemaName, n.Normalize(objectPart)
}

func (n *IdentifierNormalizer) IsQuoted(ident string) bool {
	ident = strings.TrimSpace(ident)
	return len(ident) >= 2 && ident[0] == '"' && ident[len(ident)-1] == '"'
}

func (n *IdentifierNormalizer) Unquote(ident string) string {
	ident = strings.TrimSpace(ident)
	if len(ident) >= 2 && ((ident[0] == '"' && ident[len(ident)-1] == '"') ||
		(ident[0] == '\'' && ident[len(ident)-1] == '\'')) {
		return ident[1 : len(ident)-1]
	}

	return ident
}

func (n *IdentifierNormalizer) QuoteIfNeeded(ident string) string {
	if ident == "" {
		return ident
	}

	if n.IsQuoted(ident) {
		return ident
	}

	if !identifierRe.MatchString(ident) || n.IsKeyword(ident) {
		return fmt.Sprintf(`"%s"`, strings.ReplaceAll(ident, `"`, `""`))
	}

	if !n.caseSensitive && ident != strings.ToLower(ident) {
		return fmt.Sprintf(`"%s"`, strings.ReplaceAll(ident, `"`, `""`))
	}

	return ident
}

func (n *IdentifierNormalizer) IsValidIdentifier(ident string) bool {
	ident = strings.TrimSpace(ident)
	if n.IsQuoted(ident) {
		return true
	}

	return identifierRe.MatchString(ident)
}

func (n *IdentifierNormalizer) IsKeyword(ident string) bool {
	ident = strings.ToUpper(strings.TrimSpace(ident))
	_, ok := keywordSet[ident]

	return ok
}

func (p *Parser) normalizeIdent(ident string) string {
	return p.identNormalizer().Normalize(ident)
}

func (p *Parser) splitSchemaTable(qualified string) (string, string) {
	return p.identNormalizer().SplitQualified(qualified)
}

func (p *Parser) identNormalizer() *IdentifierNormalizer {
	if p.normalizer == nil {
		p.normalizer = NewIdentifierNormalizer(p.config.CaseSensitive)
	}

	return p.normalizer
}

func unquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}

	return s
}
