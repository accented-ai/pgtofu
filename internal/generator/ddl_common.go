package generator

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/accented-ai/pgtofu/internal/differ"
)

type commentDetails struct {
	Old    string
	New    string
	HasOld bool
	HasNew bool
}

func extractCommentDetails(change differ.Change) (commentDetails, error) {
	oldComment, hasOld, err := getOptionalDetailString(change.Details, DetailKeyOldComment)
	if err != nil {
		return commentDetails{}, err
	}

	newComment, hasNew, err := getOptionalDetailString(change.Details, DetailKeyNewComment)
	if err != nil {
		return commentDetails{}, err
	}

	return commentDetails{
		Old:    oldComment,
		New:    newComment,
		HasOld: hasOld,
		HasNew: hasNew,
	}, nil
}

func isCommentChangeOnly(change differ.Change) (bool, error) {
	details, err := extractCommentDetails(change)
	if err != nil {
		return false, err
	}

	return details.HasOld && details.HasNew, nil
}

func buildCommentStatement(objectType, target, comment string, forceMultiline bool) string {
	prefix := fmt.Sprintf("COMMENT ON %s %s IS", objectType, target)

	if comment == "" {
		return prefix + " NULL;"
	}

	literal := formatSQLStringLiteral(comment)
	if !forceMultiline && !strings.Contains(comment, "\n") &&
		len(prefix)+len(literal)+2 <= generatedSQLLineLength {
		return prefix + " " + literal + ";"
	}

	lines := make([]string, 0)
	for sourceLine := range strings.SplitSeq(comment, "\n") {
		lines = append(lines, splitCommentLiterals(sourceLine, generatedSQLLineLength-1)...)
	}

	return prefix + "\n" + strings.Join(lines, "\n") + ";"
}

func splitCommentLiterals(comment string, maxLength int) []string {
	if comment == "" {
		return []string{formatSQLStringLiteral("")}
	}

	literals := make([]string, 0, len(comment)/maxLength+1)

	for len(comment) > 0 {
		var (
			end           int
			whitespaceEnd int
		)

		for offset, value := range comment {
			candidateEnd := offset + utf8.RuneLen(value)
			if len(formatSQLStringLiteral(comment[:candidateEnd])) > maxLength {
				break
			}

			end = candidateEnd
			if unicode.IsSpace(value) {
				whitespaceEnd = candidateEnd
			}
		}

		if end == 0 {
			_, size := utf8.DecodeRuneInString(comment)
			end = size
		}

		if end < len(comment) && whitespaceEnd > 0 {
			end = whitespaceEnd
		}

		literals = append(literals, formatSQLStringLiteral(comment[:end]))
		comment = comment[end:]
	}

	return literals
}

func ensureStatementTerminated(sql string) string {
	trimmed := strings.TrimRight(sql, " \t\n\r")
	if trimmed == "" {
		return ""
	}

	if strings.HasSuffix(trimmed, ";") {
		return trimmed
	}

	return trimmed + ";"
}

func appendStatement(sb *strings.Builder, statement string) {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return
	}

	if sb.Len() > 0 {
		sb.WriteString("\n\n")
	}

	sb.WriteString(ensureStatementTerminated(statement))
}
