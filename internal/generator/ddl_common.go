package generator

import (
	"fmt"
	"strings"

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
	if comment == "" {
		return fmt.Sprintf("COMMENT ON %s %s IS NULL;", objectType, target)
	}

	if forceMultiline || strings.Contains(comment, "\n") {
		lines := strings.Split(comment, "\n")
		for i := range lines {
			lines[i] = formatSQLStringLiteral(lines[i])
		}

		return fmt.Sprintf(
			"COMMENT ON %s %s IS\n%s;",
			objectType,
			target,
			strings.Join(lines, "\n"),
		)
	}

	return fmt.Sprintf(
		"COMMENT ON %s %s IS %s;",
		objectType,
		target,
		formatSQLStringLiteral(comment),
	)
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
