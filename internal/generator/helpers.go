package generator

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

type SchemaName string

func extractSchema(change *differ.Change) SchemaName {
	if change.Type == differ.ChangeTypeAddSchema || change.Type == differ.ChangeTypeDropSchema {
		return SchemaName(strings.ToLower(change.ObjectName))
	}

	if change.ObjectName == "" {
		return SchemaName(schema.DefaultSchema)
	}

	parts := strings.Split(change.ObjectName, ".")
	if len(parts) < 2 {
		return SchemaName(schema.DefaultSchema)
	}

	schemaName := strings.ToLower(parts[0])
	if schemaName == "" {
		return SchemaName(schema.DefaultSchema)
	}

	return SchemaName(schemaName)
}

func normalizeObjectName(name string) string {
	if name == "" {
		return ""
	}

	parts := strings.Split(name, ".")
	if len(parts) < 2 {
		return strings.ToLower(name)
	}

	return fmt.Sprintf("%s.%s", strings.ToLower(parts[0]), strings.ToLower(parts[1]))
}

func extractTableNameFromChange(change *differ.Change) string {
	if change.ObjectName == "" {
		return ""
	}

	parts := strings.Split(change.ObjectName, ".")
	if len(parts) < 2 {
		return change.ObjectName
	}

	return strings.ToLower(parts[len(parts)-1])
}

func getChangePriorityForTable(changeType differ.ChangeType) int {
	switch changeType {
	case differ.ChangeTypeAddTable:
		return 1
	case differ.ChangeTypeModifyTableComment:
		return 2
	case differ.ChangeTypeAddColumn:
		return 3
	case differ.ChangeTypeModifyColumnComment:
		return 4
	case differ.ChangeTypeModifyColumnType:
		return 5
	case differ.ChangeTypeModifyColumnNullability:
		return 6
	case differ.ChangeTypeModifyColumnDefault:
		return 7
	case differ.ChangeTypeAddConstraint:
		return 8
	case differ.ChangeTypeAddIndex:
		return 9
	default:
		return 100
	}
}
