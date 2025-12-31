package generator

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-z0-9]+`)

func GenerateMigrationName(changes []differ.Change) string {
	if len(changes) == 0 {
		return "no_changes"
	}

	typeCounts := make(map[differ.ChangeType]int)

	var primaryObject string

	schemas := make(map[string]bool)

	for _, change := range changes {
		typeCounts[change.Type]++
		if primaryObject == "" && change.ObjectName != "" {
			primaryObject = extractSimpleName(change.ObjectName, change.Type)
		}

		schema := extractSchemaFromChange(&change)
		if schema != "" {
			schemas[schema] = true
		}
	}

	name := buildMigrationName(typeCounts, primaryObject)
	schemaPrefix := getSchemaPrefix(schemas)

	if schemaPrefix != "" {
		name = schemaPrefix + "_" + name
	}

	return sanitizeName(name)
}

func extractSchemaFromChange(change *differ.Change) string {
	if change.Type == differ.ChangeTypeAddSchema || change.Type == differ.ChangeTypeDropSchema {
		return change.ObjectName
	}

	if change.ObjectName == "" {
		return ""
	}

	parts := strings.Split(change.ObjectName, ".")
	if len(parts) < 2 {
		return ""
	}

	schemaName := strings.ToLower(parts[0])
	if schemaName == schema.DefaultSchema {
		return ""
	}

	return schemaName
}

func getSchemaPrefix(schemas map[string]bool) string {
	if len(schemas) == 0 {
		return ""
	}

	if len(schemas) == 1 {
		for schema := range schemas {
			return schema
		}
	}

	schemaList := make([]string, 0, len(schemas))
	for schema := range schemas {
		schemaList = append(schemaList, schema)
	}

	sort.Strings(schemaList)

	if len(schemaList) <= 2 {
		return strings.Join(schemaList, "_and_")
	}

	return "multi_schema"
}

func buildMigrationName(typeCounts map[differ.ChangeType]int, primaryObject string) string {
	if len(typeCounts) == 1 {
		for changeType, count := range typeCounts {
			if count > 1 {
				return describeMultipleChanges(typeCounts)
			}

			return describeSingleChange(changeType, primaryObject)
		}
	}

	return describeMultipleChanges(typeCounts)
}

func describeSingleChange(changeType differ.ChangeType, objectName string) string {
	suffix := ""
	if objectName != "" {
		suffix = "_" + objectName
	}

	switch changeType {
	case differ.ChangeTypeAddSchema:
		return "add_schema" + suffix
	case differ.ChangeTypeDropSchema:
		return "drop_schema" + suffix
	case differ.ChangeTypeAddTable:
		return "add_table" + suffix
	case differ.ChangeTypeDropTable:
		return "drop_table" + suffix
	case differ.ChangeTypeAddColumn:
		return "add_columns" + suffix
	case differ.ChangeTypeDropColumn:
		return "drop_columns" + suffix
	case differ.ChangeTypeModifyColumnType:
		return "modify_column_types" + suffix
	case differ.ChangeTypeAddIndex:
		return "add_index" + suffix
	case differ.ChangeTypeDropIndex:
		return "drop_index" + suffix
	case differ.ChangeTypeAddConstraint:
		return "add_constraint" + suffix
	case differ.ChangeTypeDropConstraint:
		return "drop_constraint" + suffix
	case differ.ChangeTypeAddView, differ.ChangeTypeModifyView:
		return "update_view" + suffix
	case differ.ChangeTypeAddFunction, differ.ChangeTypeModifyFunction:
		return "update_function" + suffix
	case differ.ChangeTypeAddHypertable:
		return "add_hypertable" + suffix
	case differ.ChangeTypeAddCompressionPolicy:
		return "add_compression" + suffix
	case differ.ChangeTypeAddRetentionPolicy:
		return "add_retention" + suffix
	case differ.ChangeTypeAddContinuousAggregate:
		return "add_continuous_aggregate" + suffix
	default:
		return "schema_changes" //nolint:goconst
	}
}

func describeMultipleChanges(typeCounts map[differ.ChangeType]int) string {
	if len(typeCounts) == 1 {
		for _, count := range typeCounts {
			if count > 1 {
				return "schema_changes"
			}
		}
	}

	var parts []string

	if hasTableChanges(typeCounts) {
		parts = append(parts, "update_tables")
	}

	if hasIndexChanges(typeCounts) {
		parts = append(parts, "update_indexes")
	}

	if hasViewChanges(typeCounts) {
		parts = append(parts, "update_views")
	}

	if hasFunctionChanges(typeCounts) {
		parts = append(parts, "update_functions")
	}

	if hasTimescaleChanges(typeCounts) {
		parts = append(parts, "update_timescale")
	}

	if len(parts) == 1 {
		return parts[0]
	}

	if len(parts) == 0 {
		return "schema_changes"
	}

	return "schema_changes"
}

func hasTableChanges(counts map[differ.ChangeType]int) bool {
	return counts[differ.ChangeTypeAddTable] > 0 ||
		counts[differ.ChangeTypeDropTable] > 0 ||
		counts[differ.ChangeTypeAddColumn] > 0 ||
		counts[differ.ChangeTypeDropColumn] > 0 ||
		counts[differ.ChangeTypeModifyColumnType] > 0
}

func hasIndexChanges(counts map[differ.ChangeType]int) bool {
	return counts[differ.ChangeTypeAddIndex] > 0 ||
		counts[differ.ChangeTypeDropIndex] > 0 ||
		counts[differ.ChangeTypeAddConstraint] > 0 ||
		counts[differ.ChangeTypeDropConstraint] > 0
}

func hasViewChanges(counts map[differ.ChangeType]int) bool {
	return counts[differ.ChangeTypeAddView] > 0 ||
		counts[differ.ChangeTypeDropView] > 0 ||
		counts[differ.ChangeTypeModifyView] > 0
}

func hasFunctionChanges(counts map[differ.ChangeType]int) bool {
	return counts[differ.ChangeTypeAddFunction] > 0 ||
		counts[differ.ChangeTypeDropFunction] > 0 ||
		counts[differ.ChangeTypeModifyFunction] > 0
}

func hasTimescaleChanges(counts map[differ.ChangeType]int) bool {
	return counts[differ.ChangeTypeAddHypertable] > 0 ||
		counts[differ.ChangeTypeAddCompressionPolicy] > 0 ||
		counts[differ.ChangeTypeAddRetentionPolicy] > 0
}

func extractSimpleName(qualifiedName string, changeType differ.ChangeType) string {
	parts := strings.Split(qualifiedName, ".")

	var name string
	if isColumnChange(changeType) && len(parts) >= 2 {
		name = parts[len(parts)-2]
	} else {
		name = parts[len(parts)-1]
	}

	if idx := strings.Index(name, "("); idx != -1 {
		name = name[:idx]
	}

	return strings.ToLower(strings.TrimFunc(name, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	}))
}

func isColumnChange(changeType differ.ChangeType) bool {
	return changeType == differ.ChangeTypeAddColumn ||
		changeType == differ.ChangeTypeDropColumn ||
		changeType == differ.ChangeTypeModifyColumnType ||
		changeType == differ.ChangeTypeModifyColumnNullability ||
		changeType == differ.ChangeTypeModifyColumnDefault ||
		changeType == differ.ChangeTypeModifyColumnComment ||
		changeType == differ.ChangeTypeRenameColumn
}

func sanitizeName(name string) string {
	name = strings.ToLower(name)
	name = nonAlphanumericRegex.ReplaceAllString(name, "_")
	name = strings.Trim(name, "_")

	if len(name) > 60 {
		name = name[:60]
		name = strings.TrimRight(name, "_")
	}

	return name
}

func FormatMigrationFileName(version int, description string, direction Direction) string {
	return fmt.Sprintf("%06d_%s.%s.sql", version, description, direction)
}

func ParseMigrationFileName(fileName string) (int, string, Direction, error) {
	parts := strings.Split(fileName, "_")
	if len(parts) < 2 {
		return 0, "", "", fmt.Errorf("invalid migration file name: %s", fileName)
	}

	var version int
	if _, err := fmt.Sscanf(parts[0], "%06d", &version); err != nil {
		return 0, "", "", fmt.Errorf("invalid version number in: %s", fileName)
	}

	remaining := strings.Join(parts[1:], "_")
	directionParts := strings.Split(remaining, ".")

	if len(directionParts) < 3 {
		return 0, "", "", fmt.Errorf("invalid migration file format: %s", fileName)
	}

	description := directionParts[0]
	direction := Direction(directionParts[1])

	if direction != DirectionUp && direction != DirectionDown {
		return 0, "", "", fmt.Errorf("invalid direction in: %s", fileName)
	}

	return version, description, direction, nil
}
