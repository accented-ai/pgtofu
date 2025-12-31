package differ

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type ColumnComparator struct {
	options *Options
}

func NewColumnComparator(opts *Options) *ColumnComparator {
	return &ColumnComparator{options: opts}
}

func (cc *ColumnComparator) Compare(
	result *DiffResult,
	tableKey string,
	currentTable *schema.Table,
	current, desired *schema.Table,
) {
	currentCols := cc.buildColumnMap(current.Columns)
	desiredCols := cc.buildColumnMap(desired.Columns)

	cc.detectAddedColumns(result, tableKey, desired, currentCols, desiredCols)
	cc.detectDroppedColumns(result, tableKey, currentTable, currentCols, desiredCols)
	cc.detectModifiedColumns(result, tableKey, desired, currentCols, desiredCols)
}

func (cc *ColumnComparator) buildColumnMap(columns []schema.Column) map[string]*schema.Column {
	m := make(map[string]*schema.Column, len(columns))
	for i := range columns {
		key := strings.ToLower(columns[i].Name)
		m[key] = &columns[i]
	}

	return m
}

func (cc *ColumnComparator) detectAddedColumns(
	result *DiffResult,
	tableKey string,
	table *schema.Table,
	currentCols, desiredCols map[string]*schema.Column,
) {
	for key, col := range desiredCols {
		if _, exists := currentCols[key]; !exists {
			severity := cc.getAddColumnSeverity(col)

			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeAddColumn,
				Severity: severity,
				Description: fmt.Sprintf(
					"Add column: %s.%s (%s)",
					table.QualifiedName(),
					col.Name,
					col.DataType,
				),
				ObjectType: "column",
				ObjectName: tableKey,
				Details:    map[string]any{"table": table.QualifiedName(), "column": col},
			})

			cc.addColumnCommentChange(result, tableKey, table, col, "")
		}
	}
}

func (cc *ColumnComparator) getAddColumnSeverity(col *schema.Column) ChangeSeverity {
	if !col.IsNullable && col.Default == "" {
		return SeverityDataMigrationRequired
	}

	return SeveritySafe
}

func (cc *ColumnComparator) detectDroppedColumns(
	result *DiffResult,
	tableKey string,
	table *schema.Table,
	currentCols, desiredCols map[string]*schema.Column,
) {
	for key, col := range currentCols {
		if _, exists := desiredCols[key]; !exists {
			severity := SeverityPotentiallyBreaking
			if !col.IsNullable {
				severity = SeverityBreaking
			}

			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeDropColumn,
				Severity:    severity,
				Description: fmt.Sprintf("Drop column: %s.%s", table.QualifiedName(), col.Name),
				ObjectType:  "column",
				ObjectName:  tableKey,
				Details:     map[string]any{"table": table.QualifiedName(), "column": col},
			})
		}
	}
}

func (cc *ColumnComparator) detectModifiedColumns(
	result *DiffResult,
	tableKey string,
	table *schema.Table,
	currentCols, desiredCols map[string]*schema.Column,
) {
	for key, desiredCol := range desiredCols {
		currentCol, exists := currentCols[key]
		if !exists {
			continue
		}

		cc.compareColumnType(result, tableKey, table, currentCol, desiredCol)
		cc.compareColumnNullability(result, tableKey, table, currentCol, desiredCol)
		cc.compareColumnDefault(result, tableKey, table, currentCol, desiredCol)
		cc.compareColumnComment(result, tableKey, table, currentCol, desiredCol)
	}
}

func (cc *ColumnComparator) compareColumnType(
	result *DiffResult,
	tableKey string,
	table *schema.Table,
	current, desired *schema.Column,
) {
	if columnsHaveSameType(current, desired) {
		return
	}

	severity := SeverityDataMigrationRequired
	if isTypeSafeChange(current, desired) {
		severity = SeveritySafe
	}

	result.Changes = append(result.Changes, Change{
		Type:     ChangeTypeModifyColumnType,
		Severity: severity,
		Description: fmt.Sprintf(
			"Change column type: %s.%s from %s to %s",
			table.QualifiedName(),
			current.Name,
			current.FullDataType(),
			desired.FullDataType(),
		),
		ObjectType: "column",
		ObjectName: tableKey,
		Details: map[string]any{
			"table":       table.QualifiedName(),
			"column_name": current.Name,
			"old_type":    current.FullDataType(),
			"new_type":    desired.FullDataType(),
		},
	})
}

func (cc *ColumnComparator) compareColumnNullability(
	result *DiffResult,
	tableKey string,
	table *schema.Table,
	current, desired *schema.Column,
) {
	if current.IsNullable == desired.IsNullable {
		return
	}

	severity := SeveritySafe
	description := fmt.Sprintf("Make column nullable: %s.%s", table.QualifiedName(), current.Name)

	if !desired.IsNullable {
		severity = SeverityDataMigrationRequired
		description = fmt.Sprintf(
			"Make column NOT NULL: %s.%s",
			table.QualifiedName(),
			current.Name,
		)
	}

	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeModifyColumnNullability,
		Severity:    severity,
		Description: description,
		ObjectType:  "column",
		ObjectName:  tableKey,
		Details: map[string]any{
			"table":        table.QualifiedName(),
			"column_name":  current.Name,
			"old_nullable": current.IsNullable,
			"new_nullable": desired.IsNullable,
		},
	})
}

func (cc *ColumnComparator) compareColumnDefault(
	result *DiffResult,
	tableKey string,
	table *schema.Table,
	current, desired *schema.Column,
) {
	if AreDefaultsEqual(current.Default, desired.Default) {
		return
	}

	result.Changes = append(result.Changes, Change{
		Type:     ChangeTypeModifyColumnDefault,
		Severity: SeveritySafe,
		Description: fmt.Sprintf(
			"Change default value: %s.%s",
			table.QualifiedName(),
			current.Name,
		),
		ObjectType: "column",
		ObjectName: tableKey,
		Details: map[string]any{
			"table":       table.QualifiedName(),
			"column_name": current.Name,
			"old_default": current.Default,
			"new_default": desired.Default,
		},
	})
}

func (cc *ColumnComparator) compareColumnComment(
	result *DiffResult,
	tableKey string,
	table *schema.Table,
	current, desired *schema.Column,
) {
	if cc.options.IgnoreComments {
		return
	}

	currentComment := normalizeComment(current.Comment)
	desiredComment := normalizeComment(desired.Comment)

	if currentComment == desiredComment {
		return
	}

	result.Changes = append(result.Changes, Change{
		Type:     ChangeTypeModifyColumnComment,
		Severity: SeveritySafe,
		Description: fmt.Sprintf(
			"Modify column comment: %s.%s",
			table.QualifiedName(),
			current.Name,
		),
		ObjectType: "column",
		ObjectName: tableKey,
		Details: map[string]any{
			"table":       table.QualifiedName(),
			"column_name": current.Name,
			"old_comment": current.Comment,
			"new_comment": desired.Comment,
		},
	})
}

func (cc *ColumnComparator) addColumnCommentChange(
	result *DiffResult,
	tableKey string,
	table *schema.Table,
	col *schema.Column,
	oldComment string,
) {
	if cc.options.IgnoreComments || col.Comment == "" {
		return
	}

	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeModifyColumnComment,
		Severity:    SeveritySafe,
		Description: fmt.Sprintf("Add column comment: %s.%s", table.QualifiedName(), col.Name),
		ObjectType:  "column",
		ObjectName:  tableKey,
		Details: map[string]any{
			"table":       table.QualifiedName(),
			"column_name": col.Name,
			"old_comment": oldComment,
			"new_comment": col.Comment,
		},
	})
}

func isTypeSafeChange(current, desired *schema.Column) bool {
	currentType := strings.ToLower(current.DataType)
	desiredType := strings.ToLower(desired.DataType)

	if (strings.HasPrefix(currentType, "varchar") || strings.HasPrefix(currentType, "character varying")) &&
		(strings.HasPrefix(desiredType, "varchar") || strings.HasPrefix(desiredType, "character varying")) {
		if current.MaxLength != nil && desired.MaxLength != nil {
			return *desired.MaxLength >= *current.MaxLength
		}
	}

	if strings.HasPrefix(currentType, "char") && strings.HasPrefix(desiredType, "char") {
		if current.MaxLength != nil && desired.MaxLength != nil {
			return *desired.MaxLength >= *current.MaxLength
		}
	}

	if strings.HasPrefix(currentType, "numeric") &&
		strings.HasPrefix(desiredType, "numeric") {
		if current.Precision != nil && desired.Precision != nil {
			if current.Scale != nil && desired.Scale != nil {
				return *desired.Precision >= *current.Precision && *desired.Scale >= *current.Scale
			}

			return *desired.Precision >= *current.Precision
		}
	}

	safeIntegerWidenings := map[string][]string{
		"smallint": {"integer", "bigint"},
		"integer":  {"bigint"},
	}

	currentNorm := NormalizeDataType(current.DataType)
	desiredNorm := NormalizeDataType(desired.DataType)

	if allowed, exists := safeIntegerWidenings[currentNorm]; exists {
		if slices.Contains(allowed, desiredNorm) {
			return true
		}
	}

	return false
}

func NormalizeDataType(dataType string) string {
	dt := strings.ToLower(strings.TrimSpace(dataType))

	aliases := map[string]string{
		"int":               "integer",
		"int2":              "smallint",
		"int4":              "integer",
		"int8":              "bigint",
		"float":             "double precision",
		"float4":            "real",
		"float8":            "double precision",
		"serial":            "integer",
		"bigserial":         "bigint",
		"bool":              "boolean",
		"character varying": "varchar",
		"character":         "char",
		"decimal":           "numeric",
		"timestamp":         "timestamp without time zone",
		"timestamptz":       "timestamp with time zone",
		"time":              "time without time zone",
		"timetz":            "time with time zone",
	}

	if normalized, exists := aliases[dt]; exists {
		return normalized
	}

	for alias, canonical := range aliases {
		if strings.HasPrefix(dt, alias+"(") {
			return canonical + dt[len(alias):]
		}
	}

	return dt
}

func AreDefaultsEqual(default1, default2 string) bool {
	if default1 == "" && default2 == "" {
		return true
	}

	return normalizeDefault(default1) == normalizeDefault(default2)
}

func columnsHaveSameType(current, desired *schema.Column) bool {
	if NormalizeDataType(current.DataType) != NormalizeDataType(desired.DataType) {
		return false
	}

	if !intPointerEqual(current.MaxLength, desired.MaxLength) {
		return false
	}

	if !intPointerEqual(current.Precision, desired.Precision) {
		return false
	}

	if !intPointerEqual(current.Scale, desired.Scale) {
		return false
	}

	if current.IsArray != desired.IsArray {
		return false
	}

	return true
}

func intPointerEqual(a, b *int) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

func normalizeDefault(defaultValue string) string {
	if defaultValue == "" {
		return ""
	}

	def := strings.TrimSpace(defaultValue)
	def = strings.ToLower(def)

	arrayCastPattern := regexp.MustCompile(`::[a-z_][a-z0-9_]*\[\]`)
	def = arrayCastPattern.ReplaceAllString(def, "")

	typeCasts := []string{
		"::text", "::character varying", "::varchar", "::integer", "::bigint",
		"::boolean", "::timestamp with time zone", "::timestamp without time zone",
		"::timestamptz", "::jsonb", "::json",
	}
	for _, cast := range typeCasts {
		def = strings.TrimSuffix(def, cast)
	}

	replacements := map[string]string{
		"now()":                   "current_timestamp",
		"current_timestamp()":     "current_timestamp",
		"localtimestamp":          "current_timestamp",
		"transaction_timestamp()": "current_timestamp",
		"statement_timestamp()":   "current_timestamp",
		"clock_timestamp()":       "current_timestamp",
		"true":                    "'t'",
		"false":                   "'f'",
	}

	if replacement, exists := replacements[def]; exists {
		return replacement
	}

	return strings.Join(strings.Fields(def), " ")
}
