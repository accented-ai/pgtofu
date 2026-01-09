package generator

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func formatSQLStringLiteral(s string) string {
	return fmt.Sprintf("'%s'", escapeSQLString(s))
}

func QuoteIdentifier(name string) string {
	if len(name) == 0 {
		return `""`
	}

	if name[0] >= '0' && name[0] <= '9' {
		return fmt.Sprintf(`"%s"`, name)
	}

	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' {
			return fmt.Sprintf(`"%s"`, name)
		}
	}

	return name
}

func QualifiedName(schemaName, name string) string {
	if schemaName == "" {
		schemaName = schema.DefaultSchema
	}

	return fmt.Sprintf("%s.%s", QuoteIdentifier(schemaName), QuoteIdentifier(name))
}

type tokenBuffer struct {
	builder strings.Builder
}

func (tb *tokenBuffer) Write(token string) {
	token = strings.TrimSpace(token)
	if token == "" {
		return
	}

	if tb.builder.Len() > 0 {
		tb.builder.WriteByte(' ')
	}

	tb.builder.WriteString(token)
}

func (tb *tokenBuffer) String() string {
	return tb.builder.String()
}

func formatColumnDefinition(col *schema.Column) (string, error) {
	if col == nil {
		return "", errors.New("column cannot be nil")
	}

	if col.Name == "" {
		return "", errors.New("column name cannot be empty")
	}

	if strings.TrimSpace(col.DataType) == "" {
		return "", errors.New("column data type cannot be empty")
	}

	dataType := NormalizeDataType(col.FullDataType())
	defaultValue := NormalizeDefaultValue(col.Default)

	if dataType == "" {
		return "", errors.New("column data type cannot be empty")
	}

	if defaultValue != "" && strings.Contains(strings.ToLower(defaultValue), "nextval(") {
		escapedColName := regexp.QuoteMeta(col.Name)

		serialPattern := regexp.MustCompile(
			`(?i)nextval\('(?:[^']+_)?` + escapedColName + `_seq'(?:::regclass)?\)`,
		)
		if serialPattern.MatchString(defaultValue) {
			switch strings.ToLower(col.DataType) {
			case "integer", "int", "int4":
				dataType = "SERIAL"
				defaultValue = ""
			case "bigint", "int8":
				dataType = "BIGSERIAL"
				defaultValue = ""
			case "smallint", "int2":
				dataType = "SMALLSERIAL"
				defaultValue = ""
			}
		}
	}

	if col.IsArray && !strings.HasSuffix(dataType, "[]") {
		dataType += "[]"
	}

	var buf tokenBuffer
	buf.Write(QuoteIdentifier(col.Name))
	buf.Write(dataType)

	if !col.IsNullable {
		buf.Write("NOT NULL")
	}

	if defaultValue != "" {
		buf.Write("DEFAULT")
		buf.Write(defaultValue)
	}

	return buf.String(), nil
}

func formatConstraintDefinition( //nolint:cyclop,gocognit,gocyclo
	c *schema.Constraint,
) (string, error) {
	if c == nil {
		return "", errors.New("constraint cannot be nil")
	}

	var buf tokenBuffer

	if c.Name != "" {
		buf.Write("CONSTRAINT")
		buf.Write(QuoteIdentifier(c.Name))
	}

	switch c.Type {
	case "PRIMARY KEY":
		if len(c.Columns) == 0 {
			return "", errors.New("primary key constraint requires columns")
		}

		buf.Write("PRIMARY KEY")
		buf.Write(fmt.Sprintf("(%s)", quoteColumns(c.Columns)))

	case "FOREIGN KEY":
		if len(c.Columns) == 0 {
			return "", errors.New("foreign key constraint requires columns")
		}

		buf.Write("FOREIGN KEY")
		buf.Write(fmt.Sprintf("(%s)", quoteColumns(c.Columns)))

		if c.ReferencedTable == "" {
			return "", errors.New("foreign key constraint missing referenced table")
		}

		referenced := c.ReferencedTable
		if c.ReferencedSchema != "" {
			referenced = schema.QualifiedName(c.ReferencedSchema, c.ReferencedTable)
		}

		buf.Write("REFERENCES")
		buf.Write(referenced)

		if len(c.ReferencedColumns) > 0 {
			buf.Write(fmt.Sprintf("(%s)", quoteColumns(c.ReferencedColumns)))
		}

		if c.OnDelete != "" && c.OnDelete != "NO ACTION" {
			buf.Write("ON DELETE")
			buf.Write(c.OnDelete)
		}

		if c.OnUpdate != "" && c.OnUpdate != "NO ACTION" {
			buf.Write("ON UPDATE")
			buf.Write(c.OnUpdate)
		}

	case "UNIQUE":
		if len(c.Columns) == 0 {
			return "", errors.New("unique constraint requires columns")
		}

		buf.Write("UNIQUE")
		buf.Write(fmt.Sprintf("(%s)", quoteColumns(c.Columns)))

	case "CHECK":
		def := strings.TrimSpace(c.Definition)
		if def == "" {
			return "", errors.New("check constraint requires a definition")
		}

		def = NormalizeCheckConstraint(def)

		if strings.HasPrefix(strings.ToUpper(def), "CHECK") { //nolint:nestif
			lines := strings.Split(def, "\n")
			for i := range lines {
				lines[i] = strings.TrimRight(lines[i], " \t")
			}

			if len(lines) > 1 {
				minIndent := -1

				for i := 1; i < len(lines); i++ {
					if strings.TrimSpace(lines[i]) == "" {
						continue
					}

					indent := len(lines[i]) - len(strings.TrimLeft(lines[i], " \t"))
					if minIndent == -1 || indent < minIndent {
						minIndent = indent
					}
				}

				if minIndent > 0 {
					for i := 1; i < len(lines); i++ {
						if strings.TrimSpace(lines[i]) == "" {
							continue
						}

						if len(lines[i]) >= minIndent {
							lines[i] = lines[i][minIndent:]
						}

						trimmed := strings.TrimLeft(lines[i], " \t")

						if strings.TrimSpace(trimmed) == ")" || strings.TrimSpace(trimmed) == "))" {
							lines[i] = trimmed
						} else {
							lines[i] = sqlIndent + trimmed
						}
					}
				} else {
					for i := 1; i < len(lines); i++ {
						if strings.TrimSpace(lines[i]) == "" {
							continue
						}

						trimmed := strings.TrimLeft(lines[i], " \t")
						if strings.TrimSpace(trimmed) == ")" || strings.TrimSpace(trimmed) == "))" {
							lines[i] = trimmed
						} else {
							lines[i] = sqlIndent + trimmed
						}
					}
				}
			}

			buf.Write(strings.Join(lines, "\n"))
		} else {
			buf.Write("CHECK")
			buf.Write(def)
		}

	case "EXCLUDE":
		if strings.TrimSpace(c.Definition) == "" {
			return "", errors.New("exclude constraint requires a definition")
		}

		buf.Write("EXCLUDE")
		buf.Write(c.Definition)

	default:
		if c.Definition != "" {
			return c.Definition, nil
		}

		return "", errors.New("unsupported constraint type")
	}

	if c.IsDeferrable {
		buf.Write("DEFERRABLE")

		if c.InitiallyDeferred {
			buf.Write("INITIALLY DEFERRED")
		}
	}

	return buf.String(), nil
}

func quoteColumns(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		if isExpression(col) {
			quoted[i] = col
		} else {
			quoted[i] = QuoteIdentifier(col)
		}
	}

	return strings.Join(quoted, ", ")
}

func isExpression(s string) bool {
	trimmed := strings.TrimSpace(s)
	if strings.HasPrefix(trimmed, "(") {
		return true
	}

	if strings.ContainsAny(trimmed, "() -> +-*/=<>") {
		return true
	}

	return false
}

func formatIndexDefinition(idx *schema.Index) (string, error) {
	if idx == nil {
		return "", errors.New("index cannot be nil")
	}

	if idx.Name == "" {
		return "", errors.New("index name cannot be empty")
	}

	if len(idx.Columns) == 0 {
		return "", errors.New("index requires at least one column")
	}

	var buf tokenBuffer
	if idx.IsUnique {
		buf.Write("CREATE UNIQUE INDEX")
	} else {
		buf.Write("CREATE INDEX")
	}

	buf.Write(QuoteIdentifier(idx.Name))
	buf.Write("ON")
	buf.Write(QualifiedName(idx.Schema, idx.TableName))

	if idx.Type != "" && idx.Type != "btree" {
		buf.Write("USING")
		buf.Write(idx.Type)
	}

	buf.Write(fmt.Sprintf("(%s)", quoteColumns(idx.Columns)))

	if len(idx.IncludeColumns) > 0 {
		buf.Write("INCLUDE")
		buf.Write(fmt.Sprintf("(%s)", quoteColumns(idx.IncludeColumns)))
	}

	if idx.Where != "" {
		buf.Write("WHERE")
		buf.Write(NormalizeWhereClause(idx.Where))
	}

	return buf.String(), nil
}

func formatViewDefinition(v *schema.View, orReplace bool) (string, error) {
	if v == nil {
		return "", errors.New("view cannot be nil")
	}

	if v.Name == "" {
		return "", errors.New("view name cannot be empty")
	}

	if strings.TrimSpace(v.Definition) == "" {
		return "", errors.New("view definition cannot be empty")
	}

	prefix := "CREATE VIEW"
	if orReplace {
		prefix = "CREATE OR REPLACE VIEW"
	}

	return fmt.Sprintf("%s %s AS\n%s", prefix, QualifiedName(v.Schema, v.Name), v.Definition), nil
}

func formatMaterializedViewDefinition(mv *schema.MaterializedView) (string, error) {
	if mv == nil {
		return "", errors.New("materialized view cannot be nil")
	}

	if mv.Name == "" {
		return "", errors.New("materialized view name cannot be empty")
	}

	if strings.TrimSpace(mv.Definition) == "" {
		return "", errors.New("materialized view definition cannot be empty")
	}

	return fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s AS\n%s",
		QualifiedName(mv.Schema, mv.Name),
		mv.Definition,
	), nil
}

func formatFunctionDefinition(f *schema.Function, orReplace bool) (string, error) {
	if f == nil {
		return "", errors.New("function cannot be nil")
	}

	if f.Name == "" {
		return "", errors.New("function name cannot be empty")
	}

	if strings.TrimSpace(f.ReturnType) == "" {
		return "", errors.New("function return type cannot be empty")
	}

	if strings.TrimSpace(f.Language) == "" {
		return "", errors.New("function language cannot be empty")
	}

	nameUpper := strings.ToUpper(f.Name)

	schemaName := f.Schema
	if schemaName == "" {
		schemaName = schema.DefaultSchema
	}

	funcSignature := fmt.Sprintf("%s.%s", QuoteIdentifier(schemaName), nameUpper)

	argList := f.ArgumentList()
	if argList == "" {
		funcSignature += "()"
	} else {
		funcSignature += "(" + argList + ")"
	}

	body := strings.TrimSpace(f.Body)
	if strings.HasPrefix(body, "$$") && strings.HasSuffix(body, "$$") && len(body) >= 4 {
		body = strings.TrimPrefix(body, "$$")
		body = strings.TrimSuffix(body, "$$")
		body = strings.TrimSpace(body)
	}

	var sb strings.Builder
	if orReplace {
		sb.WriteString("CREATE OR REPLACE FUNCTION ")
	} else {
		sb.WriteString("CREATE FUNCTION ")
	}

	sb.WriteString(funcSignature)
	sb.WriteString("\n\n")
	sb.WriteString("RETURNS ")
	sb.WriteString(f.ReturnType)
	sb.WriteString(" AS $$\n")

	if body != "" {
		sb.WriteString(body)

		if !strings.HasSuffix(body, "\n") {
			sb.WriteString("\n")
		}
	}

	sb.WriteString("$$ ")
	sb.WriteString("LANGUAGE ")
	sb.WriteString(f.Language)

	if f.Volatility != "" && f.Volatility != "VOLATILE" {
		sb.WriteString(" ")
		sb.WriteString(f.Volatility)
	}

	if f.IsSecurityDefiner {
		sb.WriteString(" SECURITY DEFINER")
	}

	if f.IsStrict {
		sb.WriteString(" STRICT")
	}

	return sb.String(), nil
}

func formatTriggerDefinition(t *schema.Trigger) (string, error) {
	if t == nil {
		return "", errors.New("trigger cannot be nil")
	}

	if t.Name == "" {
		return "", errors.New("trigger name cannot be empty")
	}

	if len(t.Events) == 0 {
		return "", errors.New("trigger requires at least one event")
	}

	if t.Timing == "" {
		return "", errors.New("trigger timing cannot be empty")
	}

	if t.FunctionName == "" {
		return "", errors.New("trigger function name cannot be empty")
	}

	var sb strings.Builder

	sb.WriteString("CREATE TRIGGER ")
	sb.WriteString(QuoteIdentifier(t.Name))
	sb.WriteString("\n")
	sb.WriteString(t.Timing)
	sb.WriteString(" ")
	sb.WriteString(strings.Join(t.Events, " OR "))
	sb.WriteString(" ON ")
	sb.WriteString(QualifiedName(t.Schema, t.TableName))
	sb.WriteString("\n")

	if t.ForEachRow {
		sb.WriteString("FOR EACH ROW")
	} else {
		sb.WriteString("FOR EACH STATEMENT")
	}

	if t.WhenCondition != "" {
		sb.WriteString("\nWHEN ")
		sb.WriteString(t.WhenCondition)
	}

	sb.WriteString("\nEXECUTE FUNCTION ")

	funcNameUpper := strings.ToUpper(t.FunctionName)

	funcSchema := t.FunctionSchema
	if funcSchema == "" {
		funcSchema = schema.DefaultSchema
	}

	sb.WriteString(QuoteIdentifier(funcSchema))
	sb.WriteString(".")
	sb.WriteString(funcNameUpper)

	sb.WriteString("()")

	return sb.String(), nil
}

func formatCreateHypertable(ht *schema.Hypertable) (string, error) {
	if ht == nil {
		return "", errors.New("hypertable cannot be nil")
	}

	if ht.TableName == "" {
		return "", errors.New("hypertable table name cannot be empty")
	}

	if ht.TimeColumnName == "" {
		return "", errors.New("hypertable time column cannot be empty")
	}

	args := []string{
		fmt.Sprintf("'%s'", QualifiedName(ht.Schema, ht.TableName)),
		fmt.Sprintf("'%s'", ht.TimeColumnName),
	}

	if ht.PartitionInterval != "" {
		args = append(
			args,
			fmt.Sprintf("chunk_time_interval => INTERVAL '%s'", ht.PartitionInterval),
		)
	}

	if ht.SpacePartitions > 0 {
		args = append(args, fmt.Sprintf("number_partitions => %d", ht.SpacePartitions))
	}

	return fmt.Sprintf("SELECT create_hypertable(%s)", strings.Join(args, ", ")), nil
}

func formatCompressionPolicy(ht *schema.Hypertable) (string, error) {
	if ht == nil {
		return "", errors.New("hypertable cannot be nil")
	}

	if !ht.CompressionEnabled {
		return "", nil
	}

	tableName := QualifiedName(ht.Schema, ht.TableName)

	var options []string

	segmentColumns := dedupeCompressionColumns(ht.CompressionSettings.SegmentByColumns)
	if len(segmentColumns) > 0 {
		options = append(options, fmt.Sprintf("timescaledb.compress_segmentby = '%s'",
			strings.Join(segmentColumns, ",")))
	}

	orderColumns := dedupeCompressionOrderColumns(ht.CompressionSettings.OrderByColumns)
	if len(orderColumns) > 0 {
		var orderBy []string
		for _, col := range orderColumns {
			orderBy = append(orderBy, fmt.Sprintf("%s %s", col.Column, col.Direction))
		}

		options = append(
			options,
			fmt.Sprintf("timescaledb.compress_orderby = '%s'", strings.Join(orderBy, ",")),
		)
	}

	if len(options) > 0 {
		return fmt.Sprintf(
			"ALTER TABLE %s SET (timescaledb.compress, %s)",
			tableName,
			strings.Join(options, ", "),
		), nil
	}

	return fmt.Sprintf("ALTER TABLE %s SET (timescaledb.compress)", tableName), nil
}

func formatRetentionPolicy(ht *schema.Hypertable) (string, error) {
	if ht == nil {
		return "", errors.New("hypertable cannot be nil")
	}

	if ht.RetentionPolicy == nil || ht.RetentionPolicy.DropAfter == "" {
		return "", nil
	}

	tableName := QualifiedName(ht.Schema, ht.TableName)

	return fmt.Sprintf(
		"SELECT add_retention_policy('%s', INTERVAL '%s')",
		tableName,
		ht.RetentionPolicy.DropAfter,
	), nil
}

func dedupeCompressionColumns(columns []string) []string {
	result := make([]string, 0, len(columns))
	seen := make(map[string]struct{}, len(columns))

	for _, col := range columns {
		clean := strings.TrimSpace(col)
		if clean == "" {
			continue
		}

		if _, exists := seen[clean]; exists {
			continue
		}

		seen[clean] = struct{}{}
		result = append(result, clean)
	}

	return result
}

func dedupeCompressionOrderColumns(cols []schema.OrderByColumn) []schema.OrderByColumn {
	result := make([]schema.OrderByColumn, 0, len(cols))
	seen := make(map[string]struct{}, len(cols))

	for _, col := range cols {
		cleanCol := strings.TrimSpace(col.Column)
		if cleanCol == "" {
			continue
		}

		direction := strings.ToUpper(strings.TrimSpace(col.Direction))
		if direction == "" {
			direction = "ASC"
		}

		key := fmt.Sprintf("%s|%s", cleanCol, direction)
		if _, exists := seen[key]; exists {
			continue
		}

		seen[key] = struct{}{}

		result = append(result, schema.OrderByColumn{
			Column:    cleanCol,
			Direction: direction,
		})
	}

	return result
}
