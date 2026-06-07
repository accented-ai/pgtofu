package generator

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
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

		if strings.HasPrefix(strings.ToUpper(def), "CHECK") {
			buf.Write(formatCheckConstraintDefinition(def))
		} else {
			buf.Write("CHECK")
			buf.Write(def)
		}

	case "EXCLUDE":
		if strings.TrimSpace(c.Definition) == "" {
			return "", errors.New("exclude constraint requires a definition")
		}

		def := strings.TrimSpace(c.Definition)
		if !strings.HasPrefix(strings.ToUpper(def), "EXCLUDE") {
			def = "EXCLUDE " + def
		}

		lines := strings.Split(def, "\n")

		nonEmpty := lines[:0]
		for _, line := range lines {
			trimmed := strings.TrimRight(line, " \t")
			if strings.TrimSpace(trimmed) != "" {
				nonEmpty = append(nonEmpty, trimmed)
			}
		}

		lines = nonEmpty

		if len(lines) > 1 {
			minIndent := -1

			for i := 1; i < len(lines); i++ {
				indent := len(lines[i]) - len(strings.TrimLeft(lines[i], " \t"))
				if minIndent == -1 || indent < minIndent {
					minIndent = indent
				}
			}

			for i := 1; i < len(lines); i++ {
				trimmed := strings.TrimLeft(lines[i], " \t")
				if strings.TrimSpace(trimmed) == ")" {
					lines[i] = trimmed
				} else {
					lines[i] = sqlIndent + trimmed
				}
			}
		}

		buf.Write(strings.Join(lines, "\n"))

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

func formatCheckConstraintDefinition(def string) string {
	lines := compactSQLLines(def)
	if len(lines) == 0 {
		return ""
	}

	if len(lines) == 1 {
		if formatted, ok := formatSingleLineCheckConstraintDefinition(lines[0]); ok {
			return formatted
		}

		return lines[0]
	}

	lines = splitCheckFirstLine(lines)
	lines = splitLeadingClosingParens(lines)
	lines = splitAttachedClosingParens(lines)

	// Match SQLFluff's multiline CHECK layout without reflowing single-line checks.
	return indentCheckConstraintLines(lines)
}

func formatSingleLineCheckConstraintDefinition(def string) (string, bool) {
	content, ok := checkConstraintContent(def)
	if !ok {
		return "", false
	}

	content, _ = unwrapOuterParens(content)

	terms := splitTopLevelKeyword(content, "OR")
	if len(terms) < 2 || !shouldFormatSingleLineCheckTerms(terms) {
		return "", false
	}

	formatted := []string{
		"CHECK (",
		sqlIndent + "(",
	}

	for i, term := range terms {
		term = strings.TrimSpace(term)
		if i == 0 {
			formatted = append(formatted, sqlIndent+sqlIndent+term)
		} else {
			formatted = append(formatted, sqlIndent+sqlIndent+"OR "+term)
		}
	}

	formatted = append(formatted, sqlIndent+")", ")")

	return strings.Join(formatted, "\n"), true
}

func checkConstraintContent(def string) (string, bool) {
	if !strings.HasPrefix(strings.ToUpper(def), "CHECK") {
		return "", false
	}

	remainder := strings.TrimSpace(def[len("CHECK"):])
	if !strings.HasPrefix(remainder, "(") {
		return "", false
	}

	closeIdx := matchingParenIndex(remainder, 0)
	if closeIdx == -1 || strings.TrimSpace(remainder[closeIdx+1:]) != "" {
		return "", false
	}

	return strings.TrimSpace(remainder[1:closeIdx]), true
}

func shouldFormatSingleLineCheckTerms(terms []string) bool {
	for _, term := range terms {
		if countKeywordOutsideQuotes(term, "AND") >= 3 {
			return true
		}
	}

	return false
}

func compactSQLLines(sql string) []string {
	rawLines := strings.Split(sql, "\n")
	lines := make([]string, 0, len(rawLines))

	for _, line := range rawLines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			lines = append(lines, trimmed)
		}
	}

	return lines
}

func unwrapOuterParens(expr string) (string, int) {
	unwrapped := strings.TrimSpace(expr)
	count := 0

	for isWrappedInParens(unwrapped) {
		unwrapped = strings.TrimSpace(unwrapped[1 : len(unwrapped)-1])
		count++
	}

	return unwrapped, count
}

func isWrappedInParens(expr string) bool {
	expr = strings.TrimSpace(expr)
	if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
		return false
	}

	return matchingParenIndex(expr, 0) == len(expr)-1
}

func splitCheckFirstLine(lines []string) []string {
	first := strings.TrimSpace(lines[0])

	openIdx := strings.Index(first, "(")
	if openIdx == -1 || !strings.HasPrefix(strings.ToUpper(first), "CHECK") {
		lines[0] = first

		return lines
	}

	prefixEnd := openIdx + 1
	for prefixEnd < len(first) && first[prefixEnd] == '(' {
		prefixEnd++
	}

	body := strings.TrimSpace(first[prefixEnd:])
	if body == "" || parenBalance(body) > 0 {
		lines[0] = first

		return lines
	}

	split := make([]string, 0, len(lines)+1)
	split = append(split, strings.TrimSpace(first[:prefixEnd]), body)
	split = append(split, lines[1:]...)

	return split
}

func splitLeadingClosingParens(lines []string) []string {
	split := make([]string, 0, len(lines))

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if i == 0 {
			split = append(split, trimmed)

			continue
		}

		closingCount := 0
		for closingCount < len(trimmed) && trimmed[closingCount] == ')' {
			closingCount++
		}

		if closingCount > 0 && closingCount < len(trimmed) {
			split = append(split, strings.Repeat(")", closingCount))
			split = append(split, strings.TrimSpace(trimmed[closingCount:]))
		} else {
			split = append(split, trimmed)
		}
	}

	return split
}

func splitAttachedClosingParens(lines []string) []string {
	split := make([]string, 0, len(lines))

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if i == 0 || isOnlyClosingParens(trimmed) {
			split = append(split, trimmed)

			continue
		}

		body, closingParens := trimTrailingClosingParens(trimmed)
		if body == "" || closingParens == "" {
			split = append(split, trimmed)

			continue
		}

		keepClosingCount := parenBalance(body)
		if keepClosingCount < 0 {
			keepClosingCount = 0
		}

		if keepClosingCount > len(closingParens) {
			keepClosingCount = len(closingParens)
		}

		split = append(split, body+closingParens[:keepClosingCount])

		if keepClosingCount < len(closingParens) {
			split = append(split, closingParens[keepClosingCount:])
		}
	}

	return split
}

func trimTrailingClosingParens(line string) (string, string) {
	end := len(line)
	for end > 0 && line[end-1] == ')' {
		end--
	}

	if end == len(line) {
		return line, ""
	}

	return strings.TrimSpace(line[:end]), line[end:]
}

func indentCheckConstraintLines(lines []string) string {
	if len(lines) == 0 {
		return ""
	}

	formatted := make([]string, 0, len(lines))
	first := strings.TrimSpace(lines[0])
	formatted = append(formatted, first)

	baseDepth := parenBalance(first)
	if baseDepth < 1 {
		baseDepth = 1
	}

	depth := baseDepth

	for _, line := range lines[1:] {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		if isOnlyClosingParens(trimmed) {
			formatted, depth = appendIndentedClosingParens(formatted, trimmed, depth, baseDepth)

			continue
		}

		indentLevel := depth - baseDepth + 1
		if indentLevel < 0 {
			indentLevel = 0
		}

		formatted = append(formatted, strings.Repeat(sqlIndent, indentLevel)+trimmed)

		depth += parenBalance(trimmed)
		if depth < 0 {
			depth = 0
		}
	}

	return strings.Join(formatted, "\n")
}

func splitTopLevelKeyword(sql string, keyword string) []string {
	parts := []string{}
	start := 0
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		switch {
		case inSingleQuote:
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
				} else {
					inSingleQuote = false
				}
			}
		case inDoubleQuote:
			if ch == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					i++
				} else {
					inDoubleQuote = false
				}
			}
		case ch == '\'':
			inSingleQuote = true
		case ch == '"':
			inDoubleQuote = true
		case ch == '(':
			depth++
		case ch == ')':
			depth--
		default:
			if depth == 0 && matchesKeywordAt(sql, i, keyword) {
				parts = append(parts, strings.TrimSpace(sql[start:i]))
				i += len(keyword) - 1
				start = i + 1
			}
		}
	}

	parts = append(parts, strings.TrimSpace(sql[start:]))

	return parts
}

func countKeywordOutsideQuotes(sql string, keyword string) int {
	count := 0
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		switch {
		case inSingleQuote:
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
				} else {
					inSingleQuote = false
				}
			}
		case inDoubleQuote:
			if ch == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					i++
				} else {
					inDoubleQuote = false
				}
			}
		case ch == '\'':
			inSingleQuote = true
		case ch == '"':
			inDoubleQuote = true
		default:
			if matchesKeywordAt(sql, i, keyword) {
				count++
				i += len(keyword) - 1
			}
		}
	}

	return count
}

func matchesKeywordAt(sql string, idx int, keyword string) bool {
	if idx < 0 || idx+len(keyword) > len(sql) {
		return false
	}

	if !strings.EqualFold(sql[idx:idx+len(keyword)], keyword) {
		return false
	}

	return isKeywordBoundary(sql, idx-1) && isKeywordBoundary(sql, idx+len(keyword))
}

func isKeywordBoundary(sql string, idx int) bool {
	if idx < 0 || idx >= len(sql) {
		return true
	}

	ch := sql[idx]

	return (ch < 'a' || ch > 'z') &&
		(ch < 'A' || ch > 'Z') &&
		(ch < '0' || ch > '9') &&
		ch != '_'
}

func appendIndentedClosingParens(
	formatted []string,
	closingParens string,
	depth int,
	baseDepth int,
) ([]string, int) {
	remaining := len(closingParens)
	for remaining > 0 && depth > baseDepth {
		indentLevel := depth - baseDepth
		formatted = append(formatted, strings.Repeat(sqlIndent, indentLevel)+")")
		depth--
		remaining--
	}

	if remaining > 0 {
		indentLevel := depth - baseDepth
		if indentLevel < 0 {
			indentLevel = 0
		}

		formatted = append(
			formatted,
			strings.Repeat(sqlIndent, indentLevel)+strings.Repeat(")", remaining),
		)
		depth -= remaining
	}

	if depth < 0 {
		depth = 0
	}

	return formatted, depth
}

func isOnlyClosingParens(line string) bool {
	if line == "" {
		return false
	}

	for _, r := range line {
		if r != ')' {
			return false
		}
	}

	return true
}

func matchingParenIndex(sql string, openIdx int) int {
	if openIdx < 0 || openIdx >= len(sql) || sql[openIdx] != '(' {
		return -1
	}

	depth := 0
	inSingleQuote := false
	inDoubleQuote := false

	for i := openIdx; i < len(sql); i++ {
		ch := sql[i]

		switch {
		case inSingleQuote:
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
				} else {
					inSingleQuote = false
				}
			}
		case inDoubleQuote:
			if ch == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					i++
				} else {
					inDoubleQuote = false
				}
			}
		case ch == '\'':
			inSingleQuote = true
		case ch == '"':
			inDoubleQuote = true
		case ch == '(':
			depth++
		case ch == ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

func parenBalance(sql string) int {
	balance := 0
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		switch {
		case inSingleQuote:
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
				} else {
					inSingleQuote = false
				}
			}
		case inDoubleQuote:
			if ch == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					i++
				} else {
					inDoubleQuote = false
				}
			}
		case ch == '\'':
			inSingleQuote = true
		case ch == '"':
			inDoubleQuote = true
		case ch == '(':
			balance++
		case ch == ')':
			balance--
		}
	}

	return balance
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

	if idx.NullsNotDistinct {
		buf.Write("NULLS NOT DISTINCT")
	}

	if len(idx.IncludeColumns) > 0 {
		buf.Write("INCLUDE")
		buf.Write(fmt.Sprintf("(%s)", quoteColumns(idx.IncludeColumns)))
	}

	if len(idx.StorageParams) > 0 {
		buf.Write("WITH")
		buf.Write(fmt.Sprintf("(%s)", formatStorageParams(idx.StorageParams)))
	}

	if idx.Where != "" {
		buf.Write("WHERE")
		buf.Write(NormalizeWhereClause(idx.Where))
	}

	return buf.String(), nil
}

func formatStorageParams(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, fmt.Sprintf("%s = %s", k, params[k]))
	}

	return strings.Join(pairs, ", ")
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

func formatFunctionDefinition(f *schema.Function) (string, error) {
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

	funcSignature += f.ArgumentSignature()

	body := strings.TrimSpace(f.Body)
	if strings.HasPrefix(body, "$$") && strings.HasSuffix(body, "$$") && len(body) >= 4 {
		body = strings.TrimPrefix(body, "$$")
		body = strings.TrimSuffix(body, "$$")
		body = strings.TrimSpace(body)
	}

	var sb strings.Builder
	sb.WriteString("CREATE OR REPLACE FUNCTION ")
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
	sb.WriteString(t.EventList())
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
