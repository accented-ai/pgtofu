package generator

import (
	"errors"
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

const sqlIndent = "    "

func parseSchemaAndName(qualifiedName string) (string, string) {
	parts := strings.SplitN(qualifiedName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}

	return "", parts[0]
}

func parseTriggerName(name string) []string {
	parts := strings.Split(name, ".")
	if len(parts) < 2 {
		return nil
	}

	return parts
}

func formatEnumValues(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = formatSQLStringLiteral(v)
	}

	return strings.Join(quoted, ", ")
}

func buildSequenceSQL(seq *schema.Sequence) (string, error) {
	if seq == nil {
		return "", errors.New("sequence cannot be nil")
	}

	if seq.Name == "" {
		return "", errors.New("sequence name cannot be empty")
	}

	var buf tokenBuffer
	buf.Write("CREATE SEQUENCE")
	buf.Write(QualifiedName(seq.Schema, seq.Name))

	if seq.DataType != "" && seq.DataType != "bigint" {
		buf.Write("AS")
		buf.Write(seq.DataType)
	}

	if seq.Increment != 1 {
		buf.Write(fmt.Sprintf("INCREMENT BY %d", seq.Increment))
	}

	if seq.MinValue != 1 {
		buf.Write(fmt.Sprintf("MINVALUE %d", seq.MinValue))
	}

	if seq.MaxValue != 0 {
		buf.Write(fmt.Sprintf("MAXVALUE %d", seq.MaxValue))
	}

	if seq.StartValue != 1 {
		buf.Write(fmt.Sprintf("START WITH %d", seq.StartValue))
	}

	if seq.CacheSize != 1 {
		buf.Write(fmt.Sprintf("CACHE %d", seq.CacheSize))
	}

	if seq.IsCyclic {
		buf.Write("CYCLE")
	}

	return ensureStatementTerminated(buf.String()), nil
}

func buildCreateTableSQL(table *schema.Table) (string, error) {
	if table == nil {
		return "", errors.New("table cannot be nil")
	}

	if table.Name == "" {
		return "", errors.New("table name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(QualifiedName(table.Schema, table.Name))
	sb.WriteString(" (\n")

	columnCount := 0

	appendLine := func(definition string) {
		if columnCount > 0 {
			sb.WriteString(",\n")
		}

		sb.WriteString(definition)

		columnCount++
	}

	for i := range table.Columns {
		definition, err := formatColumnDefinition(&table.Columns[i])
		if err != nil {
			return "", err
		}

		appendLine(sqlIndent + definition)
	}

	for i := range table.Constraints {
		definition, err := formatConstraintDefinition(&table.Constraints[i])
		if err != nil {
			return "", err
		}

		if definition == "" {
			continue
		}

		appendLine(indentMultiline(definition))
	}

	sb.WriteString("\n)")

	if table.PartitionStrategy != nil {
		columns := make([]string, len(table.PartitionStrategy.Columns))
		for i, col := range table.PartitionStrategy.Columns {
			columns[i] = QuoteIdentifier(col)
		}

		sb.WriteString(" PARTITION BY ")
		sb.WriteString(table.PartitionStrategy.Type)
		sb.WriteString(" (")
		sb.WriteString(strings.Join(columns, ", "))
		sb.WriteString(")")
	}

	return ensureStatementTerminated(sb.String()), nil
}

func buildContinuousAggregateSQL(ca *schema.ContinuousAggregate) (string, error) {
	if ca == nil {
		return "", errors.New("continuous aggregate cannot be nil")
	}

	if ca.ViewName == "" {
		return "", errors.New("continuous aggregate view name cannot be empty")
	}

	if strings.TrimSpace(ca.Query) == "" {
		return "", errors.New("continuous aggregate query cannot be empty")
	}

	var sql strings.Builder

	sql.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW %s\nWITH (timescaledb.continuous) AS\n%s",
		QualifiedName(ca.Schema, ca.ViewName),
		ca.Query))

	if ca.WithData {
		sql.WriteString("\nWITH DATA;")
	} else {
		sql.WriteString("\nWITH NO DATA;")
	}

	if ca.RefreshPolicy != nil {
		sql.WriteString(fmt.Sprintf("\n\nSELECT add_continuous_aggregate_policy('%s',\n"+
			sqlIndent+"start_offset => INTERVAL '%s',\n"+
			sqlIndent+"end_offset => INTERVAL '%s',\n"+
			sqlIndent+"schedule_interval => INTERVAL '%s');",
			QualifiedName(ca.Schema, ca.ViewName),
			ca.RefreshPolicy.StartOffset,
			ca.RefreshPolicy.EndOffset,
			ca.RefreshPolicy.ScheduleInterval))
	}

	if ca.Comment != "" {
		sql.WriteString("\n\n")
		sql.WriteString(buildCommentStatement(
			"VIEW",
			QualifiedName(ca.Schema, ca.ViewName),
			ca.Comment,
			false,
		))
	}

	return sql.String(), nil
}

func indentMultiline(definition string) string {
	lines := strings.Split(definition, "\n")
	for i := range lines {
		lines[i] = sqlIndent + lines[i]
	}

	return strings.Join(lines, "\n")
}
