package generator

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func (b *DDLBuilder) buildAddTable(change differ.Change) (DDLStatement, error) {
	table := b.getTable(change.ObjectName, b.result.Desired)
	if table == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddTable",
			&change,
			wrapObjectNotFoundError(ErrTableNotFound, "table", change.ObjectName),
		)
	}

	tableSQL, err := buildCreateTableSQL(table)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddTable", &change, err)
	}

	var sb strings.Builder
	appendStatement(&sb, tableSQL)

	if table.PartitionStrategy != nil && len(table.PartitionStrategy.Partitions) > 0 {
		for _, partition := range table.PartitionStrategy.Partitions {
			if partition.Definition == "" {
				continue
			}

			statement := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s\n%s",
				QualifiedName(table.Schema, partition.Name),
				QualifiedName(table.Schema, table.Name),
				partition.Definition,
			)

			appendStatement(&sb, statement)
		}
	}

	return DDLStatement{
		SQL:         sb.String(),
		Description: "Add table " + table.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildDropTable(change differ.Change) (DDLStatement, error) {
	table := b.getTable(change.ObjectName, b.result.Current)
	if table == nil {
		return DDLStatement{}, newGeneratorError(
			"buildDropTable",
			&change,
			wrapObjectNotFoundError(ErrTableNotFound, "table", change.ObjectName),
		)
	}

	sql := fmt.Sprintf("DROP TABLE %s%s CASCADE;",
		b.ifExists(), QualifiedName(table.Schema, table.Name))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop table " + table.Name,
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildAddColumn(change differ.Change) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddColumn", &change, err)
	}

	column, err := getDetailColumn(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddColumn", &change, err)
	}

	table := b.getTable(tableName, b.result.Desired)
	if table == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddColumn",
			&change,
			wrapObjectNotFoundError(ErrTableNotFound, "table", tableName),
		)
	}

	definition, err := formatColumnDefinition(column)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddColumn", &change, err)
	}

	sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;",
		QualifiedName(table.Schema, table.Name),
		definition)

	isUnsafe := !column.IsNullable && column.Default == ""

	stmt := DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("Add column %s.%s", table.Name, column.Name),
		IsUnsafe:    isUnsafe,
		RequiresTx:  true,
	}

	return b.wrapWithCompressionToggle(stmt, tableName)
}

func (b *DDLBuilder) buildDropColumn(change differ.Change) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildDropColumn", &change, err)
	}

	column, err := getDetailColumn(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildDropColumn", &change, err)
	}

	table := b.getTable(tableName, b.result.Current)
	if table == nil {
		return DDLStatement{}, newGeneratorError(
			"buildDropColumn",
			&change,
			wrapObjectNotFoundError(ErrTableNotFound, "table", tableName),
		)
	}

	sql := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s%s;",
		QualifiedName(table.Schema, table.Name),
		b.ifExists(),
		QuoteIdentifier(column.Name))

	stmt := DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("Drop column %s.%s", table.Name, column.Name),
		IsUnsafe:    true,
		RequiresTx:  true,
	}

	return b.wrapWithCompressionToggle(stmt, tableName)
}

func (b *DDLBuilder) buildModifyColumnType(change differ.Change) (DDLStatement, error) {
	stmt, err := b.buildColumnTypeChange(change, b.result.Desired, DetailKeyNewType, "Modify")
	if err != nil {
		return DDLStatement{}, err
	}

	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyColumnType", &change, err)
	}

	return b.wrapWithCompressionToggle(stmt, tableName)
}

func (b *DDLBuilder) buildReverseModifyColumnType(change differ.Change) (DDLStatement, error) {
	stmt, err := b.buildColumnTypeChange(change, b.result.Current, DetailKeyOldType, "Revert")
	if err != nil {
		return DDLStatement{}, err
	}

	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildReverseModifyColumnType", &change, err)
	}

	return b.wrapWithCompressionToggle(stmt, tableName)
}

func (b *DDLBuilder) buildColumnTypeChange(
	change differ.Change,
	db *schema.Database,
	typeKey DetailKey,
	action string,
) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnTypeChange", &change, err)
	}

	columnName, err := getDetailString(change.Details, DetailKeyColumnName)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnTypeChange", &change, err)
	}

	dataType, err := getDetailString(change.Details, typeKey)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnTypeChange", &change, err)
	}

	table := b.getTable(tableName, db)
	if table == nil {
		return DDLStatement{}, newGeneratorError(
			"buildColumnTypeChange",
			&change,
			wrapObjectNotFoundError(ErrTableNotFound, "table", tableName),
		)
	}

	sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;",
		QualifiedName(table.Schema, table.Name),
		QuoteIdentifier(columnName),
		dataType)

	return DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("%s column type %s.%s", action, table.Name, columnName),
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildModifyColumnNullability(change differ.Change) (DDLStatement, error) {
	stmt, err := b.buildColumnNullabilityChange(
		change, b.result.Desired, DetailKeyNewNullable, "Modify",
	)
	if err != nil {
		return DDLStatement{}, err
	}

	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyColumnNullability", &change, err)
	}

	return b.wrapWithCompressionToggle(stmt, tableName)
}

func (b *DDLBuilder) buildReverseModifyColumnNullability(
	change differ.Change,
) (DDLStatement, error) {
	stmt, err := b.buildColumnNullabilityChange(
		change, b.result.Current, DetailKeyOldNullable, "Revert",
	)
	if err != nil {
		return DDLStatement{}, err
	}

	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError(
			"buildReverseModifyColumnNullability", &change, err,
		)
	}

	return b.wrapWithCompressionToggle(stmt, tableName)
}

func (b *DDLBuilder) buildColumnNullabilityChange(
	change differ.Change,
	db *schema.Database,
	nullableKey DetailKey,
	action string,
) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnNullabilityChange", &change, err)
	}

	columnName, err := getDetailString(change.Details, DetailKeyColumnName)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnNullabilityChange", &change, err)
	}

	nullable, err := getDetailBool(change.Details, nullableKey)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnNullabilityChange", &change, err)
	}

	table := b.getTable(tableName, db)
	if table == nil {
		return DDLStatement{}, newGeneratorError(
			"buildColumnNullabilityChange",
			&change,
			wrapObjectNotFoundError(ErrTableNotFound, "table", tableName),
		)
	}

	operation := "SET NOT NULL"
	if nullable {
		operation = "DROP NOT NULL"
	}

	sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s;",
		QualifiedName(table.Schema, table.Name),
		QuoteIdentifier(columnName),
		operation)

	return DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("%s column nullability %s.%s", action, table.Name, columnName),
		IsUnsafe:    !nullable,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildModifyColumnDefault(change differ.Change) (DDLStatement, error) {
	return b.buildColumnDefaultChange(change, b.result.Desired, DetailKeyNewDefault, "Modify")
}

func (b *DDLBuilder) buildReverseModifyColumnDefault(change differ.Change) (DDLStatement, error) {
	return b.buildColumnDefaultChange(change, b.result.Current, DetailKeyOldDefault, "Revert")
}

func (b *DDLBuilder) buildColumnDefaultChange(
	change differ.Change,
	db *schema.Database,
	defaultKey DetailKey,
	action string,
) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnDefaultChange", &change, err)
	}

	columnName, err := getDetailString(change.Details, DetailKeyColumnName)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnDefaultChange", &change, err)
	}

	defaultValue, err := getDetailString(change.Details, defaultKey)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnDefaultChange", &change, err)
	}

	table := b.getTable(tableName, db)
	if table == nil {
		return DDLStatement{}, newGeneratorError(
			"buildColumnDefaultChange",
			&change,
			wrapObjectNotFoundError(ErrTableNotFound, "table", tableName),
		)
	}

	var sql string
	if defaultValue == "" {
		sql = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;",
			QualifiedName(table.Schema, table.Name),
			QuoteIdentifier(columnName))
	} else {
		sql = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;",
			QualifiedName(table.Schema, table.Name),
			QuoteIdentifier(columnName),
			defaultValue)
	}

	return DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("%s column default %s.%s", action, table.Name, columnName),
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildModifyTableComment(change differ.Change) (DDLStatement, error) {
	return b.buildTableCommentChange(change, b.result.Desired, DetailKeyNewComment, "Modify")
}

func (b *DDLBuilder) buildReverseModifyTableComment(change differ.Change) (DDLStatement, error) {
	oldComment, _, err := getOptionalDetailString(change.Details, DetailKeyOldComment)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildReverseModifyTableComment", &change, err)
	}

	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildReverseModifyTableComment", &change, err)
	}

	if oldComment == "" {
		schemaName, name := parseSchemaAndName(tableName)
		if schemaName == "" {
			schemaName = schema.DefaultSchema
		}

		sql := buildCommentStatement(
			"TABLE",
			QualifiedName(schemaName, name),
			oldComment,
			true,
		)

		return DDLStatement{
			SQL:         sql,
			Description: "Revert table comment " + name,
			RequiresTx:  true,
		}, nil
	}

	return b.buildTableCommentChange(change, b.result.Current, DetailKeyOldComment, "Revert")
}

func (b *DDLBuilder) buildTableCommentChange(
	change differ.Change,
	db *schema.Database,
	commentKey DetailKey,
	action string,
) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildTableCommentChange", &change, err)
	}

	comment, err := getDetailString(change.Details, commentKey)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildTableCommentChange", &change, err)
	}

	table := b.getTable(tableName, db)
	if table == nil {
		return DDLStatement{}, newGeneratorError(
			"buildTableCommentChange",
			&change,
			wrapObjectNotFoundError(ErrTableNotFound, "table", tableName),
		)
	}

	sql := buildCommentStatement(
		"TABLE",
		QualifiedName(table.Schema, table.Name),
		comment,
		true,
	)

	return DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("%s table comment %s", action, table.Name),
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildModifyColumnComment(change differ.Change) (DDLStatement, error) {
	return b.buildColumnCommentChange(change, b.result.Desired, DetailKeyNewComment, "Modify")
}

func (b *DDLBuilder) buildReverseModifyColumnComment(change differ.Change) (DDLStatement, error) {
	oldComment, _, err := getOptionalDetailString(change.Details, DetailKeyOldComment)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildReverseModifyColumnComment", &change, err)
	}

	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildReverseModifyColumnComment", &change, err)
	}

	columnName, err := getDetailString(change.Details, DetailKeyColumnName)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildReverseModifyColumnComment", &change, err)
	}

	if oldComment == "" {
		schemaName, name := parseSchemaAndName(tableName)
		if schemaName == "" {
			schemaName = schema.DefaultSchema
		}

		target := fmt.Sprintf(
			"%s.%s",
			QualifiedName(schemaName, name),
			QuoteIdentifier(columnName),
		)

		sql := buildCommentStatement("COLUMN", target, oldComment, false)

		return DDLStatement{
			SQL:         sql,
			Description: fmt.Sprintf("Revert column comment %s.%s", name, columnName),
			RequiresTx:  true,
		}, nil
	}

	return b.buildColumnCommentChange(change, b.result.Current, DetailKeyOldComment, "Revert")
}

func (b *DDLBuilder) buildColumnCommentChange(
	change differ.Change,
	db *schema.Database,
	commentKey DetailKey,
	action string,
) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnCommentChange", &change, err)
	}

	columnName, err := getDetailString(change.Details, DetailKeyColumnName)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnCommentChange", &change, err)
	}

	comment, err := getDetailString(change.Details, commentKey)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildColumnCommentChange", &change, err)
	}

	table := b.getTable(tableName, db)
	if table == nil {
		return DDLStatement{}, newGeneratorError(
			"buildColumnCommentChange",
			&change,
			wrapObjectNotFoundError(ErrTableNotFound, "table", tableName),
		)
	}

	target := fmt.Sprintf(
		"%s.%s",
		QualifiedName(table.Schema, table.Name),
		QuoteIdentifier(columnName),
	)

	sql := buildCommentStatement("COLUMN", target, comment, false)

	return DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("%s column comment %s.%s", action, table.Name, columnName),
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildAddConstraint(change differ.Change) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddConstraint", &change, err)
	}

	constraint, err := getDetailConstraint(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddConstraint", &change, err)
	}

	table := b.getTable(tableName, b.result.Desired)
	if table == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddConstraint",
			&change,
			wrapObjectNotFoundError(ErrTableNotFound, "table", tableName),
		)
	}

	definition, err := formatConstraintDefinition(constraint)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddConstraint", &change, err)
	}

	sql := fmt.Sprintf("ALTER TABLE %s ADD %s;",
		QualifiedName(table.Schema, table.Name),
		definition)

	stmt := DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("Add constraint %s.%s", table.Name, constraint.Name),
		IsUnsafe:    constraint.Type == "FOREIGN KEY",
		RequiresTx:  true,
	}

	return b.wrapWithCompressionToggle(stmt, tableName)
}

func (b *DDLBuilder) buildDropConstraint(change differ.Change) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildDropConstraint", &change, err)
	}

	constraint, err := getDetailConstraint(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildDropConstraint", &change, err)
	}

	schemaName, name := parseSchemaAndName(tableName)
	if schemaName == "" {
		schemaName = schema.DefaultSchema
	}

	sql := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s%s;",
		QualifiedName(schemaName, name),
		b.ifExists(),
		QuoteIdentifier(constraint.Name))

	stmt := DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("Drop constraint %s.%s", name, constraint.Name),
		IsUnsafe:    true,
		RequiresTx:  true,
	}

	return b.wrapWithCompressionToggle(stmt, tableName)
}

func (b *DDLBuilder) buildModifyConstraint(change differ.Change) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyConstraint", &change, err)
	}

	currentConstraint, err := getCurrentConstraint(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyConstraint", &change, err)
	}

	desiredConstraint, err := getDesiredConstraint(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyConstraint", &change, err)
	}

	schemaName, name := parseSchemaAndName(tableName)
	if schemaName == "" {
		schemaName = schema.DefaultSchema
	}

	qualifiedTable := QualifiedName(schemaName, name)

	dropSQL := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s%s;",
		qualifiedTable,
		b.ifExists(),
		QuoteIdentifier(currentConstraint.Name))

	definition, err := formatConstraintDefinition(desiredConstraint)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyConstraint", &change, err)
	}

	addSQL := fmt.Sprintf("ALTER TABLE %s ADD %s;", qualifiedTable, definition)

	sql := dropSQL + "\n" + addSQL

	stmt := DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("Modify constraint %s.%s", name, desiredConstraint.Name),
		IsUnsafe:    true,
		RequiresTx:  true,
	}

	return b.wrapWithCompressionToggle(stmt, tableName)
}

func (b *DDLBuilder) buildReverseModifyConstraint(change differ.Change) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildReverseModifyConstraint", &change, err)
	}

	currentConstraint, err := getCurrentConstraint(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildReverseModifyConstraint", &change, err)
	}

	desiredConstraint, err := getDesiredConstraint(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildReverseModifyConstraint", &change, err)
	}

	schemaName, name := parseSchemaAndName(tableName)
	if schemaName == "" {
		schemaName = schema.DefaultSchema
	}

	qualifiedTable := QualifiedName(schemaName, name)

	dropSQL := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s%s;",
		qualifiedTable,
		b.ifExists(),
		QuoteIdentifier(desiredConstraint.Name))

	definition, err := formatConstraintDefinition(currentConstraint)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildReverseModifyConstraint", &change, err)
	}

	addSQL := fmt.Sprintf("ALTER TABLE %s ADD %s;", qualifiedTable, definition)

	sql := dropSQL + "\n" + addSQL

	stmt := DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("Revert constraint %s.%s", name, currentConstraint.Name),
		IsUnsafe:    true,
		RequiresTx:  true,
	}

	return b.wrapWithCompressionToggle(stmt, tableName)
}

func (b *DDLBuilder) buildAddIndex(change differ.Change) (DDLStatement, error) {
	index, err := getDetailIndex(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddIndex", &change, err)
	}

	sql, err := formatIndexDefinition(index)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddIndex", &change, err)
	}

	return DDLStatement{
		SQL:         ensureStatementTerminated(sql),
		Description: "Add index " + index.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildDropIndex(change differ.Change) (DDLStatement, error) {
	index, err := getDetailIndex(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildDropIndex", &change, err)
	}

	sql := fmt.Sprintf("DROP INDEX %s%s;",
		b.ifExists(), QualifiedName(index.Schema, index.Name))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop index " + index.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildAddPartition(change differ.Change) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddPartition", &change, err)
	}

	partition, err := getDetailPartition(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddPartition", &change, err)
	}

	tableSchema, _ := parseSchemaAndName(tableName)
	if tableSchema == "" {
		tableSchema = schema.DefaultSchema
	}

	sql := fmt.Sprintf(
		"CREATE TABLE %s%s PARTITION OF %s\n%s;",
		b.ifNotExists(),
		QualifiedName(tableSchema, partition.Name),
		tableName,
		partition.Definition,
	)

	return DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("Add partition %s to %s", partition.Name, tableName),
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildDropPartition(change differ.Change) (DDLStatement, error) {
	tableName, err := getDetailString(change.Details, DetailKeyTable)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildDropPartition", &change, err)
	}

	partition, err := getDetailPartition(change.Details)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildDropPartition", &change, err)
	}

	tableSchema, _ := parseSchemaAndName(tableName)
	if tableSchema == "" {
		tableSchema = schema.DefaultSchema
	}

	sql := fmt.Sprintf("DROP TABLE %s%s;",
		b.ifExists(), QualifiedName(tableSchema, partition.Name))

	return DDLStatement{
		SQL:         sql,
		Description: fmt.Sprintf("Drop partition %s from %s", partition.Name, tableName),
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}
