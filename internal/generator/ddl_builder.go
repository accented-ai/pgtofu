package generator

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

type DDLBuilder struct {
	idempotent bool
	result     *differ.DiffResult
	registry   *DDLBuilderRegistry
}

func NewDDLBuilder(result *differ.DiffResult, idempotent bool) *DDLBuilder {
	return &DDLBuilder{
		idempotent: idempotent,
		result:     result,
		registry:   NewDDLBuilderRegistry(),
	}
}

func (b *DDLBuilder) BuildUpStatement(change differ.Change) (DDLStatement, error) {
	return b.registry.BuildUp(change, b)
}

func (b *DDLBuilder) BuildDownStatement(change differ.Change) (DDLStatement, error) {
	return b.registry.BuildDown(change, b)
}

func (b *DDLBuilder) ifExists() string {
	if b.idempotent {
		return "IF EXISTS "
	}

	return ""
}

func (b *DDLBuilder) ifNotExists() string {
	if b.idempotent {
		return "IF NOT EXISTS "
	}

	return ""
}

func (b *DDLBuilder) buildDropTableForDown(change differ.Change) (DDLStatement, error) {
	table := b.getTable(change.ObjectName, b.result.Desired)
	if table == nil {
		return DDLStatement{}, fmt.Errorf("table not found: %s", change.ObjectName)
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

func (b *DDLBuilder) buildAddTableForDown(change differ.Change) (DDLStatement, error) {
	table := b.getTable(change.ObjectName, b.result.Current)
	if table == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddTableForDown",
			&change,
			wrapObjectNotFoundError(ErrTableNotFound, "table", change.ObjectName),
		)
	}

	tableSQL, err := buildCreateTableSQL(table)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddTableForDown", &change, err)
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

	ht := b.getHypertable(change.ObjectName, b.result.Current)
	if err := b.appendHypertableSQL(&sb, ht); err != nil {
		return DDLStatement{}, newGeneratorError("buildAddTableForDown", &change, err)
	}

	if table.Comment != "" {
		commentSQL := buildCommentStatement(
			"TABLE",
			QualifiedName(table.Schema, table.Name),
			table.Comment,
			false,
		)
		appendStatement(&sb, commentSQL)
	}

	for _, col := range table.Columns {
		if col.Comment != "" {
			target := fmt.Sprintf("%s.%s",
				QualifiedName(table.Schema, table.Name),
				QuoteIdentifier(col.Name))
			commentSQL := buildCommentStatement("COLUMN", target, col.Comment, false)
			appendStatement(&sb, commentSQL)
		}
	}

	return DDLStatement{
		SQL:         sb.String(),
		Description: "Add table " + table.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) appendHypertableSQL(sb *strings.Builder, ht *schema.Hypertable) error {
	if ht == nil {
		return nil
	}

	hypertableSQL, err := formatCreateHypertable(ht)
	if err != nil {
		return err
	}

	appendStatement(sb, hypertableSQL)

	if ht.CompressionEnabled && ht.CompressionSettings != nil {
		compressionSQL, err := formatCompressionPolicy(ht)
		if err != nil {
			return err
		}

		if compressionSQL != "" {
			appendStatement(sb, compressionSQL)
		}
	}

	if ht.RetentionPolicy != nil && ht.RetentionPolicy.DropAfter != "" {
		retentionSQL, err := formatRetentionPolicy(ht)
		if err != nil {
			return err
		}

		if retentionSQL != "" {
			appendStatement(sb, retentionSQL)
		}
	}

	return nil
}

func (b *DDLBuilder) getSchema(name string, db *schema.Database) *schema.Schema {
	for i := range db.Schemas {
		if db.Schemas[i].Name == name {
			return &db.Schemas[i]
		}
	}

	return nil
}

func (b *DDLBuilder) getExtension(name string, db *schema.Database) *schema.Extension {
	for i := range db.Extensions {
		if db.Extensions[i].Name == name {
			return &db.Extensions[i]
		}
	}

	return nil
}

func (b *DDLBuilder) getCustomType(name string, db *schema.Database) *schema.CustomType {
	for i := range db.CustomTypes {
		if db.CustomTypes[i].QualifiedName() == name {
			return &db.CustomTypes[i]
		}
	}

	return nil
}

func (b *DDLBuilder) getSequence(name string, db *schema.Database) *schema.Sequence {
	for i := range db.Sequences {
		if db.Sequences[i].QualifiedName() == name {
			return &db.Sequences[i]
		}
	}

	return nil
}

func (b *DDLBuilder) getTable(name string, db *schema.Database) *schema.Table {
	return db.GetTable(parseSchemaAndName(name))
}

func (b *DDLBuilder) getView(name string, db *schema.Database) *schema.View {
	return db.GetView(parseSchemaAndName(name))
}

func (b *DDLBuilder) findView(name string) *schema.View {
	if v := b.getView(name, b.result.Current); v != nil {
		return v
	}

	return b.getView(name, b.result.Desired)
}

func (b *DDLBuilder) getMaterializedView(
	name string,
	db *schema.Database,
) *schema.MaterializedView {
	for i := range db.MaterializedViews {
		if db.MaterializedViews[i].QualifiedName() == name {
			return &db.MaterializedViews[i]
		}
	}

	return nil
}

func (b *DDLBuilder) findMaterializedView(name string) *schema.MaterializedView {
	if mv := b.getMaterializedView(name, b.result.Current); mv != nil {
		return mv
	}

	return b.getMaterializedView(name, b.result.Desired)
}

func (b *DDLBuilder) getFunction(name string, db *schema.Database) *schema.Function {
	for i := range db.Functions {
		fn := &db.Functions[i]

		key := differ.FunctionKey(fn.Schema, fn.Name, fn.ArgumentTypes)
		if key == name {
			return fn
		}
	}

	return nil
}

func (b *DDLBuilder) getTrigger(name string, db *schema.Database) *schema.Trigger {
	parts := parseTriggerName(name)
	if len(parts) < 2 {
		return nil
	}

	var (
		nameSchema  string
		nameTable   string
		nameTrigger string
	)

	twoPart := len(parts) == 2
	if twoPart {
		nameSchema = schema.DefaultSchema
		nameTable = parts[0]
		nameTrigger = parts[1]
	} else {
		nameSchema = parts[0]
		nameTable = parts[1]
		nameTrigger = parts[2]
	}

	for i := range db.Triggers {
		t := &db.Triggers[i]

		trigSchema := t.Schema

		if schema.NormalizeSchemaName(trigSchema) == schema.NormalizeSchemaName(nameSchema) &&
			schema.NormalizeIdentifier(t.TableName) == schema.NormalizeIdentifier(nameTable) &&
			schema.NormalizeIdentifier(t.Name) == schema.NormalizeIdentifier(nameTrigger) {
			return t
		}

		if twoPart &&
			schema.NormalizeIdentifier(t.TableName) == schema.NormalizeIdentifier(nameTable) &&
			schema.NormalizeIdentifier(t.Name) == schema.NormalizeIdentifier(nameTrigger) {
			return t
		}
	}

	return nil
}

func (b *DDLBuilder) getHypertable(name string, db *schema.Database) *schema.Hypertable {
	nameSchema, nameTable := parseSchemaAndName(name)
	if nameSchema == "" {
		nameSchema = schema.DefaultSchema
	}

	nameKey := fmt.Sprintf("%s.%s", strings.ToLower(nameSchema), strings.ToLower(nameTable))

	for i := range db.Hypertables {
		ht := &db.Hypertables[i]

		htSchema := ht.Schema
		if htSchema == "" {
			htSchema = schema.DefaultSchema
		}

		htKey := fmt.Sprintf("%s.%s", strings.ToLower(htSchema), strings.ToLower(ht.TableName))
		if htKey == nameKey {
			return ht
		}
	}

	return nil
}

func (b *DDLBuilder) findHypertable(name string) *schema.Hypertable {
	if ht := b.getHypertable(name, b.result.Current); ht != nil {
		return ht
	}

	return b.getHypertable(name, b.result.Desired)
}

func (b *DDLBuilder) getContinuousAggregate(
	name string,
	db *schema.Database,
) *schema.ContinuousAggregate {
	return db.GetContinuousAggregate(parseSchemaAndName(name))
}

func (b *DDLBuilder) findContinuousAggregate(name string) *schema.ContinuousAggregate {
	if ca := b.getContinuousAggregate(name, b.result.Current); ca != nil {
		return ca
	}

	return b.getContinuousAggregate(name, b.result.Desired)
}
