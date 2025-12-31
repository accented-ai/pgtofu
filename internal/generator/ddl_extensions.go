package generator

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func (b *DDLBuilder) buildAddSchema(change differ.Change) (DDLStatement, error) {
	sch := b.getSchema(change.ObjectName, b.result.Desired)
	if sch == nil {
		schemaName := change.ObjectName
		sql := fmt.Sprintf("CREATE SCHEMA %s%s;", b.ifNotExists(), QuoteIdentifier(schemaName))

		return DDLStatement{
			SQL:         sql,
			Description: "Add schema " + schemaName,
			RequiresTx:  false,
		}, nil
	}

	sql := fmt.Sprintf("CREATE SCHEMA %s%s;", b.ifNotExists(), QuoteIdentifier(sch.Name))

	return DDLStatement{
		SQL:         sql,
		Description: "Add schema " + sch.Name,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildDropSchema(change differ.Change) (DDLStatement, error) {
	name := change.ObjectName
	if name == "" {
		return DDLStatement{}, fmt.Errorf("schema not found: %s", change.ObjectName)
	}

	sql := fmt.Sprintf("DROP SCHEMA %s%s CASCADE;", b.ifExists(), QuoteIdentifier(name))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop schema " + name,
		IsUnsafe:    true,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildAddExtension(change differ.Change) (DDLStatement, error) {
	ext := b.getExtension(change.ObjectName, b.result.Desired)
	if ext == nil {
		extName := change.ObjectName
		sql := b.buildCreateExtensionSQL(&schema.Extension{Name: extName})

		return DDLStatement{
			SQL:         sql,
			Description: "Add extension " + extName,
			RequiresTx:  false,
		}, nil
	}

	sql := b.buildCreateExtensionSQL(ext)

	return DDLStatement{
		SQL:         sql,
		Description: "Add extension " + ext.Name,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildDropExtension(change differ.Change) (DDLStatement, error) {
	name := change.ObjectName
	if name == "" {
		return DDLStatement{}, fmt.Errorf("extension not found: %s", change.ObjectName)
	}

	sql := fmt.Sprintf("DROP EXTENSION %s%s CASCADE;", b.ifExists(), QuoteIdentifier(name))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop extension " + name,
		IsUnsafe:    true,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildModifyExtension(change differ.Change) (DDLStatement, error) {
	return b.buildExtensionAlter(change, b.result.Current, b.result.Desired, "Modify")
}

func (b *DDLBuilder) buildReverseModifyExtension(change differ.Change) (DDLStatement, error) {
	return b.buildExtensionAlter(change, b.result.Desired, b.result.Current, "Revert")
}

func (b *DDLBuilder) buildExtensionAlter(
	change differ.Change,
	fromDB *schema.Database,
	toDB *schema.Database,
	action string,
) (DDLStatement, error) {
	fromExt := b.getExtension(change.ObjectName, fromDB)
	if fromExt == nil {
		return DDLStatement{}, fmt.Errorf("extension not found in source: %s", change.ObjectName)
	}

	toExt := b.getExtension(change.ObjectName, toDB)
	if toExt == nil {
		return DDLStatement{}, fmt.Errorf("extension not found in target: %s", change.ObjectName)
	}

	statements := buildExtensionAlterStatements(fromExt, toExt)
	if len(statements) == 0 {
		return DDLStatement{}, fmt.Errorf(
			"no extension alterations required for %s",
			change.ObjectName,
		)
	}

	sql := strings.Join(statements, "\n")

	description := fmt.Sprintf("%s extension %s", action, toExt.Name)

	return DDLStatement{
		SQL:         sql,
		Description: description,
		IsUnsafe:    true,
		RequiresTx:  false,
	}, nil
}

func buildExtensionAlterStatements(fromExt, toExt *schema.Extension) []string {
	var stmts []string

	sourceSchema := schema.NormalizeSchemaName(fromExt.Schema)

	targetSchema := schema.NormalizeSchemaName(toExt.Schema)
	if sourceSchema != targetSchema {
		stmts = append(stmts, fmt.Sprintf(
			"ALTER EXTENSION %s SET SCHEMA %s;",
			QuoteIdentifier(toExt.Name),
			QuoteIdentifier(targetSchema),
		))
	}

	if toExt.Version != "" && toExt.Version != fromExt.Version {
		stmts = append(stmts, fmt.Sprintf(
			"ALTER EXTENSION %s UPDATE TO %s;",
			QuoteIdentifier(toExt.Name),
			formatSQLStringLiteral(toExt.Version),
		))
	}

	return stmts
}

func (b *DDLBuilder) buildCreateExtensionSQL(ext *schema.Extension) string {
	sql := fmt.Sprintf("CREATE EXTENSION %s%s", b.ifNotExists(), QuoteIdentifier(ext.Name))

	var clauses []string
	if ext.Schema != "" {
		clauses = append(clauses, "SCHEMA "+QuoteIdentifier(ext.Schema))
	}

	if ext.Version != "" {
		clauses = append(clauses, "VERSION "+formatSQLStringLiteral(ext.Version))
	}

	if len(clauses) > 0 {
		sql += " WITH " + strings.Join(clauses, " ")
	}

	return sql + ";"
}

func (b *DDLBuilder) buildAddCustomType(change differ.Change) (DDLStatement, error) {
	ct := b.getCustomType(change.ObjectName, b.result.Desired)
	if ct == nil {
		return DDLStatement{}, fmt.Errorf("custom type not found: %s", change.ObjectName)
	}

	var sql string
	if ct.Type == "enum" {
		sql = fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);",
			QualifiedName(ct.Schema, ct.Name),
			formatEnumValues(ct.Values))
	} else {
		sql = fmt.Sprintf("CREATE TYPE %s AS %s;",
			QualifiedName(ct.Schema, ct.Name),
			ct.Definition)
	}

	return DDLStatement{
		SQL:         sql,
		Description: "Add custom type " + ct.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildDropCustomType(change differ.Change) (DDLStatement, error) {
	ct := b.getCustomType(change.ObjectName, b.result.Current)
	if ct == nil {
		return DDLStatement{}, fmt.Errorf("custom type not found: %s", change.ObjectName)
	}

	sql := fmt.Sprintf("DROP TYPE %s%s CASCADE;",
		b.ifExists(), QualifiedName(ct.Schema, ct.Name))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop custom type " + ct.Name,
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildAddSequence(change differ.Change) (DDLStatement, error) {
	seq := b.getSequence(change.ObjectName, b.result.Desired)
	if seq == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddSequence",
			&change,
			wrapObjectNotFoundError(ErrSequenceNotFound, "sequence", change.ObjectName),
		)
	}

	sql, err := buildSequenceSQL(seq)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddSequence", &change, err)
	}

	return DDLStatement{
		SQL:         ensureStatementTerminated(sql),
		Description: "Add sequence " + seq.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildDropSequence(change differ.Change) (DDLStatement, error) {
	seq := b.getSequence(change.ObjectName, b.result.Current)
	if seq == nil {
		return DDLStatement{}, newGeneratorError(
			"buildDropSequence",
			&change,
			wrapObjectNotFoundError(ErrSequenceNotFound, "sequence", change.ObjectName),
		)
	}

	sql := fmt.Sprintf("DROP SEQUENCE %s%s CASCADE;",
		b.ifExists(), QualifiedName(seq.Schema, seq.Name))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop sequence " + seq.Name,
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}
