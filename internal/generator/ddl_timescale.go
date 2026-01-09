package generator

import (
	"errors"
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func (b *DDLBuilder) buildAddHypertable(change differ.Change) (DDLStatement, error) {
	var ht *schema.Hypertable

	if htAny, ok := change.Details["hypertable"]; ok {
		if htPtr, ok := htAny.(*schema.Hypertable); ok {
			ht = htPtr
		}
	}

	if ht == nil {
		ht = b.findHypertable(change.ObjectName)
	}

	if ht == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddHypertable",
			&change,
			wrapObjectNotFoundError(ErrHypertableNotFound, "hypertable", change.ObjectName),
		)
	}

	var sb strings.Builder

	sql, err := formatCreateHypertable(ht)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddHypertable", &change, err)
	}

	appendStatement(&sb, sql)

	if ht.CompressionEnabled && ht.CompressionSettings != nil {
		compressionSQL, err := formatCompressionPolicy(ht)
		if err != nil {
			return DDLStatement{}, newGeneratorError("buildAddHypertable", &change, err)
		}

		if compressionSQL != "" {
			appendStatement(&sb, compressionSQL)
		}
	}

	if ht.RetentionPolicy != nil && ht.RetentionPolicy.DropAfter != "" {
		retentionSQL, err := formatRetentionPolicy(ht)
		if err != nil {
			return DDLStatement{}, newGeneratorError("buildAddHypertable", &change, err)
		}

		if retentionSQL != "" {
			appendStatement(&sb, retentionSQL)
		}
	}

	return DDLStatement{
		SQL:         sb.String(),
		Description: fmt.Sprintf("Convert table %s to hypertable", ht.TableName),
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildDropHypertable(change differ.Change) (DDLStatement, error) {
	return DDLStatement{
		SQL: fmt.Sprintf("-- WARNING: Cannot automatically revert hypertable %s\n"+
			"-- Manual data migration required", change.ObjectName),
		Description: fmt.Sprintf(
			"Drop hypertable %s (manual intervention required)",
			change.ObjectName,
		),
		IsUnsafe:   true,
		RequiresTx: false,
	}, nil
}

func (b *DDLBuilder) buildAddCompressionPolicy(change differ.Change) (DDLStatement, error) {
	var ht *schema.Hypertable

	if htAny, ok := change.Details["hypertable"]; ok {
		if htPtr, ok := htAny.(*schema.Hypertable); ok {
			ht = htPtr
		}
	}

	if ht == nil {
		ht = b.findHypertable(change.ObjectName)
	}

	if ht == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddCompressionPolicy",
			&change,
			wrapObjectNotFoundError(ErrHypertableNotFound, "hypertable", change.ObjectName),
		)
	}

	sql, err := formatCompressionPolicy(ht)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddCompressionPolicy", &change, err)
	}

	if strings.TrimSpace(sql) == "" {
		return DDLStatement{}, newGeneratorError(
			"buildAddCompressionPolicy",
			&change,
			errors.New("compression policy is not configured"),
		)
	}

	return DDLStatement{
		SQL:         ensureStatementTerminated(sql),
		Description: "Add compression policy for " + ht.TableName,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildDropCompressionPolicy(change differ.Change) (DDLStatement, error) {
	ht := b.findHypertable(change.ObjectName)
	if ht == nil {
		return DDLStatement{}, fmt.Errorf("hypertable not found: %s", change.ObjectName)
	}

	sql := fmt.Sprintf("SELECT remove_compression_policy('%s');",
		QualifiedName(ht.Schema, ht.TableName))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop compression policy for " + ht.TableName,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildModifyCompressionPolicy(change differ.Change) (DDLStatement, error) {
	ht := b.getHypertable(change.ObjectName, b.result.Desired)
	if ht == nil {
		return DDLStatement{}, newGeneratorError(
			"buildModifyCompressionPolicy",
			&change,
			wrapObjectNotFoundError(ErrHypertableNotFound, "hypertable", change.ObjectName),
		)
	}

	sql, err := formatCompressionPolicy(ht)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyCompressionPolicy", &change, err)
	}

	if strings.TrimSpace(sql) == "" {
		return DDLStatement{}, newGeneratorError(
			"buildModifyCompressionPolicy",
			&change,
			errors.New("compression policy is not configured"),
		)
	}

	return DDLStatement{
		SQL:         ensureStatementTerminated(sql),
		Description: "Modify compression policy for " + ht.TableName,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildReverseModifyCompressionPolicy(
	change differ.Change,
) (DDLStatement, error) {
	ht := b.getHypertable(change.ObjectName, b.result.Current)
	if ht == nil {
		return DDLStatement{}, newGeneratorError(
			"buildReverseModifyCompressionPolicy",
			&change,
			wrapObjectNotFoundError(ErrHypertableNotFound, "hypertable", change.ObjectName),
		)
	}

	sql, err := formatCompressionPolicy(ht)
	if err != nil {
		return DDLStatement{}, newGeneratorError(
			"buildReverseModifyCompressionPolicy",
			&change,
			err,
		)
	}

	if strings.TrimSpace(sql) == "" {
		return DDLStatement{}, newGeneratorError(
			"buildReverseModifyCompressionPolicy",
			&change,
			errors.New("compression policy is not configured"),
		)
	}

	return DDLStatement{
		SQL:         ensureStatementTerminated(sql),
		Description: "Restore compression policy for " + ht.TableName,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildAddRetentionPolicy(change differ.Change) (DDLStatement, error) {
	ht := b.getHypertableForRetentionPolicy(change)

	if ht == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddRetentionPolicy",
			&change,
			wrapObjectNotFoundError(ErrHypertableNotFound, "hypertable", change.ObjectName),
		)
	}

	sql, err := formatRetentionPolicy(ht)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddRetentionPolicy", &change, err)
	}

	if strings.TrimSpace(sql) == "" {
		return DDLStatement{}, newGeneratorError(
			"buildAddRetentionPolicy",
			&change,
			errors.New("retention policy is not configured"),
		)
	}

	return DDLStatement{
		SQL:         ensureStatementTerminated(sql),
		Description: "Add retention policy for " + ht.TableName,
		IsUnsafe:    true,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildDropRetentionPolicy(change differ.Change) (DDLStatement, error) {
	ht := b.findHypertable(change.ObjectName)
	if ht == nil {
		return DDLStatement{}, fmt.Errorf("hypertable not found: %s", change.ObjectName)
	}

	sql := fmt.Sprintf("SELECT remove_retention_policy('%s');",
		QualifiedName(ht.Schema, ht.TableName))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop retention policy for " + ht.TableName,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildAddContinuousAggregate(change differ.Change) (DDLStatement, error) {
	var ca *schema.ContinuousAggregate

	if caAny, ok := change.Details["aggregate"]; ok {
		if caPtr, ok := caAny.(*schema.ContinuousAggregate); ok {
			ca = caPtr
		}
	}

	if ca == nil {
		ca = b.findContinuousAggregate(change.ObjectName)
	}

	if ca == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddContinuousAggregate",
			&change,
			wrapObjectNotFoundError(
				ErrMaterializedViewNotFound,
				"continuous aggregate",
				change.ObjectName,
			),
		)
	}

	sql, err := buildContinuousAggregateSQL(ca)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddContinuousAggregate", &change, err)
	}

	return DDLStatement{
		SQL:         sql,
		Description: "Add continuous aggregate " + ca.ViewName,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) buildDropContinuousAggregate(change differ.Change) (DDLStatement, error) {
	ca := b.findContinuousAggregate(change.ObjectName)
	if ca == nil {
		return DDLStatement{}, fmt.Errorf("continuous aggregate not found: %s", change.ObjectName)
	}

	sql := fmt.Sprintf("DROP MATERIALIZED VIEW %s%s CASCADE;",
		b.ifExists(), QualifiedName(ca.Schema, ca.ViewName))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop continuous aggregate " + ca.ViewName,
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildModifyContinuousAggregate(change differ.Change) (DDLStatement, error) {
	return b.buildContinuousAggregateRecreate(change, b.result.Current, b.result.Desired, "Modify")
}

func (b *DDLBuilder) buildReverseModifyContinuousAggregate(
	change differ.Change,
) (DDLStatement, error) {
	return b.buildContinuousAggregateRecreate(change, b.result.Desired, b.result.Current, "Restore")
}

func (b *DDLBuilder) buildContinuousAggregateRecreate(
	change differ.Change,
	oldDB, newDB *schema.Database,
	action string,
) (DDLStatement, error) {
	caOld := b.getContinuousAggregate(change.ObjectName, oldDB)
	caNew := b.getContinuousAggregate(change.ObjectName, newDB)

	if caOld == nil || caNew == nil {
		return DDLStatement{}, newGeneratorError(
			"buildContinuousAggregateRecreate",
			&change,
			wrapObjectNotFoundError(
				ErrMaterializedViewNotFound,
				"continuous aggregate",
				change.ObjectName,
			),
		)
	}

	var sb strings.Builder

	dropStatement := fmt.Sprintf(
		"DROP MATERIALIZED VIEW %s%s CASCADE;",
		b.ifExists(),
		QualifiedName(caOld.Schema, caOld.ViewName),
	)
	appendStatement(&sb, dropStatement)

	definition, err := buildContinuousAggregateSQL(caNew)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildContinuousAggregateRecreate", &change, err)
	}

	appendStatement(&sb, definition)

	return DDLStatement{
		SQL:         sb.String(),
		Description: fmt.Sprintf("%s continuous aggregate %s", action, caNew.ViewName),
		IsUnsafe:    true,
		RequiresTx:  false,
	}, nil
}

func (b *DDLBuilder) getHypertableForRetentionPolicy(change differ.Change) *schema.Hypertable {
	if htAny, ok := change.Details["hypertable"]; ok {
		if htPtr, ok := htAny.(*schema.Hypertable); ok {
			return htPtr
		}
	}

	if policyAny, ok := change.Details["policy"]; ok {
		if policy, ok := policyAny.(*schema.RetentionPolicy); ok {
			return b.buildHypertableWithPolicy(change.ObjectName, policy)
		}
	}

	return b.findHypertable(change.ObjectName)
}

func (b *DDLBuilder) buildHypertableWithPolicy(
	objectName string,
	policy *schema.RetentionPolicy,
) *schema.Hypertable {
	foundHT := b.findHypertable(objectName)
	if foundHT != nil {
		htCopy := *foundHT
		htCopy.RetentionPolicy = policy

		return &htCopy
	}

	htSchema, htTable := parseSchemaAndName(objectName)

	return &schema.Hypertable{
		Schema:          htSchema,
		TableName:       htTable,
		RetentionPolicy: policy,
	}
}
