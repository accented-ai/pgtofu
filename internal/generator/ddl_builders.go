package generator

import (
	"github.com/accented-ai/pgtofu/internal/differ"
)

type schemaBuilder struct{}

func (b *schemaBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddSchema {
		return ddlBuilder.buildAddSchema(change)
	}

	return ddlBuilder.buildDropSchema(change)
}

func (b *schemaBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddSchema {
		return ddlBuilder.buildDropSchema(change)
	}

	return ddlBuilder.buildAddSchema(change)
}

type extensionBuilder struct{}

func (b *extensionBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddExtension:
		return ddlBuilder.buildAddExtension(change)
	case differ.ChangeTypeModifyExtension:
		return ddlBuilder.buildModifyExtension(change)
	}

	return ddlBuilder.buildDropExtension(change)
}

func (b *extensionBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddExtension:
		return ddlBuilder.buildDropExtension(change)
	case differ.ChangeTypeModifyExtension:
		return ddlBuilder.buildReverseModifyExtension(change)
	}

	return ddlBuilder.buildAddExtension(change)
}

type customTypeBuilder struct{}

func (b *customTypeBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddCustomType {
		return ddlBuilder.buildAddCustomType(change)
	}

	return ddlBuilder.buildDropCustomType(change)
}

func (b *customTypeBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddCustomType {
		return ddlBuilder.buildDropCustomType(change)
	}

	return ddlBuilder.buildAddCustomType(change)
}

type sequenceBuilder struct{}

func (b *sequenceBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddSequence {
		return ddlBuilder.buildAddSequence(change)
	}

	return ddlBuilder.buildDropSequence(change)
}

func (b *sequenceBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddSequence {
		return ddlBuilder.buildDropSequence(change)
	}

	return ddlBuilder.buildAddSequence(change)
}

type tableBuilder struct{}

func (b *tableBuilder) BuildUp(change differ.Change, ddlBuilder *DDLBuilder) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddTable:
		return ddlBuilder.buildAddTable(change)
	case differ.ChangeTypeDropTable:
		return ddlBuilder.buildDropTable(change)
	case differ.ChangeTypeModifyTableComment:
		return ddlBuilder.buildModifyTableComment(change)
	default:
		return ddlBuilder.buildDropTable(change)
	}
}

func (b *tableBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddTable:
		return ddlBuilder.buildDropTableForDown(change)
	case differ.ChangeTypeDropTable:
		return ddlBuilder.buildAddTable(change)
	case differ.ChangeTypeModifyTableComment:
		return ddlBuilder.buildReverseModifyTableComment(change)
	default:
		return ddlBuilder.buildAddTable(change)
	}
}

type columnBuilder struct{}

func (b *columnBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddColumn:
		return ddlBuilder.buildAddColumn(change)
	case differ.ChangeTypeDropColumn:
		return ddlBuilder.buildDropColumn(change)
	case differ.ChangeTypeModifyColumnType:
		return ddlBuilder.buildModifyColumnType(change)
	case differ.ChangeTypeModifyColumnNullability:
		return ddlBuilder.buildModifyColumnNullability(change)
	case differ.ChangeTypeModifyColumnDefault:
		return ddlBuilder.buildModifyColumnDefault(change)
	case differ.ChangeTypeModifyColumnComment:
		return ddlBuilder.buildModifyColumnComment(change)
	default:
		return ddlBuilder.buildAddColumn(change)
	}
}

func (b *columnBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddColumn:
		return ddlBuilder.buildDropColumn(change)
	case differ.ChangeTypeDropColumn:
		return ddlBuilder.buildAddColumn(change)
	case differ.ChangeTypeModifyColumnType:
		return ddlBuilder.buildReverseModifyColumnType(change)
	case differ.ChangeTypeModifyColumnNullability:
		return ddlBuilder.buildReverseModifyColumnNullability(change)
	case differ.ChangeTypeModifyColumnDefault:
		return ddlBuilder.buildReverseModifyColumnDefault(change)
	case differ.ChangeTypeModifyColumnComment:
		return ddlBuilder.buildReverseModifyColumnComment(change)
	default:
		return ddlBuilder.buildDropColumn(change)
	}
}

type constraintBuilder struct{}

func (b *constraintBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddConstraint {
		return ddlBuilder.buildAddConstraint(change)
	}

	return ddlBuilder.buildDropConstraint(change)
}

func (b *constraintBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddConstraint {
		return ddlBuilder.buildDropConstraint(change)
	}

	return ddlBuilder.buildAddConstraint(change)
}

type indexBuilder struct{}

func (b *indexBuilder) BuildUp(change differ.Change, ddlBuilder *DDLBuilder) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddIndex {
		return ddlBuilder.buildAddIndex(change)
	}

	return ddlBuilder.buildDropIndex(change)
}

func (b *indexBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddIndex {
		return ddlBuilder.buildDropIndex(change)
	}

	return ddlBuilder.buildAddIndex(change)
}

type viewBuilder struct{}

func (b *viewBuilder) BuildUp(change differ.Change, ddlBuilder *DDLBuilder) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddView:
		return ddlBuilder.buildAddView(change)
	case differ.ChangeTypeDropView:
		return ddlBuilder.buildDropView(change)
	case differ.ChangeTypeModifyView:
		return ddlBuilder.buildModifyView(change)
	default:
		return ddlBuilder.buildAddView(change)
	}
}

func (b *viewBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddView:
		return ddlBuilder.buildDropView(change)
	case differ.ChangeTypeDropView:
		return ddlBuilder.buildAddView(change)
	case differ.ChangeTypeModifyView:
		return ddlBuilder.buildRevertModifyView(change)
	default:
		return ddlBuilder.buildDropView(change)
	}
}

type materializedViewBuilder struct{}

func (b *materializedViewBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddMaterializedView:
		return ddlBuilder.buildAddMaterializedView(change)
	case differ.ChangeTypeDropMaterializedView:
		return ddlBuilder.buildDropMaterializedView(change)
	case differ.ChangeTypeModifyMaterializedView:
		return ddlBuilder.buildModifyMaterializedView(change)
	default:
		return ddlBuilder.buildAddMaterializedView(change)
	}
}

func (b *materializedViewBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddMaterializedView:
		return ddlBuilder.buildDropMaterializedView(change)
	case differ.ChangeTypeDropMaterializedView:
		return ddlBuilder.buildAddMaterializedView(change)
	case differ.ChangeTypeModifyMaterializedView:
		return ddlBuilder.buildRevertModifyMaterializedView(change)
	default:
		return ddlBuilder.buildDropMaterializedView(change)
	}
}

type functionBuilder struct{}

func (b *functionBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddFunction:
		return ddlBuilder.buildAddFunction(change)
	case differ.ChangeTypeDropFunction:
		return ddlBuilder.buildDropFunction(change)
	case differ.ChangeTypeModifyFunction:
		return ddlBuilder.buildModifyFunction(change)
	default:
		return ddlBuilder.buildAddFunction(change)
	}
}

func (b *functionBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddFunction:
		return ddlBuilder.buildDropFunction(change)
	case differ.ChangeTypeDropFunction:
		return ddlBuilder.buildAddFunction(change)
	case differ.ChangeTypeModifyFunction:
		return ddlBuilder.buildRevertModifyFunction(change)
	default:
		return ddlBuilder.buildDropFunction(change)
	}
}

type triggerBuilder struct{}

func (b *triggerBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddTrigger {
		return ddlBuilder.buildAddTrigger(change)
	}

	return ddlBuilder.buildDropTrigger(change)
}

func (b *triggerBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddTrigger {
		return ddlBuilder.buildDropTrigger(change)
	}

	return ddlBuilder.buildAddTrigger(change)
}

type hypertableBuilder struct{}

func (b *hypertableBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddHypertable {
		return ddlBuilder.buildAddHypertable(change)
	}

	return ddlBuilder.buildDropHypertable(change)
}

func (b *hypertableBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	if change.Type == differ.ChangeTypeAddHypertable {
		return ddlBuilder.buildDropHypertable(change)
	}

	return ddlBuilder.buildAddHypertable(change)
}

type timescalePolicyBuilder struct{}

func (b *timescalePolicyBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddCompressionPolicy:
		return ddlBuilder.buildAddCompressionPolicy(change)
	case differ.ChangeTypeDropCompressionPolicy:
		return ddlBuilder.buildDropCompressionPolicy(change)
	case differ.ChangeTypeModifyCompressionPolicy:
		return ddlBuilder.buildModifyCompressionPolicy(change)
	case differ.ChangeTypeAddRetentionPolicy:
		return ddlBuilder.buildAddRetentionPolicy(change)
	case differ.ChangeTypeDropRetentionPolicy:
		return ddlBuilder.buildDropRetentionPolicy(change)
	default:
		return ddlBuilder.buildAddCompressionPolicy(change)
	}
}

func (b *timescalePolicyBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddCompressionPolicy:
		return ddlBuilder.buildDropCompressionPolicy(change)
	case differ.ChangeTypeDropCompressionPolicy:
		return ddlBuilder.buildAddCompressionPolicy(change)
	case differ.ChangeTypeModifyCompressionPolicy:
		return ddlBuilder.buildReverseModifyCompressionPolicy(change)
	case differ.ChangeTypeAddRetentionPolicy:
		return ddlBuilder.buildDropRetentionPolicy(change)
	case differ.ChangeTypeDropRetentionPolicy:
		return ddlBuilder.buildAddRetentionPolicy(change)
	default:
		return ddlBuilder.buildDropCompressionPolicy(change)
	}
}

type continuousAggregateBuilder struct{}

func (b *continuousAggregateBuilder) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddContinuousAggregate:
		return ddlBuilder.buildAddContinuousAggregate(change)
	case differ.ChangeTypeDropContinuousAggregate:
		return ddlBuilder.buildDropContinuousAggregate(change)
	case differ.ChangeTypeModifyContinuousAggregate:
		return ddlBuilder.buildModifyContinuousAggregate(change)
	default:
		return ddlBuilder.buildAddContinuousAggregate(change)
	}
}

func (b *continuousAggregateBuilder) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	switch change.Type {
	case differ.ChangeTypeAddContinuousAggregate:
		return ddlBuilder.buildDropContinuousAggregate(change)
	case differ.ChangeTypeDropContinuousAggregate:
		return ddlBuilder.buildAddContinuousAggregate(change)
	case differ.ChangeTypeModifyContinuousAggregate:
		return ddlBuilder.buildReverseModifyContinuousAggregate(change)
	default:
		return ddlBuilder.buildDropContinuousAggregate(change)
	}
}
