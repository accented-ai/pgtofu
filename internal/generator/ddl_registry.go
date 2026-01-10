package generator

import (
	"fmt"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/util"
)

type StatementBuilder interface {
	BuildUp(change differ.Change, builder *DDLBuilder) (DDLStatement, error)
	BuildDown(change differ.Change, builder *DDLBuilder) (DDLStatement, error)
}

type DDLBuilderRegistry struct {
	builders map[differ.ChangeType]StatementBuilder
}

func NewDDLBuilderRegistry() *DDLBuilderRegistry {
	r := &DDLBuilderRegistry{
		builders: make(map[differ.ChangeType]StatementBuilder),
	}

	r.Register(differ.ChangeTypeAddSchema, &schemaBuilder{})
	r.Register(differ.ChangeTypeDropSchema, &schemaBuilder{})
	r.Register(differ.ChangeTypeAddExtension, &extensionBuilder{})
	r.Register(differ.ChangeTypeDropExtension, &extensionBuilder{})
	r.Register(differ.ChangeTypeModifyExtension, &extensionBuilder{})
	r.Register(differ.ChangeTypeAddCustomType, &customTypeBuilder{})
	r.Register(differ.ChangeTypeDropCustomType, &customTypeBuilder{})
	r.Register(differ.ChangeTypeAddSequence, &sequenceBuilder{})
	r.Register(differ.ChangeTypeDropSequence, &sequenceBuilder{})
	r.Register(differ.ChangeTypeAddTable, &tableBuilder{})
	r.Register(differ.ChangeTypeDropTable, &tableBuilder{})
	r.Register(differ.ChangeTypeAddColumn, &columnBuilder{})
	r.Register(differ.ChangeTypeDropColumn, &columnBuilder{})
	r.Register(differ.ChangeTypeModifyColumnType, &columnBuilder{})
	r.Register(differ.ChangeTypeModifyColumnNullability, &columnBuilder{})
	r.Register(differ.ChangeTypeModifyColumnDefault, &columnBuilder{})
	r.Register(differ.ChangeTypeModifyColumnComment, &columnBuilder{})
	r.Register(differ.ChangeTypeModifyTableComment, &tableBuilder{})
	r.Register(differ.ChangeTypeAddConstraint, &constraintBuilder{})
	r.Register(differ.ChangeTypeDropConstraint, &constraintBuilder{})
	r.Register(differ.ChangeTypeModifyConstraint, &constraintBuilder{})
	r.Register(differ.ChangeTypeAddIndex, &indexBuilder{})
	r.Register(differ.ChangeTypeDropIndex, &indexBuilder{})
	r.Register(differ.ChangeTypeAddPartition, &partitionBuilder{})
	r.Register(differ.ChangeTypeDropPartition, &partitionBuilder{})
	r.Register(differ.ChangeTypeAddView, &viewBuilder{})
	r.Register(differ.ChangeTypeDropView, &viewBuilder{})
	r.Register(differ.ChangeTypeModifyView, &viewBuilder{})
	r.Register(differ.ChangeTypeAddMaterializedView, &materializedViewBuilder{})
	r.Register(differ.ChangeTypeDropMaterializedView, &materializedViewBuilder{})
	r.Register(differ.ChangeTypeModifyMaterializedView, &materializedViewBuilder{})
	r.Register(differ.ChangeTypeAddFunction, &functionBuilder{})
	r.Register(differ.ChangeTypeDropFunction, &functionBuilder{})
	r.Register(differ.ChangeTypeModifyFunction, &functionBuilder{})
	r.Register(differ.ChangeTypeAddTrigger, &triggerBuilder{})
	r.Register(differ.ChangeTypeDropTrigger, &triggerBuilder{})
	r.Register(differ.ChangeTypeAddHypertable, &hypertableBuilder{})
	r.Register(differ.ChangeTypeDropHypertable, &hypertableBuilder{})
	r.Register(differ.ChangeTypeAddCompressionPolicy, &timescalePolicyBuilder{})
	r.Register(differ.ChangeTypeDropCompressionPolicy, &timescalePolicyBuilder{})
	r.Register(differ.ChangeTypeModifyCompressionPolicy, &timescalePolicyBuilder{})
	r.Register(differ.ChangeTypeAddRetentionPolicy, &timescalePolicyBuilder{})
	r.Register(differ.ChangeTypeDropRetentionPolicy, &timescalePolicyBuilder{})
	r.Register(differ.ChangeTypeAddContinuousAggregate, &continuousAggregateBuilder{})
	r.Register(differ.ChangeTypeDropContinuousAggregate, &continuousAggregateBuilder{})
	r.Register(differ.ChangeTypeModifyContinuousAggregate, &continuousAggregateBuilder{})

	return r
}

func (r *DDLBuilderRegistry) Register(changeType differ.ChangeType, builder StatementBuilder) {
	r.builders[changeType] = builder
}

func (r *DDLBuilderRegistry) BuildUp(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	builder, ok := r.builders[change.Type]
	if !ok {
		return DDLStatement{}, newGeneratorError(
			"BuildUp",
			&change,
			util.WrapError(fmt.Sprintf("change type %s", change.Type), ErrUnsupportedChangeType),
		)
	}

	return builder.BuildUp(change, ddlBuilder) //nolint:wrapcheck
}

func (r *DDLBuilderRegistry) BuildDown(
	change differ.Change,
	ddlBuilder *DDLBuilder,
) (DDLStatement, error) {
	inverseMap := map[differ.ChangeType]differ.ChangeType{
		differ.ChangeTypeAddSchema:               differ.ChangeTypeDropSchema,
		differ.ChangeTypeDropSchema:              differ.ChangeTypeAddSchema,
		differ.ChangeTypeAddExtension:            differ.ChangeTypeDropExtension,
		differ.ChangeTypeDropExtension:           differ.ChangeTypeAddExtension,
		differ.ChangeTypeAddCustomType:           differ.ChangeTypeDropCustomType,
		differ.ChangeTypeDropCustomType:          differ.ChangeTypeAddCustomType,
		differ.ChangeTypeAddSequence:             differ.ChangeTypeDropSequence,
		differ.ChangeTypeDropSequence:            differ.ChangeTypeAddSequence,
		differ.ChangeTypeAddTable:                differ.ChangeTypeDropTable,
		differ.ChangeTypeDropTable:               differ.ChangeTypeAddTable,
		differ.ChangeTypeAddColumn:               differ.ChangeTypeDropColumn,
		differ.ChangeTypeDropColumn:              differ.ChangeTypeAddColumn,
		differ.ChangeTypeAddConstraint:           differ.ChangeTypeDropConstraint,
		differ.ChangeTypeDropConstraint:          differ.ChangeTypeAddConstraint,
		differ.ChangeTypeAddIndex:                differ.ChangeTypeDropIndex,
		differ.ChangeTypeDropIndex:               differ.ChangeTypeAddIndex,
		differ.ChangeTypeAddPartition:            differ.ChangeTypeDropPartition,
		differ.ChangeTypeDropPartition:           differ.ChangeTypeAddPartition,
		differ.ChangeTypeAddView:                 differ.ChangeTypeDropView,
		differ.ChangeTypeDropView:                differ.ChangeTypeAddView,
		differ.ChangeTypeAddMaterializedView:     differ.ChangeTypeDropMaterializedView,
		differ.ChangeTypeDropMaterializedView:    differ.ChangeTypeAddMaterializedView,
		differ.ChangeTypeAddFunction:             differ.ChangeTypeDropFunction,
		differ.ChangeTypeDropFunction:            differ.ChangeTypeAddFunction,
		differ.ChangeTypeAddTrigger:              differ.ChangeTypeDropTrigger,
		differ.ChangeTypeDropTrigger:             differ.ChangeTypeAddTrigger,
		differ.ChangeTypeAddHypertable:           differ.ChangeTypeDropHypertable,
		differ.ChangeTypeDropHypertable:          differ.ChangeTypeAddHypertable,
		differ.ChangeTypeAddCompressionPolicy:    differ.ChangeTypeDropCompressionPolicy,
		differ.ChangeTypeDropCompressionPolicy:   differ.ChangeTypeAddCompressionPolicy,
		differ.ChangeTypeAddRetentionPolicy:      differ.ChangeTypeDropRetentionPolicy,
		differ.ChangeTypeDropRetentionPolicy:     differ.ChangeTypeAddRetentionPolicy,
		differ.ChangeTypeAddContinuousAggregate:  differ.ChangeTypeDropContinuousAggregate,
		differ.ChangeTypeDropContinuousAggregate: differ.ChangeTypeAddContinuousAggregate,
	}

	reverseMap := map[differ.ChangeType]differ.ChangeType{
		differ.ChangeTypeModifyColumnType:          differ.ChangeTypeModifyColumnType,
		differ.ChangeTypeModifyColumnNullability:   differ.ChangeTypeModifyColumnNullability,
		differ.ChangeTypeModifyColumnDefault:       differ.ChangeTypeModifyColumnDefault,
		differ.ChangeTypeModifyColumnComment:       differ.ChangeTypeModifyColumnComment,
		differ.ChangeTypeModifyExtension:           differ.ChangeTypeModifyExtension,
		differ.ChangeTypeModifyTableComment:        differ.ChangeTypeModifyTableComment,
		differ.ChangeTypeModifyView:                differ.ChangeTypeModifyView,
		differ.ChangeTypeModifyMaterializedView:    differ.ChangeTypeModifyMaterializedView,
		differ.ChangeTypeModifyFunction:            differ.ChangeTypeModifyFunction,
		differ.ChangeTypeModifyCompressionPolicy:   differ.ChangeTypeModifyCompressionPolicy,
		differ.ChangeTypeModifyContinuousAggregate: differ.ChangeTypeModifyContinuousAggregate,
		differ.ChangeTypeModifyConstraint:          differ.ChangeTypeModifyConstraint,
	}

	var targetType differ.ChangeType
	if inverse, ok := inverseMap[change.Type]; ok {
		targetType = inverse
	} else if reverse, ok := reverseMap[change.Type]; ok {
		targetType = reverse
	} else {
		return DDLStatement{}, newGeneratorError(
			"BuildDown",
			&change,
			util.WrapError(fmt.Sprintf("change type %s", change.Type), ErrUnsupportedChangeType),
		)
	}

	builder, ok := r.builders[targetType]
	if !ok {
		return DDLStatement{}, newGeneratorError(
			"BuildDown",
			&change,
			util.WrapError(fmt.Sprintf("change type %s", targetType), ErrUnsupportedChangeType),
		)
	}

	if inverse, ok := inverseMap[change.Type]; ok {
		switch change.Type {
		case differ.ChangeTypeAddTable:
			return ddlBuilder.buildDropTableForDown(change)
		case differ.ChangeTypeDropTable:
			return ddlBuilder.buildAddTableForDown(change)
		case differ.ChangeTypeDropTrigger:
			return ddlBuilder.buildAddTriggerForDown(change)
		case differ.ChangeTypeDropView:
			return ddlBuilder.buildAddViewForDown(change)
		case differ.ChangeTypeDropMaterializedView:
			return ddlBuilder.buildAddMaterializedViewForDown(change)
		case differ.ChangeTypeDropFunction:
			return ddlBuilder.buildAddFunctionForDown(change)
		}

		inverseChange := change
		inverseChange.Type = inverse

		return builder.BuildUp(inverseChange, ddlBuilder) //nolint:wrapcheck
	}

	return builder.BuildDown(change, ddlBuilder) //nolint:wrapcheck
}
