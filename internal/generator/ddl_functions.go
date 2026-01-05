package generator

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func (b *DDLBuilder) buildAddFunction(change differ.Change) (DDLStatement, error) {
	fn := b.getFunction(change.ObjectName, b.result.Desired)
	if fn == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddFunction",
			&change,
			wrapObjectNotFoundError(ErrFunctionNotFound, "function", change.ObjectName),
		)
	}

	definition, err := formatFunctionDefinition(fn, true)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddFunction", &change, err)
	}

	return DDLStatement{
		SQL:         ensureStatementTerminated(definition),
		Description: "Add function " + fn.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildDropFunction(change differ.Change) (DDLStatement, error) {
	fn := b.getFunction(change.ObjectName, b.result.Current)
	if fn == nil {
		fn = b.getFunction(change.ObjectName, b.result.Desired)
		if fn == nil {
			return DDLStatement{}, fmt.Errorf("function not found: %s", change.ObjectName)
		}
	}

	argTypes := "()"
	if len(fn.ArgumentTypes) > 0 {
		argTypes = "(" + strings.Join(fn.ArgumentTypes, ", ") + ")"
	}

	sql := fmt.Sprintf("DROP FUNCTION %s%s%s CASCADE;",
		b.ifExists(),
		QualifiedName(fn.Schema, fn.Name),
		argTypes)

	return DDLStatement{
		SQL:         sql,
		Description: "Drop function " + fn.Name,
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildModifyFunction(change differ.Change) (DDLStatement, error) {
	comment, err := extractCommentDetails(change)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyFunction", &change, err)
	}

	if comment.HasOld && comment.HasNew {
		fn := b.getFunction(change.ObjectName, b.result.Desired)
		if fn == nil {
			return DDLStatement{}, newGeneratorError(
				"buildModifyFunction",
				&change,
				wrapObjectNotFoundError(ErrFunctionNotFound, "function", change.ObjectName),
			)
		}

		qualifiedTarget, formattedTarget := functionCommentTargets(fn)

		target := formattedTarget
		if comment.New == "" {
			target = qualifiedTarget
		}

		sql := buildCommentStatement("FUNCTION", target, comment.New, true)

		return DDLStatement{
			SQL:         sql,
			Description: "Modify function comment " + fn.Name,
			RequiresTx:  true,
		}, nil
	}

	fn := b.getFunction(change.ObjectName, b.result.Desired)
	if fn == nil {
		return DDLStatement{}, newGeneratorError(
			"buildModifyFunction",
			&change,
			wrapObjectNotFoundError(ErrFunctionNotFound, "function", change.ObjectName),
		)
	}

	definition, err := formatFunctionDefinition(fn, true)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyFunction", &change, err)
	}

	var sb strings.Builder
	appendStatement(&sb, definition)

	if fn.Comment != "" {
		_, formattedTarget := functionCommentTargets(fn)
		commentSQL := buildCommentStatement("FUNCTION", formattedTarget, fn.Comment, true)
		appendStatement(&sb, commentSQL)
	}

	return DDLStatement{
		SQL:         sb.String(),
		Description: "Modify function " + fn.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildRevertModifyFunction(change differ.Change) (DDLStatement, error) {
	comment, err := extractCommentDetails(change)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildRevertModifyFunction", &change, err)
	}

	if comment.HasOld && comment.HasNew {
		if comment.Old == "" {
			fn := b.getFunction(change.ObjectName, b.result.Desired)
			if fn == nil {
				return DDLStatement{}, newGeneratorError(
					"buildRevertModifyFunction",
					&change,
					wrapObjectNotFoundError(ErrFunctionNotFound, "function", change.ObjectName),
				)
			}

			qualifiedTarget, _ := functionCommentTargets(fn)
			sql := buildCommentStatement("FUNCTION", qualifiedTarget, comment.Old, true)

			return DDLStatement{
				SQL:         sql,
				Description: "Revert function comment " + fn.Name,
				RequiresTx:  true,
			}, nil
		}

		fn := b.getFunction(change.ObjectName, b.result.Current)
		if fn == nil {
			return DDLStatement{}, newGeneratorError(
				"buildRevertModifyFunction",
				&change,
				wrapObjectNotFoundError(ErrFunctionNotFound, "function", change.ObjectName),
			)
		}

		_, formattedTarget := functionCommentTargets(fn)
		sql := buildCommentStatement("FUNCTION", formattedTarget, comment.Old, true)

		return DDLStatement{
			SQL:         sql,
			Description: "Revert function comment " + fn.Name,
			RequiresTx:  true,
		}, nil
	}

	fn := b.getFunction(change.ObjectName, b.result.Current)
	if fn == nil {
		return DDLStatement{}, newGeneratorError(
			"buildRevertModifyFunction",
			&change,
			wrapObjectNotFoundError(ErrFunctionNotFound, "function", change.ObjectName),
		)
	}

	definition, err := formatFunctionDefinition(fn, true)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildRevertModifyFunction", &change, err)
	}

	return DDLStatement{
		SQL:         ensureStatementTerminated(definition),
		Description: "Revert function " + fn.Name,
		RequiresTx:  true,
	}, nil
}

func functionCommentTargets(fn *schema.Function) (string, string) {
	schemaName := fn.Schema
	if schemaName == "" {
		schemaName = schema.DefaultSchema
	}

	qualified := QualifiedName(schemaName, fn.Name) + fn.ArgumentList()

	return qualified, fmt.Sprintf(
		"%s.%s%s",
		QuoteIdentifier(schemaName),
		strings.ToUpper(fn.Name),
		fn.ArgumentList(),
	)
}

func (b *DDLBuilder) buildAddTrigger(change differ.Change) (DDLStatement, error) {
	trigger := b.getTrigger(change.ObjectName, b.result.Desired)
	if trigger == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddTrigger",
			&change,
			wrapObjectNotFoundError(ErrTriggerNotFound, "trigger", change.ObjectName),
		)
	}

	definition, err := formatTriggerDefinition(trigger)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddTrigger", &change, err)
	}

	return DDLStatement{
		SQL:         ensureStatementTerminated(definition),
		Description: "Add trigger " + trigger.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildAddTriggerForDown(change differ.Change) (DDLStatement, error) {
	trigger := b.getTrigger(change.ObjectName, b.result.Current)
	if trigger == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddTriggerForDown",
			&change,
			wrapObjectNotFoundError(ErrTriggerNotFound, "trigger", change.ObjectName),
		)
	}

	definition, err := formatTriggerDefinition(trigger)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddTriggerForDown", &change, err)
	}

	return DDLStatement{
		SQL:         ensureStatementTerminated(definition),
		Description: "Add trigger " + trigger.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildDropTrigger(change differ.Change) (DDLStatement, error) {
	trigger := b.getTrigger(change.ObjectName, b.result.Current)
	if trigger == nil {
		trigger = b.getTrigger(change.ObjectName, b.result.Desired)
		if trigger == nil {
			return DDLStatement{}, fmt.Errorf("trigger not found: %s", change.ObjectName)
		}
	}

	sql := fmt.Sprintf("DROP TRIGGER %s%s ON %s CASCADE;",
		b.ifExists(),
		QuoteIdentifier(trigger.Name),
		QualifiedName(trigger.Schema, trigger.TableName))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop trigger " + trigger.Name,
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}
