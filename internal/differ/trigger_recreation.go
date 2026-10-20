package differ

import (
	"slices"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func (tc *TriggerComparator) prepareRecreations(result *DiffResult) {
	for i := range result.Changes {
		change := &result.Changes[i]
		if change.Type != ChangeTypeModifyTrigger {
			continue
		}

		current, _ := change.Details["current"].(*schema.Trigger)

		desired, _ := change.Details["desired"].(*schema.Trigger)
		if current == nil || desired == nil || !tc.needsRecreation(current, result) {
			continue
		}

		add := *change
		add.Type = ChangeTypeAddTrigger
		add.Description = "Recreate trigger: " + change.ObjectName
		add.Details = map[string]any{"trigger": desired}

		*change = Change{
			Type: ChangeTypeDropTrigger, Severity: SeverityPotentiallyBreaking,
			Description: "Drop trigger for recreation: " + change.ObjectName,
			ObjectType:  "trigger", ObjectName: change.ObjectName,
			Details:           map[string]any{"trigger": current, "will_be_recreated": true},
			DependsOn:         tc.buildTriggerDependencies(current, result.Current, false),
			RollbackDependsOn: tc.buildTriggerDependencies(current, result.Current, true),
		}
		result.Changes = append(result.Changes, add)
	}
}

func (tc *TriggerComparator) needsRecreation(trigger *schema.Trigger, result *DiffResult) bool {
	dependencies := tc.buildTriggerDependencies(trigger, result.Current, false)
	for i := range result.Changes {
		change := &result.Changes[i]
		if change.Type == ChangeTypeDropFunction &&
			slices.Contains(dependencies, change.ObjectName) {
			return true
		}

		if triggerColumnChangeDependsOn(change, trigger) {
			return true
		}
	}

	return false
}

func triggerColumnChangeDependsOn(change *Change, trigger *schema.Trigger) bool {
	if change.Type != ChangeTypeDropColumn && change.Type != ChangeTypeModifyColumnType {
		return false
	}

	table, column, ok := getColumnFromChange(change)
	if !ok || !strings.EqualFold(table, trigger.QualifiedTableName()) {
		return false
	}

	return slices.ContainsFunc(trigger.UpdateColumns, func(name string) bool {
		return strings.EqualFold(name, column)
	}) || indexExpressionUsesColumn(trigger.WhenCondition, schema.NormalizeIdentifier(column))
}

func triggerChangeDependsOn(change, other *Change) bool {
	if other.Type == ChangeTypeDropTrigger {
		trigger, _ := other.Details["trigger"].(*schema.Trigger)

		return trigger != nil && (triggerColumnChangeDependsOn(change, trigger) ||
			(change.Type == ChangeTypeAddTrigger && change.ObjectName == other.ObjectName))
	}

	if change.Type == ChangeTypeAddTrigger && other.Type == ChangeTypeAddColumn {
		trigger, _ := change.Details["trigger"].(*schema.Trigger)
		table, _, ok := getColumnFromChange(other)

		return trigger != nil && ok && strings.EqualFold(table, trigger.QualifiedTableName())
	}

	return false
}
