package differ

import (
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func (d *Differ) processContinuousAggregateRecreationForColumnChanges(result *DiffResult) {
	tablesWithColumnChanges := d.findTablesWithColumnChangesAffectingCAs(result.Changes)
	if len(tablesWithColumnChanges) == 0 {
		return
	}

	d.processContinuousAggregatesForColumnChanges(result, tablesWithColumnChanges)
}

func (d *Differ) findTablesWithColumnChangesAffectingCAs(changes []Change) map[string]bool {
	tables := make(map[string]bool)

	for _, change := range changes {
		if change.Type == ChangeTypeModifyColumnType ||
			change.Type == ChangeTypeModifyColumnNullability {
			tableName, _ := change.Details["table"].(string)
			if tableName != "" {
				tables[strings.ToLower(tableName)] = true
			}
		}
	}

	return tables
}

func (d *Differ) processContinuousAggregatesForColumnChanges(
	result *DiffResult,
	tablesWithColumnChanges map[string]bool,
) {
	currentCAs := buildContinuousAggregateMap(result.Current.ContinuousAggregates)
	desiredCAs := buildContinuousAggregateMap(result.Desired.ContinuousAggregates)

	casWithChanges := make(map[string]int)

	for i, change := range result.Changes {
		if change.Type == ChangeTypeModifyContinuousAggregate ||
			change.Type == ChangeTypeDropContinuousAggregate {
			casWithChanges[change.ObjectName] = i
		}
	}

	for key, desiredCA := range desiredCAs {
		hypertableName := desiredCA.QualifiedHypertableName()
		if !caHypertableMatchesTables(hypertableName, tablesWithColumnChanges) {
			continue
		}

		if idx, hasChange := casWithChanges[key]; hasChange {
			if result.Changes[idx].Type == ChangeTypeModifyContinuousAggregate {
				d.convertModifyCAToDropAdd(result, idx, key, desiredCA)
			}
		} else {
			if currentCA, exists := currentCAs[key]; exists {
				d.addCARecreationChanges(result, key, currentCA, desiredCA)
			}
		}
	}
}

func caHypertableMatchesTables(hypertableName string, tables map[string]bool) bool {
	hypertableLower := strings.ToLower(hypertableName)

	if tables[hypertableLower] {
		return true
	}

	parts := strings.Split(hypertableLower, ".")

	if len(parts) == 2 && (parts[0] == schema.DefaultSchema || parts[0] == "public") {
		if tables[parts[1]] {
			return true
		}
	}

	if len(parts) == 1 {
		if tables["public."+hypertableLower] || tables[schema.DefaultSchema+"."+hypertableLower] {
			return true
		}
	}

	return false
}

func (d *Differ) convertModifyCAToDropAdd(
	result *DiffResult,
	idx int,
	key string,
	desiredCA *schema.ContinuousAggregate,
) {
	originalChange := result.Changes[idx]

	var currentCA *schema.ContinuousAggregate

	if current, ok := originalChange.Details["current"].(*schema.ContinuousAggregate); ok {
		currentCA = current
	} else {
		for i := range result.Current.ContinuousAggregates {
			ca := &result.Current.ContinuousAggregates[i]
			if ViewKey(ca.Schema, ca.ViewName) == key {
				currentCA = ca
				break
			}
		}
	}

	if currentCA == nil {
		return
	}

	result.Changes[idx] = Change{
		Type:        ChangeTypeDropContinuousAggregate,
		Severity:    SeverityBreaking,
		Description: "Drop continuous aggregate for column change: " + currentCA.QualifiedViewName(),
		ObjectType:  "continuous_aggregate",
		ObjectName:  key,
		Details: map[string]any{
			"aggregate":         currentCA,
			"for_column_change": true,
			"original_change":   originalChange,
			"will_be_recreated": true,
		},
	}

	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeAddContinuousAggregate,
		Severity:    SeveritySafe,
		Description: "Recreate continuous aggregate: " + desiredCA.QualifiedViewName(),
		ObjectType:  "continuous_aggregate",
		ObjectName:  key,
		Details: map[string]any{
			"aggregate":         desiredCA,
			"for_column_change": true,
			"is_recreation":     true,
		},
		DependsOn: []string{desiredCA.QualifiedHypertableName()},
	})
}

func (d *Differ) addCARecreationChanges(
	result *DiffResult,
	key string,
	currentCA, desiredCA *schema.ContinuousAggregate,
) {
	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeDropContinuousAggregate,
		Severity:    SeverityBreaking,
		Description: "Drop continuous aggregate for column change: " + currentCA.QualifiedViewName(),
		ObjectType:  "continuous_aggregate",
		ObjectName:  key,
		Details: map[string]any{
			"aggregate":         currentCA,
			"for_column_change": true,
			"will_be_recreated": true,
		},
	})

	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeAddContinuousAggregate,
		Severity:    SeveritySafe,
		Description: "Recreate continuous aggregate after column change: " + desiredCA.QualifiedViewName(),
		ObjectType:  "continuous_aggregate",
		ObjectName:  key,
		Details: map[string]any{
			"aggregate":         desiredCA,
			"for_column_change": true,
			"is_recreation":     true,
		},
		DependsOn: []string{desiredCA.QualifiedHypertableName()},
	})
}
