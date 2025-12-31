package differ

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type FunctionComparator struct {
	options *Options
}

func NewFunctionComparator(opts *Options) *FunctionComparator {
	return &FunctionComparator{options: opts}
}

func (fc *FunctionComparator) Compare(result *DiffResult) {
	currentFuncs := buildFunctionMap(result.Current.Functions)
	desiredFuncs := buildFunctionMap(result.Desired.Functions)

	fc.detectAddedFunctions(result, currentFuncs, desiredFuncs)
	fc.detectDroppedFunctions(result, currentFuncs, desiredFuncs)
	fc.detectModifiedFunctions(result, currentFuncs, desiredFuncs, result.Current.Triggers)
}

func (fc *FunctionComparator) detectAddedFunctions(
	result *DiffResult,
	currentFuncs, desiredFuncs map[string]*schema.Function,
) {
	for key, fn := range desiredFuncs {
		if _, exists := currentFuncs[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeAddFunction,
				Severity:    SeveritySafe,
				Description: "Add function: " + fn.Signature(),
				ObjectType:  "function",
				ObjectName:  key,
				Details: map[string]any{
					"function": fn,
				},
			})

			if !fc.options.IgnoreComments && fn.Comment != "" {
				result.Changes = append(result.Changes, Change{
					Type:        ChangeTypeModifyFunction,
					Severity:    SeveritySafe,
					Description: "Add function comment: " + fn.Signature(),
					ObjectType:  "function",
					ObjectName:  key,
					Details: map[string]any{
						"function":    fn,
						"old_comment": "",
						"new_comment": fn.Comment,
					},
				})
			}
		}
	}
}

func (fc *FunctionComparator) detectDroppedFunctions(
	result *DiffResult,
	currentFuncs, desiredFuncs map[string]*schema.Function,
) {
	for key, fn := range currentFuncs {
		if _, exists := desiredFuncs[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeDropFunction,
				Severity:    SeverityBreaking,
				Description: "Drop function: " + fn.Signature(),
				ObjectType:  "function",
				ObjectName:  key,
				Details: map[string]any{
					"function": fn,
				},
			})
		}
	}
}

func (fc *FunctionComparator) detectModifiedFunctions(
	result *DiffResult,
	currentFuncs, desiredFuncs map[string]*schema.Function,
	triggers []schema.Trigger,
) {
	for key, desiredFn := range desiredFuncs {
		currentFn, exists := currentFuncs[key]
		if !exists {
			continue
		}

		sigEqual := currentFn.Signature() == desiredFn.Signature()
		retEqual := NormalizeDataType(
			currentFn.ReturnType,
		) == NormalizeDataType(
			desiredFn.ReturnType,
		)
		langEqual := strings.EqualFold(currentFn.Language, desiredFn.Language)
		volEqual := currentFn.Volatility == desiredFn.Volatility
		secDefEqual := currentFn.IsSecurityDefiner == desiredFn.IsSecurityDefiner
		strictEqual := currentFn.IsStrict == desiredFn.IsStrict
		body1 := normalizeFunctionBody(currentFn.Body)
		body2 := normalizeFunctionBody(desiredFn.Body)
		bodyEqual := body1 == body2
		currentComment := normalizeComment(currentFn.Comment)
		desiredComment := normalizeComment(desiredFn.Comment)
		commentEqual := currentComment == desiredComment

		funcBodyEqual := sigEqual && retEqual && langEqual && volEqual && secDefEqual &&
			strictEqual && bodyEqual

		if !funcBodyEqual {
			severity := SeverityPotentiallyBreaking
			if isTriggerFunction(triggers, currentFn) {
				severity = SeverityBreaking
			}

			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeModifyFunction,
				Severity:    severity,
				Description: "Modify function: " + desiredFn.Signature(),
				ObjectType:  "function",
				ObjectName:  key,
				Details: map[string]any{
					"current": currentFn,
					"desired": desiredFn,
				},
			})
		}

		if !fc.options.IgnoreComments && !commentEqual && funcBodyEqual {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeModifyFunction,
				Severity:    SeveritySafe,
				Description: "Modify function comment: " + desiredFn.Signature(),
				ObjectType:  "function",
				ObjectName:  key,
				Details: map[string]any{
					"function":    desiredFn,
					"old_comment": currentFn.Comment,
					"new_comment": desiredFn.Comment,
				},
			})
		}
	}
}

type TriggerComparator struct{}

func NewTriggerComparator() *TriggerComparator {
	return &TriggerComparator{}
}

func (tc *TriggerComparator) Compare(result *DiffResult) {
	currentTriggers := buildTriggerMap(result.Current.Triggers)
	desiredTriggers := buildTriggerMap(result.Desired.Triggers)

	tc.detectAddedTriggers(result, currentTriggers, desiredTriggers)
	tc.detectDroppedTriggers(result, currentTriggers, desiredTriggers)
	tc.detectModifiedTriggers(result, currentTriggers, desiredTriggers)
}

func (tc *TriggerComparator) detectAddedTriggers(
	result *DiffResult,
	currentTriggers, desiredTriggers map[string]*schema.Trigger,
) {
	for key, trigger := range desiredTriggers {
		if _, exists := currentTriggers[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeAddTrigger,
				Severity: SeveritySafe,
				Description: fmt.Sprintf(
					"Add trigger: %s on %s",
					trigger.Name,
					trigger.QualifiedTableName(),
				),
				ObjectType: "trigger",
				ObjectName: key,
				Details: map[string]any{
					"trigger": trigger,
				},
				DependsOn: tc.buildTriggerDependencies(trigger, result.Desired, true),
			})
		}
	}
}

func (tc *TriggerComparator) detectDroppedTriggers(
	result *DiffResult,
	currentTriggers, desiredTriggers map[string]*schema.Trigger,
) {
	for key, trigger := range currentTriggers {
		if _, exists := desiredTriggers[key]; !exists {
			if tc.isInheritedPartitionTrigger(trigger, result.Desired) {
				continue
			}

			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeDropTrigger,
				Severity: SeverityBreaking,
				Description: fmt.Sprintf(
					"Drop trigger: %s from %s",
					trigger.Name,
					trigger.QualifiedTableName(),
				),
				ObjectType: "trigger",
				ObjectName: key,
				Details: map[string]any{
					"trigger": trigger,
				},
				DependsOn: tc.buildTriggerDependencies(trigger, result.Current, false),
			})
		}
	}
}

func (tc *TriggerComparator) detectModifiedTriggers(
	result *DiffResult,
	currentTriggers, desiredTriggers map[string]*schema.Trigger,
) {
	for key, desiredTrigger := range desiredTriggers {
		currentTrigger, exists := currentTriggers[key]
		if !exists {
			continue
		}

		if areTriggersEqual(currentTrigger, desiredTrigger) {
			continue
		}

		result.Changes = append(result.Changes, Change{
			Type:     ChangeTypeModifyTrigger,
			Severity: SeverityBreaking,
			Description: fmt.Sprintf(
				"Modify trigger: %s on %s",
				desiredTrigger.Name,
				desiredTrigger.QualifiedTableName(),
			),
			ObjectType: "trigger",
			ObjectName: key,
			Details: map[string]any{
				"current": currentTrigger,
				"desired": desiredTrigger,
			},
			DependsOn: tc.buildTriggerDependencies(desiredTrigger, result.Desired, true),
		})
	}
}

func buildFunctionMap(functions []schema.Function) map[string]*schema.Function {
	m := make(map[string]*schema.Function, len(functions))
	for i := range functions {
		fn := &functions[i]
		key := FunctionKey(fn.Schema, fn.Name, fn.ArgumentTypes)
		m[key] = fn
	}

	return m
}

func buildTriggerMap(triggers []schema.Trigger) map[string]*schema.Trigger {
	result := make(map[string]*schema.Trigger, len(triggers))
	for i := range triggers {
		t := &triggers[i]
		key := triggerKey(t)
		result[key] = t
	}

	return result
}

func triggerKey(trigger *schema.Trigger) string {
	return fmt.Sprintf("%s.%s.%s",
		normalizeSchema(trigger.Schema),
		strings.ToLower(trigger.TableName),
		strings.ToLower(trigger.Name))
}

func areTriggersEqual(t1, t2 *schema.Trigger) bool {
	if t1.QualifiedTableName() != t2.QualifiedTableName() {
		return false
	}

	if t1.Timing != t2.Timing {
		return false
	}

	if !equalStringSlicesSorted(t1.Events, t2.Events) {
		return false
	}

	if t1.ForEachRow != t2.ForEachRow {
		return false
	}

	if t1.QualifiedFunctionName() != t2.QualifiedFunctionName() {
		return false
	}

	when1 := normalizeExpression(t1.WhenCondition)
	when2 := normalizeExpression(t2.WhenCondition)

	return when1 == when2
}

func isTriggerFunction(triggers []schema.Trigger, fn *schema.Function) bool {
	funcKey := fn.QualifiedName()
	for i := range triggers {
		if triggers[i].QualifiedFunctionName() == funcKey {
			return true
		}
	}

	return false
}

func (tc *TriggerComparator) buildTriggerDependencies(
	trigger *schema.Trigger,
	db *schema.Database,
	includeTable bool,
) []string {
	var deps []string

	if includeTable {
		deps = append(deps, trigger.QualifiedTableName())
	}

	deps = append(deps, tc.resolveTriggerFunctionKeys(trigger, db)...)
	deps = append(deps, trigger.QualifiedFunctionName())

	return dedupeDependencies(deps)
}

func (tc *TriggerComparator) resolveTriggerFunctionKeys(
	trigger *schema.Trigger,
	db *schema.Database,
) []string {
	normalizedSchema := schema.NormalizeSchemaName(trigger.FunctionSchema)
	normalizedName := schema.NormalizeIdentifier(trigger.FunctionName)

	var keys []string

	for i := range db.Functions {
		fn := &db.Functions[i]
		if schema.NormalizeSchemaName(fn.Schema) == normalizedSchema &&
			schema.NormalizeIdentifier(fn.Name) == normalizedName {
			keys = append(keys, FunctionKey(fn.Schema, fn.Name, fn.ArgumentTypes))
		}
	}

	if len(keys) == 0 {
		keys = append(keys, FunctionKey(normalizedSchema, normalizedName, nil))
	}

	return keys
}

func (tc *TriggerComparator) isInheritedPartitionTrigger(
	trigger *schema.Trigger,
	desiredDB *schema.Database,
) bool {
	parentTable := tc.findParentTableForPartition(trigger.Schema, trigger.TableName, desiredDB)
	if parentTable == nil {
		return false
	}

	parentTriggerKey := triggerKey(&schema.Trigger{
		Schema:    trigger.Schema,
		TableName: parentTable.Name,
		Name:      trigger.Name,
	})

	for i := range desiredDB.Triggers {
		desiredTrigger := &desiredDB.Triggers[i]
		if triggerKey(desiredTrigger) == parentTriggerKey {
			return true
		}
	}

	return false
}

func (tc *TriggerComparator) findParentTableForPartition(
	partitionSchema, partitionName string,
	db *schema.Database,
) *schema.Table {
	for i := range db.Tables {
		table := &db.Tables[i]
		if table.PartitionStrategy == nil {
			continue
		}

		if schema.NormalizeSchemaName(table.Schema) == schema.NormalizeSchemaName(partitionSchema) {
			for j := range table.PartitionStrategy.Partitions {
				partition := &table.PartitionStrategy.Partitions[j]
				if schema.NormalizeIdentifier(
					partition.Name,
				) == schema.NormalizeIdentifier(
					partitionName,
				) {
					return table
				}
			}
		}
	}

	return nil
}

func dedupeDependencies(deps []string) []string {
	seen := make(map[string]struct{}, len(deps))

	var result []string //nolint:prealloc

	for _, dep := range deps {
		dep = strings.TrimSpace(dep)
		if dep == "" {
			continue
		}

		key := strings.ToLower(dep)
		if _, exists := seen[key]; exists {
			continue
		}

		seen[key] = struct{}{}

		result = append(result, dep)
	}

	return result
}

func normalizeFunctionBody(body string) string {
	normalized := strings.TrimSpace(body)

	normalized = strings.TrimPrefix(normalized, "$$")
	normalized = strings.TrimSuffix(normalized, "$$")

	if strings.HasPrefix(normalized, "$") && strings.Contains(normalized, "$") {
		endTag := strings.Index(normalized[1:], "$")
		if endTag > 0 {
			tag := normalized[:endTag+2]
			normalized = strings.TrimPrefix(normalized, tag)
			normalized = strings.TrimSuffix(normalized, tag)
		}
	}

	normalized = strings.Join(strings.Fields(normalized), " ")
	normalized = strings.ToLower(normalized)

	return normalized
}
