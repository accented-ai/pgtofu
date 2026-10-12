package differ

import (
	"slices"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	wasmquery "github.com/wasilibs/go-pgquery"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func (d *Differ) preparePolicyDependencies(result *DiffResult) {
	for i := range result.Current.Tables {
		current := &result.Current.Tables[i]

		desired := result.Desired.GetTable(current.Schema, current.Name)
		if desired == nil {
			continue
		}

		for j := range current.Policies {
			policy := &current.Policies[j]
			if next := policyMap(desired.Policies)[policy.Name]; next != nil &&
				policyNeedsRecreation(current, policy, result) {
				name := current.QualifiedName() + "." + policy.Name
				result.Changes = slices.DeleteFunc(result.Changes, func(change Change) bool {
					return change.Type == ChangeTypeModifyPolicy && change.ObjectName == name
				})
				result.Changes = append(
					result.Changes,
					policyChange(
						ChangeTypeDropPolicy,
						current,
						policy,
						map[string]any{"policy": policy, "recreate": true},
					),
					policyChange(
						ChangeTypeAddPolicy,
						desired,
						next,
						map[string]any{"policy": next},
					),
				)
			}
		}
	}

	for i := range result.Changes {
		change := &result.Changes[i]
		if policy, table := changePolicy(change, false); policy != nil {
			change.DependsOn = policyDependencies(table, policy, result.Desired)
		}

		if policy, table := changePolicy(change, true); policy != nil {
			change.RollbackDependsOn = policyDependencies(table, policy, result.Current)
		}
	}
}

func policyNeedsRecreation(table *schema.Table, policy *schema.Policy, result *DiffResult) bool {
	dependencies := policyDependencies(table.QualifiedName(), policy, result.Current)
	for i := range result.Changes {
		change := &result.Changes[i]
		switch change.Type {
		case ChangeTypeModifyColumnType, ChangeTypeDropColumn:
			target, column, ok := getColumnFromChange(change)
			if ok && policyUsesColumn(table.QualifiedName(), policy, target, column) {
				return true
			}
		case ChangeTypeDropTable, ChangeTypeDropFunction, ChangeTypeDropCustomType:
			if tableMatchesDependency(change.ObjectName, dependencies) {
				return true
			}
		}
	}

	return false
}

func changePolicy(change *Change, current bool) (*schema.Policy, string) {
	key := "policy"

	switch change.Type {
	case ChangeTypeModifyPolicy:
		key = "desired"
		if current {
			key = "current"
		}
	case ChangeTypeAddPolicy:
		if current {
			return nil, ""
		}
	case ChangeTypeDropPolicy:
		if !current {
			return nil, ""
		}
	default:
		return nil, ""
	}

	policy, _ := change.Details[key].(*schema.Policy)
	table, _ := change.Details["table"].(string)

	return policy, table
}

func policyDependencies(table string, policy *schema.Policy, db *schema.Database) []string {
	dependencies := make([]string, 0, 1+len(db.Tables)+len(db.Functions))

	dependencies = append(dependencies, table)
	for _, expression := range []string{policy.UsingExpression, policy.CheckExpression} {
		dependencies = append(dependencies, policyExpressionRelations(expression)...)
		for _, typeName := range policyExpressionTypes(expression) {
			for _, customType := range db.CustomTypes {
				if typeName == TableKey(customType.Schema, customType.Name) {
					dependencies = append(dependencies, typeName)
				}
			}
		}

		dependencies = append(dependencies,
			resolveFunctionDependencies(extractFunctionCallReferences(expression), db.Functions)...)
	}

	return dedupeDependencies(dependencies)
}

func policyUsesColumn(owner string, policy *schema.Policy, table, column string) bool {
	relations := make([]string, 0, 1)

	relations = append(relations, owner)
	for _, expression := range []string{policy.UsingExpression, policy.CheckExpression} {
		relations = append(relations, policyExpressionRelations(expression)...)
	}

	if !tableMatchesDependency(table, relations) {
		return false
	}

	for _, expression := range []string{policy.UsingExpression, policy.CheckExpression} {
		if indexExpressionUsesColumn(expression, schema.NormalizeIdentifier(column)) {
			return true
		}

		if policyExpressionUsesWholeRow(expression, table) {
			return true
		}
	}

	return false
}

func visitPolicyExpression(expression string, visit func(protoreflect.Message)) {
	if expression == "" {
		return
	}

	viewASTNormalizeMu.Lock()
	defer viewASTNormalizeMu.Unlock()

	tree, err := wasmquery.Parse("SELECT " + expression)
	if err != nil {
		return
	}

	walkPolicyMessage(tree.ProtoReflect(), visit)
}

func policyExpressionRelations(expression string) []string {
	var relations []string

	visitPolicyExpression(expression, func(message protoreflect.Message) {
		if relation, ok := message.Interface().(*pgquery.RangeVar); ok {
			relations = append(relations, TableKey(relation.GetSchemaname(), relation.GetRelname()))
		}
	})

	return dedupeDependencies(relations)
}

func policyExpressionTypes(expression string) []string {
	var types []string

	visitPolicyExpression(expression, func(message protoreflect.Message) {
		if typeName, ok := message.Interface().(*pgquery.TypeName); ok {
			names := typeName.GetNames()
			if len(names) == 1 {
				types = append(types, TableKey("", names[0].GetString_().GetSval()))
			} else if len(names) == 2 {
				types = append(
					types,
					TableKey(names[0].GetString_().GetSval(), names[1].GetString_().GetSval()),
				)
			}
		}
	})

	return dedupeDependencies(types)
}

func policyExpressionUsesWholeRow(expression, table string) bool {
	parts := splitIdentifierParts(table)
	if len(parts) == 0 {
		return false
	}

	name := schema.NormalizeIdentifier(parts[len(parts)-1])
	usesWholeRow := false

	visitPolicyExpression(expression, func(message protoreflect.Message) {
		if column, ok := message.Interface().(*pgquery.ColumnRef); ok {
			fields := column.GetFields()
			for _, field := range fields {
				if field.GetAStar() != nil {
					usesWholeRow = true
				}
			}

			if len(fields) == 1 &&
				schema.NormalizeIdentifier(fields[0].GetString_().GetSval()) == name {
				usesWholeRow = true
			}
		}
	})

	return usesWholeRow
}

func walkPolicyMessage(message protoreflect.Message, visit func(protoreflect.Message)) {
	visit(message)

	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind {
			return true
		}

		if field.IsList() {
			list := value.List()
			for i := range list.Len() {
				walkPolicyMessage(list.Get(i).Message(), visit)
			}
		} else {
			walkPolicyMessage(value.Message(), visit)
		}

		return true
	})
}

func policyChangeDependsOn(change, other *Change) bool {
	policy, owner := changePolicy(change, false)
	if policy != nil {
		if other.Type == ChangeTypeDropPolicy && change.ObjectName == other.ObjectName {
			return true
		}

		switch other.Type {
		case ChangeTypeAddColumn, ChangeTypeModifyColumnType:
			table, column, ok := getColumnFromChange(other)
			return ok && policyUsesColumn(owner, policy, table, column)
		case ChangeTypeAddCustomType:
			return true
		}
	}

	previous, owner := changePolicy(other, true)
	if previous == nil {
		return false
	}

	switch change.Type {
	case ChangeTypeDropTable, ChangeTypeDropFunction, ChangeTypeDropCustomType:
		return tableMatchesDependency(change.ObjectName, other.RollbackDependsOn)
	case ChangeTypeDropColumn, ChangeTypeModifyColumnType:
		table, column, ok := getColumnFromChange(change)
		return ok && policyUsesColumn(owner, previous, table, column)
	}

	return false
}
