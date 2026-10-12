package differ

import (
	"fmt"
	"slices"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func (d *Differ) compareRowSecurity(result *DiffResult) {
	for i := range result.Desired.Tables {
		desired := &result.Desired.Tables[i]

		current := result.Current.GetTable(desired.Schema, desired.Name)
		if current == nil {
			current = &schema.Table{Schema: desired.Schema, Name: desired.Name}
		}

		if current.RowSecurityEnabled != desired.RowSecurityEnabled ||
			current.RowSecurityForced != desired.RowSecurityForced {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeModifyTableRowSecurity,
				Severity:    SeverityPotentiallyBreaking,
				Description: "Modify row security: " + desired.QualifiedName(),
				ObjectType:  "table", ObjectName: TableKey(desired.Schema, desired.Name),
				Details:   map[string]any{"current": current, "desired": desired},
				DependsOn: []string{TableKey(desired.Schema, desired.Name)},
			})
		}

		d.compareTablePolicies(result, current, desired)
	}

	for i := range result.Current.Tables {
		current := &result.Current.Tables[i]
		if result.Desired.GetTable(current.Schema, current.Name) == nil {
			d.compareTablePolicies(
				result,
				current,
				&schema.Table{Schema: current.Schema, Name: current.Name},
			)
		}
	}
}

func (d *Differ) compareTablePolicies(result *DiffResult, current, desired *schema.Table) {
	currentPolicies := policyMap(current.Policies)
	desiredPolicies := policyMap(desired.Policies)

	for name, policy := range desiredPolicies {
		if previous, exists := currentPolicies[name]; exists {
			if !policiesEqual(previous, policy) {
				result.Changes = append(
					result.Changes,
					policyChange(ChangeTypeModifyPolicy, desired,
						policy, map[string]any{"current": previous, "desired": policy}),
				)
			}
		} else {
			result.Changes = append(result.Changes, policyChange(ChangeTypeAddPolicy, desired,
				policy, map[string]any{"policy": policy}))
		}
	}

	for name, policy := range currentPolicies {
		if _, exists := desiredPolicies[name]; !exists {
			result.Changes = append(result.Changes, policyChange(ChangeTypeDropPolicy, current,
				policy, map[string]any{"policy": policy}))
		}
	}
}

func policyChange(
	kind ChangeType,
	table *schema.Table,
	policy *schema.Policy,
	details map[string]any,
) Change {
	details["table"] = table.QualifiedName()
	details["schema"] = table.Schema
	details["table_name"] = table.Name

	return Change{
		Type: kind, Severity: SeverityPotentiallyBreaking,
		Description: fmt.Sprintf("%s policy %s on %s", strings.ReplaceAll(string(kind), "_", " "),
			policy.Name, table.QualifiedName()),
		ObjectType: "policy", ObjectName: table.QualifiedName() + "." + policy.Name,
		Details: details,
	}
}

func policyMap(policies []schema.Policy) map[string]*schema.Policy {
	result := make(map[string]*schema.Policy, len(policies))
	for i := range policies {
		result[policies[i].Name] = &policies[i]
	}

	return result
}

func policiesEqual(current, desired *schema.Policy) bool {
	return policyCommand(current.Command) == policyCommand(desired.Command) &&
		current.Permissive == desired.Permissive &&
		slices.Equal(policyRoleNames(current.Roles), policyRoleNames(desired.Roles)) &&
		policyExpressionsEqual(current.UsingExpression, desired.UsingExpression) &&
		policyExpressionsEqual(current.CheckExpression, desired.CheckExpression)
}

func policyCommand(command string) string {
	if command == "" {
		return "ALL"
	}

	return strings.ToUpper(command)
}

func policyRoleNames(roles []schema.PolicyRole) []string {
	if len(roles) == 0 {
		return []string{"PUBLIC"}
	}

	names := make([]string, 0, len(roles))
	for _, role := range roles {
		if role.Public {
			names = append(names, "PUBLIC")
		} else {
			names = append(names, "ROLE:"+role.Name)
		}
	}

	slices.Sort(names)

	return slices.Compact(names)
}
