package generator

import (
	"errors"
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

type rowSecurityBuilder struct{}

func (b *rowSecurityBuilder) BuildUp(change differ.Change, _ *DDLBuilder) (DDLStatement, error) {
	return buildRowSecurityChange(change, false)
}

func (b *rowSecurityBuilder) BuildDown(change differ.Change, _ *DDLBuilder) (DDLStatement, error) {
	return buildRowSecurityChange(change, true)
}

func buildRowSecurityChange(change differ.Change, down bool) (DDLStatement, error) {
	current, currentOK := change.Details["current"].(*schema.Table)

	desired, desiredOK := change.Details["desired"].(*schema.Table)
	if !currentOK || !desiredOK || current == nil || desired == nil {
		return DDLStatement{}, errors.New("row security change requires current and desired tables")
	}

	if down {
		current, desired = desired, current
	}

	return DDLStatement{
		SQL:         tableRowSecuritySQL(current, desired),
		Description: change.Description, RequiresTx: true, IsUnsafe: true,
	}, nil
}

func tableRowSecuritySQL(current, desired *schema.Table) string {
	target := policyQualifiedName(desired.Schema, desired.Name)

	var statements []string

	if current.RowSecurityEnabled != desired.RowSecurityEnabled {
		action := "DISABLE"
		if desired.RowSecurityEnabled {
			action = "ENABLE"
		}

		statements = append(
			statements,
			fmt.Sprintf("ALTER TABLE %s %s ROW LEVEL SECURITY;", target, action),
		)
	}

	if current.RowSecurityForced != desired.RowSecurityForced {
		action := "NO FORCE"
		if desired.RowSecurityForced {
			action = "FORCE"
		}

		statements = append(
			statements,
			fmt.Sprintf("ALTER TABLE %s %s ROW LEVEL SECURITY;", target, action),
		)
	}

	return strings.Join(statements, "\n")
}

type policyBuilder struct{}

func (b *policyBuilder) BuildUp(change differ.Change, ddl *DDLBuilder) (DDLStatement, error) {
	return buildPolicyChange(change, ddl, false)
}

func (b *policyBuilder) BuildDown(change differ.Change, ddl *DDLBuilder) (DDLStatement, error) {
	return buildPolicyChange(change, ddl, true)
}

func buildPolicyChange(change differ.Change, ddl *DDLBuilder, down bool) (DDLStatement, error) {
	key := "policy"
	if change.Type == differ.ChangeTypeModifyPolicy {
		key = "desired"
		if down {
			key = "current"
		}
	}

	policy, ok := change.Details[key].(*schema.Policy)
	if !ok || policy == nil {
		return DDLStatement{}, errors.New("policy change requires a policy definition")
	}

	schemaName, _ := change.Details["schema"].(string)

	tableName, _ := change.Details["table_name"].(string)
	if tableName == "" || policy.Name == "" {
		return DDLStatement{}, errors.New("policy change requires table and policy names")
	}

	target := policyQualifiedName(schemaName, tableName)
	drop := fmt.Sprintf(
		"DROP POLICY %s%s ON %s;",
		ddl.ifExists(),
		policyIdentifier(policy.Name),
		target,
	)
	sql := drop

	if change.Type != differ.ChangeTypeDropPolicy {
		create, err := createPolicySQL(schemaName, tableName, policy, ddl.idempotent)
		if err != nil {
			return DDLStatement{}, err
		}

		sql = create
		if change.Type == differ.ChangeTypeModifyPolicy {
			sql = drop + "\n" + create
		}
	}

	return DDLStatement{
		SQL:         sql,
		Description: change.Description,
		RequiresTx:  true,
		IsUnsafe:    true,
	}, nil
}

func createPolicySQL(
	schemaName, tableName string,
	policy *schema.Policy,
	idempotent bool,
) (string, error) {
	command := strings.ToUpper(policy.Command)
	if command == "" {
		command = "ALL"
	}

	switch command {
	case "ALL", "SELECT", "INSERT", "UPDATE", "DELETE":
	default:
		return "", fmt.Errorf("unsupported policy command %s", policy.Command)
	}

	mode := "RESTRICTIVE"
	if policy.Permissive {
		mode = "PERMISSIVE"
	}

	roles := make([]string, 0, len(policy.Roles))
	for _, role := range policy.Roles {
		switch {
		case role.Public:
			roles = append(roles, "PUBLIC")
		case role.Name != "":
			roles = append(roles, policyIdentifier(role.Name))
		default:
			return "", errors.New("policy role requires a name or PUBLIC")
		}
	}

	if len(roles) == 0 {
		roles = append(roles, "PUBLIC")
	}

	sql := fmt.Sprintf("CREATE POLICY %s ON %s AS %s FOR %s TO %s",
		policyIdentifier(policy.Name), policyQualifiedName(schemaName, tableName),
		mode, command, strings.Join(roles, ", "))
	if policy.UsingExpression != "" {
		sql += "\n    USING (" + policy.UsingExpression + ")"
	}

	if policy.CheckExpression != "" {
		sql += "\n    WITH CHECK (" + policy.CheckExpression + ")"
	}

	sql += ";"
	if !idempotent {
		return sql, nil
	}

	if schemaName == "" {
		schemaName = schema.DefaultSchema
	}

	body := fmt.Sprintf(`BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_policy p
        JOIN pg_catalog.pg_class c ON c.oid = p.polrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND p.polname = %s
    ) THEN
        %s
    END IF;
END;`, formatSQLStringLiteral(schemaName), formatSQLStringLiteral(tableName),
		formatSQLStringLiteral(policy.Name), sql)

	tag := "$pgtofu_policy$"
	for strings.Contains(body, tag) {
		tag = "$" + strings.Trim(tag, "$") + "_$"
	}

	return "DO " + tag + "\n" + body + "\n" + tag + ";", nil
}

func policyIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func policyQualifiedName(schemaName, tableName string) string {
	if schemaName == "" {
		schemaName = schema.DefaultSchema
	}

	return policyIdentifier(schemaName) + "." + policyIdentifier(tableName)
}
