package parser

import (
	"errors"
	"fmt"
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	wasmquery "github.com/wasilibs/go-pgquery"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

type PolicyParser struct{}

func (p *PolicyParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreatePolicy}
}

func (p *PolicyParser) Parse(_ *Parser, stmt Statement, db *schema.Database) error {
	rowSecurityParseMu.Lock()
	defer rowSecurityParseMu.Unlock()

	tree, err := wasmquery.Parse(stmt.NormalizedSQL())
	if err != nil {
		return util.WrapError("parse CREATE POLICY", err)
	}

	if len(tree.GetStmts()) != 1 {
		return errors.New("expected one CREATE POLICY statement")
	}

	statement := tree.GetStmts()[0].GetStmt().GetCreatePolicyStmt()
	if statement == nil {
		return errors.New("expected CREATE POLICY")
	}

	relation := statement.GetTable()

	table := db.GetTable(relation.GetSchemaname(), relation.GetRelname())
	if table == nil {
		return fmt.Errorf("policy table %s not found",
			schema.QualifiedName(relation.GetSchemaname(), relation.GetRelname()))
	}

	policy := schema.Policy{
		Name:       schema.TruncateIdentifier(statement.GetPolicyName()),
		Command:    strings.ToUpper(statement.GetCmdName()),
		Permissive: statement.GetPermissive(),
	}
	for _, existing := range table.Policies {
		if existing.Name == policy.Name {
			return fmt.Errorf("duplicate policy %s on %s", policy.Name, table.QualifiedName())
		}
	}

	if err := validatePolicyCommand(statement); err != nil {
		return err
	}

	for _, node := range statement.GetRoles() {
		role := node.GetRoleSpec()
		switch role.GetRoletype() {
		case pgquery.RoleSpecType_ROLESPEC_PUBLIC:
			policy.Roles = append(policy.Roles, schema.PolicyRole{Public: true})
		case pgquery.RoleSpecType_ROLESPEC_CSTRING:
			policy.Roles = append(
				policy.Roles,
				schema.PolicyRole{Name: schema.TruncateIdentifier(role.GetRolename())},
			)
		default:
			return errors.New(
				"policy roles must name a role or PUBLIC; session-dependent roles cannot be resolved from schema SQL",
			)
		}
	}

	policy.UsingExpression, err = deparsePolicyExpression(tree.GetVersion(), statement.GetQual())
	if err != nil {
		return err
	}

	policy.CheckExpression, err = deparsePolicyExpression(
		tree.GetVersion(),
		statement.GetWithCheck(),
	)
	if err != nil {
		return err
	}

	table.Policies = append(table.Policies, policy)

	return nil
}

func validatePolicyCommand(statement *pgquery.CreatePolicyStmt) error {
	switch strings.ToUpper(statement.GetCmdName()) {
	case "ALL", "UPDATE":
	case "SELECT", "DELETE":
		if statement.GetWithCheck() != nil {
			return errors.New("SELECT and DELETE policies cannot have WITH CHECK")
		}
	case "INSERT":
		if statement.GetQual() != nil {
			return errors.New("INSERT policies cannot have USING")
		}
	default:
		return fmt.Errorf("unsupported policy command %s", statement.GetCmdName())
	}

	return nil
}

func deparsePolicyExpression(version int32, node *pgquery.Node) (string, error) {
	if node == nil {
		return "", nil
	}

	tree := &pgquery.ParseResult{Version: version, Stmts: []*pgquery.RawStmt{{
		Stmt: &pgquery.Node{Node: &pgquery.Node_SelectStmt{SelectStmt: &pgquery.SelectStmt{
			TargetList: []*pgquery.Node{
				{Node: &pgquery.Node_ResTarget{ResTarget: &pgquery.ResTarget{Val: node}}},
			},
		}}},
	}}}

	sql, err := wasmquery.Deparse(tree)
	if err != nil {
		return "", util.WrapError("deparse policy expression", err)
	}

	return strings.TrimPrefix(sql, "SELECT "), nil
}
