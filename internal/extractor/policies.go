package extractor

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

const queryPolicies = `
	SELECT p.polname,
		CASE p.polcmd
			WHEN '*' THEN 'ALL'
			WHEN 'r' THEN 'SELECT'
			WHEN 'a' THEN 'INSERT'
			WHEN 'w' THEN 'UPDATE'
			WHEN 'd' THEN 'DELETE'
		END,
		p.polpermissive,
		ARRAY(SELECT CASE WHEN role_oid = 0 THEN ''
			ELSE pg_catalog.pg_get_userbyid(role_oid)::text END
			FROM unnest(p.polroles) AS role_oid ORDER BY role_oid),
		pg_catalog.pg_get_expr(p.polqual, p.polrelid),
		pg_catalog.pg_get_expr(p.polwithcheck, p.polrelid)
	FROM pg_catalog.pg_policy p
	JOIN pg_catalog.pg_class c ON c.oid = p.polrelid
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = $1 AND c.relname = $2
	ORDER BY p.polname`

func (e *Extractor) extractPolicies(ctx context.Context, table *schema.Table) error {
	var policies []schema.Policy

	err := e.queryHelper.FetchAll(ctx, queryPolicies, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var (
			policy schema.Policy
			roles  []string
		)
		if err := rows.Scan(&policy.Name, &policy.Command, &policy.Permissive, &roles,
			scanner.String("using"), scanner.String("check")); err != nil {
			return util.WrapError("scan policy", err)
		}

		for _, role := range roles {
			policy.Roles = append(policy.Roles, schema.PolicyRole{Name: role, Public: role == ""})
		}

		policy.UsingExpression = scanner.GetString("using")
		policy.CheckExpression = scanner.GetString("check")
		policies = append(policies, policy)

		return nil
	}, table.Schema, table.Name)
	if err != nil {
		return util.WrapError("fetch policies", err)
	}

	table.Policies = policies

	return nil
}
