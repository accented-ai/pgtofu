package differ

import (
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/accented-ai/pgtofu/internal/postgresparse"
)

func expandBetween(expression string) string {
	tree, err := postgresparse.Parse("SELECT " + expression)
	if err != nil || len(tree.GetStmts()) != 1 {
		return expression
	}

	statement := tree.GetStmts()[0].GetStmt().GetSelectStmt()
	if statement == nil || len(statement.GetTargetList()) != 1 {
		return expression
	}

	unsupportedRange := false

	walkPolicyMessage(tree.ProtoReflect(), func(message protoreflect.Message) {
		node, ok := message.Interface().(*pgquery.Node)
		if !ok || node.GetAExpr() == nil {
			return
		}

		between := node.GetAExpr()
		switch between.GetKind() {
		case pgquery.A_Expr_Kind_AEXPR_NOT_BETWEEN,
			pgquery.A_Expr_Kind_AEXPR_BETWEEN_SYM,
			pgquery.A_Expr_Kind_AEXPR_NOT_BETWEEN_SYM:
			unsupportedRange = true

			return
		}

		if between.GetKind() != pgquery.A_Expr_Kind_AEXPR_BETWEEN {
			return
		}

		bounds := between.GetRexpr().GetList().GetItems()
		if len(bounds) != 2 {
			return
		}

		// PostgreSQL lowers BETWEEN to these ordered comparisons, repeating the operand.
		node.Node = pgquery.MakeBoolExprNode(pgquery.BoolExprType_AND_EXPR, []*pgquery.Node{
			pgquery.MakeAExprNode(pgquery.A_Expr_Kind_AEXPR_OP,
				[]*pgquery.Node{pgquery.MakeStrNode(">=")}, between.GetLexpr(), bounds[0], -1),
			pgquery.MakeAExprNode(pgquery.A_Expr_Kind_AEXPR_OP,
				[]*pgquery.Node{pgquery.MakeStrNode("<=")}, between.GetLexpr(), bounds[1], -1),
		}, -1).GetNode()
	})

	// The deparser can drop operand grouping for unsupported range forms.
	if unsupportedRange {
		return expression
	}

	sql, err := postgresparse.Deparse(tree)
	if err != nil {
		return expression
	}

	return strings.ToLower(strings.TrimPrefix(sql, "SELECT "))
}
