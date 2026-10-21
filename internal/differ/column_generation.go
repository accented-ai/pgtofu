package differ

import (
	"fmt"
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/accented-ai/pgtofu/internal/postgresparse"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func validateColumnGeneration(current, desired *schema.Database) error {
	for i := range desired.Tables {
		table := &desired.Tables[i]

		before := current.GetTable(table.Schema, table.Name)
		if before == nil {
			continue
		}

		for j := range table.Columns {
			column := &table.Columns[j]

			old := before.GetColumn(column.Name)
			if old == nil || columnsHaveSameGeneration(old, column) {
				continue
			}

			return fmt.Errorf(
				"generated column definition differs for %s.%s; use a manual migration to preserve existing data and dependencies",
				table.QualifiedName(),
				column.Name,
			)
		}
	}

	return nil
}

func columnsHaveSameGeneration(current, desired *schema.Column) bool {
	if current.IsGenerated != desired.IsGenerated {
		return false
	}

	if !current.IsGenerated {
		return true
	}

	if current.GeneratedStorage() != desired.GeneratedStorage() {
		return false
	}

	left, leftOK := canonicalGeneratedColumnExpression(current.GenerationExpression)

	right, rightOK := canonicalGeneratedColumnExpression(desired.GenerationExpression)
	if leftOK && rightOK {
		return left == right
	}

	return strings.TrimSpace(
		current.GenerationExpression,
	) == strings.TrimSpace(
		desired.GenerationExpression,
	)
}

func canonicalGeneratedColumnExpression(expression string) (string, bool) {
	tree, err := postgresparse.Parse("SELECT " + expression)
	if err != nil || len(tree.GetStmts()) != 1 {
		return "", false
	}

	statement := tree.GetStmts()[0].GetStmt().GetSelectStmt()
	if statement == nil || len(statement.GetTargetList()) != 1 {
		return "", false
	}

	walkPolicyMessage(tree.ProtoReflect(), func(message protoreflect.Message) {
		node, ok := message.Interface().(*pgquery.Node)
		if !ok || node.GetTypeCast() == nil {
			return
		}

		cast := node.GetTypeCast()
		if isGeneratedLiteralTextCast(cast) {
			return
		}

		names := cast.GetTypeName().GetNames()
		if len(names) != 0 && names[len(names)-1].GetString_() != nil {
			name := names[len(names)-1].GetString_()
			// Generation casts can truncate or round stored values; view normalization ignores them.
			name.Sval = "__pgtofu_generated_cast_" + name.GetSval()
		}
	})

	return canonicalPostgresNode(statement.GetTargetList()[0].GetResTarget().GetVal()), true
}

func isGeneratedLiteralTextCast(cast *pgquery.TypeCast) bool {
	typeName := cast.GetTypeName()
	if !strings.EqualFold(postgresNodeListLastString(typeName.GetNames()), "text") ||
		len(typeName.GetTypmods()) != 0 {
		return false
	}

	if constant := cast.GetArg().GetAConst(); constant != nil {
		return constant.GetSval() != nil || constant.GetIsnull()
	}

	if array := cast.GetArg().GetAArrayExpr(); array != nil {
		for _, element := range array.GetElements() {
			if element.GetAConst() == nil || element.GetAConst().GetSval() == nil {
				return false
			}
		}

		return true
	}

	return false
}

func generatedColumnDependencies(column *schema.Column, functions []schema.Function) []string {
	if !column.IsGenerated {
		return nil
	}

	return resolveFunctionDependencies(
		extractFunctionCallReferences(column.GenerationExpression),
		functions,
	)
}

func generatedColumnDependsOn(change, other *Change) bool {
	if change.ObjectName != other.ObjectName {
		return false
	}

	column, _ := change.Details["column"].(*schema.Column)

	otherColumn, _ := other.Details["column"].(*schema.Column)
	if column == nil || otherColumn == nil {
		return false
	}

	if change.Type == ChangeTypeAddColumn && other.Type == ChangeTypeAddColumn {
		return column.IsGenerated && !otherColumn.IsGenerated
	}

	if change.Type == ChangeTypeDropColumn && other.Type == ChangeTypeDropColumn {
		return !column.IsGenerated && otherColumn.IsGenerated
	}

	return false
}
