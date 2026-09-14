package generator

import (
	"fmt"
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	wasmquery "github.com/wasilibs/go-pgquery"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type viewColumnRename struct {
	from string
	to   string
}

func buildViewColumnRenameSQL(
	view *schema.View,
	currentDefinition,
	desiredDefinition string,
) string {
	currentColumns, currentOK := viewOutputColumnNames(currentDefinition)

	desiredColumns, desiredOK := viewOutputColumnNames(desiredDefinition)
	if !currentOK || !desiredOK || len(desiredColumns) < len(currentColumns) {
		return ""
	}

	renames := make([]viewColumnRename, 0)

	for index, currentColumn := range currentColumns {
		desiredColumn := desiredColumns[index]
		if currentColumn != desiredColumn {
			renames = append(renames, viewColumnRename{
				from: currentColumn,
				to:   desiredColumn,
			})
		}
	}

	if len(renames) == 0 {
		return ""
	}

	qualifiedView := QualifiedName(view.Schema, view.Name)
	if !viewColumnRenamesConflict(renames, currentColumns) {
		return formatViewColumnRenames(qualifiedView, renames)
	}

	temporaryRenames := make([]viewColumnRename, 0, len(renames)*2)
	finalRenames := make([]viewColumnRename, 0, len(renames))
	reservedNames := make(map[string]struct{}, len(currentColumns)+len(desiredColumns))

	for _, name := range append(currentColumns, desiredColumns...) {
		reservedNames[name] = struct{}{}
	}

	for index, rename := range renames {
		temporaryName := uniqueTemporaryViewColumnName(index, reservedNames)
		reservedNames[temporaryName] = struct{}{}
		temporaryRenames = append(
			temporaryRenames,
			viewColumnRename{from: rename.from, to: temporaryName},
		)
		finalRenames = append(
			finalRenames,
			viewColumnRename{from: temporaryName, to: rename.to},
		)
	}

	return formatViewColumnRenames(
		qualifiedView,
		append(temporaryRenames, finalRenames...),
	)
}

func viewOutputColumnNames(definition string) ([]string, bool) {
	definition = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(definition), ";"))
	if definition == "" {
		return nil, false
	}

	viewQueryFormatMu.Lock()
	defer viewQueryFormatMu.Unlock()

	tree, err := wasmquery.Parse(definition)
	if err != nil || len(tree.GetStmts()) != 1 {
		return nil, false
	}

	statement := leftmostViewSelect(tree.GetStmts()[0].GetStmt().GetSelectStmt())
	if statement == nil || len(statement.GetTargetList()) == 0 {
		return nil, false
	}

	columns := make([]string, 0, len(statement.GetTargetList()))
	for _, targetNode := range statement.GetTargetList() {
		target := targetNode.GetResTarget()
		if target == nil {
			return nil, false
		}

		name := target.GetName()
		if name == "" {
			name = implicitViewColumnName(target.GetVal())
		}

		if name == "" {
			return nil, false
		}

		columns = append(columns, name)
	}

	return columns, true
}

func leftmostViewSelect(statement *pgquery.SelectStmt) *pgquery.SelectStmt {
	for statement != nil && len(statement.GetTargetList()) == 0 {
		statement = statement.GetLarg()
	}

	return statement
}

func implicitViewColumnName(node *pgquery.Node) string {
	if node == nil {
		return ""
	}

	if column := node.GetColumnRef(); column != nil {
		return postgresNodeListLastString(column.GetFields())
	}

	if function := node.GetFuncCall(); function != nil {
		return postgresNodeListLastString(function.GetFuncname())
	}

	if cast := node.GetTypeCast(); cast != nil {
		return implicitViewColumnName(cast.GetArg())
	}

	if collate := node.GetCollateClause(); collate != nil {
		return implicitViewColumnName(collate.GetArg())
	}

	return ""
}

func postgresNodeListLastString(nodes []*pgquery.Node) string {
	if len(nodes) == 0 || nodes[len(nodes)-1].GetString_() == nil {
		return ""
	}

	return nodes[len(nodes)-1].GetString_().GetSval()
}

func viewColumnRenamesConflict(renames []viewColumnRename, currentColumns []string) bool {
	currentNames := make(map[string]struct{}, len(currentColumns))
	for _, column := range currentColumns {
		currentNames[column] = struct{}{}
	}

	for _, rename := range renames {
		if _, exists := currentNames[rename.to]; exists {
			return true
		}
	}

	return false
}

func uniqueTemporaryViewColumnName(index int, reservedNames map[string]struct{}) string {
	for suffix := index + 1; ; suffix++ {
		name := fmt.Sprintf("__pgtofu_view_column_%d", suffix)
		if _, exists := reservedNames[name]; !exists {
			return name
		}
	}
}

func formatViewColumnRenames(qualifiedView string, renames []viewColumnRename) string {
	statements := make([]string, 0, len(renames))
	for _, rename := range renames {
		statements = append(statements, fmt.Sprintf(
			"ALTER VIEW %s RENAME COLUMN %s TO %s;",
			qualifiedView,
			QuoteIdentifier(rename.from),
			QuoteIdentifier(rename.to),
		))
	}

	return strings.Join(statements, "\n")
}
