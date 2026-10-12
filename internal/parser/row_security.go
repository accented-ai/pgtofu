package parser

import (
	"errors"
	"fmt"
	"sync"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	wasmquery "github.com/wasilibs/go-pgquery"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

// The WASM parser retains a runtime for each concurrent call.
var rowSecurityParseMu sync.Mutex //nolint:gochecknoglobals

func (p *Parser) parseRowSecurity(sql string, db *schema.Database) (bool, error) {
	tokens, err := NewLexer(sql).Tokenize()
	if err != nil {
		return false, err
	}

	if !hasRowSecurityClause(tokens) {
		return false, nil
	}

	rowSecurityParseMu.Lock()
	defer rowSecurityParseMu.Unlock()

	tree, err := wasmquery.Parse(sql)
	if err != nil {
		return false, util.WrapError("parse ALTER TABLE", err)
	}

	if len(tree.GetStmts()) != 1 {
		return false, errors.New("expected one ALTER TABLE statement")
	}

	statement := tree.GetStmts()[0].GetStmt().GetAlterTableStmt()
	if statement == nil {
		return false, errors.New("expected ALTER TABLE")
	}

	handled := false

	for _, node := range statement.GetCmds() {
		switch node.GetAlterTableCmd().GetSubtype() {
		case pgquery.AlterTableType_AT_EnableRowSecurity,
			pgquery.AlterTableType_AT_DisableRowSecurity,
			pgquery.AlterTableType_AT_ForceRowSecurity,
			pgquery.AlterTableType_AT_NoForceRowSecurity:
			handled = true
		}
	}

	if !handled {
		return false, nil
	}

	relation := statement.GetRelation()

	table := db.GetTable(relation.GetSchemaname(), relation.GetRelname())
	if table == nil {
		return true, fmt.Errorf("row security table %s not found",
			schema.QualifiedName(relation.GetSchemaname(), relation.GetRelname()))
	}

	enabled, forced := table.RowSecurityEnabled, table.RowSecurityForced

	for _, node := range statement.GetCmds() {
		switch node.GetAlterTableCmd().GetSubtype() {
		case pgquery.AlterTableType_AT_EnableRowSecurity:
			enabled = true
		case pgquery.AlterTableType_AT_DisableRowSecurity:
			enabled = false
		case pgquery.AlterTableType_AT_ForceRowSecurity:
			forced = true
		case pgquery.AlterTableType_AT_NoForceRowSecurity:
			forced = false
		default:
			return true, errors.New(
				"declare row security separately from other ALTER TABLE actions",
			)
		}
	}

	table.RowSecurityEnabled, table.RowSecurityForced = enabled, forced

	return true, nil
}

func hasRowSecurityClause(tokens []Token) bool {
	for i := range tokens {
		if upperLiteral(tokens, i) != "ROW" {
			continue
		}

		level := nextNonCommentIndex(tokens, i+1)

		security := nextNonCommentIndex(tokens, level+1)
		if upperLiteral(tokens, level) == "LEVEL" && upperLiteral(tokens, security) == "SECURITY" {
			return true
		}
	}

	return false
}
