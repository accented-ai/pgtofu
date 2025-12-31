package parser

import "strings"

type StatementType int

const (
	StmtUnknown StatementType = iota
	StmtCreateTable
	StmtCreateIndex
	StmtCreateView
	StmtCreateMaterializedView
	StmtCreateFunction
	StmtCreateTrigger
	StmtCreateExtension
	StmtCreateType
	StmtCreateSequence
	StmtCreateSchema
	StmtAlterTable
	StmtComment
	StmtSelectCreateHypertable
	StmtSelectAddCompressionPolicy
	StmtSelectAddRetentionPolicy
	StmtSelectAddContinuousAggregatePolicy
	StmtDoBlock
)

type Statement struct {
	Type   StatementType
	SQL    string
	Tokens []Token
	Line   int
}

func (s Statement) NormalizedSQL() string {
	return strings.TrimSpace(stripLeadingComments(s.SQL))
}

func DetectStatementType(tokens []Token) StatementType { //nolint:cyclop,gocyclo
	var parts []string //nolint:prealloc

	for _, token := range tokens {
		switch token.Type {
		case TokenComment, TokenComma:
			continue
		}

		lit := strings.ToUpper(strings.TrimSpace(token.Literal))
		if lit == "" {
			continue
		}

		parts = append(parts, lit)
		if len(parts) >= 6 {
			break
		}
	}

	if len(parts) == 0 {
		return StmtUnknown
	}

	switch parts[0] {
	case "CREATE":
		if len(parts) < 2 {
			return StmtUnknown
		}

		switch parts[1] {
		case "TABLE":
			return StmtCreateTable
		case "UNIQUE":
			return StmtCreateIndex
		case "INDEX":
			return StmtCreateIndex
		case "MATERIALIZED":
			if len(parts) > 2 && parts[2] == "VIEW" {
				return StmtCreateMaterializedView
			}

			return StmtUnknown
		case "VIEW":
			return StmtCreateView
		case "FUNCTION":
			return StmtCreateFunction
		case "TRIGGER":
			return StmtCreateTrigger
		case "EXTENSION":
			return StmtCreateExtension
		case "TYPE":
			return StmtCreateType
		case "SEQUENCE":
			return StmtCreateSequence
		case "SCHEMA":
			return StmtCreateSchema
		case "OR":
			if len(parts) > 3 && parts[2] == "REPLACE" {
				switch parts[3] {
				case "VIEW":
					return StmtCreateView
				case "FUNCTION":
					return StmtCreateFunction
				}
			}
		}
	case "ALTER":
		if len(parts) > 1 && parts[1] == "TABLE" {
			return StmtAlterTable
		}
	case "COMMENT":
		if len(parts) > 1 && parts[1] == "ON" {
			return StmtComment
		}
	case "SELECT":
		if len(parts) < 2 {
			return StmtUnknown
		}

		switch parts[1] {
		case "CREATE_HYPERTABLE":
			return StmtSelectCreateHypertable
		case "ADD_COMPRESSION_POLICY":
			return StmtSelectAddCompressionPolicy
		case "ADD_RETENTION_POLICY":
			return StmtSelectAddRetentionPolicy
		case "ADD_CONTINUOUS_AGGREGATE_POLICY":
			return StmtSelectAddContinuousAggregatePolicy
		}
	case "DO":
		return StmtDoBlock
	}

	return StmtUnknown
}

func detectStatementTypeFromSQL(sql string) StatementType {
	normalized := strings.TrimSpace(stripLeadingComments(sql))
	if normalized == "" {
		return StmtUnknown
	}

	upper := strings.ToUpper(normalized)

	switch {
	case strings.HasPrefix(upper, "CREATE TABLE"):
		return StmtCreateTable
	case strings.HasPrefix(upper, "CREATE UNIQUE INDEX"),
		strings.HasPrefix(upper, "CREATE INDEX"):
		return StmtCreateIndex
	case strings.HasPrefix(upper, "CREATE MATERIALIZED VIEW"):
		return StmtCreateMaterializedView
	case strings.HasPrefix(upper, "CREATE OR REPLACE VIEW"),
		strings.HasPrefix(upper, "CREATE VIEW"):
		return StmtCreateView
	case strings.HasPrefix(upper, "CREATE OR REPLACE FUNCTION"),
		strings.HasPrefix(upper, "CREATE FUNCTION"):
		return StmtCreateFunction
	case strings.HasPrefix(upper, "CREATE TRIGGER"):
		return StmtCreateTrigger
	case strings.HasPrefix(upper, "CREATE EXTENSION"):
		return StmtCreateExtension
	case strings.HasPrefix(upper, "CREATE TYPE"):
		return StmtCreateType
	case strings.HasPrefix(upper, "CREATE SEQUENCE"):
		return StmtCreateSequence
	case strings.HasPrefix(upper, "CREATE SCHEMA"):
		return StmtCreateSchema
	case strings.HasPrefix(upper, "ALTER TABLE"):
		return StmtAlterTable
	case strings.HasPrefix(upper, "COMMENT ON"):
		return StmtComment
	case strings.HasPrefix(upper, "SELECT CREATE_HYPERTABLE"):
		return StmtSelectCreateHypertable
	case strings.HasPrefix(upper, "SELECT ADD_COMPRESSION_POLICY"):
		return StmtSelectAddCompressionPolicy
	case strings.HasPrefix(upper, "SELECT ADD_RETENTION_POLICY"):
		return StmtSelectAddRetentionPolicy
	case strings.HasPrefix(upper, "SELECT ADD_CONTINUOUS_AGGREGATE_POLICY"):
		return StmtSelectAddContinuousAggregatePolicy
	case strings.HasPrefix(upper, "DO"):
		return StmtDoBlock
	default:
		return StmtUnknown
	}
}

func determineStatementType(tokens []Token, sql string) StatementType {
	if stmtType := DetectStatementType(tokens); stmtType != StmtUnknown {
		return stmtType
	}

	return detectStatementTypeFromSQL(sql)
}
