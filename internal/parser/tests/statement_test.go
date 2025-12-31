package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/parser"
)

func TestDetectStatementTypeFromTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sql      string
		expected parser.StatementType
	}{
		{
			name:     "create table",
			sql:      "CREATE TABLE public.users (id INT);",
			expected: parser.StmtCreateTable,
		},
		{
			name:     "create unique index",
			sql:      "CREATE UNIQUE INDEX idx_users_name ON users (name);",
			expected: parser.StmtCreateIndex,
		},
		{
			name:     "create materialized view",
			sql:      "CREATE MATERIALIZED VIEW mv AS SELECT 1;",
			expected: parser.StmtCreateMaterializedView,
		},
		{
			name:     "alter table",
			sql:      "ALTER TABLE users ADD COLUMN age INT;",
			expected: parser.StmtAlterTable,
		},
		{
			name:     "timescaledb create hypertable",
			sql:      "SELECT create_hypertable('metrics', 'time');",
			expected: parser.StmtSelectCreateHypertable,
		},
		{
			name:     "do block",
			sql:      "DO $$ BEGIN PERFORM 1; END $$;",
			expected: parser.StmtDoBlock,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tokens, err := parser.NewLexer(tt.sql).Tokenize()
			require.NoError(t, err)

			if len(tokens) > 0 && tokens[len(tokens)-1].Type == parser.TokenEOF {
				tokens = tokens[:len(tokens)-1]
			}

			require.Equal(t, tt.expected, parser.DetectStatementType(tokens))
		})
	}
}
