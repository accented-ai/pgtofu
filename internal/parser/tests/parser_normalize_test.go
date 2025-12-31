package parser_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/parser"
)

func TestNormalization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		sql1  string
		sql2  string
		equal bool
	}{
		{
			name:  "whitespace difference",
			sql1:  "SELECT * FROM users WHERE id = 1",
			sql2:  "SELECT   *   FROM   users   WHERE   id=1",
			equal: true,
		},
		{
			name:  "case difference in keywords",
			sql1:  "select * from users",
			sql2:  "SELECT * FROM users",
			equal: true,
		},
		{
			name:  "trailing semicolon",
			sql1:  "SELECT * FROM users;",
			sql2:  "SELECT * FROM users",
			equal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := parser.CompareSQL(tt.sql1, tt.sql2)
			if result != tt.equal {
				t.Errorf("CompareSQL() = %v, want %v", result, tt.equal)
			}
		})
	}
}
