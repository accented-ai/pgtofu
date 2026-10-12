package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestParseRowSecurity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, actions   string
		enabled, forced bool
	}{
		{"enable", "ENABLE ROW LEVEL SECURITY", true, false},
		{"force independently", "FORCE ROW LEVEL SECURITY", false, true},
		{"enable and force", "ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY", true, true},
		{
			"disable keeps force",
			"FORCE ROW LEVEL SECURITY, ENABLE ROW LEVEL SECURITY, DISABLE ROW LEVEL SECURITY",
			false,
			true,
		},
		{
			"no force keeps enable",
			"ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY, NO FORCE ROW LEVEL SECURITY",
			true,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := parser.New()
			db := &schema.Database{}
			require.NoError(t, p.ParseSQL(`CREATE TABLE "tenant"."documents" (id integer);
				ALTER TABLE ONLY "tenant"."documents" `+tt.actions+`;`, db))
			require.Empty(t, p.GetErrors())
			require.Empty(t, p.GetWarnings())
			table := requireSingleTable(t, db)
			assert.Equal(t, tt.enabled, table.RowSecurityEnabled)
			assert.Equal(t, tt.forced, table.RowSecurityForced)
		})
	}
}

func TestParseRowSecurityRejectsMissingTableAndMixedActions(t *testing.T) {
	t.Parallel()

	for _, sql := range []string{
		`ALTER TABLE missing ENABLE ROW LEVEL SECURITY;`,
		`ALTER TABLE documents ENABLE ROW LEVEL SECURITY, ADD COLUMN ignored text;`,
	} {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()

			p := parser.New()
			db := &schema.Database{}
			require.NoError(t, p.ParseSQL(`CREATE TABLE documents (id integer);`+sql, db))
			require.Len(t, p.GetErrors(), 1)
			assert.False(t, db.Tables[0].RowSecurityEnabled)
		})
	}
}
