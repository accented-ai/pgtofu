package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestParseViewSecurityInvoker(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		option string
		want   bool
	}{
		{"", false},
		{"security_invoker", true},
		{"security_invoker = TRUE", true},
		{"security_invoker = false", false},
		{"security_invoker = 1", true},
		{"security_invoker = 0", false},
		{"security_invoker = on", true},
		{"security_invoker = off", false},
		{"security_invoker = 'Y'", true},
		{"security_invoker = 'of'", false},
		{"security_invoker = 'tru'", true},
		{"\"security_invoker\" /* policy */ = 'no'", false},
	} {
		t.Run(test.option, func(t *testing.T) {
			t.Parallel()

			clause := ""
			if test.option != "" {
				clause = " WITH (" + test.option + ")"
			}

			p := parser.New()
			db := &schema.Database{}
			require.NoError(t, p.ParseSQL("CREATE VIEW visible"+clause+" AS SELECT 1", db))
			require.Empty(t, p.GetErrors())
			require.Len(t, db.Views, 1)
			assert.Equal(t, test.want, db.Views[0].SecurityInvoker)
		})
	}
}

func TestParseViewRejectsInvalidInvokerPolicy(t *testing.T) {
	t.Parallel()

	for _, option := range []string{
		"bogus.security_invoker = true",
		"security_invoker = 'o'",
		"security_invoker = 'sometimes'",
		"security_invoker = 2",
		"security_invoker = 1.5",
		"security_invoker = off[]",
		"security_invoker = off(1)",
		"security_invoker = true, security_invoker = false",
	} {
		t.Run(option, func(t *testing.T) {
			t.Parallel()

			p := parser.New()
			db := &schema.Database{}

			err := p.ParseSQL("CREATE VIEW visible WITH ("+option+") AS SELECT 1", db)
			if err == nil {
				require.NotEmpty(t, p.GetErrors())
			}

			assert.Empty(t, db.Views)
		})
	}
}
