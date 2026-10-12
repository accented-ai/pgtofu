//nolint:testpackage // Exercises private policy expression normalization.
package differ

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPolicyExpressionNormalization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		current, desired string
		equal            bool
	}{
		{
			"(tenant_id = current_setting('app.tenant'::text, true))",
			"tenant_id = pg_catalog.current_setting('app.tenant', TRUE)", true,
		},
		{"tenant_id = current_organization()", "tenant_id = public.current_organization()", true},
		{"state = ANY (ARRAY['open'::text, 'closed'::text])", "state IN ('open', 'closed')", true},
		{
			"state <> ALL (ARRAY['open'::text, 'closed'::text])",
			"state NOT IN ('open', 'closed')",
			true,
		},
		{"((id >= 1) AND (id <= 5))", "id BETWEEN 1 AND 5", true},
		{"tenant_id = 'A'::text", "tenant_id = 'a'", false},
		{`"Tenant" = 'a'`, `tenant = 'a'`, false},
		{"tenant_id::integer = 1", "tenant_id = 1", false},
		{"tenant_id = '1'::integer", "tenant_id = '1'", false},
		{"tenant_id = 'A'::public.text", "tenant_id = 'A'", false},
		{"tenant_id OPERATOR(other.=) 'a'", "tenant_id = 'a'", false},
		{"tenant_id = ANY (ARRAY[1, 2]::bigint[])", "tenant_id IN (1, 2)", false},
		{"", "true", false},
	}
	for _, tt := range tests {
		t.Run(tt.desired+" / "+tt.current, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.equal, policyExpressionsEqual(tt.current, tt.desired))
		})
	}
}
