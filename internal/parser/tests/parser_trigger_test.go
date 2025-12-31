package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestParseCreateTrigger_WithSameNameOnDifferentTables(t *testing.T) {
	t.Parallel()

	sql := `
CREATE TABLE public.orders (
    id UUID PRIMARY KEY,
    updated_at TIMESTAMPTZ
);

CREATE TRIGGER set_updated_at
BEFORE UPDATE ON public.orders
FOR EACH ROW
EXECUTE FUNCTION UPDATE_TIMESTAMP();

CREATE TABLE public.order_audits (
    id UUID PRIMARY KEY,
    updated_at TIMESTAMPTZ
);

CREATE TRIGGER set_updated_at
BEFORE UPDATE ON public.order_audits
FOR EACH ROW
EXECUTE FUNCTION UPDATE_TIMESTAMP();
`

	db := parseSQL(t, sql)

	require.Len(t, db.Triggers, 2, "expected two triggers with the same name on different tables")

	var (
		orderTrigger *schema.Trigger
		auditTrigger *schema.Trigger
	)

	for i := range db.Triggers {
		tr := &db.Triggers[i]

		switch tr.TableName {
		case "orders":
			orderTrigger = tr
		case "order_audits":
			auditTrigger = tr
		}
	}

	require.NotNil(t, orderTrigger, "expected trigger for public.orders")
	require.NotNil(t, auditTrigger, "expected trigger for public.order_audits")

	require.Equal(t, "set_updated_at", orderTrigger.Name)
	require.Equal(t, "set_updated_at", auditTrigger.Name)
}
