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

func TestParseCreateConstraintTrigger(t *testing.T) {
	t.Parallel()

	sql := `
CREATE TABLE inventory.stock_entries (id UUID PRIMARY KEY);
CREATE FUNCTION inventory.validate_stock_entry() RETURNS TRIGGER AS $$
BEGIN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER stock_entry_integrity
AFTER INSERT OR UPDATE OF id
ON inventory.stock_entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION inventory.validate_stock_entry();
`

	db := parseSQL(t, sql)
	require.Len(t, db.Triggers, 1)

	trigger := db.Triggers[0]
	require.Equal(t, "stock_entry_integrity", trigger.Name)
	require.Equal(t, "inventory", trigger.Schema)
	require.Equal(t, "stock_entries", trigger.TableName)
	require.True(t, trigger.IsConstraint)
	require.True(t, trigger.IsDeferrable)
	require.True(t, trigger.InitiallyDeferred)
	require.Equal(t, []string{"INSERT", "UPDATE"}, trigger.Events)
	require.Equal(t, []string{"id"}, trigger.UpdateColumns)
	require.True(t, trigger.ForEachRow)
	require.Equal(t, "inventory", trigger.FunctionSchema)
	require.Equal(t, "validate_stock_entry", trigger.FunctionName)
}
