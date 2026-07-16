package differ_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestFunctionDependency_AddedColumnPrecedesReferencingFunction(t *testing.T) {
	t.Parallel()

	current := &schema.Database{Tables: []schema.Table{{
		Schema: "inventory",
		Name:   "products",
		Columns: []schema.Column{{
			Name:       "id",
			DataType:   "UUID",
			IsNullable: false,
		}},
	}}}
	desired := &schema.Database{
		Tables: []schema.Table{{
			Schema: "inventory",
			Name:   "products",
			Columns: []schema.Column{
				{Name: "id", DataType: "UUID", IsNullable: false},
				{Name: "supplier_id", DataType: "UUID", IsNullable: true},
			},
		}},
		Functions: []schema.Function{{
			Schema:        "inventory",
			Name:          "validate_product_supplier",
			ArgumentTypes: []string{"UUID"},
			ReturnType:    "VOID",
			Language:      "plpgsql",
			Volatility:    schema.VolatilityVolatile,
			Body: `DECLARE
    product_row inventory.products%ROWTYPE;
BEGIN
    SELECT product.* INTO product_row
    FROM inventory.products AS product;
    IF product_row.supplier_id IS NULL THEN
        RETURN;
    END IF;
END;`,
		}},
	}

	result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)

	columnIndex := -1
	functionIndex := -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddColumn:
			columnIndex = i
		case differ.ChangeTypeAddFunction:
			functionIndex = i
		}
	}

	require.NotEqual(t, -1, columnIndex)
	require.NotEqual(t, -1, functionIndex)
	require.Less(t, columnIndex, functionIndex,
		"a referenced table's new columns must exist before its function is created")
}
