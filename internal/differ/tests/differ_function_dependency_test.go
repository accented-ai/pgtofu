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

func TestFunctionDependency_AddedFunctionPrecedesReferencingCheckConstraint(t *testing.T) {
	t.Parallel()

	current := &schema.Database{Tables: []schema.Table{{
		Schema: "inventory",
		Name:   "products",
		Columns: []schema.Column{{
			Name:       "category_codes",
			DataType:   "TEXT[]",
			IsNullable: false,
		}},
	}}}
	function := schema.Function{
		Schema:        "inventory",
		Name:          "valid_category_codes",
		ArgumentTypes: []string{"TEXT[]"},
		ArgumentNames: []string{"category_codes"},
		ReturnType:    "BOOLEAN",
		Language:      "sql",
		Volatility:    schema.VolatilityImmutable,
		Body:          "SELECT ARRAY_POSITION(category_codes, NULL) IS NULL",
	}
	desired := &schema.Database{
		Tables: []schema.Table{{
			Schema:  "inventory",
			Name:    "products",
			Columns: current.Tables[0].Columns,
			Constraints: []schema.Constraint{{
				Name: "products_category_codes_check",
				Type: schema.ConstraintCheck,
				Definition: "CHECK (inventory.valid_category_codes(" +
					"category_codes))",
				CheckExpression: "CHECK (inventory.valid_category_codes(" +
					"category_codes))",
			}},
		}},
		Functions: []schema.Function{function},
	}

	result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)

	functionKey := differ.FunctionKey(function.Schema, function.Name, function.ArgumentTypes)
	constraintIndex := -1
	functionIndex := -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddConstraint:
			constraintIndex = i

			require.Contains(t, change.DependsOn, functionKey)
		case differ.ChangeTypeAddFunction:
			functionIndex = i
		}
	}

	require.NotEqual(t, -1, constraintIndex)
	require.NotEqual(t, -1, functionIndex)
	require.Less(t, functionIndex, constraintIndex,
		"a function must exist before a CHECK constraint that calls it")
}

func TestFunctionDependency_AddedFunctionPrecedesNewTableCheck(t *testing.T) {
	t.Parallel()

	function := schema.Function{
		Schema:        schema.DefaultSchema,
		Name:          "valid_labels",
		ArgumentTypes: []string{"TEXT[]"},
		ArgumentNames: []string{"labels"},
		ReturnType:    "BOOLEAN",
		Language:      "sql",
		Volatility:    schema.VolatilityImmutable,
		Body:          "SELECT ARRAY_POSITION(labels, NULL) IS NULL",
	}
	desired := &schema.Database{
		Tables: []schema.Table{{
			Schema: schema.DefaultSchema,
			Name:   "products",
			Columns: []schema.Column{{
				Name:       "labels",
				DataType:   "TEXT[]",
				IsNullable: false,
			}},
			Constraints: []schema.Constraint{{
				Name:            "products_labels_check",
				Type:            schema.ConstraintCheck,
				Definition:      "CHECK (public.valid_labels(labels))",
				CheckExpression: "CHECK (public.valid_labels(labels))",
			}},
		}},
		Functions: []schema.Function{function},
	}

	result, err := differ.New(differ.DefaultOptions()).Compare(&schema.Database{}, desired)
	require.NoError(t, err)

	functionKey := differ.FunctionKey(function.Schema, function.Name, function.ArgumentTypes)
	tableIndex := -1
	functionIndex := -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddTable:
			tableIndex = i

			require.Contains(t, change.DependsOn, functionKey)
		case differ.ChangeTypeAddFunction:
			functionIndex = i
		}
	}

	require.NotEqual(t, -1, tableIndex)
	require.NotEqual(t, -1, functionIndex)
	require.Less(t, functionIndex, tableIndex,
		"a function must exist before a new table with an inline CHECK that calls it")
}
