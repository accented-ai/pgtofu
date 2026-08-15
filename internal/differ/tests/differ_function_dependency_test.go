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

func TestFunctionDependency_AddedFunctionPrecedesReferencingFunction(t *testing.T) {
	t.Parallel()

	currentCaller := schema.Function{
		Schema:        "analytics",
		Name:          "record_is_publishable",
		ArgumentTypes: []string{"UUID", "UUID"},
		ReturnType:    "BOOLEAN",
		Language:      "sql",
		Volatility:    schema.VolatilityStable,
		Body:          "SELECT record_id IS NOT NULL",
	}
	helper := schema.Function{
		Schema:        "utilities",
		Name:          "record_has_scope",
		ArgumentTypes: []string{"UUID", "UUID"},
		ReturnType:    "BOOLEAN",
		Language:      "sql",
		Volatility:    schema.VolatilityStable,
		Body:          "SELECT record_id IS NOT NULL AND member_id IS NOT NULL",
	}
	desiredCaller := currentCaller
	desiredCaller.Body = `SELECT utilities.record_has_scope(record_id, member_id)`

	current := &schema.Database{Functions: []schema.Function{currentCaller}}
	desired := &schema.Database{Functions: []schema.Function{desiredCaller, helper}}

	result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)

	helperKey := differ.FunctionKey(helper.Schema, helper.Name, helper.ArgumentTypes)
	helperIndex := -1
	callerIndex := -1

	for i, change := range result.Changes {
		switch {
		case change.Type == differ.ChangeTypeAddFunction && change.ObjectName == helperKey:
			helperIndex = i
		case change.Type == differ.ChangeTypeModifyFunction &&
			change.ObjectName == differ.FunctionKey(
				desiredCaller.Schema,
				desiredCaller.Name,
				desiredCaller.ArgumentTypes,
			):
			callerIndex = i

			require.Contains(t, change.DependsOn, helperKey)
		}
	}

	require.NotEqual(t, -1, helperIndex)
	require.NotEqual(t, -1, callerIndex)
	require.Less(t, helperIndex, callerIndex,
		"a called function must exist before its caller is replaced")
}

func TestFunctionDependency_RecursiveFunctionDoesNotDependOnItself(t *testing.T) {
	t.Parallel()

	function := schema.Function{
		Schema:        "analytics",
		Name:          "walk_records",
		ArgumentTypes: []string{"INTEGER"},
		ReturnType:    "INTEGER",
		Language:      "sql",
		Volatility:    schema.VolatilityStable,
		Body: `SELECT CASE
    WHEN depth <= 0 THEN 0
    ELSE analytics.walk_records(depth - 1)
END`,
	}

	result, err := differ.New(differ.DefaultOptions()).Compare(
		&schema.Database{},
		&schema.Database{Functions: []schema.Function{function}},
	)
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	require.Empty(t, result.Changes[0].DependsOn)
}

func TestFunctionDependency_AddedFunctionPrecedesAddedCaller(t *testing.T) {
	t.Parallel()

	helper := schema.Function{
		Schema:        "utilities",
		Name:          "record_is_active",
		ArgumentTypes: []string{"UUID"},
		ReturnType:    "BOOLEAN",
		Language:      "sql",
		Volatility:    schema.VolatilityStable,
		Body:          "SELECT record_id IS NOT NULL",
	}
	caller := schema.Function{
		Schema:        "analytics",
		Name:          "active_record_count",
		ArgumentTypes: []string{"UUID"},
		ReturnType:    "INTEGER",
		Language:      "sql",
		Volatility:    schema.VolatilityStable,
		Body: `SELECT CASE
    WHEN utilities.record_is_active(record_id) THEN 1
    ELSE 0
END`,
	}

	result, err := differ.New(differ.DefaultOptions()).Compare(
		&schema.Database{},
		&schema.Database{Functions: []schema.Function{caller, helper}},
	)
	require.NoError(t, err)

	helperKey := differ.FunctionKey(helper.Schema, helper.Name, helper.ArgumentTypes)
	callerKey := differ.FunctionKey(caller.Schema, caller.Name, caller.ArgumentTypes)
	helperIndex := -1
	callerIndex := -1

	for i, change := range result.Changes {
		switch change.ObjectName {
		case helperKey:
			helperIndex = i
		case callerKey:
			callerIndex = i

			require.Contains(t, change.DependsOn, helperKey)
		}
	}

	require.NotEqual(t, -1, helperIndex)
	require.NotEqual(t, -1, callerIndex)
	require.Less(t, helperIndex, callerIndex,
		"a new function must exist before another new function that calls it")
}

func TestFunctionDependency_ModifiedMutualCallersDoNotCreateOrderingCycle(t *testing.T) {
	t.Parallel()

	first := schema.Function{
		Schema:        "analytics",
		Name:          "first_metric",
		ArgumentTypes: []string{"INTEGER"},
		ReturnType:    "INTEGER",
		Language:      "sql",
		Volatility:    schema.VolatilityStable,
		Body:          "SELECT analytics.second_metric(value)",
	}
	second := schema.Function{
		Schema:        "analytics",
		Name:          "second_metric",
		ArgumentTypes: []string{"INTEGER"},
		ReturnType:    "INTEGER",
		Language:      "sql",
		Volatility:    schema.VolatilityStable,
		Body:          "SELECT analytics.first_metric(value)",
	}
	desiredFirst := first
	desiredFirst.Body = "SELECT analytics.second_metric(value) + 1"
	desiredSecond := second
	desiredSecond.Body = "SELECT analytics.first_metric(value) + 1"

	result, err := differ.New(differ.DefaultOptions()).Compare(
		&schema.Database{Functions: []schema.Function{first, second}},
		&schema.Database{Functions: []schema.Function{desiredFirst, desiredSecond}},
	)
	require.NoError(t, err)
	require.Len(t, result.Changes, 2)

	for _, change := range result.Changes {
		require.Empty(t, change.DependsOn,
			"already-existing callees do not need creation-order dependencies")
	}
}
