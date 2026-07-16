package differ_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestFunctionComparator_NormalizesArgumentTypeIdentity(t *testing.T) {
	t.Parallel()

	current := &schema.Database{Functions: []schema.Function{
		{
			Schema:        "example",
			Name:          "assert_integrity",
			ArgumentTypes: []string{"uuid", "text[]"},
			ReturnType:    "void",
			Language:      "plpgsql",
			Volatility:    schema.VolatilityVolatile,
			Body:          "BEGIN RETURN; END;",
		},
	}}
	desired := &schema.Database{Functions: []schema.Function{
		{
			Schema:        "example",
			Name:          "assert_integrity",
			ArgumentTypes: []string{"UUID", "TEXT[]"},
			ReturnType:    "VOID",
			Language:      "plpgsql",
			Volatility:    schema.VolatilityVolatile,
			Body:          "BEGIN RETURN; END;",
		},
	}}

	result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)
	require.Empty(t, result.Changes,
		"PostgreSQL type keyword casing must not create different function identities")
}

func TestFunctionKey_NormalizesAliasesAndArrays(t *testing.T) {
	t.Parallel()

	require.Equal(t,
		differ.FunctionKey("public", "sample", []string{"integer", "integer[]"}),
		differ.FunctionKey("PUBLIC", "SAMPLE", []string{"INT4", "INT[]"}),
	)
}
