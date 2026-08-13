package differ_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestFunctionComparatorParallelSafety(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		current  string
		desired  string
		wantDiff bool
	}{
		{
			name:    "empty is equivalent to default unsafe",
			current: "",
			desired: schema.ParallelSafetyUnsafe,
		},
		{
			name:     "unsafe to safe",
			current:  schema.ParallelSafetyUnsafe,
			desired:  schema.ParallelSafetySafe,
			wantDiff: true,
		},
		{
			name:     "safe to restricted",
			current:  schema.ParallelSafetySafe,
			desired:  schema.ParallelSafetyRestricted,
			wantDiff: true,
		},
	}

	makeFunction := func(parallelSafety string) schema.Function {
		return schema.Function{
			Schema:         schema.DefaultSchema,
			Name:           "parallel_example",
			ReturnType:     "integer",
			Language:       "sql",
			Volatility:     schema.VolatilityImmutable,
			ParallelSafety: parallelSafety,
			Body:           "SELECT 1;",
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			current := &schema.Database{Functions: []schema.Function{makeFunction(tt.current)}}
			desired := &schema.Database{Functions: []schema.Function{makeFunction(tt.desired)}}

			result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
			require.NoError(t, err)

			if !tt.wantDiff {
				require.Empty(t, result.Changes)

				return
			}

			require.Len(t, result.Changes, 1)
			require.Equal(t, differ.ChangeTypeModifyFunction, result.Changes[0].Type)
		})
	}
}
