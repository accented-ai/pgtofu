package differ_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/accented-ai/pgtofu/internal/differ"
)

func TestNormalizeAnyArrayToIN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "basic ANY ARRAY with text",
			input:    "status = ANY (ARRAY['active', 'pending'])",
			expected: "status in ( 'active', 'pending' )",
		},
		{
			name:     "ANY ARRAY with type casts",
			input:    "status = ANY (ARRAY['active'::text, 'pending'::text])",
			expected: "status in ( 'active', 'pending' )",
		},
		{
			name:     "multiple ANY ARRAY clauses",
			input:    "x = ANY (ARRAY['a', 'b']) AND y = ANY (ARRAY['c', 'd'])",
			expected: "x in ( 'a', 'b' ) and y in ( 'c', 'd' )",
		},
		{
			name:     "ANY ARRAY with numbers",
			input:    "id = ANY (ARRAY[1, 2, 3])",
			expected: "id in ( 1, 2, 3 )",
		},
		{
			name:     "IN clause remains unchanged",
			input:    "status IN ('active', 'pending')",
			expected: "status in ('active', 'pending')",
		},
		{
			name:     "simple equals stays unchanged",
			input:    "status = 'active'",
			expected: "status = 'active'",
		},
		{
			name:     "double parentheses around array",
			input:    "x = ANY ((ARRAY['a', 'b']))",
			expected: "x in ( 'a', 'b' )",
		},
		{
			name:     "with column type casts",
			input:    "(category)::text = ANY (ARRAY['product'::text, 'service'::text])",
			expected: "(category) in ( 'product', 'service' )",
		},
		{
			name:     "array with text[] cast on whole array",
			input:    "x = ANY ((ARRAY['a', 'b'])::text[])",
			expected: "x in ( 'a', 'b' )",
		},
		{
			name:     "array with character varying casts and text[] array cast",
			input:    "x = ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[])",
			expected: "x in ( 'a', 'b' )",
		},
		{
			name:     "postgresql exact output format",
			input:    "((a.category)::text = ANY ((ARRAY['type_a'::character varying, 'type_b'::character varying])::text[]))",
			expected: "((a.category) in ( 'type_a', 'type_b' ))",
		},
	}

	vn := differ.NewViewNormalizer()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := vn.NormalizeText(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}
