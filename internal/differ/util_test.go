package differ //nolint:testpackage // testing internal function

import (
	"testing"
)

func TestNormalizeExpression_InClauseVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple IN clause from SQL",
			input:    "CHECK (status IN ('pending', 'processing', 'completed'))",
			expected: "status in ('pending', 'processing', 'completed')",
		},
		{
			name: "PostgreSQL ANY ARRAY format",
			input: "CHECK (((status)::text = ANY " +
				"(ARRAY['pending'::text, 'processing'::text, 'completed'::text])))",
			expected: "status in ('pending', 'processing', 'completed')",
		},
		{
			name: "VARCHAR column with text cast",
			input: "CHECK (((category)::text = ANY " +
				"(ARRAY['alpha'::text, 'beta'::text, 'gamma'::text])))",
			expected: "category in ('alpha', 'beta', 'gamma')",
		},
		{
			name:     "simple IN from SQL file",
			input:    "CHECK (category IN ('alpha', 'beta', 'gamma'))",
			expected: "category in ('alpha', 'beta', 'gamma')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := normalizeExpression(tt.input)
			if result != tt.expected {
				t.Errorf(
					"normalizeExpression() mismatch:\n  input:    %q\n  got:      %q\n  expected: %q",
					tt.input,
					result,
					tt.expected,
				)
			}
		})
	}
}

func TestNormalizeExpression_CompareEquivalentConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fromSQL      string
		fromPostgres string
	}{
		{
			name:    "status IN clause",
			fromSQL: "CHECK (status IN ('pending', 'processing', 'completed'))",
			fromPostgres: "CHECK (((status)::text = ANY " +
				"(ARRAY['pending'::text, 'processing'::text, 'completed'::text])))",
		},
		{
			name:         "quoted identifier (reserved keyword)",
			fromSQL:      "CHECK (position IN (0, 1, 2))",
			fromPostgres: `CHECK (("position" = ANY (ARRAY[0, 1, 2])))`,
		},
		{
			name:         "partial index WHERE clause with type cast",
			fromSQL:      "kind = 'active'",
			fromPostgres: "((kind)::text = 'active'::text)",
		},
		{
			name:    "category IN clause with five values",
			fromSQL: "CHECK (category IN ('type_a', 'type_b', 'type_c', 'type_d', 'type_e'))",
			fromPostgres: "CHECK (((category)::text = ANY (ARRAY['type_a'::text, " +
				"'type_b'::text, 'type_c'::text, 'type_d'::text, 'type_e'::text])))",
		},
		{
			name:    "character varying cast",
			fromSQL: "CHECK (status IN ('ACTIVE', 'PAUSED'))",
			fromPostgres: "CHECK (((status)::character varying = ANY " +
				"(ARRAY['ACTIVE'::character varying, 'PAUSED'::character varying])))",
		},
		{
			name:         "no spaces in array",
			fromSQL:      "CHECK (status IN ('a', 'b', 'c'))",
			fromPostgres: "CHECK (((status)::text = ANY (ARRAY['a'::text,'b'::text,'c'::text])))",
		},
		{
			name:         "array literal format",
			fromSQL:      "CHECK (status IN ('a', 'b', 'c'))",
			fromPostgres: "CHECK ((status = ANY ('{a,b,c}'::text[])))",
		},
		{
			name:    "array with text cast on column and text[] cast on array",
			fromSQL: "CHECK (category IN ('a', 'b', 'c', 'd', 'e'))",
			fromPostgres: "CHECK (((category)::text = ANY ((ARRAY['a'::character varying, " +
				"'b'::character varying, 'c'::character varying, 'd'::character varying, " +
				"'e'::character varying])::text[])))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			normalizedSQL := normalizeExpression(tt.fromSQL)
			normalizedPG := normalizeExpression(tt.fromPostgres)

			if normalizedSQL != normalizedPG {
				t.Errorf(
					"Normalized expressions don't match:\n  SQL:      %q\n  Postgres: %q",
					normalizedSQL, normalizedPG,
				)
			}
		})
	}
}
