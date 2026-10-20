package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/accented-ai/pgtofu/internal/generator"
)

func TestNormalizeWhereClause_QuantifiedComparisonSpacing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		where string
		want  string
	}{
		{
			name:  "any",
			where: "priority = ANY (ARRAY[1, 2, 3])",
			want:  "priority = ANY(ARRAY[1, 2, 3])",
		},
		{
			name:  "all",
			where: "state <> ALL (ARRAY['completed', 'cancelled'])",
			want:  "state <> ALL(ARRAY['completed', 'cancelled'])",
		},
		{
			name:  "some lowercase with multiline whitespace",
			where: "score >= some\n\t(ARRAY[10, 20])",
			want:  "score >= SOME(ARRAY[10, 20])",
		},
		{
			name:  "string literals and quoted identifiers",
			where: "note = 'ANY (value)' AND \"ALL (states)\" = SOME (states)",
			want:  "note = 'ANY (value)' AND \"ALL (states)\" = SOME(states)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, generator.NormalizeWhereClause(tt.where))
		})
	}
}

func TestNormalizeCheckConstraint_BtrimFunctionName(t *testing.T) {
	t.Parallel()

	constraint := "CHECK (btrim(label) <> '' AND note <> 'btrim(label)' " +
		"AND \"btrim\" <> '')"
	want := "CHECK (BTRIM(label) <> '' AND note <> 'btrim(label)' " +
		"AND \"btrim\" <> '')"

	assert.Equal(t, want, generator.NormalizeCheckConstraint(constraint))
}

func TestExpressionFormattingPreservesCastsAndLiteralContents(t *testing.T) {
	t.Parallel()

	for _, test := range []struct{ input, want string }{
		{"(payload ->> 'count')::integer > 0", "(payload ->> 'count')::INTEGER > 0"},
		{"'12'::integer", "'12'::INTEGER"},
		{"'ready'::catalog.status", "'ready'::catalog.status"},
		{"ARRAY['1'::integer, '2'::integer]", "ARRAY['1'::INTEGER, '2'::INTEGER]"},
		{"'Keep ::integer and (12) verbatim'", "'Keep ::integer and (12) verbatim'"},
		{"now()::date", "NOW()::DATE"},
	} {
		for _, normalize := range []func(string) string{
			generator.NormalizeSQL,
			generator.NormalizeCheckConstraint,
			generator.NormalizeDefaultValue,
		} {
			assert.Equal(t, test.want, normalize(test.input))
		}
	}
}

func TestMultilineChecksRetainNumericCastQualifiers(t *testing.T) {
	t.Parallel()

	current := foreignKeySchema(t, "")
	desired := foreignKeySchema(t, `CREATE TABLE catalog.prices (
    amount double precision,
    CONSTRAINT checked_unit_interval_with_explicit_numeric_bounds
    CHECK (amount >= (0)::double precision AND amount <= (1)::double precision)
);`)
	up, _ := foreignKeyMigrationSQL(t, current, desired, 1)
	assert.Contains(t, up, "::DOUBLE PRECISION")
	assert.NotContains(t, up, "pgtofu_identifier_")
}
