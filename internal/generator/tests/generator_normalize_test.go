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
