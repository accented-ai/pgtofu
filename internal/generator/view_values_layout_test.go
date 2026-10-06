package generator //nolint:testpackage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompactViewValuesSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name: "keeps literal whitespace and alias columns",
			query: `SELECT scope.label
FROM
    (
        VALUES
            ('two  spaces', COALESCE(
                TRUE,
                FALSE
            ))
    ) AS scope(label, enabled)`,
			want: "SELECT scope.label\nFROM (VALUES ('two  spaces', COALESCE(TRUE, FALSE))) AS scope(label, enabled)",
		},
		{
			name: "compacts a short join source",
			query: `SELECT record.id
FROM records AS record
CROSS JOIN
    (
        VALUES
            (1),
            (2)
    ) AS scope(id)`,
			want: "SELECT record.id\nFROM records AS record\nCROSS JOIN (VALUES (1), (2)) AS scope(id)",
		},
		{
			name: "preserves multiline literals",
			query: `SELECT scope.label
FROM
    (
        VALUES
            ('first
second')
    ) AS scope(label)`,
		},
		{
			name: "preserves comments",
			query: `SELECT scope.enabled
FROM
    (
        VALUES
            (TRUE) -- explanatory comment
    ) AS scope(enabled)`,
		},
		{
			name: "preserves SELECT subqueries",
			query: `SELECT scope.enabled
FROM
    (
        SELECT TRUE AS enabled
    ) AS scope`,
		},
		{
			name: "respects compact line length",
			query: "SELECT scope.label\nFROM\n    (\n        VALUES\n            ('" +
				strings.Repeat("x", 70) + "')\n    ) AS scope(label)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want := tt.want
			if want == "" {
				want = tt.query
			}

			got, err := compactViewValuesSources(tt.query)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}
