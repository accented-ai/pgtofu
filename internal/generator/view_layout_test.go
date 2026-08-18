package generator //nolint:testpackage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatViewArrayConstructors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name: "indents simple multiline constructor",
			query: `WHERE
    item.state = ANY(
        ARRAY ['active'::TEXT,
        'pending'::TEXT]
    )`,
			want: `WHERE
    item.state = ANY(
        ARRAY [
            'active'::TEXT,
            'pending'::TEXT
        ]
    )`,
		},
		{
			name:  "preserves compact constructor",
			query: "SELECT ARRAY ['active'::TEXT, 'pending'::TEXT]",
			want:  "SELECT ARRAY ['active'::TEXT, 'pending'::TEXT]",
		},
		{
			name: "ignores commas nested in element expressions",
			query: `SELECT ANY(
    ARRAY [COALESCE(primary_state, 'active'),
    COALESCE(secondary_state, 'pending')]
)`,
			want: `SELECT ANY(
    ARRAY [
        COALESCE(primary_state, 'active'),
        COALESCE(secondary_state, 'pending')
    ]
)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := formatViewArrayConstructors(tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
