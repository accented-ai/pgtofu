package differ //nolint:testpackage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalPostgresViewResolvesAliasScopes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		current string
		desired string
		equal   bool
	}{
		{
			name: "renames a correlated outer reference",
			current: `SELECT r.id FROM reporting.records r WHERE EXISTS (
                SELECT 1 FROM reporting.links l WHERE l.record_id = r.id
            )`,
			desired: `SELECT record.id FROM reporting.records record WHERE EXISTS (
                SELECT 1 FROM reporting.links link WHERE link.record_id = record.id
            )`,
			equal: true,
		},
		{
			name: "inner alias shadows outer alias",
			current: `SELECT r.id FROM reporting.records r WHERE EXISTS (
                SELECT 1 FROM reporting.records r WHERE r.id > 0
            )`,
			desired: `SELECT record.id FROM reporting.records record WHERE EXISTS (
                SELECT 1 FROM reporting.records inner_record WHERE inner_record.id > 0
            )`,
			equal: true,
		},
		{
			name: "unaliased local relation shadows outer alias",
			current: `SELECT r.id FROM reporting.records r WHERE EXISTS (
                SELECT 1 FROM reporting.r WHERE r.id > 0
            )`,
			desired: `SELECT record.id FROM reporting.records record WHERE EXISTS (
                SELECT 1 FROM reporting.r WHERE r.id > 0
            )`,
			equal: true,
		},
		{
			name: "CTE sees enclosing query before local FROM aliases",
			current: `SELECT r.id FROM reporting.records r WHERE EXISTS (
                WITH matches AS (SELECT l.id FROM reporting.links l WHERE l.record_id = r.id)
                SELECT 1 FROM matches m JOIN reporting.records r ON m.id = r.id
            )`,
			desired: `SELECT record.id FROM reporting.records record WHERE EXISTS (
                WITH matches AS (SELECT link.id FROM reporting.links link WHERE link.record_id = record.id)
                SELECT 1 FROM matches matched JOIN reporting.records inner_record ON matched.id = inner_record.id
            )`,
			equal: true,
		},
		{
			name: "JOIN condition sees outer alias before a later FROM item",
			current: `SELECT r.id FROM reporting.records r WHERE EXISTS (
                SELECT 1 FROM reporting.links l JOIN reporting.targets t ON l.record_id = r.id,
                    reporting.other_records r
            )`,
			desired: `SELECT record.id FROM reporting.records record WHERE EXISTS (
                SELECT 1 FROM reporting.links link JOIN reporting.targets target ON link.record_id = record.id,
                    reporting.other_records other_record
            )`,
			equal: true,
		},
		{
			name:    "keeps self join bindings distinct",
			current: "SELECT a.id FROM reporting.records a JOIN reporting.records b ON a.parent_id = b.id",
			desired: "SELECT a.id FROM reporting.records a JOIN reporting.records b ON b.parent_id = a.id",
		},
		{
			name: "keeps outer and inner references distinct",
			current: `SELECT r.id FROM reporting.records r WHERE EXISTS (
                SELECT 1 FROM reporting.links l WHERE l.record_id = r.id
            )`,
			desired: `SELECT r.id FROM reporting.records r WHERE EXISTS (
                SELECT 1 FROM reporting.links l WHERE l.record_id = l.id
            )`,
		},
		{
			name:    "keeps alias column names distinct",
			current: "SELECT a.id FROM reporting.records AS a(id, other_id)",
			desired: "SELECT b.id FROM reporting.records AS b(other_id, id)",
		},
		{
			name:    "keeps whole row output names distinct",
			current: "SELECT a FROM reporting.records a",
			desired: "SELECT b FROM reporting.records b",
		},
		{
			name:    "keeps whole row and unqualified column references distinct",
			current: "SELECT a FROM reporting.records a",
			desired: "SELECT a FROM reporting.records b",
		},
		{
			name:    "keeps string literals distinct",
			current: "SELECT a.id, 'a.id' AS label FROM reporting.records a",
			desired: "SELECT b.id, 'b.id' AS label FROM reporting.records b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			current, currentOK := canonicalPostgresView(tt.current, nil)
			desired, desiredOK := canonicalPostgresView(tt.desired, nil)

			require.True(t, currentOK)
			require.True(t, desiredOK)
			require.Equal(t, tt.equal, current == desired)
		})
	}
}
