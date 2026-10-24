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
			name: "JOIN condition excludes preceding comma sources",
			current: `SELECT r.id FROM reporting.records r WHERE EXISTS (
                SELECT 1 FROM reporting.links r,
                    reporting.links l JOIN reporting.records t ON t.id = r.id
            )`,
			desired: `SELECT record.id FROM reporting.records record WHERE EXISTS (
                SELECT 1 FROM reporting.links local_link,
                    reporting.links link JOIN reporting.records target ON target.id = record.id
            )`,
			equal: true,
		},
		{
			name: "lateral subquery separates inner and outward aliases",
			current: `SELECT selected.id, link.record_id
                FROM reporting.selections selected
                CROSS JOIN LATERAL (
                    SELECT link_1.record_id FROM reporting.links link_1
                    WHERE link_1.id = selected.link_id OFFSET 0
                ) link`,
			desired: `SELECT selected.id, link.record_id
                FROM reporting.selections selected
                CROSS JOIN LATERAL (
                    SELECT link.record_id FROM reporting.links link
                    WHERE link.id = selected.link_id OFFSET 0
                ) link`,
			equal: true,
		},
		{
			name: "non-lateral subquery retains enclosing correlation",
			current: `SELECT r.id FROM reporting.records r WHERE EXISTS (
                SELECT derived.id FROM reporting.links r,
                    (SELECT r.id) derived
            )`,
			desired: `SELECT record.id FROM reporting.records record WHERE EXISTS (
                SELECT result.id FROM reporting.links link,
                    (SELECT record.id) result
            )`,
			equal: true,
		},
		{
			name: "lateral subquery sees preceding comma source",
			current: `SELECT result.id FROM reporting.records r,
                LATERAL (SELECT r.id) result`,
			desired: `SELECT derived.id FROM reporting.records record,
                LATERAL (SELECT record.id) derived`,
			equal: true,
		},
		{
			name: "nested lateral subquery sees all preceding join sources",
			current: `SELECT result.id FROM reporting.records r
                JOIN reporting.links l ON l.record_id = r.id
                CROSS JOIN LATERAL (
                    SELECT nested.id FROM LATERAL (
                        SELECT l.id WHERE l.record_id = r.id
                    ) nested
                ) result`,
			desired: `SELECT derived.id FROM reporting.records record
                JOIN reporting.links link ON link.record_id = record.id
                CROSS JOIN LATERAL (
                    SELECT inner_result.id FROM LATERAL (
                        SELECT link.id WHERE link.record_id = record.id
                    ) inner_result
                ) derived`,
			equal: true,
		},
		{
			name: "keeps lateral correlation distinct from local reference",
			current: `SELECT r.id FROM reporting.records r
                CROSS JOIN LATERAL (
                    SELECT l.id FROM reporting.links l WHERE l.record_id = r.id
                ) result`,
			desired: `SELECT record.id FROM reporting.records record
                CROSS JOIN LATERAL (
                    SELECT link.id FROM reporting.links link WHERE link.record_id = link.id
                ) derived`,
		},
		{
			name: "keeps lateral visibility distinct",
			current: `SELECT r.id FROM reporting.records r WHERE EXISTS (
                SELECT derived.id FROM reporting.links r,
                    (SELECT r.id) derived
            )`,
			desired: `SELECT r.id FROM reporting.records r WHERE EXISTS (
                SELECT derived.id FROM reporting.links r,
                    LATERAL (SELECT r.id) derived
            )`,
		},
		{
			name: "keeps derived base schemas distinct",
			current: `SELECT result.id FROM (
                SELECT r.id FROM reporting.records r
            ) result`,
			desired: `SELECT derived.id FROM (
                SELECT record.id FROM archive.records record
            ) derived`,
		},
		{
			name: "keeps derived column aliases distinct",
			current: `SELECT result.id FROM (
                SELECT r.id, r.parent_id FROM reporting.records r
            ) result(id, parent_id)`,
			desired: `SELECT derived.id FROM (
                SELECT record.id, record.parent_id FROM reporting.records record
            ) derived(parent_id, id)`,
		},
		{
			name: "keeps schema qualified references distinct",
			current: `SELECT reporting.records.id FROM reporting.records
                CROSS JOIN LATERAL (SELECT l.id FROM reporting.links l) result`,
			desired: `SELECT archive.records.id FROM reporting.records
                CROSS JOIN LATERAL (SELECT link.id FROM reporting.links link) derived`,
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

			current, currentOK, _ := canonicalPostgresView(tt.current, nil)
			desired, desiredOK, _ := canonicalPostgresView(tt.desired, nil)

			require.True(t, currentOK)
			require.True(t, desiredOK)
			require.Equal(t, tt.equal, current == desired)
			require.Equal(t, tt.equal, NewViewNormalizer().definitionsEqual(tt.current, tt.desired))
		})
	}
}
