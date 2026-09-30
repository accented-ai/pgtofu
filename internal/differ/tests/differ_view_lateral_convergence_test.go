package differ_test

import (
	"strings"
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_LateralViewConvergence(t *testing.T) {
	t.Parallel()

	// PostgreSQL expands relation and CTE wildcards, names anonymous lateral
	// outputs, and rewrites row distinctness into individual comparisons.
	const source = `
WITH primary_candidates AS (
    SELECT item.id AS item_id, picked.id AS candidate_id
    FROM inventory.items AS item
    JOIN LATERAL (
        SELECT scoped_authority.*
        FROM inventory.authorities AS scoped_authority
        WHERE scoped_authority.item_id = item.id
        OFFSET 0
    ) AS authority ON TRUE
    JOIN LATERAL (
        SELECT candidate.id
        FROM LATERAL (
            SELECT candidate.id
            FROM inventory.candidates AS candidate
            WHERE candidate.item_id = item.id
                AND ROW(candidate.evidence #>> '{location,start}', candidate.evidence #>> '{location,end}')
                    IS DISTINCT FROM ROW(item.start_offset::text, item.end_offset::text)
            OFFSET 0
        ) AS candidate
        JOIN LATERAL (
            SELECT 1
            FROM inventory.authorities AS scoped_authority
            WHERE scoped_authority.candidate_id = candidate.id AND scoped_authority.active
            OFFSET 0
        ) AS candidate_authority ON TRUE
        ORDER BY candidate.id
        LIMIT 1
    ) AS picked ON TRUE
), fallback_candidates AS (
    SELECT item.id AS item_id, item.fallback_id AS candidate_id
    FROM inventory.items AS item
), combined_candidates AS (
    SELECT * FROM primary_candidates
    UNION ALL
    SELECT * FROM fallback_candidates
)
SELECT item_id, candidate_id FROM combined_candidates`

	const deparsed = `
WITH primary_candidates AS (
    SELECT item.id AS item_id, picked.id AS candidate_id
    FROM inventory.items item
    JOIN LATERAL (
        SELECT scoped_authority.item_id, scoped_authority.candidate_id, scoped_authority.active
        FROM inventory.authorities scoped_authority
        WHERE scoped_authority.item_id = item.id
        OFFSET 0
    ) authority ON TRUE
    JOIN LATERAL (
        SELECT candidate.id
        FROM LATERAL (
            SELECT candidate.id
            FROM inventory.candidates candidate
            WHERE candidate.item_id = item.id
                AND (candidate.evidence #>> '{location,start}'::text[] IS DISTINCT FROM item.start_offset::text
                    OR candidate.evidence #>> '{location,end}'::text[] IS DISTINCT FROM item.end_offset::text)
            OFFSET 0
        ) candidate
        JOIN LATERAL (
            SELECT 1 AS "?column?"
            FROM inventory.authorities scoped_authority
            WHERE scoped_authority.candidate_id = candidate.id AND scoped_authority.active
            OFFSET 0
        ) candidate_authority ON TRUE
        ORDER BY candidate.id
        LIMIT 1
    ) picked ON TRUE
), fallback_candidates AS (
    SELECT item.id AS item_id, item.fallback_id AS candidate_id
    FROM inventory.items item
), combined_candidates AS (
    SELECT primary_candidates.item_id, primary_candidates.candidate_id FROM primary_candidates
    UNION ALL
    SELECT fallback_candidates.item_id, fallback_candidates.candidate_id FROM fallback_candidates
)
SELECT item_id, candidate_id FROM combined_candidates`

	tests := []struct {
		name    string
		before  string
		after   string
		changed bool
	}{
		{name: "converges after PostgreSQL deparsing"},
		{
			name:    "changed lateral limit",
			before:  "LIMIT 1",
			after:   "LIMIT 2",
			changed: true,
		},
		{
			name:    "changed lateral offset",
			before:  "OFFSET 0",
			after:   "OFFSET 1",
			changed: true,
		},
		{
			name:    "changed nested lateral predicate",
			before:  "AND scoped_authority.active",
			after:   "AND NOT scoped_authority.active",
			changed: true,
		},
		{
			name:    "changed row comparison",
			before:  "ROW(item.start_offset::text, item.end_offset::text)",
			after:   "ROW(item.start_offset::text, item.start_offset::text)",
			changed: true,
		},
		{
			name:    "changed projection",
			before:  "SELECT item_id, candidate_id FROM combined_candidates",
			after:   "SELECT item_id FROM combined_candidates",
			changed: true,
		},
		{
			name: "removed nested lateral join",
			before: `JOIN LATERAL (
            SELECT 1
            FROM inventory.authorities AS scoped_authority
            WHERE scoped_authority.candidate_id = candidate.id AND scoped_authority.active
            OFFSET 0
        ) AS candidate_authority ON TRUE`,
			changed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			desired := source
			if tt.before != "" {
				if !strings.Contains(source, tt.before) {
					t.Fatal("replacement must change the desired definition")
				}

				desired = strings.Replace(source, tt.before, tt.after, 1)
			}

			currentDB := &schema.Database{Views: []schema.View{
				{
					Schema:     "inventory",
					Name:       "authorities",
					Definition: "SELECT record.item_id, record.candidate_id, record.active FROM inventory.authority_records record",
				},
				{Schema: "inventory", Name: "picked_candidates", Definition: deparsed},
			}}
			desiredDB := &schema.Database{Views: append([]schema.View(nil), currentDB.Views...)}
			desiredDB.Views[1].Definition = desired

			assertViewDefinitionChange(t, currentDB, desiredDB, tt.changed)
		})
	}
}

func assertViewDefinitionChange(t *testing.T, current, desired *schema.Database, changed bool) {
	t.Helper()

	result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	if err != nil {
		t.Fatalf("compare views: %v", err)
	}

	if !changed {
		if len(result.Changes) != 0 {
			t.Fatalf("expected convergence, got changes: %+v", result.Changes)
		}

		return
	}

	for _, change := range result.Changes {
		if change.Type == differ.ChangeTypeModifyView ||
			change.Type == differ.ChangeTypeModifyMaterializedView {
			return
		}
	}

	t.Fatalf("expected a modified view, got changes: %+v", result.Changes)
}
