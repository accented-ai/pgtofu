package differ_test

import (
	"strings"
	"testing"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_ViewConvergesWithRenamedCTEAliases(t *testing.T) {
	t.Parallel()

	const source = `
WITH reviewed_records AS (
    SELECT r.id AS record_id, d.payload -> 'review' AS decision
    FROM reporting.records AS r
    INNER JOIN reporting.documents AS d ON r.document_id = d.id
    WHERE d.active
        AND ('reason=' || (d.payload #>> '{review,reason}')) = ANY(r.notes)
)
SELECT r.id,
    CASE
        WHEN COALESCE(d.payload ->> 'mode', '') NOT IN ('failed', 'cancelled')
            AND (d.payload #>> '{review,outcome}' = 'held' OR review.decision IS NOT NULL)
            THEN 'pending'
        ELSE 'other'
    END AS classification
FROM reporting.records AS r
INNER JOIN reporting.documents AS d ON r.document_id = d.id
LEFT JOIN reviewed_records AS review ON r.id = review.record_id`

	const stored = `
WITH reviewed_records AS (
    SELECT r_1.id AS record_id, d_1.payload -> 'review'::text AS decision
    FROM reporting.records r_1
    JOIN reporting.documents d_1 ON r_1.document_id = d_1.id
    WHERE d_1.active
        AND (('reason='::text || (d_1.payload #>> '{review,reason}'::text[])) = ANY(r_1.notes))
)
SELECT r.id,
    CASE
        WHEN (COALESCE(d.payload ->> 'mode'::text, ''::text) <> ALL(ARRAY['failed'::text, 'cancelled'::text]))
            AND ((d.payload #>> '{review,outcome}'::text[]) = 'held'::text OR review.decision IS NOT NULL)
            THEN 'pending'::text
        ELSE 'other'::text
    END AS classification
FROM reporting.records r
JOIN reporting.documents d ON r.document_id = d.id
LEFT JOIN reviewed_records review ON r.id = review.record_id`

	tests := []struct {
		name    string
		before  string
		after   string
		changed bool
	}{
		{name: "converges after PostgreSQL deparsing"},
		{
			name:    "changed CTE predicate",
			before:  "WHERE d.active",
			after:   "WHERE NOT d.active",
			changed: true,
		},
		{
			name:    "changed concatenation",
			before:  "'reason='",
			after:   "'reason_code='",
			changed: true,
		},
		{name: "changed membership", before: "'cancelled'", after: "'blocked'", changed: true},
		{
			name:    "changed classification",
			before:  "THEN 'pending'",
			after:   "THEN 'resolved'",
			changed: true,
		},
		{
			name:    "changed join",
			before:  "LEFT JOIN reviewed_records",
			after:   "INNER JOIN reviewed_records",
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

			assertViewDefinitionChange(
				t,
				&schema.Database{
					Views: []schema.View{
						{Schema: "reporting", Name: "record_classifications", Definition: stored},
					},
				},
				&schema.Database{
					Views: []schema.View{
						{Schema: "reporting", Name: "record_classifications", Definition: desired},
					},
				},
				tt.changed,
			)
		})
	}
}
