package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
)

func TestNormalizeViewDefinitionRealWorldIdempotency(t *testing.T) {
	t.Parallel()

	source := `SELECT
    c.category,
    c.name,
    c.status,
    c.is_enabled,
    c.created_at,
    MAX(e.event_time) as last_event_time,
    COUNT(e.event_id) as total_events_24h,
    MAX(e.event_id) as latest_event_id,
    c.expected_rate,
    CASE
        WHEN c.expected_rate IS NOT NULL
            AND COUNT(e.event_id) < c.expected_rate * 24 * 0.5
        THEN 'LOW_ACTIVITY'
        WHEN MAX(e.event_time) < NOW() - INTERVAL '5 minutes'
        THEN 'INACTIVE'
        ELSE 'ACTIVE'
    END as activity_status
FROM configurations c
LEFT JOIN events e ON c.category = e.category AND c.name = e.name
    AND e.event_time > NOW() - INTERVAL '24 hours'
WHERE c.is_enabled = TRUE
GROUP BY c.category, c.name, c.status, c.is_enabled, c.created_at, c.expected_rate
ORDER BY c.category, c.name`

	formatted := ` SELECT c.category,
    c.name,
    c.status,
    c.is_enabled,
    c.created_at,
    max(e.event_time) AS last_event_time,
    count(e.event_id) AS total_events_24h,
    max(e.event_id) AS latest_event_id,
    c.expected_rate,
        CASE
            WHEN ((c.expected_rate IS NOT NULL)
            AND ((count(e.event_id))::numeric < (((c.expected_rate * 24))::numeric * 0.5))) THEN 'LOW_ACTIVITY'::text
            WHEN (max(e.event_time) < (now() - '00:05:00'::interval)) THEN 'INACTIVE'::text
            ELSE 'ACTIVE'::text
        END AS activity_status
   FROM (configurations c
     LEFT JOIN events e ON ((((c.category)::text = (e.category)::text)
     AND ((c.name)::text = (e.name)::text) AND (e.event_time > (now() - '24:00:00'::interval)))))
  WHERE (c.is_enabled = true)
  GROUP BY c.category, c.name, c.status, c.is_enabled, c.created_at, c.expected_rate
  ORDER BY c.category, c.name`

	normalizedSource := differ.NormalizeViewDefinition(source)
	normalizedFormatted := differ.NormalizeViewDefinition(formatted)

	if normalizedSource != normalizedFormatted {
		t.Logf("Source normalized:\n%s", normalizedSource)
		t.Logf("\nFormatted normalized:\n%s", normalizedFormatted)
		t.Fatalf("normalized definitions should match")
	}
}
