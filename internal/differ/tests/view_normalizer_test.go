package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
)

func TestNormalizeViewDefinitionMatchesPostgresFormatting(t *testing.T) {
	t.Parallel()

	source := `
SELECT
    u.team,
    u.username,
    u.role,
    u.is_active,
    MAX(a.performed_at) as last_action_at,
    COUNT(a.id) as actions_in_window,
    CASE
        WHEN COUNT(a.id) = 0 THEN 'IDLE'
        WHEN MAX(a.performed_at) < NOW() - INTERVAL '10 minutes' THEN 'STALE'
        ELSE 'ACTIVE'
    END as activity_status
FROM users u
LEFT JOIN actions a ON u.id = a.user_id
    AND a.performed_at > NOW() - INTERVAL '1 hour'
WHERE u.is_active = TRUE
GROUP BY u.team, u.username, u.role, u.is_active
ORDER BY u.team, u.username;`

	formatted := `
 SELECT u.team,
    u.username,
    u.role,
    u.is_active,
    max(a.performed_at) AS last_action_at,
    count(a.id) AS actions_in_window,
        CASE
            WHEN (count(a.id) = 0) THEN 'IDLE'::text
            WHEN (max(a.performed_at) < (now() - '00:10:00'::interval)) THEN 'STALE'::text
            ELSE 'ACTIVE'::text
        END AS activity_status
   FROM (users u
     LEFT JOIN actions a ON (((u.id = a.user_id) AND (a.performed_at > (now() - '01:00:00'::interval)))))
  WHERE (u.is_active = true)
  GROUP BY u.team, u.username, u.role, u.is_active
  ORDER BY u.team, u.username;`

	normalizedSource := differ.NormalizeViewDefinition(source)
	normalizedFormatted := differ.NormalizeViewDefinition(formatted)

	if normalizedSource != normalizedFormatted {
		vn := differ.NewViewNormalizer()
		t.Logf("formatted before parser: %s", vn.NormalizeText(formatted))
		t.Fatalf("normalized definitions should match\nsource: %s\nformatted: %s",
			normalizedSource, normalizedFormatted)
	}
}

func TestNormalizeViewDefinitionHandlesOrderByAlias(t *testing.T) {
	t.Parallel()

	source := `
WITH session_sequences AS (
    SELECT
        events.team,
        events.user_id,
        events.event_id,
        LAG(events.event_id) OVER (PARTITION BY events.team, events.user_id ORDER BY events.event_id) as prev_event_id,
        events.occurred_at
    FROM events
    WHERE events.occurred_at > NOW() - INTERVAL '3 days'
)
SELECT
    team,
    user_id,
    prev_event_id + 1 as gap_start_id,
    event_id - 1 as gap_end_id,
    event_id - prev_event_id - 1 as gap_size,
    occurred_at as detected_at
FROM session_sequences
WHERE event_id - prev_event_id > 1
ORDER BY team, user_id, gap_start_id;
`

	formatted := `
 WITH session_sequences AS (
         SELECT events.team,
            events.user_id,
            events.event_id,
            lag(events.event_id)
            OVER (PARTITION BY events.team, events.user_id ORDER BY events.event_id) AS prev_event_id,
            events.occurred_at
           FROM events
          WHERE (events.occurred_at > (now() - '3 days'::interval))
        )
 SELECT team,
    user_id,
    (prev_event_id + 1) AS gap_start_id,
    (event_id - 1) AS gap_end_id,
    ((event_id - prev_event_id) - 1) AS gap_size,
    occurred_at AS detected_at
   FROM session_sequences
  WHERE ((event_id - prev_event_id) > 1)
  ORDER BY team, user_id, (prev_event_id + 1);
`

	assertNormalizedEqual(t, source, formatted)
}

func TestNormalizeViewDefinitionHandlesGroupByAlias(t *testing.T) {
	t.Parallel()

	source := `
SELECT
    team,
    actor,
    time_bucket('1 hour', occurred_at) AS bucket,
    COUNT(*) as event_count
FROM events
GROUP BY team, actor, bucket;
`

	formatted := `
 SELECT team,
    actor,
    time_bucket('01:00:00'::interval, occurred_at) AS bucket,
    count(*) AS event_count
   FROM events
  GROUP BY team, actor, (time_bucket('01:00:00'::interval, occurred_at));
`

	assertNormalizedEqual(t, source, formatted)
}

func assertNormalizedEqual(t *testing.T, a, b string) {
	t.Helper()

	normalizedA := differ.NormalizeViewDefinition(a)
	normalizedB := differ.NormalizeViewDefinition(b)

	if normalizedA != normalizedB {
		t.Fatalf("normalized definitions should match\nA: %s\nB: %s", normalizedA, normalizedB)
	}
}

func TestNormalizeViewDefinitionHandlesQualifiedCTEColumns(t *testing.T) {
	t.Parallel()

	source := `
WITH event_sequences AS (
    SELECT
        team,
        actor,
        event_id,
        LAG(event_id) OVER (PARTITION BY team, actor ORDER BY event_id) as previous_id,
        occurred_at
    FROM events
    WHERE occurred_at > NOW() - INTERVAL '1 day'
)
SELECT
    team,
    actor,
    previous_id + 1 as gap_start_id,
    event_id - 1 as gap_end_id,
    event_id - previous_id - 1 as gap_size,
    occurred_at as detected_at
FROM event_sequences
WHERE event_id - previous_id > 1
ORDER BY team, actor, gap_start_id;
`

	formatted := `
 WITH event_sequences AS (
         SELECT events.team,
            events.actor,
            events.event_id,
            lag(events.event_id)
            OVER (PARTITION BY events.team, events.actor ORDER BY events.event_id) AS previous_id,
            events.occurred_at
           FROM events
          WHERE (events.occurred_at > (now() - '1 day'::interval))
        )
 SELECT team,
    actor,
    (previous_id + 1) AS gap_start_id,
    (event_id - 1) AS gap_end_id,
    ((event_id - previous_id) - 1) AS gap_size,
    occurred_at AS detected_at
   FROM event_sequences
  WHERE ((event_id - previous_id) > 1)
  ORDER BY team, actor, (previous_id + 1);
`

	assertNormalizedEqual(t, source, formatted)
}
