package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestViewFormattingUsesLintCompatibleCTELayout(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema: "reporting",
		Name:   "current_records",
		Definition: `WITH direct_records AS (
    SELECT record.id, record.status
    FROM reporting.records AS record
    INNER JOIN reporting.record_links AS link
        ON record.id = link.record_id
    INNER JOIN reporting.record_windows AS record_window
        ON record.rank BETWEEN record_window.minimum_rank AND record_window.maximum_rank
),
scoped_records AS (
    SELECT record.id, record.status
    FROM reporting.records AS record
    INNER JOIN reporting.record_scopes AS scope
        ON record.id = scope.record_id
        AND record.scope = scope.scope
        AND record.owner_id IS NOT DISTINCT FROM scope.owner_id
    WHERE record.is_visible = false
        AND record.status IS DISTINCT FROM 'archived'
)
SELECT * FROM direct_records
UNION ALL
SELECT * FROM scoped_records`,
	}
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{Views: []schema.View{view}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddView,
			ObjectName: differ.ViewKey(view.Schema, view.Name),
		}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	assert.Contains(t, statement.SQL, `WITH
direct_records AS (
    SELECT
        record.id,`)
	assert.Contains(t, statement.SQL, `scoped_records AS (
    SELECT
        record.id,`)
	assert.Contains(t, statement.SQL,
		`INNER JOIN reporting.record_links AS link ON record.id = link.record_id`)
	assert.Contains(t, statement.SQL, `INNER JOIN reporting.record_windows AS record_window
        ON
            record.rank BETWEEN record_window.minimum_rank
            AND record_window.maximum_rank`)
	assert.Contains(t, statement.SQL, `INNER JOIN reporting.record_scopes AS scope
        ON
            record.id = scope.record_id
            AND record.scope = scope.scope
            AND record.owner_id IS NOT DISTINCT FROM scope.owner_id`)
	assert.Contains(t, statement.SQL, `SELECT *
FROM direct_records
UNION ALL
SELECT *
FROM scoped_records`)
	assert.Contains(t, statement.SQL, "FROM reporting.records AS record")
	assert.Contains(t, statement.SQL, `WHERE
        record.is_visible = FALSE
        AND record.status IS DISTINCT FROM 'archived'`)
	assert.NotContains(t, statement.SQL, "WITH\n    direct_records")
}

func TestViewFormattingCompactsOnlySafeSourceLines(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema: "reporting",
		Name:   "record_activity",
		Definition: `SELECT r.id, label.value
FROM reporting.short_records AS r
INNER JOIN reporting.short_links AS link USING (record_id)
CROSS JOIN UNNEST(r.labels) AS label(value)
LEFT JOIN reporting.records_with_an_intentionally_long_descriptive_name AS detailed_record
    ON r.id = detailed_record.source_record_id
WHERE EXISTS (
    SELECT 1
    FROM (
        SELECT event.record_id
        FROM reporting.audit_events AS event
    ) AS recent_event
    WHERE recent_event.record_id = r.id
)`,
	}
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{Views: []schema.View{view}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddView,
			ObjectName: differ.ViewKey(view.Schema, view.Name),
		}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	assert.Contains(t, statement.SQL, "FROM reporting.short_records AS r")
	assert.Contains(t, statement.SQL,
		"INNER JOIN reporting.short_links AS link USING (record_id)")
	assert.Contains(t, statement.SQL, "CROSS JOIN UNNEST(r.labels) AS label(value)")
	assert.Contains(t, statement.SQL, `LEFT JOIN
    reporting.records_with_an_intentionally_long_descriptive_name AS detailed_record
    ON r.id = detailed_record.source_record_id`)
	assert.Contains(t, statement.SQL, `FROM
            (
                SELECT event.record_id
                FROM reporting.audit_events AS event`)

	for line := range strings.SplitSeq(statement.SQL, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "FROM ") || strings.Contains(trimmed, " JOIN ") {
			assert.LessOrEqual(t, len(line), 80, "compacted source line exceeds limit")
		}
	}
}

func TestViewFormattingIndentsMultilineArrayConstructors(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema: "reporting",
		Name:   "visible_records",
		Definition: `SELECT record.id
FROM reporting.records AS record
WHERE record.visibility = ANY(
    ARRAY['visible_to_all_authenticated_reporting_users'::TEXT,
        'visible_only_during_an_active_manual_reporting_review'::TEXT,
        'visible_to_reporting_administrators'::TEXT]
)`,
	}
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{Views: []schema.View{view}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddView,
			ObjectName: differ.ViewKey(view.Schema, view.Name),
		}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	assert.Contains(t, statement.SQL, `WHERE
    record.visibility = ANY(
        ARRAY [
            'visible_to_all_authenticated_reporting_users'::TEXT,
            'visible_only_during_an_active_manual_reporting_review'::TEXT,
            'visible_to_reporting_administrators'::TEXT
        ]
    )`)
}

func TestViewFormattingAlignsLateralAndJoinClauses(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema: "reporting",
		Name:   "visible_records",
		Definition: `SELECT record.id, authority.semantics
FROM reporting.records AS record
INNER JOIN LATERAL (
    SELECT scoped_authority.semantics
    FROM reporting.record_authority AS scoped_authority
    WHERE scoped_authority.record_id = record.id
    OFFSET 0
) AS authority ON TRUE
INNER JOIN reporting.projections AS projection
    ON record.id = projection.record_id
    AND record.tenant_id = projection.tenant_id
WHERE projection.status = 'ready'
    AND EXISTS (
        SELECT 1
        FROM reporting.record_members AS member
        INNER JOIN LATERAL (
            SELECT scoped_visible.id
            FROM reporting.visible_members AS scoped_visible
            WHERE scoped_visible.id = member.visible_member_id
            OFFSET 0
        ) AS visible_member ON TRUE
        WHERE member.record_id = record.id
    )`,
	}
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{Views: []schema.View{view}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddView,
			ObjectName: differ.ViewKey(view.Schema, view.Name),
		}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	expected := `CREATE VIEW reporting.visible_records AS
SELECT
    record.id,
    authority.semantics
FROM reporting.records AS record
INNER JOIN
    LATERAL (
        SELECT scoped_authority.semantics
        FROM reporting.record_authority AS scoped_authority
        WHERE
            scoped_authority.record_id = record.id
        OFFSET 0
    ) AS authority
    ON TRUE
INNER JOIN reporting.projections AS projection
    ON
        record.id = projection.record_id
        AND record.tenant_id = projection.tenant_id
WHERE
    projection.status = 'ready'
    AND EXISTS (
        SELECT 1
        FROM reporting.record_members AS member
        INNER JOIN
            LATERAL (
                SELECT scoped_visible.id
                FROM reporting.visible_members AS scoped_visible
                WHERE
                    scoped_visible.id = member.visible_member_id
                OFFSET 0
            ) AS visible_member
            ON TRUE
        WHERE
            member.record_id = record.id
    );`
	require.Equal(t, expected, statement.SQL)
}

func TestViewFormattingOrdersJoinConditionRelations(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema: "reporting",
		Name:   "record_ownership",
		Definition: `SELECT record.id, owner.id AS owner_id, score.value
FROM reporting.records AS record
INNER JOIN reporting.owners AS owner
    ON owner.id = record.owner_id
    AND owner.tenant_id = record.tenant_id
    AND owner.region IS NOT DISTINCT FROM record.region
LEFT JOIN reporting.scores AS score
    ON score.record_id = record.id
    AND score.maximum_value < record.minimum_score`,
	}
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{Views: []schema.View{view}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddView,
			ObjectName: differ.ViewKey(view.Schema, view.Name),
		}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	assert.Contains(t, statement.SQL, `INNER JOIN reporting.owners AS owner
    ON
        record.owner_id = owner.id
        AND record.tenant_id = owner.tenant_id
        AND record.region IS NOT DISTINCT FROM owner.region`)
	assert.Contains(t, statement.SQL, `LEFT JOIN reporting.scores AS score
    ON
        record.id = score.record_id
        AND record.minimum_score > score.maximum_value`)
	assert.NotContains(t, statement.SQL, "owner.id = record.owner_id")
	assert.NotContains(t, statement.SQL, "score.maximum_value < record.minimum_score")
}

func TestViewFormattingIndentsMultilineCaseResultsAndJSONPathCast(t *testing.T) {
	t.Parallel()

	view := schema.View{
		Schema: "reporting",
		Name:   "record_decisions",
		Definition: `SELECT
    record.id,
    CASE
        WHEN record.status = 'rejected' THEN JSONB_BUILD_OBJECT(
            'outcome', 'blocked',
            'confidence', record.confidence
        )
        WHEN record.status = 'approved' THEN JSONB_BUILD_OBJECT(
            'outcome', 'ready',
            'confidence', record.confidence
        )
        ELSE '{}'::JSONB
    END AS decision,
    JSONB_PATH_EXISTS(
        record.evidence,
        '$?(@."confidence" >= 0.80)'::jsonpath
    ) AS has_confident_evidence
FROM reporting.records AS record`,
	}
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{Views: []schema.View{view}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddView,
			ObjectName: differ.ViewKey(view.Schema, view.Name),
		}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	assert.Contains(t, statement.SQL, `CASE
        WHEN record.status = 'rejected'
            THEN JSONB_BUILD_OBJECT(
                'outcome',
                'blocked',
                'confidence',
                record.confidence
            )
        WHEN record.status = 'approved'
            THEN JSONB_BUILD_OBJECT(
                'outcome',
                'ready',
                'confidence',
                record.confidence
            )
        ELSE '{}'::JSONB
    END AS decision`)
	assert.Contains(t, statement.SQL, `'$?(@."confidence" >= 0.80)'::JSONPATH`)
	assert.NotContains(t, statement.SQL, "::jsonpath")
}
