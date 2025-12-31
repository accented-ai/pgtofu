package parser_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestParseCreateView(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sql          string
		wantView     string
		wantSchema   string
		materialized bool
	}{
		{
			name: "simple view",
			sql: `CREATE VIEW active_users AS
				SELECT * FROM users WHERE is_active = TRUE;`,
			wantView:     "active_users",
			wantSchema:   schema.DefaultSchema,
			materialized: false,
		},
		{
			name: "materialized view",
			sql: `CREATE MATERIALIZED VIEW user_stats AS
				SELECT COUNT(*) as total FROM users;`,
			wantView:     "user_stats",
			wantSchema:   schema.DefaultSchema,
			materialized: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQL(t, tt.sql)

			if tt.materialized { //nolint:nestif
				if len(db.MaterializedViews) != 1 {
					t.Fatalf("expected 1 materialized view, got %d", len(db.MaterializedViews))
				}

				mv := db.MaterializedViews[0]
				if mv.Name != tt.wantView {
					t.Errorf("view name = %v, want %v", mv.Name, tt.wantView)
				}

				if mv.Schema != tt.wantSchema {
					t.Errorf("view schema = %v, want %v", mv.Schema, tt.wantSchema)
				}
			} else {
				if len(db.Views) != 1 {
					t.Fatalf("expected 1 view, got %d", len(db.Views))
				}

				view := db.Views[0]
				if view.Name != tt.wantView {
					t.Errorf("view name = %v, want %v", view.Name, tt.wantView)
				}

				if view.Schema != tt.wantSchema {
					t.Errorf("view schema = %v, want %v", view.Schema, tt.wantSchema)
				}
			}
		})
	}
}

func TestParseContinuousAggregateWithPolicy(t *testing.T) {
	t.Parallel()

	sql := `
CREATE MATERIALIZED VIEW public.hourly_metrics_summary
WITH (timescaledb.continuous) AS
SELECT
    category,
    time_bucket('1 hour', time) AS bucket,
    COUNT(*) as count
FROM metrics
GROUP BY category, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('public.hourly_metrics_summary',
    start_offset => INTERVAL '2 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => true
);
`

	db := parseSQL(t, sql)

	if len(db.ContinuousAggregates) != 1 {
		t.Fatalf("expected 1 continuous aggregate, got %d", len(db.ContinuousAggregates))
	}

	cagg := db.ContinuousAggregates[0]
	if cagg.ViewName != "hourly_metrics_summary" {
		t.Errorf("view name = %v, want hourly_metrics_summary", cagg.ViewName)
	}

	if cagg.Schema != schema.DefaultSchema {
		t.Errorf("view schema = %v, want %v", cagg.Schema, schema.DefaultSchema)
	}

	if cagg.RefreshPolicy == nil {
		t.Fatalf("expected refresh policy to be parsed")
	}

	if cagg.RefreshPolicy.StartOffset != "2 days" {
		t.Errorf("start offset = %v, want 2 days", cagg.RefreshPolicy.StartOffset)
	}

	if cagg.RefreshPolicy.EndOffset != "1 hour" {
		t.Errorf("end offset = %v, want 1 hour", cagg.RefreshPolicy.EndOffset)
	}

	if cagg.RefreshPolicy.ScheduleInterval != "1 hour" {
		t.Errorf("schedule interval = %v, want 1 hour", cagg.RefreshPolicy.ScheduleInterval)
	}
}
