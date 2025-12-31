package parser_test

import (
	"testing"
)

func TestParseCreateFunction(t *testing.T) {
	t.Parallel()

	sql := `CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
	NEW.updated_at = NOW();
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`

	db := parseSQL(t, sql)

	if len(db.Functions) != 1 {
		t.Fatalf("expected 1 function, got %d", len(db.Functions))
	}

	fn := db.Functions[0]
	if fn.Name != "update_timestamp" {
		t.Errorf("function name = %v, want update_timestamp", fn.Name)
	}

	if fn.Language != "plpgsql" {
		t.Errorf("function language = %v, want plpgsql", fn.Language)
	}
}

func TestParseTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupSQL       string
		triggerSQL     string
		wantTrigger    string
		wantTiming     string
		wantForEachRow bool
		wantEvents     []string
	}{
		{
			name: "BEFORE UPDATE trigger",
			setupSQL: `CREATE TABLE users (id BIGINT, updated_at TIMESTAMPTZ);
			CREATE FUNCTION update_timestamp() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;`,
			triggerSQL: `CREATE TRIGGER set_updated_at
				BEFORE UPDATE ON users
				FOR EACH ROW
				EXECUTE FUNCTION update_timestamp();`,
			wantTrigger:    "set_updated_at",
			wantTiming:     "BEFORE",
			wantForEachRow: true,
			wantEvents:     []string{"UPDATE"},
		},
		{
			name: "AFTER INSERT OR UPDATE OR DELETE trigger",
			setupSQL: `CREATE TABLE logs (id BIGINT);
			CREATE FUNCTION log_changes() RETURNS TRIGGER AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;`,
			triggerSQL: `CREATE TRIGGER audit_log
				AFTER INSERT OR UPDATE OR DELETE ON logs
				FOR EACH STATEMENT
				EXECUTE FUNCTION log_changes();`,
			wantTrigger:    "audit_log",
			wantTiming:     "AFTER",
			wantForEachRow: false,
			wantEvents:     []string{"INSERT", "UPDATE", "DELETE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQLWithSetup(t, tt.setupSQL, tt.triggerSQL)

			if len(db.Triggers) != 1 {
				t.Fatalf("expected 1 trigger, got %d", len(db.Triggers))
			}

			trigger := db.Triggers[0]
			if trigger.Name != tt.wantTrigger {
				t.Errorf("trigger name = %v, want %v", trigger.Name, tt.wantTrigger)
			}

			if trigger.Timing != tt.wantTiming {
				t.Errorf("trigger timing = %v, want %v", trigger.Timing, tt.wantTiming)
			}

			if trigger.ForEachRow != tt.wantForEachRow {
				t.Errorf("trigger ForEachRow = %v, want %v", trigger.ForEachRow, tt.wantForEachRow)
			}

			if len(trigger.Events) != len(tt.wantEvents) {
				t.Errorf(
					"trigger events count = %v, want %v",
					len(trigger.Events),
					len(tt.wantEvents),
				)
			}
		})
	}
}
