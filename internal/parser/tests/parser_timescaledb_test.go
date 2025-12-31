package parser_test

import (
	"testing"
)

func TestParseTimescaleDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setupSQL string
		tsdbSQL  string
		wantHT   bool
	}{
		{
			name:     "create hypertable",
			setupSQL: `CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION);`,
			tsdbSQL:  `SELECT create_hypertable('metrics', 'time');`,
			wantHT:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := parseSQLWithSetup(t, tt.setupSQL, tt.tsdbSQL)

			if tt.wantHT {
				if len(db.Hypertables) != 1 {
					t.Fatalf("expected 1 hypertable, got %d", len(db.Hypertables))
				}

				ht := db.Hypertables[0]
				if ht.TableName != "metrics" {
					t.Errorf("hypertable name = %v, want metrics", ht.TableName)
				}

				if ht.TimeColumnName != "time" {
					t.Errorf("time column = %v, want time", ht.TimeColumnName)
				}
			}
		})
	}
}
