package parser_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func parseSQL(t *testing.T, sql string) *schema.Database {
	t.Helper()

	p := parser.New()
	db := &schema.Database{}

	if err := p.ParseSQL(sql, db); err != nil {
		t.Fatalf("ParseSQL() error = %v", err)
	}

	return db
}

func parseSQLWithSetup(t *testing.T, setupSQL, mainSQL string) *schema.Database {
	t.Helper()

	p := parser.New()
	db := &schema.Database{}

	if err := p.ParseSQL(setupSQL, db); err != nil {
		t.Fatalf("ParseSQL(setup) error = %v", err)
	}

	if err := p.ParseSQL(mainSQL, db); err != nil {
		t.Fatalf("ParseSQL(main) error = %v", err)
	}

	return db
}

func requireSingleTable(t *testing.T, db *schema.Database) *schema.Table {
	t.Helper()

	if len(db.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(db.Tables))
	}

	return &db.Tables[0]
}
