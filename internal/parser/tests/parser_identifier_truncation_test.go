package parser_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestParser_ConstraintName_TruncatedToPostgresLimit(t *testing.T) {
	t.Parallel()

	// PostgreSQL silently truncates explicit identifiers to NAMEDATALEN-1
	// bytes when storing them in pg_catalog, so the parser must do the same
	// to avoid spurious drift between parsed SQL and extracted state.
	const longName = "widgets_some_really_long_descriptive_constraint_name_for_testing_check"

	if len(longName) <= schema.MaxIdentifierLength {
		t.Fatalf("test fixture is not longer than the limit; update it")
	}

	expectedName := longName[:schema.MaxIdentifierLength]

	sql := `CREATE TABLE widgets (
		id UUID PRIMARY KEY,
		code TEXT NOT NULL DEFAULT '',
		CONSTRAINT ` + longName + ` CHECK (code <> '')
	);`

	db := parseSQL(t, sql)

	if len(db.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(db.Tables))
	}

	var found *schema.Constraint

	for i := range db.Tables[0].Constraints {
		c := &db.Tables[0].Constraints[i]
		if c.Type == schema.ConstraintCheck {
			found = c
			break
		}
	}

	if found == nil {
		t.Fatal("expected to find CHECK constraint, got none")
	}

	if found.Name != expectedName {
		t.Errorf(
			"constraint name not truncated to %d bytes\n  got:  %q (%d bytes)\n  want: %q (%d bytes)",
			schema.MaxIdentifierLength,
			found.Name,
			len(found.Name),
			expectedName,
			len(expectedName),
		)
	}
}
