package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestParser_ImplicitConstraintNamesRemainUniqueAfterTruncation(t *testing.T) {
	t.Parallel()

	db := parseSQL(t, `CREATE TABLE test_schema.table_with_an_exceptionally_long_name (
		column_with_an_exceptionally_long_name TEXT NOT NULL UNIQUE
			CHECK (length(column_with_an_exceptionally_long_name) = 64)
	);`)
	table := requireSingleTable(t, db)

	require.Len(t, table.Constraints, 2)
	require.Len(t, table.Indexes, 1)

	constraintNames := make(map[string]struct{}, len(table.Constraints))
	for _, constraint := range table.Constraints {
		assert.LessOrEqual(t, len(constraint.Name), schema.MaxIdentifierLength)
		assert.NotContains(t, constraintNames, constraint.Name)
		constraintNames[constraint.Name] = struct{}{}
	}

	uniqueName := table.Constraints[0].Name
	checkName := table.Constraints[1].Name

	assert.Equal(
		t,
		"table_with_an_exceptionally_long_name_column_with_an_exceptiona",
		uniqueName,
	)
	assert.Equal(
		t,
		uniqueName[:schema.MaxIdentifierLength-1]+"1",
		checkName,
	)
	assert.Equal(t, uniqueName, table.Indexes[0].Name)
}
