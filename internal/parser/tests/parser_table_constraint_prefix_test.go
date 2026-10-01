package parser_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestParseColumnsWithConstraintKeywordPrefixes(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"checkpoint_id", "checklist", "CHECKPOINT_ID", "check$point",
		"unique_key", "uniqueness", "exclude_reason", "excluded_at",
		"constraint_name", "primary_key", "foreign_key", "exclude",
		`"check"`, `"unique"`, `"exclude"`, `"constraint"`, `"primary"`, `"foreign"`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			p := parser.New()
			db := &schema.Database{}
			err := p.ParseSQL(fmt.Sprintf(`CREATE TABLE records (
                %s UUID NOT NULL,
                PRIMARY KEY (%s)
            );`, name, name), db)
			require.NoError(t, err)
			require.Empty(t, p.GetWarnings(), "valid columns must not be discarded as constraints")

			table := requireSingleTable(t, db)
			require.Len(t, table.Columns, 1)

			wantName := strings.ToLower(strings.Trim(name, `"`))
			require.Equal(t, wantName, table.Columns[0].Name)
			require.Equal(t, "UUID", table.Columns[0].DataType)
			require.False(t, table.Columns[0].IsNullable)
			require.Len(t, table.Constraints, 1)
			require.Equal(t, []string{wantName}, table.Constraints[0].Columns)
		})
	}
}

func TestParseTableConstraintsWithKeywordBoundaries(t *testing.T) {
	t.Parallel()

	p := parser.New()
	db := &schema.Database{}
	err := p.ParseSQL(`CREATE TABLE records (
        id BIGINT NOT NULL,
        parent_id BIGINT,
        label TEXT,
        PRIMARY
            KEY (id),
        FOREIGN	KEY (parent_id) REFERENCES parents(id),
        UNIQUE(label),
        CHECK(id > 0),
        CONSTRAINT
            records_label_check CHECK (label <> ''),
        EXCLUDE USING gist (id WITH =)
    );`, db)
	require.NoError(t, err)
	require.Empty(t, p.GetWarnings())

	table := requireSingleTable(t, db)
	require.Len(t, table.Columns, 3)
	require.Len(t, table.Constraints, 6)

	types := make([]string, 0, len(table.Constraints))
	for _, constraint := range table.Constraints {
		types = append(types, constraint.Type)
	}

	require.ElementsMatch(
		t,
		[]string{"PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK", "CHECK", "EXCLUDE"},
		types,
	)
}
