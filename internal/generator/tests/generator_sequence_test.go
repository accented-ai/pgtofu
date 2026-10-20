package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestGenerateSequenceModificationAndRollback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		change func(*schema.Sequence)
	}{
		{"data type", func(s *schema.Sequence) { s.DataType = "integer" }},
		{"start", func(s *schema.Sequence) { s.StartValue = 20 }},
		{"cache", func(s *schema.Sequence) { s.CacheSize = 8 }},
		{"increment", func(s *schema.Sequence) { s.Increment = 3 }},
		{"minimum", func(s *schema.Sequence) { s.MinValue = 0 }},
		{"maximum", func(s *schema.Sequence) { s.MaxValue = 200 }},
		{"cycle", func(s *schema.Sequence) { s.IsCyclic = true }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			before := schema.Sequence{
				Schema: "Ops", Name: "Counter", DataType: "bigint",
				StartValue: 10, MinValue: 1, MaxValue: 100, Increment: 1, CacheSize: 1,
			}
			after := before
			tt.change(&after)

			current := &schema.Database{Sequences: []schema.Sequence{before}}
			desired := &schema.Database{Sequences: []schema.Sequence{after}}
			result, err := differ.New(nil).Compare(current, desired)
			require.NoError(t, err)
			require.Len(t, result.Changes, 1)
			require.Equal(t, differ.ChangeTypeModifySequence, result.Changes[0].Type)

			ddl := generator.NewDDLBuilder(result, false)
			up, err := ddl.BuildUpStatement(result.Changes[0])
			require.NoError(t, err)
			down, err := ddl.BuildDownStatement(result.Changes[0])
			require.NoError(t, err)

			for _, statement := range []generator.DDLStatement{up, down} {
				require.True(t, strings.HasPrefix(statement.SQL, "ALTER SEQUENCE "))
				require.NotContains(t, statement.SQL, "RESTART", "do not reset the live counter")
				require.True(t, statement.RequiresTx)
			}

			require.Equal(t, after, parseGeneratedSequence(t,
				strings.Replace(up.SQL, "ALTER SEQUENCE", "CREATE SEQUENCE", 1)))
			require.Equal(t, before, parseGeneratedSequence(t,
				strings.Replace(down.SQL, "ALTER SEQUENCE", "CREATE SEQUENCE", 1)))
		})
	}
}

func TestGenerateSequenceCreationAndDropRestoreOptions(t *testing.T) {
	t.Parallel()

	for _, sql := range []string{
		`CREATE SEQUENCE "Ops"."Counter" AS SMALLINT INCREMENT 2
			MINVALUE 0 MAXVALUE 100 START 12 CACHE 4 CYCLE;`,
		`CREATE SEQUENCE "Ops"."Counter" INCREMENT -1 MINVALUE 1 MAXVALUE 10 START 1;`,
		`CREATE SEQUENCE "Ops"."Counter" INCREMENT -1 MINVALUE -100 MAXVALUE 0;`,
	} {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()

			sequence := parseGeneratedSequence(t, sql)
			populated := &schema.Database{Sequences: []schema.Sequence{sequence}}
			empty := &schema.Database{}

			for _, dropping := range []bool{false, true} {
				current, desired := empty, populated
				if dropping {
					current, desired = populated, empty
				}

				result, err := differ.New(nil).Compare(current, desired)
				require.NoError(t, err)
				require.Len(t, result.Changes, 1)
				ddl := generator.NewDDLBuilder(result, false)
				up, err := ddl.BuildUpStatement(result.Changes[0])
				require.NoError(t, err)
				down, err := ddl.BuildDownStatement(result.Changes[0])
				require.NoError(t, err)

				creation, removal := up, down
				if dropping {
					creation, removal = down, up
				}

				require.Equal(t, sequence, parseGeneratedSequence(t, creation.SQL))
				require.Contains(t, removal.SQL, `DROP SEQUENCE "Ops"."Counter"`)
			}
		})
	}
}

func parseGeneratedSequence(t *testing.T, sql string) schema.Sequence {
	t.Helper()

	p := parser.New()
	db := &schema.Database{}
	require.NoError(t, p.ParseSQL(sql, db))
	require.Empty(t, p.GetErrors())
	require.Len(t, db.Sequences, 1)

	return db.Sequences[0]
}
