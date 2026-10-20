package parser_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestSequenceOptionsMatchPostgresCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want schema.Sequence
	}{
		{
			name: "implicit ascending defaults",
			sql:  `CREATE SEQUENCE counter;`,
			want: schema.Sequence{
				Schema: "public", Name: "counter", DataType: "bigint",
				StartValue: 1, MinValue: 1, MaxValue: math.MaxInt64, Increment: 1, CacheSize: 1,
			},
		},
		{
			name: "explicit bigint options",
			sql: `CREATE SEQUENCE ops.counter AS BIGINT INCREMENT BY 1
				MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;`,
			want: schema.Sequence{
				Schema: "ops", Name: "counter", DataType: "bigint",
				StartValue: 1, MinValue: 1, MaxValue: math.MaxInt64, Increment: 1, CacheSize: 1,
			},
		},
		{
			name: "smallint defaults",
			sql:  `CREATE SEQUENCE counter AS SMALLINT;`,
			want: schema.Sequence{
				Schema: "public", Name: "counter", DataType: "smallint",
				StartValue: 1, MinValue: 1, MaxValue: math.MaxInt16, Increment: 1, CacheSize: 1,
			},
		},
		{
			name: "integer defaults",
			sql:  `CREATE SEQUENCE counter AS INT;`,
			want: schema.Sequence{
				Schema: "public", Name: "counter", DataType: "integer",
				StartValue: 1, MinValue: 1, MaxValue: math.MaxInt32, Increment: 1, CacheSize: 1,
			},
		},
		{
			name: "descending defaults",
			sql:  `CREATE SEQUENCE counter INCREMENT -2 NO MINVALUE NO MAXVALUE;`,
			want: schema.Sequence{
				Schema: "public", Name: "counter", DataType: "bigint",
				StartValue: -1, MinValue: math.MinInt64, MaxValue: -1, Increment: -2, CacheSize: 1,
			},
		},
		{
			name: "smallint descending defaults",
			sql:  `CREATE SEQUENCE counter AS SMALLINT INCREMENT BY -1;`,
			want: schema.Sequence{
				Schema: "public", Name: "counter", DataType: "smallint",
				StartValue: -1, MinValue: math.MinInt16, MaxValue: -1, Increment: -1, CacheSize: 1,
			},
		},
		{
			name: "custom ascending bounds determine start",
			sql:  `CREATE SEQUENCE counter MINVALUE 5 MAXVALUE 20 CACHE 8 CYCLE;`,
			want: schema.Sequence{
				Schema:     "public",
				Name:       "counter",
				DataType:   "bigint",
				StartValue: 5,
				MinValue:   5,
				MaxValue:   20,
				Increment:  1,
				CacheSize:  8,
				IsCyclic:   true,
			},
		},
		{
			name: "custom descending bounds determine start",
			sql:  `CREATE SEQUENCE counter AS INTEGER INCREMENT -3 MINVALUE -100 MAXVALUE 0;`,
			want: schema.Sequence{
				Schema: "public", Name: "counter", DataType: "integer",
				StartValue: 0, MinValue: -100, MaxValue: 0, Increment: -3, CacheSize: 1,
			},
		},
		{
			name: "quoted identity and explicit start",
			sql: `CREATE SEQUENCE IF NOT EXISTS "Ops"."Counter" AS INT2
				INCREMENT 3 MINVALUE 0 MAXVALUE 100 START 12 CACHE 4 CYCLE;`,
			want: schema.Sequence{
				Schema:     "Ops",
				Name:       "Counter",
				DataType:   "smallint",
				StartValue: 12,
				MinValue:   0,
				MaxValue:   100,
				Increment:  3,
				CacheSize:  4,
				IsCyclic:   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := parser.New()
			desired := &schema.Database{}
			require.NoError(t, p.ParseSQL(tt.sql, desired))
			require.Empty(t, p.GetErrors())
			require.Equal(t, []schema.Sequence{tt.want}, desired.Sequences)

			current := &schema.Database{Sequences: []schema.Sequence{tt.want}}
			result, err := differ.New(nil).Compare(current, desired)
			require.NoError(t, err)
			require.Empty(t, result.Changes, "an unchanged sequence must not generate drift")
		})
	}
}

func TestSequenceRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	for _, options := range []string{
		"INCREMENT 0", "CACHE 0", "MINVALUE 10 MAXVALUE 5",
		"AS SMALLINT MAXVALUE 40000", "MINVALUE 5 START 1",
		"INCREMENT 1 INCREMENT 2", "AS TEXT",
	} {
		t.Run(options, func(t *testing.T) {
			t.Parallel()

			p := parser.New()
			db := &schema.Database{}
			err := p.ParseSQL("CREATE SEQUENCE counter "+options+";", db)
			require.True(t, err != nil || len(p.GetErrors()) > 0)
			require.Empty(t, db.Sequences)
		})
	}
}
