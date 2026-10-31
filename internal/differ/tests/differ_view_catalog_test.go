package differ_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

type catalogViewCase struct {
	name         string
	current      string
	original     string
	bound        string
	desired      string
	desiredBound string
	equal        bool
}

func TestViewCatalogBindings(t *testing.T) {
	t.Parallel()

	tests := []catalogViewCase{
		{
			name:         "identical presentation with different search-path bindings",
			current:      "SELECT r.id FROM records r",
			bound:        "SELECT r.id FROM archive.records r",
			desired:      "SELECT r.id FROM records r",
			desiredBound: "SELECT r.id FROM public.records r",
		},
		{
			name:    "quoted function case stays distinct",
			current: `SELECT public."Allowed"(r.id) AS valid FROM records r`,
			bound:   `SELECT public."Allowed"(r.id) AS valid FROM public.records r`,
			desired: "SELECT public.allowed(r.id) AS valid FROM public.records r",
		},
		{
			name:    "public relation and function",
			current: "SELECT r.id FROM records r WHERE allowed(r.id)",
			bound:   "SELECT r.id FROM public.records r WHERE public.allowed(r.id)",
			desired: "SELECT record.id FROM public.records record WHERE public.allowed(record.id)",
			equal:   true,
		},
		{
			name:    "non-public relation stays distinct despite unqualified output",
			current: "SELECT id FROM records",
			bound:   "SELECT id FROM archive.records",
			desired: "SELECT id FROM public.records",
		},
		{
			name:    "non-public function stays distinct",
			current: "SELECT allowed(r.id) AS valid FROM records r",
			bound:   "SELECT archive.allowed(r.id) AS valid FROM public.records r",
			desired: "SELECT public.allowed(r.id) AS valid FROM public.records r",
		},
		{
			name:    "builtin does not become a public function",
			current: "SELECT lower(r.value) AS value FROM records r",
			bound:   "SELECT lower(r.value) AS value FROM public.records r",
			desired: "SELECT public.lower(r.value) AS value FROM public.records r",
		},
		{
			name:    "public function does not become a builtin",
			current: "SELECT lower(r.value) AS value FROM records r",
			bound:   "SELECT public.lower(r.value) AS value FROM public.records r",
			desired: "SELECT pg_catalog.lower(r.value) AS value FROM public.records r",
		},
		{
			name:    "unchanged qualified builtin presentation stays equal",
			current: "SELECT pg_catalog.lower('MiXeD'::text) AS value",
			bound:   "SELECT lower('MiXeD'::text) AS value",
			desired: "SELECT pg_catalog.lower('MiXeD'::text) AS value",
			equal:   true,
		},
		{
			name:    "quoted schema case stays distinct",
			current: `SELECT r.id FROM "Public".records r`,
			bound:   `SELECT r.id FROM "Public".records r`,
			desired: "SELECT r.id FROM public.records r",
		},
		{
			name:    "quoted relation case stays distinct",
			current: `SELECT r.id FROM public."Records" r`,
			bound:   `SELECT r.id FROM public."Records" r`,
			desired: "SELECT r.id FROM public.records r",
		},
		{
			name:    "unqualified quoted relation case stays distinct",
			current: `SELECT id FROM "Records"`,
			bound:   `SELECT id FROM public."Records"`,
			desired: `SELECT id FROM "records"`,
		},
		{
			name:    "unqualified source preserves alias equivalence",
			current: "SELECT r.id FROM records r WHERE allowed(r.id)",
			bound:   "SELECT r.id FROM public.records r WHERE public.allowed(r.id)",
			desired: "SELECT record.id FROM records record WHERE allowed(record.id)",
			equal:   true,
		},
		{
			name:    "quoted column case stays distinct",
			current: `SELECT r."ID" FROM records r`,
			bound:   `SELECT r."ID" FROM public.records r`,
			desired: "SELECT r.id FROM public.records r",
		},
		{
			name:    "CTE does not become a public relation",
			current: "WITH records AS (SELECT 1 AS id) SELECT r.id FROM records r",
			bound:   "WITH records AS (SELECT 1 AS id) SELECT r.id FROM records r",
			desired: "WITH records AS (SELECT 1 AS id) SELECT r.id FROM public.records r",
		},
		{
			name: "correlation stays distinct from local alias",
			current: `SELECT r.id FROM records r WHERE EXISTS (
				SELECT 1 FROM links l WHERE l.record_id = r.id)`,
			bound: `SELECT r.id FROM public.records r WHERE EXISTS (
				SELECT 1 FROM public.links l WHERE l.record_id = r.id)`,
			desired: `SELECT r.id FROM public.records r WHERE EXISTS (
				SELECT 1 FROM public.links l WHERE l.record_id = l.id)`,
		},
		{
			name:    "explicit type namespace remains distinct",
			current: "SELECT r.value FROM records r",
			bound:   "SELECT r.value FROM public.records r",
			desired: "SELECT r.value::public.text FROM public.records r",
		},
		{
			name:    "explicit operator namespace remains distinct",
			current: "SELECT r.id = 0 AS valid FROM records r",
			bound:   "SELECT r.id = 0 AS valid FROM public.records r",
			desired: "SELECT r.id OPERATOR(public.=) 0 AS valid FROM public.records r",
		},
		{
			name:    "builtin column cast changes the typed result",
			current: "SELECT r.id FROM records r",
			bound:   "SELECT r.id FROM public.records r",
			desired: "SELECT r.id::pg_catalog.text FROM public.records r",
		},
		{
			name:    "qualified similar escape remains distinct",
			current: "SELECT r.value SIMILAR TO 'a%' AS valid FROM records r",
			bound:   "SELECT r.value SIMILAR TO 'a%' AS valid FROM public.records r",
			desired: "SELECT r.value ~ public.similar_to_escape('a%') AS valid FROM public.records r",
		},
		{
			name:    "qualified unknown cast remains distinct",
			current: "SELECT NULL AS value",
			bound:   "SELECT NULL AS value",
			desired: "SELECT NULL::public.unknown AS value",
		},
		{
			name:    "raw literal cast does not hide a type shadow",
			current: "SELECT 'ready'::text AS value",
			bound:   "SELECT 'ready'::public.text AS value",
			desired: "SELECT 'ready' AS value",
		},
		{
			name:    "literal cast preserves its result type",
			current: "SELECT 'ready' AS value",
			bound:   "SELECT 'ready' AS value",
			desired: "SELECT 'ready'::pg_catalog.varchar AS value",
		},
		{
			name:    "literal array cast stays distinct",
			current: "SELECT '{ready}' AS value",
			bound:   "SELECT '{ready}' AS value",
			desired: "SELECT '{ready}'::pg_catalog.text[] AS value",
		},
		{
			name:    "column type modifier stays distinct",
			current: "SELECT r.value FROM records r",
			bound:   "SELECT r.value FROM public.records r",
			desired: "SELECT r.value::pg_catalog.varchar(3) FROM public.records r",
		},
		{
			name:    "quoted builtin type case stays distinct",
			current: "SELECT 'ready' AS value",
			bound:   "SELECT 'ready' AS value",
			desired: `SELECT 'ready'::pg_catalog."Text" AS value`,
		},
		{
			name:    "source typed NULL changes the CASE result",
			current: "SELECT CASE WHEN true THEN '1' END AS value",
			bound:   "SELECT CASE WHEN true THEN '1'::text ELSE NULL::text END AS value",
			desired: "SELECT CASE WHEN true THEN '1' ELSE NULL::pg_catalog.int4 END AS value",
		},
		{
			name:    "catalog builtin LIKE text literal deparsing",
			current: "SELECT r.value ~~ 'a%'::text AS valid FROM records r",
			bound:   "SELECT r.value ~~ 'a%'::text AS valid FROM public.records r",
			desired: "SELECT r.value LIKE 'a%' AS valid FROM public.records r",
			equal:   true,
		},
		{
			name:    "catalog builtin membership deparsing",
			current: "SELECT r.value = ANY (ARRAY['a'::text, 'b'::text]) AS valid FROM records r",
			bound:   "SELECT r.value = ANY (ARRAY['a'::text, 'b'::text]) AS valid FROM public.records r",
			desired: "SELECT r.value IN ('a', 'b') AS valid FROM public.records r",
			equal:   true,
		},
		{
			name:    "catalog builtin similar escape deparsing",
			current: "SELECT r.value ~ similar_to_escape('a%'::text) AS valid FROM records r",
			bound:   "SELECT r.value ~ similar_to_escape('a%'::text) AS valid FROM public.records r",
			desired: "SELECT r.value SIMILAR TO 'a%' AS valid FROM public.records r",
			equal:   true,
		},
		{
			name:    "catalog builtin unknown NULL deparsing",
			current: "SELECT NULL::unknown AS value",
			bound:   "SELECT NULL::unknown AS value",
			desired: "SELECT NULL AS value",
			equal:   true,
		},
		{
			name:    "qualified builtin text literal stays equivalent",
			current: "SELECT 'ready'::pg_catalog.text AS value",
			bound:   "SELECT 'ready'::pg_catalog.text AS value",
			desired: "SELECT 'ready' AS value",
			equal:   true,
		},
		{
			name:    "catalog builtin implicit CASE default deparsing",
			current: "SELECT CASE WHEN r.id > 0 THEN r.value ELSE NULL::text END AS value FROM records r",
			bound:   "SELECT CASE WHEN r.id > 0 THEN r.value ELSE NULL::text END AS value FROM public.records r",
			desired: "SELECT CASE WHEN r.id > 0 THEN r.value END AS value FROM public.records r",
			equal:   true,
		},
		{
			name:    "raw similar escape does not hide a function shadow",
			current: "SELECT r.value ~ similar_to_escape('a%') AS valid FROM records r",
			bound:   "SELECT r.value ~ public.similar_to_escape('a%') AS valid FROM public.records r",
			desired: "SELECT r.value SIMILAR TO 'a%' AS valid FROM public.records r",
		},
		{
			name:         "catalog CASE arms preserve result coercions",
			current:      "SELECT CASE WHEN true THEN 1 ELSE NULL::int4 END AS value",
			bound:        "SELECT CASE WHEN true THEN 1 ELSE NULL::int4 END AS value",
			desired:      "SELECT CASE WHEN true THEN 1::int8 ELSE NULL::int8 END AS value",
			desiredBound: "SELECT CASE WHEN true THEN 1::int8 ELSE NULL::int8 END AS value",
		},
		{
			name:    "catalog CASE default preserves a domain cast",
			current: "SELECT CASE WHEN true THEN 'ready' ELSE NULL::public.text END AS value",
			bound:   "SELECT CASE WHEN true THEN 'ready' ELSE NULL::public.text END AS value",
			desired: "SELECT CASE WHEN true THEN 'ready' END AS value",
		},
		{
			name:     "editing the presentation invalidates catalog evidence",
			current:  "SELECT r.id FROM public.changed r",
			original: "SELECT r.id FROM records r",
			bound:    "SELECT r.id FROM public.records r",
			desired:  "SELECT r.id FROM public.records r",
		},
	}

	assertViewCatalogCases(t, tests)
}

func TestViewCatalogCastEvidence(t *testing.T) {
	t.Parallel()

	tests := []catalogViewCase{
		{
			name:    "catalog builtin function argument text deparsing",
			current: "SELECT encode(r.payload, 'hex'::text) AS value FROM records r",
			bound:   "SELECT encode(r.payload, 'hex'::text) AS value FROM public.records r",
			desired: "SELECT ENCODE(r.payload, 'hex') AS value FROM records r",
			equal:   true,
		},
		{
			name:    "catalog literal evidence keeps qualified builtin presentation",
			current: "SELECT pg_catalog.encode(r.payload, 'hex'::pg_catalog.text) AS value FROM records r",
			bound:   "SELECT encode(r.payload, 'hex'::text) AS value FROM public.records r",
			desired: "SELECT pg_catalog.encode(r.payload, 'hex') AS value FROM records r",
			equal:   true,
		},
		{
			name:    "catalog literal evidence accepts qualified desired type",
			current: "SELECT encode(r.payload, 'hex'::text) AS value FROM records r",
			bound:   "SELECT encode(r.payload, 'hex'::text) AS value FROM public.records r",
			desired: "SELECT encode(r.payload, 'hex'::pg_catalog.text) AS value FROM records r",
			equal:   true,
		},
		{
			name:    "catalog literal evidence cannot move between identical constants",
			current: "SELECT 'ready'::text AS first, 'ready'::text AS second",
			bound:   "SELECT 'ready'::public.text AS first, 'ready'::text AS second",
			desired: "SELECT 'ready' AS first, 'ready'::text AS second",
		},
		{
			name:    "catalog literal evidence requires the same literal",
			current: "SELECT 'ready'::text AS value",
			bound:   "SELECT 'changed'::text AS value",
			desired: "SELECT 'ready' AS value",
		},
		{
			name:    "catalog literal evidence requires the same expression",
			current: "SELECT 'ready'::text AS value",
			bound:   "SELECT lower('ready'::text) AS value",
			desired: "SELECT 'ready' AS value",
		},
		{
			name:    "catalog literal evidence cannot move between reordered nodes",
			current: "SELECT 'ready'::text AS first, 'other'::text AS second",
			bound:   "SELECT 'other'::text AS first, 'ready'::text AS second",
			desired: "SELECT 'ready' AS first, 'other' AS second",
		},
		{
			name:    "catalog CASE evidence preserves a resolved domain",
			current: "SELECT CASE WHEN true THEN 'ready' ELSE NULL::text END AS value",
			bound:   "SELECT CASE WHEN true THEN 'ready' ELSE NULL::public.text END AS value",
			desired: "SELECT CASE WHEN true THEN 'ready' END AS value",
		},
		{
			name:    "catalog CASE evidence requires matching result arms",
			current: "SELECT CASE WHEN true THEN 'ready' ELSE NULL::text END AS value",
			bound:   "SELECT CASE WHEN true THEN 'changed' ELSE NULL::text END AS value",
			desired: "SELECT CASE WHEN true THEN 'ready' END AS value",
		},
		{
			name: "catalog builtin joined CASE default deparsing",
			current: `SELECT r.id FROM records r LEFT JOIN records linked ON linked.value =
				CASE WHEN r.id = 1 THEN r.value WHEN r.id = 2 THEN linked.value ELSE NULL::text END`,
			bound: `SELECT r.id FROM public.records r LEFT JOIN public.records linked ON linked.value =
				CASE WHEN r.id = 1 THEN r.value WHEN r.id = 2 THEN linked.value ELSE NULL::text END`,
			desired: `SELECT r.id FROM records r LEFT JOIN records linked ON linked.value =
				CASE WHEN r.id = 1 THEN r.value WHEN r.id = 2 THEN linked.value END`,
			equal: true,
		},
		{
			name:    "catalog CASE evidence accepts builtin type aliases",
			current: "SELECT CASE WHEN r.id > 0 THEN r.id ELSE NULL::integer END AS value FROM records r",
			bound:   "SELECT CASE WHEN r.id > 0 THEN r.id ELSE NULL::pg_catalog.int4 END AS value FROM public.records r",
			desired: "SELECT CASE WHEN r.id > 0 THEN r.id END AS value FROM records r",
			equal:   true,
		},
		{
			name:    "catalog CASE evidence rejects a different result type",
			current: "SELECT CASE WHEN r.id > 0 THEN r.id ELSE NULL::integer END AS value FROM records r",
			bound:   "SELECT CASE WHEN r.id > 0 THEN r.id ELSE NULL::pg_catalog.int8 END AS value FROM public.records r",
			desired: "SELECT CASE WHEN r.id > 0 THEN r.id END AS value FROM records r",
		},
	}

	assertViewCatalogCases(t, tests)
}

func assertViewCatalogCases(t *testing.T, tests []catalogViewCase) {
	t.Helper()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			original := test.original
			if original == "" {
				original = test.current
			}

			current := schema.View{
				Schema: "public", Name: "visible", Definition: test.current,
				CatalogDefinition: &schema.CatalogViewDefinition{
					Original: original, Definition: test.bound,
				},
			}

			desired := schema.View{Schema: "public", Name: "visible", Definition: test.desired}

			if test.desiredBound != "" {
				desired.CatalogDefinition = &schema.CatalogViewDefinition{
					Original: test.desired, Definition: test.desiredBound,
				}
			}

			comparison := differ.NewViewComparator(differ.DefaultOptions())
			assert.Equal(t, test.equal, comparison.AreEqual(current, desired))
			assert.Equal(t, test.equal, comparison.AreEqual(desired, current))
		})
	}
}
