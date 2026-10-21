package differ_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestCatalogDefaultIdentity(t *testing.T) {
	t.Parallel()

	snapshot := func(presentation, catalog string) *schema.Database {
		column := schema.Column{Name: "label", DataType: "text", Default: presentation}
		if catalog != "" {
			column.CatalogDefault = &schema.CatalogDefault{
				Original:   presentation,
				Expression: catalog,
			}
		}

		return &schema.Database{
			Tables: []schema.Table{
				{Schema: "public", Name: "notes", Columns: []schema.Column{column}},
			},
		}
	}

	for _, test := range []struct {
		name, presentation, current, desired, source string
		equal                                        bool
	}{
		{
			name: "qualified source", presentation: "label_value()", current: "public.label_value()",
			source: "PUBLIC.label_value()", equal: true,
		},
		{
			name: "other schema source", presentation: "label_value()", current: "public.label_value()",
			source: "other.label_value()",
		},
		{
			name: "same bare different bindings", presentation: "label_value()", current: "public.label_value()",
			desired: "other.label_value()", source: "label_value()",
		},
		{
			name: "quoted identifier case", presentation: `"LabelValue"()`, current: `public."LabelValue"()`,
			source: `public."labelvalue"()`,
		},
		{
			name: "qualified quoted identity", presentation: `other."LabelValue"()`,
			current: `other."LabelValue"()`, source: `other."labelvalue"()`,
		},
		{
			name: "qualified literal identity", presentation: "other.label_value('Mixed'::text)",
			current: "other.label_value('Mixed'::text)", source: "other.label_value('mixed'::text)",
		},
		{
			name: "bare decimal argument", presentation: "label_value(1.5)",
			current: "public.label_value(1.5)", source: "label_value(1.5)", equal: true,
		},
		{
			name: "bare dotted literal", presentation: "label_value('a.b')",
			current: "public.label_value('a.b'::text)", source: "label_value('a.b')", equal: true,
		},
		{
			name: "bare dotted identifier", presentation: `"label.value"()`,
			current: `public."label.value"()`, source: `"label.value"()`, equal: true,
		},
		{
			name: "literal case", presentation: "label_value('Mixed')", current: "public.label_value('Mixed'::text)",
			source: "public.label_value('mixed'::text)",
		},
		{
			name: "overload casts", presentation: "label_value('Mixed'::text)", current: "public.label_value('Mixed'::text)",
			source: "public.label_value('Mixed')",
		},
		{
			name: "drop stale canonical", presentation: "label_value()", current: "public.label_value()",
			desired: "public.label_value()",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			current := snapshot(test.presentation, test.current)
			desired := snapshot(test.source, test.desired)
			result, err := differ.New(nil).Compare(current, desired)
			require.NoError(t, err)

			if test.equal {
				assert.Empty(t, result.Changes)
			} else {
				require.Len(t, result.Changes, 1)
				assert.Equal(
					t,
					desired.Tables[0].Columns[0].DefaultSQL(),
					result.Changes[0].Details["new_default"],
				)
			}
		})
	}
}
