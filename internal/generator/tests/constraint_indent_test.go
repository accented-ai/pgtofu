package generator_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDDLBuilder_ConstraintIndentation_GroupedOR(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "status", DataType: "text", IsNullable: false, Position: 2},
					{Name: "reason", DataType: "text", IsNullable: false, Position: 3},
				},
				Constraints: []schema.Constraint{
					{
						Name: "items_status_reason_check",
						Type: "CHECK",
						Definition: "CHECK ((\n" +
							"        status IN ('pending', 'generated')\n" +
							"        AND reason = ''\n" +
							"    )\n" +
							"    OR (\n" +
							"        status IN ('skipped', 'failed')\n" +
							"        AND reason <> ''\n" +
							"    ))",
						CheckExpression: "((\n" +
							"        status IN ('pending', 'generated')\n" +
							"        AND reason = ''\n" +
							"    )\n" +
							"    OR (\n" +
							"        status IN ('skipped', 'failed')\n" +
							"        AND reason <> ''\n" +
							"    ))",
					},
				},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: "public.items"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	expected := "CREATE TABLE public.items (\n" +
		"    id UUID NOT NULL,\n" +
		"    status TEXT NOT NULL,\n" +
		"    reason TEXT NOT NULL,\n" +
		"    CONSTRAINT items_status_reason_check CHECK ((\n" +
		"        status IN ('pending', 'generated')\n" +
		"        AND reason = ''\n" +
		"    )\n" +
		"    OR (\n" +
		"        status IN ('skipped', 'failed')\n" +
		"        AND reason <> ''\n" +
		"    ))\n" +
		");"
	require.Equal(t, expected, stmt.SQL)
}

func TestDDLBuilder_ConstraintIndentation_FlatOR(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "source", DataType: "text", IsNullable: true, Position: 2},
					{Name: "version", DataType: "text", IsNullable: true, Position: 3},
				},
				Constraints: []schema.Constraint{
					{
						Name: "items_source_check",
						Type: "CHECK",
						Definition: "CHECK (\n" +
							"        source IS NULL\n" +
							"        OR source = 'manual'\n" +
							"        OR (\n" +
							"            source = 'auto'\n" +
							"            AND version IS NOT NULL\n" +
							"            AND version <> ''\n" +
							"        )\n" +
							"    )",
						CheckExpression: "(\n" +
							"        source IS NULL\n" +
							"        OR source = 'manual'\n" +
							"        OR (\n" +
							"            source = 'auto'\n" +
							"            AND version IS NOT NULL\n" +
							"            AND version <> ''\n" +
							"        )\n" +
							"    )",
					},
				},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: "public.items"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	expected := "CREATE TABLE public.items (\n" +
		"    id UUID NOT NULL,\n" +
		"    source TEXT,\n" +
		"    version TEXT,\n" +
		"    CONSTRAINT items_source_check CHECK (\n" +
		"        source IS NULL\n" +
		"        OR source = 'manual'\n" +
		"        OR (\n" +
		"            source = 'auto'\n" +
		"            AND version IS NOT NULL\n" +
		"            AND version <> ''\n" +
		"        )\n" +
		"    )\n" +
		");"
	require.Equal(t, expected, stmt.SQL)
}
