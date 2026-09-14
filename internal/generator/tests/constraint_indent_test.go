package generator_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDDLBuilder_LongConstraintDefinitionsWrapForLinters(t *testing.T) {
	t.Parallel()

	columns := []schema.Column{
		{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
		{Name: "account_id", DataType: "uuid", IsNullable: false, Position: 2},
		{Name: "parent_record_id", DataType: "uuid", IsNullable: false, Position: 3},
		{Name: "catalog_partition", DataType: "text", IsNullable: false, Position: 4},
		{Name: "external_reference", DataType: "text", IsNullable: false, Position: 5},
		{Name: "state", DataType: "text", IsNullable: false, Position: 6},
		{Name: "regional_catalog_namespace", DataType: "text", IsNullable: false, Position: 7},
		{Name: "source_system_identifier", DataType: "text", IsNullable: false, Position: 8},
	}
	table := schema.Table{
		Schema:  "reporting",
		Name:    "records",
		Columns: columns,
		Constraints: []schema.Constraint{
			{
				Name:              "records_parent_record_assignment_identifier_fkey",
				Type:              "FOREIGN KEY",
				Columns:           []string{"account_id", "parent_record_id"},
				ReferencedSchema:  "reporting",
				ReferencedTable:   "parent_records_with_extended_retention_metadata",
				ReferencedColumns: []string{"account_id", "id"},
				OnDelete:          "RESTRICT",
			},
			{
				Name: "records_catalog_partition_external_reference_unique",
				Type: "UNIQUE",
				Columns: []string{
					"account_id",
					"parent_record_id",
					"catalog_partition",
					"external_reference",
					"regional_catalog_namespace",
					"source_system_identifier",
				},
			},
			{
				Name: "records_state_and_external_reference_consistency_check",
				Type: "CHECK",
				Definition: "CHECK (((state = 'pending_review') AND " +
					"(external_reference IS NOT NULL) AND (catalog_partition IS NOT NULL)) OR " +
					"((state = 'archived_after_manual_review') AND " +
					"(external_reference IS NOT NULL) AND (catalog_partition IS NOT NULL)))",
			},
		},
	}
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{Tables: []schema.Table{table}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddTable,
			ObjectName: differ.TableKey(table.Schema, table.Name),
		}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	require.Contains(t, statement.SQL, "FOREIGN KEY (\n")
	require.Contains(t, statement.SQL, "UNIQUE (\n")
	require.Contains(t, statement.SQL, "CHECK (\n")
	require.Contains(t, statement.SQL, "state = 'pending_review'")
	require.NotContains(t, statement.SQL, "STATE =")
	assertGeneratedSQLLineLengths(t, statement.SQL)

	constraint := schema.Constraint{
		Name: "records_extended_state_transition_policy_check",
		Type: "CHECK",
		Definition: "CHECK (state = " +
			"'ready_after_all_required_external_catalog_validations_have_completed_successfully')",
	}
	result = &differ.DiffResult{
		Current: &schema.Database{Tables: []schema.Table{{
			Schema: table.Schema,
			Name:   table.Name,
		}}},
		Desired: &schema.Database{Tables: []schema.Table{{
			Schema:      table.Schema,
			Name:        table.Name,
			Constraints: []schema.Constraint{constraint},
		}}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddConstraint,
			ObjectName: differ.TableKey(table.Schema, table.Name),
			Details: map[string]any{
				"table":      table.QualifiedName(),
				"constraint": &constraint,
			},
		}},
	}

	statement, err = generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)
	require.Contains(t, statement.SQL, "CHECK (\n")
	assertGeneratedSQLLineLengths(t, statement.SQL)
}

func assertGeneratedSQLLineLengths(t *testing.T, sql string) {
	t.Helper()

	for line := range strings.SplitSeq(sql, "\n") {
		require.LessOrEqual(t, utf8.RuneCountInString(line), 170, line)
	}
}

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

func TestDDLBuilder_ConstraintIndentation_FirstLinePredicate(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "score", DataType: "double precision", IsNullable: true, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name: "items_score_check",
						Type: "CHECK",
						Definition: "CHECK (score IS NULL\n" +
							"        OR (score >= 0 AND score <= 1))",
						CheckExpression: "score IS NULL\n" +
							"        OR (score >= 0 AND score <= 1)",
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
		"    score DOUBLE PRECISION,\n" +
		"    CONSTRAINT items_score_check CHECK (\n" +
		"        score IS NULL\n" +
		"        OR (score >= 0 AND score <= 1)\n" +
		"    )\n" +
		");"
	require.Equal(t, expected, stmt.SQL)
}

func TestDDLBuilder_ConstraintIndentation_FlatORNestedGroups(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "source_type", DataType: "text", IsNullable: true, Position: 1},
					{Name: "source_version", DataType: "text", IsNullable: true, Position: 2},
					{Name: "group_id", DataType: "uuid", IsNullable: true, Position: 3},
				},
				Constraints: []schema.Constraint{
					{
						Name: "items_source_type_check",
						Type: "CHECK",
						Definition: "CHECK (source_type IS NULL\n" +
							"        OR source_type = 'manual'\n" +
							"    OR (\n" +
							"        source_type = 'automated'\n" +
							"        AND source_version IS NOT NULL\n" +
							"        AND source_version <> ''\n" +
							"    )\n" +
							"    OR (\n" +
							"        source_type = 'grouped'\n" +
							"        AND group_id IS NOT NULL\n" +
							"    ))",
						CheckExpression: "source_type IS NULL\n" +
							"        OR source_type = 'manual'\n" +
							"    OR (\n" +
							"        source_type = 'automated'\n" +
							"        AND source_version IS NOT NULL\n" +
							"        AND source_version <> ''\n" +
							"    )\n" +
							"    OR (\n" +
							"        source_type = 'grouped'\n" +
							"        AND group_id IS NOT NULL\n" +
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
		"    source_type TEXT,\n" +
		"    source_version TEXT,\n" +
		"    group_id UUID,\n" +
		"    CONSTRAINT items_source_type_check CHECK (\n" +
		"        source_type IS NULL\n" +
		"        OR source_type = 'manual'\n" +
		"        OR (\n" +
		"            source_type = 'automated'\n" +
		"            AND source_version IS NOT NULL\n" +
		"            AND source_version <> ''\n" +
		"        )\n" +
		"        OR (\n" +
		"            source_type = 'grouped'\n" +
		"            AND group_id IS NOT NULL\n" +
		"        )\n" +
		"    )\n" +
		");"
	require.Equal(t, expected, stmt.SQL)
}

func TestDDLBuilder_ConstraintIndentation_InListFirstLine(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "category", DataType: "text", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{
						Name: "items_category_check",
						Type: "CHECK",
						Definition: "CHECK (category IN (\n" +
							"        '',\n" +
							"        'alpha'\n" +
							"    ))",
						CheckExpression: "category IN (\n" +
							"        '',\n" +
							"        'alpha'\n" +
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
		"    category TEXT NOT NULL,\n" +
		"    CONSTRAINT items_category_check CHECK (category IN (\n" +
		"        '',\n" +
		"        'alpha'\n" +
		"    ))\n" +
		");"
	require.Equal(t, expected, stmt.SQL)
}

func TestDDLBuilder_ConstraintIndentation_MultilineFunctionCall(t *testing.T) {
	t.Parallel()

	constraint := schema.Constraint{
		Name: "records_source_check",
		Type: schema.ConstraintCheck,
		Definition: "CHECK (REQUIRE_ONE(\n" +
			"    source_id,\n" +
			"    import_id,\n" +
			"    run_id\n" +
			") = 1)",
		CheckExpression: "REQUIRE_ONE(\n" +
			"    source_id,\n" +
			"    import_id,\n" +
			"    run_id\n" +
			") = 1",
	}
	table := schema.Table{
		Schema: "reporting",
		Name:   "records",
		Columns: []schema.Column{
			{Name: "source_id", DataType: "uuid", IsNullable: true, Position: 1},
			{Name: "import_id", DataType: "uuid", IsNullable: true, Position: 2},
			{Name: "run_id", DataType: "uuid", IsNullable: true, Position: 3},
		},
		Constraints: []schema.Constraint{constraint},
	}
	result := &differ.DiffResult{
		Current: &schema.Database{Tables: []schema.Table{{
			Schema:  table.Schema,
			Name:    table.Name,
			Columns: table.Columns,
		}}},
		Desired: &schema.Database{Tables: []schema.Table{table}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddConstraint,
			ObjectName: differ.TableKey(table.Schema, table.Name),
			Details: map[string]any{
				"table":      table.QualifiedName(),
				"constraint": &constraint,
			},
		}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	expected := "ALTER TABLE reporting.records ADD CONSTRAINT records_source_check CHECK (\n" +
		"    REQUIRE_ONE(\n" +
		"        source_id,\n" +
		"        import_id,\n" +
		"        run_id\n" +
		"    )\n" +
		"    = 1\n" +
		");"
	require.Equal(t, expected, statement.SQL)
}

func TestDDLBuilder_ConstraintIndentation_DownMigrationExtractedCheck(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "state", DataType: "text", IsNullable: true, Position: 2},
					{Name: "source", DataType: "text", IsNullable: true, Position: 3},
					{Name: "source_version", DataType: "text", IsNullable: true, Position: 4},
					{Name: "score", DataType: "double precision", IsNullable: true, Position: 5},
					{Name: "metadata", DataType: "jsonb", IsNullable: false, Position: 6},
					{
						Name:       "item_ids",
						DataType:   "uuid",
						IsArray:    true,
						IsNullable: false,
						Position:   7,
					},
				},
				Constraints: []schema.Constraint{
					{Name: "items_pkey", Type: "PRIMARY KEY", Columns: []string{"id"}},
					{
						Name: "items_state_rule_check",
						Type: "CHECK",
						Definition: "CHECK ((((state = 'pending') AND (source IS NULL) " +
							"AND (source_version IS NULL) AND (score IS NULL)) OR " +
							"((state = 'active') AND (source IS NOT NULL) " +
							"AND (source_version IS NOT NULL) AND (score IS NOT NULL))))",
					},
					{
						Name: "items_source_check",
						Type: "CHECK",
						Definition: "CHECK (((source IS NULL) OR (source = 'manual') OR " +
							"((source = 'automated') AND (source_version IS NOT NULL))))",
					},
					{
						Name:       "items_metadata_check",
						Type:       "CHECK",
						Definition: "CHECK ((jsonb_typeof(metadata) = 'object'))",
					},
					{
						Name:       "items_item_ids_check",
						Type:       "CHECK",
						Definition: "CHECK ((cardinality(item_ids) > 0))",
					},
				},
			},
		},
	}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeDropTable, ObjectName: "public.items"},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])
	require.NoError(t, err)

	expected := "CREATE TABLE public.items (\n" +
		"    id UUID NOT NULL,\n" +
		"    state TEXT,\n" +
		"    source TEXT,\n" +
		"    source_version TEXT,\n" +
		"    score DOUBLE PRECISION,\n" +
		"    metadata JSONB NOT NULL,\n" +
		"    item_ids UUID[] NOT NULL,\n" +
		"    CONSTRAINT items_pkey PRIMARY KEY (id),\n" +
		"    CONSTRAINT items_state_rule_check CHECK (\n" +
		"        (\n" +
		"            ((state = 'pending') AND (source IS NULL) AND (source_version IS NULL) AND (score IS NULL))\n" +
		"            OR ((state = 'active') AND (source IS NOT NULL) " +
		"AND (source_version IS NOT NULL) AND (score IS NOT NULL))\n" +
		"        )\n" +
		"    ),\n" +
		"    CONSTRAINT items_source_check CHECK (((source IS NULL) OR (source = 'manual') OR " +
		"((source = 'automated') AND (source_version IS NOT NULL)))),\n" +
		"    CONSTRAINT items_metadata_check CHECK ((JSONB_TYPEOF(metadata) = 'object')),\n" +
		"    CONSTRAINT items_item_ids_check CHECK ((CARDINALITY(item_ids) > 0))\n" +
		");"
	require.Equal(t, expected, stmt.SQL)
}

func TestDDLBuilder_JSONBCheckPreservesConjoinedPredicates(t *testing.T) {
	t.Parallel()

	constraint := schema.Constraint{
		Name: "records_policy_check",
		Type: schema.ConstraintCheck,
		Definition: `CHECK (
    JSONB_TYPEOF(policy) = 'object'
    AND policy ->> 'version' = 'reporting_policy.v1'
    AND policy -> 'enabled' = 'false'::JSONB
    AND policy -> 'automatic' = 'true'::JSONB
    AND policy ->> 'visibility' = 'internal'
)`,
	}
	table := schema.Table{
		Schema: "reporting",
		Name:   "records",
		Columns: []schema.Column{
			{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
			{Name: "policy", DataType: "jsonb", IsNullable: false, Position: 2},
		},
		Constraints: []schema.Constraint{constraint},
	}
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{Tables: []schema.Table{table}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddTable,
			ObjectName: differ.TableKey(table.Schema, table.Name),
		}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)
	require.Contains(t, statement.SQL, "AND policy -> 'enabled' = 'false'")
	require.Contains(t, statement.SQL, "AND policy -> 'automatic' = 'true'")
	require.Contains(t, statement.SQL, "AND policy ->> 'visibility' = 'internal'")
	require.NotContains(t, statement.SQL, "'FALSE'")
	require.NotContains(t, statement.SQL, "'TRUE'")

	_, err = pgquery.Parse(statement.SQL)
	require.NoError(t, err)
}

func TestDDLBuilder_ConstraintIndentation_InListWithLogicalContinuation(t *testing.T) {
	t.Parallel()

	constraint := schema.Constraint{
		Name: "records_state_check",
		Type: schema.ConstraintCheck,
		Definition: `CHECK (state NOT IN (
    'approved',
    'rejected',
    'archived'
)
OR decided_at IS NOT NULL)`,
	}
	table := schema.Table{
		Schema: "reporting",
		Name:   "records",
		Columns: []schema.Column{
			{Name: "state", DataType: "text", IsNullable: false, Position: 1},
			{Name: "decided_at", DataType: "timestamptz", IsNullable: true, Position: 2},
		},
		Constraints: []schema.Constraint{constraint},
	}
	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{Tables: []schema.Table{table}},
		Changes: []differ.Change{{
			Type:       differ.ChangeTypeAddTable,
			ObjectName: differ.TableKey(table.Schema, table.Name),
		}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	expected := `CREATE TABLE reporting.records (
    state TEXT NOT NULL,
    decided_at TIMESTAMPTZ,
    CONSTRAINT records_state_check CHECK (
        state NOT IN (
            'approved',
            'rejected',
            'archived'
        )
        OR decided_at IS NOT NULL
    )
);`
	require.Equal(t, expected, statement.SQL)
}
