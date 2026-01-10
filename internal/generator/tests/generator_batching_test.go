package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestBatchingKeepsSingleTableChangesTogether(t *testing.T) {
	t.Parallel()

	currentTable := schema.Table{
		Schema: schema.DefaultSchema,
		Name:   "job_progress",
		Columns: []schema.Column{
			{Name: "provider", DataType: "VARCHAR(50)", IsNullable: false, Position: 1},
			{Name: "job_name", DataType: "VARCHAR(50)", IsNullable: false, Position: 2},
			{Name: "job_type", DataType: "VARCHAR(50)", IsNullable: false, Position: 3},
			{Name: "last_item_id", DataType: "BIGINT", IsNullable: true, Position: 4},
			{Name: "last_item_time", DataType: "TIMESTAMPTZ", IsNullable: true, Position: 5},
			{Name: "last_batch_id", DataType: "BIGINT", IsNullable: true, Position: 6},
			{Name: "total_processed", DataType: "BIGINT", IsNullable: true, Position: 7},
			{Name: "batch_size", DataType: "INTEGER", IsNullable: true, Position: 8},
			{Name: "last_error", DataType: "TEXT", IsNullable: true, Position: 9},
			{Name: "error_count", DataType: "INTEGER", IsNullable: true, Position: 10},
			{Name: "created_at", DataType: "TIMESTAMPTZ", IsNullable: false, Position: 11},
			{Name: "updated_at", DataType: "TIMESTAMPTZ", IsNullable: false, Position: 12},
		},
		Constraints: []schema.Constraint{
			{
				Name:    "job_progress_pkey",
				Type:    "PRIMARY KEY",
				Columns: []string{"provider", "job_name", "job_type"},
			},
		},
		Indexes: []schema.Index{
			{
				Schema:    schema.DefaultSchema,
				Name:      "idx_job_progress_provider_type",
				TableName: "job_progress",
				Columns:   []string{"provider", "job_type"},
			},
		},
		Comment: "Tracks job progress",
	}

	currentView := schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "job_status",
		Definition: "SELECT provider, job_name, job_type, last_item_id FROM job_progress",
		Comment:    "Monitoring view for job health",
	}

	desiredTable := schema.Table{
		Schema: schema.DefaultSchema,
		Name:   "job_progress",
		Columns: []schema.Column{
			{Name: "source", DataType: "VARCHAR(50)", IsNullable: false, Position: 1},
			{Name: "job_name", DataType: "VARCHAR(50)", IsNullable: false, Position: 2},
			{Name: "job_type", DataType: "VARCHAR(20)", IsNullable: false, Position: 3},
			{Name: "last_run_time", DataType: "TIMESTAMPTZ", IsNullable: true, Position: 4},
			{Name: "last_source_id", DataType: "BIGINT", IsNullable: true, Position: 5},
			{Name: "total_runs", DataType: "BIGINT", IsNullable: true, Position: 6},
			{Name: "batch_size", DataType: "INTEGER", IsNullable: true, Position: 7},
			{Name: "last_error", DataType: "TEXT", IsNullable: true, Position: 8},
			{Name: "error_count", DataType: "INTEGER", IsNullable: true, Position: 9},
			{Name: "created_at", DataType: "TIMESTAMPTZ", IsNullable: false, Position: 10},
			{Name: "updated_at", DataType: "TIMESTAMPTZ", IsNullable: false, Position: 11},
		},
		Constraints: []schema.Constraint{
			{
				Name:    "job_progress_pkey_new",
				Type:    "PRIMARY KEY",
				Columns: []string{"source", "job_name", "job_type"},
			},
		},
		Indexes: []schema.Index{
			{
				Schema:    schema.DefaultSchema,
				Name:      "idx_job_progress_source_type",
				TableName: "job_progress",
				Columns:   []string{"source", "job_type"},
			},
		},
		Comment: "Unified job progress tracking",
	}

	desiredView := schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "job_status",
		Definition: "SELECT source, job_name, job_type, last_run_time FROM job_progress",
		Comment:    "Monitoring view for job health",
	}

	current := &schema.Database{
		Tables: []schema.Table{currentTable},
		Views:  []schema.View{currentView},
	}

	desired := &schema.Database{
		Tables: []schema.Table{desiredTable},
		Views:  []schema.View{desiredView},
	}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	require.Greater(t, len(diffResult.Changes), 15,
		"expected more than 15 changes to test batching behavior")

	opts := testOptions()
	opts.MaxOperationsPerFile = 15
	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)

	assert.Len(t, genResult.Migrations, 1,
		"all changes to a single table should be kept in one migration file")
}

func TestBatchingRespectsViewDependencies(t *testing.T) {
	t.Parallel()

	currentTable := schema.Table{
		Schema: schema.DefaultSchema,
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", DataType: "INTEGER", IsNullable: false, Position: 1},
			{Name: "name", DataType: "VARCHAR(50)", IsNullable: false, Position: 2},
			{Name: "status", DataType: "VARCHAR(20)", IsNullable: true, Position: 3},
		},
	}

	currentView := schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "active_users",
		Definition: "SELECT id, name FROM users WHERE status = 'active'",
		Comment:    "Active users view",
	}

	desiredColumns := []schema.Column{
		{Name: "id", DataType: "INTEGER", IsNullable: false, Position: 1},
		{Name: "name", DataType: "VARCHAR(100)", IsNullable: false, Position: 2},
		{Name: "status", DataType: "VARCHAR(50)", IsNullable: true, Position: 3},
	}

	for i := 4; i <= 23; i++ {
		desiredColumns = append(desiredColumns, schema.Column{
			Name:       "field" + string(rune('a'+i-4)),
			DataType:   "TEXT",
			IsNullable: true,
			Position:   i,
		})
	}

	desiredTable := schema.Table{
		Schema:  schema.DefaultSchema,
		Name:    "users",
		Columns: desiredColumns,
	}

	desiredView := schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "active_users",
		Definition: "SELECT id, name FROM users WHERE status = 'active'",
		Comment:    "Active users view",
	}

	current := &schema.Database{
		Tables: []schema.Table{currentTable},
		Views:  []schema.View{currentView},
	}

	desired := &schema.Database{
		Tables: []schema.Table{desiredTable},
		Views:  []schema.View{desiredView},
	}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.MaxOperationsPerFile = 15
	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)

	require.NotEmpty(t, genResult.Migrations)

	firstMigration := genResult.Migrations[0].UpFile.Content

	hasDropView := contains(firstMigration, "DROP VIEW")
	hasAlterColumn := contains(firstMigration, "ALTER COLUMN")
	hasType := contains(firstMigration, "TYPE")
	hasColumnTypeChange := hasAlterColumn && hasType

	if hasColumnTypeChange && hasDropView {
		hasCreateView := contains(firstMigration, "CREATE VIEW")
		hasCreateOrReplace := contains(firstMigration, "CREATE OR REPLACE VIEW")
		assert.True(t, hasCreateView || hasCreateOrReplace,
			"view dropped for recreation should be recreated in the same migration")
	}
}

func TestBatchingKeepsConstraintChangesTogetherWithTable(t *testing.T) {
	t.Parallel()

	currentTable := schema.Table{
		Schema: schema.DefaultSchema,
		Name:   "items",
		Columns: []schema.Column{
			{Name: "old_key", DataType: "VARCHAR(50)", IsNullable: false, Position: 1},
			{Name: "category", DataType: "VARCHAR(50)", IsNullable: false, Position: 2},
			{Name: "item_type", DataType: "VARCHAR(50)", IsNullable: false, Position: 3},
		},
		Constraints: []schema.Constraint{{
			Name:    "items_pkey",
			Type:    "PRIMARY KEY",
			Columns: []string{"old_key", "category", "item_type"},
		}},
	}

	desiredColumns := []schema.Column{
		{Name: "new_key", DataType: "VARCHAR(50)", IsNullable: false, Position: 1},
		{Name: "category", DataType: "VARCHAR(50)", IsNullable: false, Position: 2},
		{Name: "item_type", DataType: "VARCHAR(50)", IsNullable: false, Position: 3},
	}

	for i := 4; i <= 23; i++ {
		desiredColumns = append(desiredColumns, schema.Column{
			Name:       "extra_col" + string(rune('a'+i-4)),
			DataType:   "TEXT",
			IsNullable: true,
			Position:   i,
		})
	}

	desiredTable := schema.Table{
		Schema:  schema.DefaultSchema,
		Name:    "items",
		Columns: desiredColumns,
		Constraints: []schema.Constraint{{
			Name:    "items_pkey_new",
			Type:    "PRIMARY KEY",
			Columns: []string{"new_key", "category", "item_type"},
		}},
	}

	current := &schema.Database{Tables: []schema.Table{currentTable}}
	desired := &schema.Database{Tables: []schema.Table{desiredTable}}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.MaxOperationsPerFile = 15
	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)

	var sb strings.Builder

	for _, m := range genResult.Migrations {
		if m.UpFile != nil {
			sb.WriteString(m.UpFile.Content)
		}
	}

	allMigrations := sb.String()

	hasDropConstraint := contains(allMigrations, "DROP CONSTRAINT")
	hasAddConstraint := contains(allMigrations, "ADD CONSTRAINT")

	if hasDropConstraint && hasAddConstraint {
		firstMigration := genResult.Migrations[0].UpFile.Content
		hasDrop := contains(firstMigration, "DROP CONSTRAINT")
		hasAdd := contains(firstMigration, "ADD CONSTRAINT")
		assert.True(t, hasDrop && hasAdd,
			"DROP and ADD CONSTRAINT should be in same migration")
	}
}

func TestBatchingKeepsIndexChangesTogetherWithColumnChanges(t *testing.T) {
	t.Parallel()

	currentTable := schema.Table{
		Schema: schema.DefaultSchema,
		Name:   "events",
		Columns: []schema.Column{
			{Name: "id", DataType: "INTEGER", IsNullable: false, Position: 1},
		},
		Comment: "Events table",
	}

	desiredColumns := []schema.Column{
		{Name: "id", DataType: "INTEGER", IsNullable: false, Position: 1},
		{Name: "category", DataType: "VARCHAR(50)", IsNullable: false, Position: 2},
	}

	for i := 3; i <= 22; i++ {
		desiredColumns = append(desiredColumns, schema.Column{
			Name:       "col" + string(rune('a'+i-3)),
			DataType:   "TEXT",
			IsNullable: true,
			Position:   i,
		})
	}

	desiredTable := schema.Table{
		Schema:  schema.DefaultSchema,
		Name:    "events",
		Columns: desiredColumns,
		Indexes: []schema.Index{
			{
				Schema:    schema.DefaultSchema,
				Name:      "idx_events_category",
				TableName: "events",
				Columns:   []string{"category"},
			},
		},
		Comment: "Events table",
	}

	current := &schema.Database{Tables: []schema.Table{currentTable}}
	desired := &schema.Database{Tables: []schema.Table{desiredTable}}

	d := differ.New(differ.DefaultOptions())
	diffResult, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.MaxOperationsPerFile = 15
	gen := generator.New(opts)

	genResult, err := gen.Generate(diffResult)
	require.NoError(t, err)

	var migrationWithCategoryColumn *generator.MigrationFile

	for _, m := range genResult.Migrations {
		if m.UpFile != nil && contains(m.UpFile.Content, "ADD COLUMN category") {
			migrationWithCategoryColumn = m.UpFile
			break
		}
	}

	if migrationWithCategoryColumn != nil {
		assert.True(t, contains(migrationWithCategoryColumn.Content, "CREATE INDEX") &&
			contains(migrationWithCategoryColumn.Content, "idx_events_category"),
			"index on new column should be in the same migration as the column addition")
	}
}
