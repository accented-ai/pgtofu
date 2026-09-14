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

func TestDDLBuilder_ViewOperations(t *testing.T) {
	t.Parallel()

	view := &schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "active_users",
		Definition: "SELECT * FROM users WHERE active = true",
	}

	tests := []struct {
		name       string
		changeType differ.ChangeType
		wantSQL    string
		wantUnsafe bool
		useReplace bool
	}{
		{
			name:       "add view",
			changeType: differ.ChangeTypeAddView,
			wantSQL:    "CREATE VIEW",
			useReplace: false,
		},
		{
			name:       "modify view",
			changeType: differ.ChangeTypeModifyView,
			wantSQL:    "CREATE OR REPLACE VIEW",
			useReplace: true,
		},
		{
			name:       "drop view",
			changeType: differ.ChangeTypeDropView,
			wantSQL:    "DROP VIEW",
			wantUnsafe: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var current, desired *schema.Database
			if tt.changeType == differ.ChangeTypeDropView {
				current = &schema.Database{Views: []schema.View{*view}}
				desired = &schema.Database{}
			} else {
				current = &schema.Database{}
				desired = &schema.Database{Views: []schema.View{*view}}
			}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{{Type: tt.changeType, ObjectName: "public.active_users"}},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)
			assert.Contains(t, stmt.SQL, tt.wantSQL)
			assert.Equal(t, tt.wantUnsafe, stmt.IsUnsafe)
		})
	}
}

func TestDDLBuilder_ModifyViewRenamesOutputColumns(t *testing.T) {
	t.Parallel()

	currentView := schema.View{
		Schema: schema.DefaultSchema,
		Name:   "status_source",
		Definition: `SELECT
record_id AS legacy_record_id,
is_ready AS legacy_is_ready
FROM records`,
	}
	desiredView := schema.View{
		Schema: schema.DefaultSchema,
		Name:   "status_source",
		Definition: `SELECT
record_id AS source_record_id,
is_ready AS source_is_ready
FROM records`,
	}
	change := differ.Change{
		Type:       differ.ChangeTypeModifyView,
		ObjectName: "public.status_source",
	}
	result := &differ.DiffResult{
		Current: &schema.Database{Views: []schema.View{currentView}},
		Desired: &schema.Database{Views: []schema.View{desiredView}},
		Changes: []differ.Change{change},
	}
	builder := generator.NewDDLBuilder(result, true)

	up, err := builder.BuildUpStatement(change)
	require.NoError(t, err)
	down, err := builder.BuildDownStatement(change)
	require.NoError(t, err)

	assert.Contains(t, up.SQL,
		"ALTER VIEW public.status_source RENAME COLUMN legacy_record_id TO source_record_id;")
	assert.Contains(t, up.SQL,
		"ALTER VIEW public.status_source RENAME COLUMN legacy_is_ready TO source_is_ready;")
	assert.Less(t,
		strings.Index(up.SQL, "ALTER VIEW"),
		strings.Index(up.SQL, "CREATE OR REPLACE VIEW"),
	)
	assert.Contains(t, down.SQL,
		"ALTER VIEW public.status_source RENAME COLUMN source_record_id TO legacy_record_id;")
	assert.Contains(t, down.SQL,
		"ALTER VIEW public.status_source RENAME COLUMN source_is_ready TO legacy_is_ready;")
	assert.Less(t,
		strings.Index(down.SQL, "ALTER VIEW"),
		strings.Index(down.SQL, "CREATE OR REPLACE VIEW"),
	)
}

func TestDDLBuilder_ModifyViewRenamesConflictingOutputColumnsThroughTemporaryNames(t *testing.T) {
	t.Parallel()

	currentView := schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "status_source",
		Definition: "SELECT primary_status, fallback_status FROM records",
	}
	desiredView := schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "status_source",
		Definition: "SELECT primary_status AS fallback_status, fallback_status AS primary_status FROM records",
	}
	change := differ.Change{
		Type:       differ.ChangeTypeModifyView,
		ObjectName: "public.status_source",
	}
	result := &differ.DiffResult{
		Current: &schema.Database{Views: []schema.View{currentView}},
		Desired: &schema.Database{Views: []schema.View{desiredView}},
		Changes: []differ.Change{change},
	}
	builder := generator.NewDDLBuilder(result, true)

	up, err := builder.BuildUpStatement(change)
	require.NoError(t, err)

	firstTemporaryRename := strings.Index(up.SQL,
		"RENAME COLUMN primary_status TO __pgtofu_view_column_1")
	secondTemporaryRename := strings.Index(up.SQL,
		"RENAME COLUMN fallback_status TO __pgtofu_view_column_2")
	firstFinalRename := strings.Index(up.SQL,
		"RENAME COLUMN __pgtofu_view_column_1 TO fallback_status")
	secondFinalRename := strings.Index(up.SQL,
		"RENAME COLUMN __pgtofu_view_column_2 TO primary_status")

	assert.GreaterOrEqual(t, firstTemporaryRename, 0)
	assert.Greater(t, secondTemporaryRename, firstTemporaryRename)
	assert.Greater(t, firstFinalRename, secondTemporaryRename)
	assert.Greater(t, secondFinalRename, firstFinalRename)
}

func TestGeneratorOrdersDependentViewChangesForUpAndDown(t *testing.T) {
	t.Parallel()

	current := &schema.Database{Views: []schema.View{
		{
			Schema:     "source_data",
			Name:       "status_source",
			Definition: "SELECT record_id AS legacy_record_id FROM records",
		},
		{
			Schema:     "reporting",
			Name:       "status_summary",
			Definition: "SELECT legacy_record_id FROM source_data.status_source",
		},
	}}
	desired := &schema.Database{Views: []schema.View{
		{
			Schema:     "source_data",
			Name:       "status_source",
			Definition: "SELECT record_id AS source_record_id FROM records",
		},
		{
			Schema:     "reporting",
			Name:       "status_summary",
			Definition: "SELECT source_record_id FROM source_data.status_source",
		},
	}}

	d := differ.New(differ.DefaultOptions())
	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	opts := testOptions()
	opts.MaxOperationsPerFile = 1
	generated, err := generator.New(opts).Generate(result)
	require.NoError(t, err)
	require.Len(t, generated.Migrations, 1,
		"dependent view modifications must remain in one reversible migration")

	up := generated.Migrations[0].UpFile.Content
	down := generated.Migrations[0].DownFile.Content

	require.Contains(t, up, "ALTER VIEW source_data.status_source")
	require.Contains(t, up, "CREATE OR REPLACE VIEW reporting.status_summary")
	require.Contains(t, down, "ALTER VIEW source_data.status_source")
	require.Contains(t, down, "CREATE OR REPLACE VIEW reporting.status_summary")
	assert.Less(t,
		strings.Index(up, "ALTER VIEW source_data.status_source"),
		strings.Index(up, "CREATE OR REPLACE VIEW reporting.status_summary"),
	)
	assert.Less(t,
		strings.Index(down, "ALTER VIEW source_data.status_source"),
		strings.Index(down, "CREATE OR REPLACE VIEW reporting.status_summary"),
	)
}
