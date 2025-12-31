package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestGenerator_EmptyResult(t *testing.T) {
	t.Parallel()

	result := &differ.DiffResult{
		Current: &schema.Database{},
		Desired: &schema.Database{},
		Changes: []differ.Change{},
	}

	gen := generator.New(testOptions())
	genResult, err := gen.Generate(result)

	require.NoError(t, err)
	assert.Empty(t, genResult.Migrations)
	assert.NotEmpty(t, genResult.Warnings)
	assert.Contains(t, genResult.Warnings[0], "No changes")
}

func TestGenerator_GenerateWithChanges(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddTable, ObjectName: userTable},
		},
	}

	gen := generator.New(testOptions())
	genResult, err := gen.Generate(result)

	require.NoError(t, err)
	assert.Len(t, genResult.Migrations, 1)

	migration := genResult.Migrations[0]
	require.NotNil(t, migration.UpFile)
	require.NotNil(t, migration.DownFile)

	assert.Contains(t, migration.UpFile.Content, "CREATE TABLE")
	assert.Contains(t, migration.UpFile.Content, "BEGIN;")
	assert.Contains(t, migration.UpFile.Content, "COMMIT;")
	assert.Contains(t, migration.DownFile.Content, "DROP TABLE")
}

func TestGenerator_BatchChanges(t *testing.T) {
	t.Parallel()

	changes := make([]differ.Change, 25)
	for i := range changes {
		changes[i] = differ.Change{
			Type:       differ.ChangeTypeAddColumn,
			ObjectName: "public.users.column_" + string(rune(i)),
		}
	}

	gen := generator.New(generator.DefaultOptions())
	gen.Options.MaxOperationsPerFile = 10

	batches := gen.GroupChangesBySchema(changes)

	assert.Len(t, batches, 3)
	assert.Len(t, batches[0], 10)
	assert.Len(t, batches[1], 10)
	assert.Len(t, batches[2], 5)
}

func TestGenerator_TransactionMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mode       generator.TransactionMode
		statements []generator.DDLStatement
		wantTx     bool
	}{
		{
			name:       "always mode",
			mode:       generator.TransactionModeAlways,
			statements: []generator.DDLStatement{{SQL: "CREATE TABLE users (id int)"}},
			wantTx:     true,
		},
		{
			name:       "never mode",
			mode:       generator.TransactionModeNever,
			statements: []generator.DDLStatement{{SQL: "CREATE TABLE users (id int)"}},
			wantTx:     false,
		},
		{
			name: "auto mode with transactional DDL",
			mode: generator.TransactionModeAuto,
			statements: []generator.DDLStatement{
				{SQL: "CREATE TABLE users (id int)", RequiresTx: true},
			},
			wantTx: true,
		},
		{
			name: "auto mode with non-transactional",
			mode: generator.TransactionModeAuto,
			statements: []generator.DDLStatement{
				{SQL: "SELECT create_hypertable('users', 'time')", CannotUseTx: true},
			},
			wantTx: false,
		},
		{
			name: "auto mode mixed statements",
			mode: generator.TransactionModeAuto,
			statements: []generator.DDLStatement{
				{SQL: "CREATE TABLE users (id int)", RequiresTx: true},
				{SQL: "SELECT create_hypertable('users', 'time')", CannotUseTx: true},
			},
			wantTx: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gen := generator.New(generator.DefaultOptions())
			gen.Options.TransactionMode = tt.mode

			result := gen.ShouldUseTransaction(tt.statements)
			assert.Equal(t, tt.wantTx, result)
		})
	}
}

func TestGenerator_IdempotentMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		idempotent bool
		changeType differ.ChangeType
		wantSQL    string
	}{
		{
			name:       "idempotent drop table",
			idempotent: true,
			changeType: differ.ChangeTypeDropTable,
			wantSQL:    "IF EXISTS",
		},
		{
			name:       "non-idempotent drop table",
			idempotent: false,
			changeType: differ.ChangeTypeDropTable,
			wantSQL:    "DROP TABLE",
		},
		{
			name:       "idempotent add extension",
			idempotent: true,
			changeType: differ.ChangeTypeAddExtension,
			wantSQL:    "IF NOT EXISTS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var current, desired *schema.Database

			switch tt.changeType {
			case differ.ChangeTypeDropTable:
				current = &schema.Database{
					Tables: []schema.Table{{Schema: schema.DefaultSchema, Name: "users"}},
				}
				desired = &schema.Database{}
			case differ.ChangeTypeAddExtension:
				current = &schema.Database{}
				desired = &schema.Database{
					Extensions: []schema.Extension{{Name: "timescaledb"}},
				}
			}

			objectName := userTable
			if tt.changeType == differ.ChangeTypeAddExtension {
				objectName = "timescaledb"
			}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{{Type: tt.changeType, ObjectName: objectName}},
			}

			builder := generator.NewDDLBuilder(result, tt.idempotent)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)
			assert.Contains(t, stmt.SQL, tt.wantSQL)
		})
	}
}

func TestGenerator_CommentGeneration(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{{Schema: schema.DefaultSchema, Name: "users"}},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{{Type: differ.ChangeTypeAddTable, ObjectName: userTable}},
	}

	tests := []struct {
		name            string
		includeComments bool
		wantContains    []string
		wantNotContains []string
	}{
		{
			name:            "with comments",
			includeComments: true,
			wantContains:    []string{"Migration:", "Generated:", "Changes:"},
		},
		{
			name:            "without comments",
			includeComments: false,
			wantNotContains: []string{"Migration:", "Generated:", "Changes:"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := testOptions()
			opts.IncludeComments = tt.includeComments

			gen := generator.New(opts)
			genResult, err := gen.Generate(result)

			require.NoError(t, err)
			require.NotEmpty(t, genResult.Migrations)

			content := genResult.Migrations[0].UpFile.Content

			for _, want := range tt.wantContains {
				assert.Contains(t, content, want)
			}

			for _, notWant := range tt.wantNotContains {
				assert.NotContains(t, content, notWant)
			}
		})
	}
}
