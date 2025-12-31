package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestGenerator_FileWriting(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

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
		Changes: []differ.Change{{Type: differ.ChangeTypeAddTable, ObjectName: userTable}},
	}

	opts := generator.DefaultOptions()
	opts.OutputDir = tmpDir
	opts.GenerateDownMigrations = true

	gen := generator.New(opts)
	genResult, err := gen.Generate(result)

	require.NoError(t, err)
	assert.Equal(t, 2, genResult.FilesGenerated)

	upFile := filepath.Join(tmpDir, "000001_add_table_users.up.sql")
	downFile := filepath.Join(tmpDir, "000001_add_table_users.down.sql")

	upContent, err := os.ReadFile(upFile)
	require.NoError(t, err)
	assert.Contains(t, string(upContent), "CREATE TABLE")

	downContent, err := os.ReadFile(downFile)
	require.NoError(t, err)
	assert.Contains(t, string(downContent), "DROP TABLE")
}

func TestGenerator_NextVersion(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	existingFiles := []string{
		"000001_initial.up.sql",
		"000001_initial.down.sql",
		"000002_add_users.up.sql",
		"000002_add_users.down.sql",
	}

	for _, filename := range existingFiles {
		err := os.WriteFile(filepath.Join(tmpDir, filename), []byte(""), 0o644)
		require.NoError(t, err)
	}

	opts := generator.DefaultOptions()
	opts.OutputDir = tmpDir

	gen := generator.New(opts)
	version, err := gen.GetNextMigrationVersion()

	require.NoError(t, err)
	assert.Equal(t, 3, version)
}

func TestGenerateResult_Summary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		result   *generator.GenerateResult
		wantText []string
	}{
		{
			name: "with migrations",
			result: &generator.GenerateResult{
				Migrations: []generator.MigrationPair{
					{
						Version:     1,
						Description: "add_users",
						UpFile:      &generator.MigrationFile{FileName: "000001_add_users.up.sql"},
						DownFile: &generator.MigrationFile{
							FileName: "000001_add_users.down.sql",
						},
					},
				},
				Warnings:       []string{"Warning 1", "Warning 2"},
				FilesGenerated: 2,
			},
			wantText: []string{"Migrations Generated: 1", "Files Created: 2", "Warnings: 2"},
		},
		{
			name: "no migrations",
			result: &generator.GenerateResult{
				Migrations:     []generator.MigrationPair{},
				FilesGenerated: 0,
			},
			wantText: []string{"No migrations generated"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			summary := tt.result.Summary()
			for _, want := range tt.wantText {
				assert.Contains(t, summary, want)
			}
		})
	}
}
