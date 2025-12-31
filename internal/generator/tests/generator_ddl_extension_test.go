package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDDLBuilder_ExtensionOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		changeType     differ.ChangeType
		extension      *schema.Extension
		previous       *schema.Extension
		wantSQL        []string
		wantUnsafe     bool
		wantRequiresTx bool
	}{
		{
			name:       "add extension",
			changeType: differ.ChangeTypeAddExtension,
			extension: &schema.Extension{
				Name: "uuid-ossp",
			},
			wantSQL:        []string{"CREATE EXTENSION", "IF NOT EXISTS", "uuid-ossp"},
			wantUnsafe:     false,
			wantRequiresTx: false,
		},
		{
			name:       "add extension with schema",
			changeType: differ.ChangeTypeAddExtension,
			extension: &schema.Extension{
				Name:   "pg_trgm",
				Schema: "extensions",
			},
			wantSQL: []string{
				"CREATE EXTENSION",
				"IF NOT EXISTS",
				"pg_trgm",
				"WITH SCHEMA extensions",
			},
			wantUnsafe:     false,
			wantRequiresTx: false,
		},
		{
			name:       "add extension with version",
			changeType: differ.ChangeTypeAddExtension,
			extension: &schema.Extension{
				Name:    "postgis",
				Version: "3.4.0",
			},
			wantSQL: []string{
				"CREATE EXTENSION",
				"VERSION '3.4.0'",
			},
			wantUnsafe:     false,
			wantRequiresTx: false,
		},
		{
			name:       "add extension to public schema",
			changeType: differ.ChangeTypeAddExtension,
			extension: &schema.Extension{
				Name:   "postgis",
				Schema: schema.DefaultSchema,
			},
			wantSQL: []string{
				"CREATE EXTENSION",
				"IF NOT EXISTS",
				"postgis",
				"WITH SCHEMA public",
			},
			wantUnsafe:     false,
			wantRequiresTx: false,
		},
		{
			name:       "modify extension schema",
			changeType: differ.ChangeTypeModifyExtension,
			extension: &schema.Extension{
				Name:   "btree_gin",
				Schema: "custom",
			},
			wantSQL: []string{
				"ALTER EXTENSION",
				"SET SCHEMA",
				"custom",
			},
			wantUnsafe:     true,
			wantRequiresTx: false,
		},
		{
			name:       "modify extension version",
			changeType: differ.ChangeTypeModifyExtension,
			extension: &schema.Extension{
				Name:    "timescaledb",
				Version: "2.14.0",
			},
			previous: &schema.Extension{
				Name:    "timescaledb",
				Version: "2.13.1",
			},
			wantSQL: []string{
				"ALTER EXTENSION",
				"UPDATE TO",
				"'2.14.0'",
			},
			wantUnsafe:     true,
			wantRequiresTx: false,
		},
		{
			name:       "drop extension",
			changeType: differ.ChangeTypeDropExtension,
			extension: &schema.Extension{
				Name: "old_extension",
			},
			wantSQL:        []string{"DROP EXTENSION", "IF EXISTS", "old_extension", "CASCADE"},
			wantUnsafe:     true,
			wantRequiresTx: false,
		},
		{
			name:       "add extension with quoted name",
			changeType: differ.ChangeTypeAddExtension,
			extension: &schema.Extension{
				Name: "uuid-ossp",
			},
			wantSQL:        []string{"CREATE EXTENSION", "IF NOT EXISTS", `"uuid-ossp"`},
			wantUnsafe:     false,
			wantRequiresTx: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var current, desired *schema.Database

			switch tt.changeType {
			case differ.ChangeTypeDropExtension:
				prev := tt.extension
				if tt.previous != nil {
					prev = tt.previous
				}

				current = &schema.Database{Extensions: []schema.Extension{*prev}}
				desired = &schema.Database{}
			case differ.ChangeTypeModifyExtension:
				prev := schema.Extension{Name: tt.extension.Name}
				if tt.previous != nil {
					prev = *tt.previous
				}

				current = &schema.Database{Extensions: []schema.Extension{prev}}
				desired = &schema.Database{Extensions: []schema.Extension{*tt.extension}}
			default:
				current = &schema.Database{}
				desired = &schema.Database{Extensions: []schema.Extension{*tt.extension}}
			}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{{Type: tt.changeType, ObjectName: tt.extension.Name}},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)

			for _, want := range tt.wantSQL {
				assert.Contains(t, stmt.SQL, want)
			}

			assert.Equal(t, tt.wantUnsafe, stmt.IsUnsafe)
			assert.Equal(t, tt.wantRequiresTx, stmt.RequiresTx)
		})
	}
}

func TestDDLBuilder_ExtensionIdempotent(t *testing.T) {
	t.Parallel()

	extension := &schema.Extension{
		Name: "test_extension",
	}

	current := &schema.Database{Extensions: []schema.Extension{*extension}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeDropExtension, ObjectName: extension.Name},
		},
	}

	builderIdempotent := generator.NewDDLBuilder(result, true)
	builderNonIdempotent := generator.NewDDLBuilder(result, false)

	stmtIdempotent, err := builderIdempotent.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	stmtNonIdempotent, err := builderNonIdempotent.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	assert.Contains(t, stmtIdempotent.SQL, "IF EXISTS")
	assert.NotContains(t, stmtNonIdempotent.SQL, "IF EXISTS")
}

func TestDDLBuilder_ExtensionDownMigration(t *testing.T) {
	t.Parallel()

	extension := &schema.Extension{
		Name: "reverted_extension",
	}

	current := &schema.Database{}
	desired := &schema.Database{Extensions: []schema.Extension{*extension}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddExtension, ObjectName: extension.Name},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "DROP EXTENSION")
	assert.Contains(t, stmt.SQL, "reverted_extension")
}

func TestDDLBuilder_ExtensionSchemaHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		extension  *schema.Extension
		wantSQL    []string
		notWantSQL []string
	}{
		{
			name: "extension with custom schema",
			extension: &schema.Extension{
				Name:   "pg_trgm",
				Schema: "extensions",
			},
			wantSQL: []string{
				"CREATE EXTENSION",
				"IF NOT EXISTS",
				"pg_trgm",
				"WITH SCHEMA extensions",
			},
			notWantSQL: []string{"SCHEMA public"},
		},
		{
			name: "extension with public schema",
			extension: &schema.Extension{
				Name:   "uuid-ossp",
				Schema: schema.DefaultSchema,
			},
			wantSQL: []string{
				"CREATE EXTENSION",
				"IF NOT EXISTS",
				"uuid-ossp",
				"WITH SCHEMA public",
			},
		},
		{
			name: "extension with empty schema",
			extension: &schema.Extension{
				Name:   "postgis",
				Schema: "",
			},
			wantSQL:    []string{"CREATE EXTENSION", "IF NOT EXISTS", "postgis"},
			notWantSQL: []string{"SCHEMA"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			current := &schema.Database{}
			desired := &schema.Database{Extensions: []schema.Extension{*tt.extension}}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{
					{Type: differ.ChangeTypeAddExtension, ObjectName: tt.extension.Name},
				},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)

			for _, want := range tt.wantSQL {
				assert.Contains(t, stmt.SQL, want)
			}

			for _, notWant := range tt.notWantSQL {
				assert.NotContains(t, stmt.SQL, notWant)
			}
		})
	}
}

func TestDDLBuilder_ExtensionDownFromDropUsesObjectName(t *testing.T) {
	t.Parallel()

	ext := &schema.Extension{Name: "pgcrypto"}

	current := &schema.Database{Extensions: []schema.Extension{*ext}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{Type: differ.ChangeTypeDropExtension, ObjectName: ext.Name},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "CREATE EXTENSION")
	assert.Contains(t, stmt.SQL, "pgcrypto")
}
