package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestGenerateMigrationName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		changes  []differ.Change
		expected string
	}{
		{
			name:     "no changes",
			changes:  []differ.Change{},
			expected: "no_changes",
		},
		{
			name: "single table add",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddTable, ObjectName: userTable},
			},
			expected: "add_table_users",
		},
		{
			name: "single column add",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddColumn, ObjectName: "public.users.email"},
			},
			expected: "add_columns_users",
		},
		{
			name: "multiple table changes",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddTable, ObjectName: userTable},
				{Type: differ.ChangeTypeAddTable, ObjectName: "public.orders"},
			},
			expected: "schema_changes",
		},
		{
			name: "mixed change types",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddTable},
				{Type: differ.ChangeTypeAddIndex},
				{Type: differ.ChangeTypeAddView},
			},
			expected: "schema_changes",
		},
		{
			name: "index changes",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddIndex, ObjectName: "public.idx_users_email"},
			},
			expected: "add_index_idx_users_email",
		},
		{
			name: "constraint changes",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddConstraint, ObjectName: "public.users_pkey"},
			},
			expected: "add_constraint_users_pkey",
		},
		{
			name: "view changes",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddView, ObjectName: "public.active_users"},
			},
			expected: "update_view_active_users",
		},
		{
			name: "function changes",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddFunction, ObjectName: "public.calculate_total"},
			},
			expected: "update_function_calculate_total",
		},
		{
			name: "timescale hypertable",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddHypertable, ObjectName: "public.metrics"},
			},
			expected: "add_hypertable_metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := generator.GenerateMigrationName(tt.changes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatMigrationFileName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		version     int
		description string
		direction   generator.Direction
		expected    string
	}{
		{1, "add_users_table", generator.DirectionUp, "000001_add_users_table.up.sql"},
		{42, "modify_schema", generator.DirectionDown, "000042_modify_schema.down.sql"},
		{999, "complex_change", generator.DirectionUp, "000999_complex_change.up.sql"},
		{1000000, "large_version", generator.DirectionUp, "1000000_large_version.up.sql"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()

			result := generator.FormatMigrationFileName(tt.version, tt.description, tt.direction)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseMigrationFileName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		fileName    string
		wantVersion int
		wantDesc    string
		wantDir     generator.Direction
		wantErr     bool
	}{
		{
			name:        "valid up migration",
			fileName:    "000001_add_users.up.sql",
			wantVersion: 1,
			wantDesc:    "add_users",
			wantDir:     generator.DirectionUp,
		},
		{
			name:        "valid down migration",
			fileName:    "000042_drop_table.down.sql",
			wantVersion: 42,
			wantDesc:    "drop_table",
			wantDir:     generator.DirectionDown,
		},
		{
			name:        "description with underscores",
			fileName:    "000003_add_user_email_index.up.sql",
			wantVersion: 3,
			wantDesc:    "add_user_email_index",
			wantDir:     generator.DirectionUp,
		},
		{
			name:     "invalid format no version",
			fileName: "invalid.sql",
			wantErr:  true,
		},
		{
			name:     "invalid format no direction",
			fileName: "000001_test.sql",
			wantErr:  true,
		},
		{
			name:     "invalid direction",
			fileName: "000001_test.sideways.sql",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			version, desc, dir, err := generator.ParseMigrationFileName(tt.fileName)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantVersion, version)
			assert.Equal(t, tt.wantDesc, desc)
			assert.Equal(t, tt.wantDir, dir)
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"users", "users"},
		{"user_roles", "user_roles"},
		{"User", `"User"`},
		{"user-roles", `"user-roles"`},
		{"user roles", `"user roles"`},
		{"123invalid", `"123invalid"`},
		{"valid123", "valid123"},
		{"_private", "_private"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			result := generator.QuoteIdentifier(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQualifiedName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		schema   string
		name     string
		expected string
	}{
		{schema.DefaultSchema, "users", "public.users"},
		{"", "users", "public.users"},
		{"myschema", "users", "myschema.users"},
		{schema.DefaultSchema, "User", `public."User"`},
		{"MySchema", "users", `"MySchema".users`},
		{"my_schema", "user_table", "my_schema.user_table"},
	}

	for _, tt := range tests {
		t.Run(tt.schema+"."+tt.name, func(t *testing.T) {
			t.Parallel()

			result := generator.QualifiedName(tt.schema, tt.name)
			assert.Equal(t, tt.expected, result)
		})
	}
}
