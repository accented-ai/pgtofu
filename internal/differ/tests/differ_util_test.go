package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
)

func TestDiffer_HasBreakingChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		changes  []differ.Change
		expected bool
	}{
		{
			name: "has breaking changes",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddColumn, Severity: differ.SeveritySafe},
				{Type: differ.ChangeTypeDropTable, Severity: differ.SeverityBreaking},
			},
			expected: true,
		},
		{
			name: "no breaking changes",
			changes: []differ.Change{
				{Type: differ.ChangeTypeAddColumn, Severity: differ.SeveritySafe},
				{Type: differ.ChangeTypeAddIndex, Severity: differ.SeveritySafe},
			},
			expected: false,
		},
		{
			name: "has data migration required",
			changes: []differ.Change{
				{
					Type:     differ.ChangeTypeModifyColumnType,
					Severity: differ.SeverityDataMigrationRequired,
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := &differ.DiffResult{Changes: tt.changes}
			if result.HasBreakingChanges() != tt.expected {
				t.Errorf(
					"expected HasBreakingChanges=%v, got %v",
					tt.expected,
					result.HasBreakingChanges(),
				)
			}
		})
	}
}

func TestDiffer_GetChangesBySeverity(t *testing.T) {
	t.Parallel()

	result := &differ.DiffResult{
		Changes: []differ.Change{
			{Type: differ.ChangeTypeAddColumn, Severity: differ.SeveritySafe},
			{Type: differ.ChangeTypeAddIndex, Severity: differ.SeveritySafe},
			{Type: differ.ChangeTypeDropTable, Severity: differ.SeverityBreaking},
			{
				Type:     differ.ChangeTypeModifyColumnType,
				Severity: differ.SeverityDataMigrationRequired,
			},
		},
	}

	tests := []struct {
		severity differ.ChangeSeverity
		expected int
	}{
		{differ.SeveritySafe, 2},
		{differ.SeverityBreaking, 1},
		{differ.SeverityDataMigrationRequired, 1},
		{differ.SeverityPotentiallyBreaking, 0},
	}

	for _, tt := range tests {
		t.Run(string(tt.severity), func(t *testing.T) {
			t.Parallel()

			changes := result.GetChangesBySeverity(tt.severity)
			if len(changes) != tt.expected {
				t.Errorf(
					"expected %d changes with severity %s, got %d",
					tt.expected,
					tt.severity,
					len(changes),
				)
			}
		})
	}
}

func TestNormalizeDataType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"INT", "integer"},
		{"int4", "integer"},
		{"int8", "bigint"},
		{"BOOL", "boolean"},
		{"VARCHAR", "varchar"},
		{"character varying", "varchar"},
		{"TIMESTAMPTZ", "timestamp with time zone"},
		{"DECIMAL", "numeric"},
		{"FLOAT8", "double precision"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			result := differ.NormalizeDataType(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeDataType(%s) = %s, want %s", tt.input, result, tt.expected)
			}
		})
	}
}

func TestAreDefaultsEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		default1 string
		default2 string
		expected bool
	}{
		{"", "", true},
		{"NOW()", "CURRENT_TIMESTAMP", true},
		{"now()", "current_timestamp", true},
		{"'hello'", "'hello'", true},
		{"'hello'", "'world'", false},
		{"0", "0", true},
		{"true", "'t'", true},
		{"false", "'f'", true},
		{"TRUE", "'t'", true},
		{"FALSE", "'f'", true},
		{"'{}'::text[]", "'{}'", true},
		{"'{}'::TEXT[]", "'{}'", true},
		{"'{}'::integer[]", "'{}'", true},
		{"'{}'::uuid[]", "'{}'", true},
		{"'{1,2}'::integer[]", "'{3,4}'::integer[]", false},
	}

	for _, tt := range tests {
		t.Run(tt.default1+" vs "+tt.default2, func(t *testing.T) {
			t.Parallel()

			result := differ.AreDefaultsEqual(tt.default1, tt.default2)
			if result != tt.expected {
				t.Errorf(
					"AreDefaultsEqual(%s, %s) = %v, want %v",
					tt.default1,
					tt.default2,
					result,
					tt.expected,
				)
			}
		})
	}
}
