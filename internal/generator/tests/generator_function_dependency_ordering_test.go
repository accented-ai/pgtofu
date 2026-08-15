package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestFunctionDependencyOrdersProviderSchemaBeforeCallerSchema(t *testing.T) {
	t.Parallel()

	currentCaller := schema.Function{
		Schema:        "analytics",
		Name:          "record_is_publishable",
		ArgumentTypes: []string{"UUID", "UUID"},
		ArgumentNames: []string{"record_id", "member_id"},
		ReturnType:    "BOOLEAN",
		Language:      "sql",
		Volatility:    schema.VolatilityStable,
		Body:          "SELECT record_id IS NOT NULL",
	}
	helper := schema.Function{
		Schema:        "utilities",
		Name:          "record_has_scope",
		ArgumentTypes: []string{"UUID", "UUID"},
		ArgumentNames: []string{"record_id", "member_id"},
		ReturnType:    "BOOLEAN",
		Language:      "sql",
		Volatility:    schema.VolatilityStable,
		Body:          "SELECT record_id IS NOT NULL AND member_id IS NOT NULL",
	}
	desiredCaller := currentCaller
	desiredCaller.Body = "SELECT utilities.record_has_scope(record_id, member_id)"

	diffResult, err := differ.New(differ.DefaultOptions()).Compare(
		&schema.Database{Functions: []schema.Function{currentCaller}},
		&schema.Database{Functions: []schema.Function{desiredCaller, helper}},
	)
	require.NoError(t, err)

	opts := testOptions()
	opts.MaxOperationsPerFile = 10
	genResult, err := generator.New(opts).Generate(diffResult)
	require.NoError(t, err)

	helperMigration := -1
	callerMigration := -1

	for i, migration := range genResult.Migrations {
		require.NotNil(t, migration.UpFile)

		sql := strings.ToLower(migration.UpFile.Content)
		if strings.Contains(sql, "create or replace function utilities.record_has_scope") {
			helperMigration = i
		}

		if strings.Contains(sql, "create or replace function analytics.record_is_publishable") {
			callerMigration = i
		}
	}

	require.NotEqual(t, -1, helperMigration)
	require.NotEqual(t, -1, callerMigration)
	require.Less(t, helperMigration, callerMigration,
		"the provider schema migration must run before the caller schema migration")
}
