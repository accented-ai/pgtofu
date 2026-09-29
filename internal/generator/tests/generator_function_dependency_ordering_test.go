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

func TestFunctionDependencyOrdersNewOverloadBeforeSQLCallerWithJSONBOperator(t *testing.T) {
	t.Parallel()

	existingOverload := schema.Function{
		Schema:        "example",
		Name:          "unit_contracts",
		ArgumentTypes: []string{"TEXT", "UUID", "UUID[]"},
		ReturnType:    "INTEGER",
		Language:      "sql",
		Body:          "SELECT 1",
	}
	newOverload := existingOverload
	newOverload.ArgumentTypes = []string{"TEXT", "UUID", "UUID[]", "NUMERIC", "TEXT[]"}
	caller := schema.Function{
		Schema:        "example",
		Name:          "contract_digest",
		ArgumentTypes: []string{"TEXT", "UUID", "TEXT[]"},
		ArgumentNames: []string{"source_name", "source_id", "surfaces"},
		ReturnType:    "INTEGER",
		Language:      "sql",
		Body: `SELECT CASE
			WHEN '{}'::JSONB ?| surfaces THEN 0
			ELSE example.unit_contracts(
				source_name, source_id, ARRAY[source_id], 0::NUMERIC, surfaces
			)
		END`,
	}

	result, err := differ.New(differ.DefaultOptions()).Compare(
		&schema.Database{Functions: []schema.Function{existingOverload}},
		&schema.Database{Functions: []schema.Function{caller, existingOverload, newOverload}},
	)
	require.NoError(t, err)

	providerKey := differ.FunctionKey(
		newOverload.Schema, newOverload.Name, newOverload.ArgumentTypes,
	)

	callerKey := differ.FunctionKey(caller.Schema, caller.Name, caller.ArgumentTypes)

	for _, change := range result.Changes {
		if change.ObjectName == callerKey {
			require.Contains(t, change.DependsOn, providerKey)
		}
	}

	opts := testOptions()
	opts.MaxOperationsPerFile = 10
	generated, err := generator.New(opts).Generate(result)
	require.NoError(t, err)
	require.Len(t, generated.Migrations, 1)

	up := generated.Migrations[0].UpFile.Content
	providerPosition := strings.Index(up, "CREATE OR REPLACE FUNCTION example.UNIT_CONTRACTS(")
	callerPosition := strings.Index(up, "CREATE OR REPLACE FUNCTION example.CONTRACT_DIGEST(")

	require.GreaterOrEqual(t, providerPosition, 0)
	require.GreaterOrEqual(t, callerPosition, 0)
	require.Less(t, providerPosition, callerPosition)

	down := generated.Migrations[0].DownFile.Content
	callerDrop := strings.Index(down, "DROP FUNCTION IF EXISTS example.contract_digest(")
	providerDrop := strings.Index(down, "DROP FUNCTION IF EXISTS example.unit_contracts(")

	require.GreaterOrEqual(t, callerDrop, 0)
	require.GreaterOrEqual(t, providerDrop, 0)
	require.Less(t, callerDrop, providerDrop)
}
