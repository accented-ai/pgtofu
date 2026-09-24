package generator_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestViewColumnRemovalFailsMigrationGeneration(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		currentSQL string
		desiredSQL string
		direction  string
	}{
		{
			name:       "forward removal",
			currentSQL: "SELECT id, state FROM reporting.jobs",
			desiredSQL: "SELECT id FROM reporting.jobs",
			direction:  "up",
		},
		{
			name:       "rollback removal",
			currentSQL: "SELECT id FROM reporting.jobs",
			desiredSQL: "SELECT id, state FROM reporting.jobs",
			direction:  "down",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			current := &schema.Database{Views: []schema.View{{
				Schema: "reporting", Name: "job_summary", Definition: test.currentSQL,
			}}}
			desired := &schema.Database{Views: []schema.View{{
				Schema: "reporting", Name: "job_summary", Definition: test.desiredSQL,
			}}}

			result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
			require.NoError(t, err)
			require.NotEmpty(t, result.Changes)

			generated, err := generator.New(testOptions()).Generate(result)
			require.ErrorContains(t, err, test.direction+" migration")
			require.ErrorContains(t, err, "reporting.job_summary")
			require.Nil(t, generated)
		})
	}
}

func TestAddedViewColumnCanOmitRollbackMigration(t *testing.T) {
	t.Parallel()

	current := &schema.Database{Views: []schema.View{{
		Schema: "reporting", Name: "job_summary",
		Definition: "SELECT id FROM reporting.jobs",
	}}}
	desired := &schema.Database{Views: []schema.View{{
		Schema: "reporting", Name: "job_summary",
		Definition: "SELECT id, state FROM reporting.jobs",
	}}}

	result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)

	options := testOptions()
	options.GenerateDownMigrations = false
	generated, err := generator.New(options).Generate(result)
	require.NoError(t, err)
	require.NotEmpty(t, generated.Migrations)
}
