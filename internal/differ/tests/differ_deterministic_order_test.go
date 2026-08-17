package differ_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffOrderingIsDeterministic(t *testing.T) {
	t.Parallel()

	current, desired := deterministicOrderDatabases()

	var expected []string

	for iteration := range 100 {
		result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
		require.NoError(t, err)

		actual := make([]string, len(result.Changes))
		for i, change := range result.Changes {
			actual[i] = fmt.Sprintf(
				"%02d|%s|%s|%s",
				change.Order,
				change.Type,
				change.ObjectName,
				change.Description,
			)
		}

		if iteration == 0 {
			expected = actual

			continue
		}

		assert.Equal(t, expected, actual, "change ordering differed on iteration %d", iteration)
	}

	assert.Equal(t, []string{
		"Add column: reporting.jobs.alpha_note (TEXT)",
		"Add column: reporting.jobs.beta_actor (TEXT)",
		"Add column: reporting.jobs.gamma_run_id (UUID)",
		"Add column: reporting.jobs.zeta_source_id (UUID)",
	}, changeDescriptionsOfType(t, current, desired, differ.ChangeTypeAddColumn))
	assert.Equal(t, []string{
		"Add CHECK constraint: jobs_actor_check on reporting.jobs",
		"Add CHECK constraint: jobs_note_check on reporting.jobs",
		"Add CHECK constraint: jobs_source_check on reporting.jobs",
		"Add FOREIGN KEY constraint: jobs_zeta_source_id_fkey on reporting.jobs",
	}, changeDescriptionsOfType(t, current, desired, differ.ChangeTypeAddConstraint))
}

func deterministicOrderDatabases() (*schema.Database, *schema.Database) {
	primaryKey := schema.Constraint{
		Name:    "jobs_pkey",
		Type:    schema.ConstraintPrimaryKey,
		Columns: []string{"id"},
	}
	sourceTable := schema.Table{
		Schema: "reporting",
		Name:   "sources",
		Columns: []schema.Column{{
			Name:       "id",
			DataType:   "UUID",
			Position:   1,
			IsNullable: false,
		}},
		Constraints: []schema.Constraint{{
			Name:    "sources_pkey",
			Type:    schema.ConstraintPrimaryKey,
			Columns: []string{"id"},
		}},
	}
	currentJobs := schema.Table{
		Schema: "reporting",
		Name:   "jobs",
		Columns: []schema.Column{
			{Name: "id", DataType: "UUID", Position: 1, IsNullable: false},
			{Name: "legacy_group_id", DataType: "UUID", Position: 2, IsNullable: false},
		},
		Constraints: []schema.Constraint{primaryKey},
		Comment:     "Stores queued reporting work.",
	}
	desiredJobs := schema.Table{
		Schema: "reporting",
		Name:   "jobs",
		Columns: []schema.Column{
			{Name: "id", DataType: "UUID", Position: 1, IsNullable: false},
			{Name: "legacy_group_id", DataType: "UUID", Position: 2, IsNullable: true},
			{
				Name:       "zeta_source_id",
				DataType:   "UUID",
				Position:   3,
				IsNullable: true,
				Comment:    "Source that requested this job.",
			},
			{Name: "alpha_note", DataType: "TEXT", Position: 4, IsNullable: true},
			{
				Name:       "beta_actor",
				DataType:   "TEXT",
				Position:   5,
				IsNullable: false,
				Default:    "'system'::text",
			},
			{
				Name:       "gamma_run_id",
				DataType:   "UUID",
				Position:   6,
				IsNullable: true,
				Comment:    "Run that produced this job.",
			},
		},
		Constraints: []schema.Constraint{
			primaryKey,
			{
				Name:              "jobs_zeta_source_id_fkey",
				Type:              schema.ConstraintForeignKey,
				Columns:           []string{"zeta_source_id"},
				ReferencedSchema:  "reporting",
				ReferencedTable:   "sources",
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
			},
			{
				Name:            "jobs_note_check",
				Type:            schema.ConstraintCheck,
				CheckExpression: "alpha_note IS NULL OR LENGTH(alpha_note) <= 500",
			},
			{
				Name:            "jobs_actor_check",
				Type:            schema.ConstraintCheck,
				CheckExpression: "BTRIM(beta_actor) <> ''",
			},
			{
				Name:            "jobs_source_check",
				Type:            schema.ConstraintCheck,
				CheckExpression: "NUM_NONNULLS(legacy_group_id, zeta_source_id, gamma_run_id) = 1",
			},
		},
		Indexes: []schema.Index{
			{
				Schema:    "reporting",
				TableName: "jobs",
				Name:      "uq_jobs_zeta_source",
				Columns:   []string{"zeta_source_id"},
				Type:      schema.IndexTypeBTree,
				IsUnique:  true,
				Where:     "zeta_source_id IS NOT NULL",
			},
			{
				Schema:    "reporting",
				TableName: "jobs",
				Name:      "uq_jobs_gamma_run",
				Columns:   []string{"gamma_run_id"},
				Type:      schema.IndexTypeBTree,
				IsUnique:  true,
				Where:     "gamma_run_id IS NOT NULL",
			},
		},
		Comment: "Stores queued reporting work and its request provenance.",
	}

	return &schema.Database{Tables: []schema.Table{currentJobs, sourceTable}},
		&schema.Database{Tables: []schema.Table{desiredJobs, sourceTable}}
}

func changeDescriptionsOfType(
	t *testing.T,
	current, desired *schema.Database,
	changeType differ.ChangeType,
) []string {
	t.Helper()

	result, err := differ.New(differ.DefaultOptions()).Compare(current, desired)
	require.NoError(t, err)

	descriptions := make([]string, 0)

	for _, change := range result.Changes {
		if change.Type == changeType {
			descriptions = append(descriptions, change.Description)
		}
	}

	return descriptions
}
