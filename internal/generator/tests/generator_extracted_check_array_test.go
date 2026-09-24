package generator_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestCheckRollbackFormatsExtractedArrayComparison(t *testing.T) {
	t.Parallel()

	current := &schema.Constraint{
		Name: "jobs_state_check",
		Type: "CHECK",
		Definition: "CHECK (((state <> ALL (ARRAY['queued'::text, " +
			"'reserved'::text, 'staged'::text, 'active'::text, " +
			"'released'::text, 'cancelled'::text])) OR (decided_at IS NOT NULL)))",
	}
	desired := &schema.Constraint{
		Name:       current.Name,
		Type:       current.Type,
		Definition: "CHECK (state <> 'queued' OR decided_at IS NOT NULL)",
	}
	change := differ.Change{
		Type:       differ.ChangeTypeModifyConstraint,
		ObjectName: "public.jobs",
		Details: map[string]any{
			"table":   "public.jobs",
			"current": current,
			"desired": desired,
		},
	}
	result := &differ.DiffResult{
		Current: &schema.Database{Tables: []schema.Table{{Schema: "public", Name: "jobs"}}},
		Desired: &schema.Database{Tables: []schema.Table{{Schema: "public", Name: "jobs"}}},
	}

	statement, err := generator.NewDDLBuilder(result, true).BuildDownStatement(change)
	require.NoError(t, err)
	require.Contains(t, statement.SQL, "ADD CONSTRAINT jobs_state_check CHECK")
	require.Contains(t, statement.SQL, "'queued'")
	require.Contains(t, statement.SQL, "'released'")

	dropped := differ.Change{
		Type:       differ.ChangeTypeDropConstraint,
		ObjectName: "public.jobs",
		Details: map[string]any{
			"table":      "public.jobs",
			"constraint": current,
		},
	}
	statement, err = generator.NewDDLBuilder(result, true).BuildDownStatement(dropped)
	require.NoError(t, err)
	require.Contains(t, statement.SQL, "ADD CONSTRAINT jobs_state_check CHECK")
}
