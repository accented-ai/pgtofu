package generator //nolint:testpackage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatViewQueryPreservesHyphenatedAnyArrayLiteral(t *testing.T) {
	t.Parallel()

	formatted, err := formatViewQuery(`
		SELECT 'value' = ANY(ARRAY[
			'pipeline-primary-v12',
			'pipeline-fallback-v1'
		])
	`)
	require.NoError(t, err)
	assert.Contains(t, formatted, "'pipeline-primary-v12'")
	assert.Contains(t, formatted, "'pipeline-fallback-v1'")
	assert.NotContains(t, formatted, "'pipeline - primary - v12'")
}

func TestFormatViewQueryWrapsLongJSONAccess(t *testing.T) {
	t.Parallel()

	const longJSONPath = "{workflow,authority,initial_decision," +
		"faithfulness_self_check,localized_instruction_language_is_semantically_correct}"

	formatted, err := formatViewQuery(`
        SELECT
            CASE
                WHEN (
                    record_dependency.selection_evidence #>> '` + longJSONPath + `'
                ) = 'true' THEN 'ready'
                ELSE 'pending'
            END AS state
        FROM reporting.record_dependencies AS record_dependency
    `)
	require.NoError(t, err)

	assert.Contains(t, formatted,
		"record_dependency.selection_evidence\n"+
			strings.Repeat(" ", 16)+
			"#>> '"+longJSONPath+"'",
	)

	for line := range strings.SplitSeq(formatted, "\n") {
		assert.LessOrEqual(t, len(line), generatedSQLLineLength)
	}
}
