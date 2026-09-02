package generator //nolint:testpackage

import (
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
