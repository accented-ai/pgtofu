package schema_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestIdentifierAllocatorPreservesUniqueSuffixesWithinPostgresLimit(t *testing.T) {
	t.Parallel()

	logicalName := strings.Repeat("a", schema.MaxIdentifierLength+10)
	allocator := schema.NewIdentifierAllocator(
		strings.Repeat("a", schema.MaxIdentifierLength-1) + "1",
	)

	first := allocator.Allocate(logicalName)
	second := allocator.Allocate(logicalName)
	third := allocator.Allocate(logicalName)

	assert.Equal(t, strings.Repeat("a", schema.MaxIdentifierLength), first)
	assert.Equal(t, strings.Repeat("a", schema.MaxIdentifierLength-1)+"2", second)
	assert.Equal(t, strings.Repeat("a", schema.MaxIdentifierLength-1)+"3", third)
	assert.Len(t, first, schema.MaxIdentifierLength)
	assert.Len(t, second, schema.MaxIdentifierLength)
	assert.Len(t, third, schema.MaxIdentifierLength)
}

func TestTruncateIdentifierPreservesUTF8(t *testing.T) {
	t.Parallel()

	identifier := strings.Repeat("a", schema.MaxIdentifierLength-1) + string(rune(0x754c))
	truncated := schema.TruncateIdentifier(identifier)

	require.True(t, utf8.ValidString(truncated))
	assert.Equal(t, strings.Repeat("a", schema.MaxIdentifierLength-1), truncated)
}
