package differ_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDifferDetectsExtensionSchemaChange(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Extensions: []schema.Extension{{Name: "pg_trgm", Schema: schema.DefaultSchema}},
	}
	desired := &schema.Database{
		Extensions: []schema.Extension{{Name: "pg_trgm", Schema: "extensions"}},
	}

	df := differ.New(nil)
	result, err := df.Compare(current, desired)
	require.NoError(t, err)

	require.Len(t, result.Changes, 1)
	change := result.Changes[0]
	assert.Equal(t, differ.ChangeTypeModifyExtension, change.Type)
	assert.Equal(t, "pg_trgm", change.ObjectName)
}

func TestDifferDetectsExtensionVersionChange(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Extensions: []schema.Extension{{Name: "postgis", Version: "3.3"}},
	}
	desired := &schema.Database{
		Extensions: []schema.Extension{{Name: "postgis", Version: "3.4"}},
	}

	df := differ.New(nil)
	result, err := df.Compare(current, desired)
	require.NoError(t, err)

	require.Len(t, result.Changes, 1)
	change := result.Changes[0]
	assert.Equal(t, differ.ChangeTypeModifyExtension, change.Type)
	assert.Equal(t, "postgis", change.ObjectName)
}

func TestDifferIgnoresVersionWhenNotSpecified(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Extensions: []schema.Extension{{Name: "pgcrypto", Version: "1.3"}},
	}
	desired := &schema.Database{
		Extensions: []schema.Extension{{Name: "pgcrypto"}},
	}

	df := differ.New(nil)
	result, err := df.Compare(current, desired)
	require.NoError(t, err)

	assert.Empty(t, result.Changes)
}
