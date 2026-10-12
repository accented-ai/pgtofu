package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
)

func TestPolicyCommentChangeLeavesDefinitionInPlace(t *testing.T) {
	t.Parallel()

	current := policySchema(t, `CREATE TABLE documents (id integer);
		CREATE POLICY "Scope""Policy" ON documents USING (id > 0);
		COMMENT ON POLICY "Scope""Policy" ON documents IS 'Scope''s boundary';`)
	desired := policySchema(t, `CREATE TABLE documents (id integer);
		CREATE POLICY "Scope""Policy" ON documents USING (id > 0);`)
	result, err := differ.New(nil).Compare(current, desired)
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	assert.Equal(t, differ.SeveritySafe, result.Changes[0].Severity)
	ddl := generator.NewDDLBuilder(result, false)
	up, err := ddl.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Equal(t, `COMMENT ON POLICY "Scope""Policy" ON "public"."documents" IS NULL;`, up.SQL)

	down, err := ddl.BuildDownStatement(result.Changes[0])
	require.NoError(t, err)
	assert.Contains(t, down.SQL, `'Scope''s boundary';`)
	assert.NotContains(t, down.SQL, "DROP POLICY")

	options := differ.DefaultOptions()
	options.IgnoreComments = true
	ignored, err := differ.New(options).Compare(current, desired)
	require.NoError(t, err)
	assert.Empty(t, ignored.Changes)
}

func TestPolicyRecreationPreservesIgnoredComments(t *testing.T) {
	t.Parallel()

	current := policySchema(t, `CREATE TABLE documents (id integer);
		CREATE POLICY scope ON documents USING (id > 0);
		COMMENT ON POLICY scope ON documents IS 'Keep this description';`)
	desired := policySchema(t, `CREATE TABLE documents (id bigint);
		CREATE POLICY scope ON documents USING (id > 0);`)
	options := differ.DefaultOptions()
	options.IgnoreComments = true
	result, err := differ.New(options).Compare(current, desired)
	require.NoError(t, err)

	changes := result.GetChangesByType(differ.ChangeTypeAddPolicy)
	require.Len(t, changes, 1)

	ddl := generator.NewDDLBuilder(result, false)
	up, err := ddl.BuildUpStatement(changes[0])
	require.NoError(t, err)
	assert.Contains(
		t,
		up.SQL,
		`COMMENT ON POLICY "scope" ON "public"."documents" IS 'Keep this description';`,
	)
}
