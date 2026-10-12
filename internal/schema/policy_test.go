package schema_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestRowSecurityJSONRoundTrip(t *testing.T) {
	t.Parallel()

	db := schema.Database{Tables: []schema.Table{{
		Schema: "public", Name: "documents", RowSecurityEnabled: true, RowSecurityForced: true,
		Policies: []schema.Policy{
			{
				Name:            "tenant",
				Command:         "ALL",
				Permissive:      true,
				Roles:           []schema.PolicyRole{{Public: true}, {Name: "PUBLIC"}},
				UsingExpression: "tenant_id = current_user",
				CheckExpression: "tenant_id = current_user",
			},
		},
	}}}
	data, _ := json.Marshal(db)

	var restored schema.Database
	require.NoError(t, json.Unmarshal(data, &restored))
	assert.Equal(t, db, restored)
}

func TestLegacyTableHasRowSecurityDisabled(t *testing.T) {
	t.Parallel()

	var db schema.Database
	require.NoError(
		t,
		json.Unmarshal(
			[]byte(`{"tables":[{"schema":"public","name":"documents","columns":[]}]}`),
			&db,
		),
	)
	require.Len(t, db.Tables, 1)
	assert.False(t, db.Tables[0].RowSecurityEnabled)
	assert.False(t, db.Tables[0].RowSecurityForced)
	assert.Empty(t, db.Tables[0].Policies)
}
