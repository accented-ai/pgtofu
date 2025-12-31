package generator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDDLBuilder_ViewOperations(t *testing.T) {
	t.Parallel()

	view := &schema.View{
		Schema:     schema.DefaultSchema,
		Name:       "active_users",
		Definition: "SELECT * FROM users WHERE active = true",
	}

	tests := []struct {
		name       string
		changeType differ.ChangeType
		wantSQL    string
		wantUnsafe bool
		useReplace bool
	}{
		{
			name:       "add view",
			changeType: differ.ChangeTypeAddView,
			wantSQL:    "CREATE VIEW",
			useReplace: false,
		},
		{
			name:       "modify view",
			changeType: differ.ChangeTypeModifyView,
			wantSQL:    "CREATE OR REPLACE VIEW",
			useReplace: true,
		},
		{
			name:       "drop view",
			changeType: differ.ChangeTypeDropView,
			wantSQL:    "DROP VIEW",
			wantUnsafe: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var current, desired *schema.Database
			if tt.changeType == differ.ChangeTypeDropView {
				current = &schema.Database{Views: []schema.View{*view}}
				desired = &schema.Database{}
			} else {
				current = &schema.Database{}
				desired = &schema.Database{Views: []schema.View{*view}}
			}

			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{{Type: tt.changeType, ObjectName: "public.active_users"}},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)
			assert.Contains(t, stmt.SQL, tt.wantSQL)
			assert.Equal(t, tt.wantUnsafe, stmt.IsUnsafe)
		})
	}
}
