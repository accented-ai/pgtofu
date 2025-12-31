package generator_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDDLBuilder_TriggerOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		changeType     differ.ChangeType
		trigger        *schema.Trigger
		wantSQL        []string
		wantUnsafe     bool
		wantRequiresTx bool
	}{
		{
			name:       "add BEFORE UPDATE trigger FOR EACH ROW",
			changeType: differ.ChangeTypeAddTrigger,
			trigger: &schema.Trigger{
				Schema:         schema.DefaultSchema,
				Name:           "set_updated_at",
				TableName:      "users",
				Timing:         "BEFORE",
				Events:         []string{"UPDATE"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "update_timestamp",
			},
			wantSQL: []string{
				"CREATE TRIGGER",
				"set_updated_at",
				"BEFORE UPDATE",
				"FOR EACH ROW",
				"EXECUTE FUNCTION public.UPDATE_TIMESTAMP",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add AFTER INSERT OR UPDATE trigger FOR EACH STATEMENT",
			changeType: differ.ChangeTypeAddTrigger,
			trigger: &schema.Trigger{
				Schema:         schema.DefaultSchema,
				Name:           "audit_log",
				TableName:      "orders",
				Timing:         "AFTER",
				Events:         []string{"INSERT", "UPDATE"},
				ForEachRow:     false,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "log_changes",
			},
			wantSQL: []string{
				"CREATE TRIGGER",
				"audit_log",
				"AFTER INSERT OR UPDATE",
				"FOR EACH STATEMENT",
				"EXECUTE FUNCTION public.LOG_CHANGES",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add INSTEAD OF trigger",
			changeType: differ.ChangeTypeAddTrigger,
			trigger: &schema.Trigger{
				Schema:         schema.DefaultSchema,
				Name:           "view_trigger",
				TableName:      "user_view",
				Timing:         "INSTEAD OF",
				Events:         []string{"INSERT"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "handle_view_insert",
			},
			wantSQL: []string{
				"CREATE TRIGGER",
				"view_trigger",
				"INSTEAD OF INSERT",
				"FOR EACH ROW",
				"EXECUTE FUNCTION public.HANDLE_VIEW_INSERT",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add trigger with WHEN condition",
			changeType: differ.ChangeTypeAddTrigger,
			trigger: &schema.Trigger{
				Schema:         schema.DefaultSchema,
				Name:           "conditional_trigger",
				TableName:      "orders",
				Timing:         "BEFORE",
				Events:         []string{"UPDATE"},
				ForEachRow:     true,
				WhenCondition:  "NEW.status = 'completed'",
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "handle_completion",
			},
			wantSQL: []string{
				"CREATE TRIGGER",
				"conditional_trigger",
				"WHEN",
				"NEW.status = 'completed'",
				"EXECUTE FUNCTION public.HANDLE_COMPLETION",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add trigger with multiple events",
			changeType: differ.ChangeTypeAddTrigger,
			trigger: &schema.Trigger{
				Schema:         schema.DefaultSchema,
				Name:           "multi_event_trigger",
				TableName:      "logs",
				Timing:         "AFTER",
				Events:         []string{"INSERT", "UPDATE", "DELETE"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "log_all_changes",
			},
			wantSQL: []string{
				"CREATE TRIGGER",
				"multi_event_trigger",
				"AFTER INSERT OR UPDATE OR DELETE",
				"FOR EACH ROW",
				"EXECUTE FUNCTION public.LOG_ALL_CHANGES",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add trigger with schema-qualified table",
			changeType: differ.ChangeTypeAddTrigger,
			trigger: &schema.Trigger{
				Schema:         "app",
				Name:           "schema_trigger",
				TableName:      "users",
				Timing:         "BEFORE",
				Events:         []string{"UPDATE"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "update_timestamp",
			},
			wantSQL:        []string{"CREATE TRIGGER", "schema_trigger", "ON app.users"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add trigger with schema-qualified function",
			changeType: differ.ChangeTypeAddTrigger,
			trigger: &schema.Trigger{
				Schema:         schema.DefaultSchema,
				Name:           "qualified_function_trigger",
				TableName:      "users",
				Timing:         "BEFORE",
				Events:         []string{"UPDATE"},
				ForEachRow:     true,
				FunctionSchema: "app",
				FunctionName:   "custom_function",
			},
			wantSQL: []string{
				"CREATE TRIGGER",
				"qualified_function_trigger",
				"EXECUTE FUNCTION app.CUSTOM_FUNCTION",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "drop trigger",
			changeType: differ.ChangeTypeDropTrigger,
			trigger: &schema.Trigger{
				Schema:         schema.DefaultSchema,
				Name:           "old_trigger",
				TableName:      "users",
				Timing:         "BEFORE",
				Events:         []string{"UPDATE"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "update_timestamp",
			},
			wantSQL: []string{
				"DROP TRIGGER",
				"IF EXISTS",
				"old_trigger",
				"ON public.users",
				"CASCADE",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "drop trigger with schema",
			changeType: differ.ChangeTypeDropTrigger,
			trigger: &schema.Trigger{
				Schema:         "app",
				Name:           "schema_trigger",
				TableName:      "users",
				Timing:         "BEFORE",
				Events:         []string{"UPDATE"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "update_timestamp",
			},
			wantSQL: []string{
				"DROP TRIGGER",
				"IF EXISTS",
				"schema_trigger",
				"ON app.users",
				"CASCADE",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var current, desired *schema.Database
			if tt.changeType == differ.ChangeTypeDropTrigger {
				current = &schema.Database{Triggers: []schema.Trigger{*tt.trigger}}
				desired = &schema.Database{}
			} else {
				current = &schema.Database{}
				desired = &schema.Database{Triggers: []schema.Trigger{*tt.trigger}}
			}

			objectName := fmt.Sprintf("%s.%s",
				strings.ToLower(tt.trigger.TableName),
				strings.ToLower(tt.trigger.Name))
			result := &differ.DiffResult{
				Current: current,
				Desired: desired,
				Changes: []differ.Change{{Type: tt.changeType, ObjectName: objectName}},
			}

			builder := generator.NewDDLBuilder(result, true)
			stmt, err := builder.BuildUpStatement(result.Changes[0])

			require.NoError(t, err)

			for _, want := range tt.wantSQL {
				assert.Contains(t, stmt.SQL, want)
			}

			assert.Equal(t, tt.wantUnsafe, stmt.IsUnsafe)
			assert.Equal(t, tt.wantRequiresTx, stmt.RequiresTx)
		})
	}
}

func TestDDLBuilder_TriggerIdempotent(t *testing.T) {
	t.Parallel()

	trigger := &schema.Trigger{
		Schema:         schema.DefaultSchema,
		Name:           "test_trigger",
		TableName:      "users",
		Timing:         "BEFORE",
		Events:         []string{"UPDATE"},
		ForEachRow:     true,
		FunctionSchema: schema.DefaultSchema,
		FunctionName:   "update_timestamp",
	}

	current := &schema.Database{Triggers: []schema.Trigger{*trigger}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type: differ.ChangeTypeDropTrigger,
				ObjectName: fmt.Sprintf(
					"%s.%s",
					strings.ToLower(trigger.TableName),
					strings.ToLower(trigger.Name),
				),
			},
		},
	}

	builderIdempotent := generator.NewDDLBuilder(result, true)
	builderNonIdempotent := generator.NewDDLBuilder(result, false)

	stmtIdempotent, err := builderIdempotent.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	stmtNonIdempotent, err := builderNonIdempotent.BuildUpStatement(result.Changes[0])
	require.NoError(t, err)

	assert.Contains(t, stmtIdempotent.SQL, "IF EXISTS")
	assert.NotContains(t, stmtNonIdempotent.SQL, "IF EXISTS")
}

func TestDDLBuilder_TriggerDownMigration(t *testing.T) {
	t.Parallel()

	trigger := &schema.Trigger{
		Schema:         schema.DefaultSchema,
		Name:           "reverted_trigger",
		TableName:      "users",
		Timing:         "BEFORE",
		Events:         []string{"UPDATE"},
		ForEachRow:     true,
		FunctionSchema: schema.DefaultSchema,
		FunctionName:   "update_timestamp",
	}

	current := &schema.Database{Triggers: []schema.Trigger{*trigger}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeAddTrigger,
				ObjectName: fmt.Sprintf("%s.%s", trigger.TableName, trigger.Name),
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "DROP TRIGGER")
	assert.Contains(t, stmt.SQL, "IF EXISTS")
	assert.Contains(t, stmt.SQL, "reverted_trigger")
}

func TestDDLBuilder_TriggerWithQuotedName(t *testing.T) {
	t.Parallel()

	trigger := &schema.Trigger{
		Schema:         schema.DefaultSchema,
		Name:           "Trigger-Name",
		TableName:      "users",
		Timing:         "BEFORE",
		Events:         []string{"UPDATE"},
		ForEachRow:     true,
		FunctionSchema: schema.DefaultSchema,
		FunctionName:   "update_timestamp",
	}

	current := &schema.Database{}
	desired := &schema.Database{Triggers: []schema.Trigger{*trigger}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeAddTrigger,
				ObjectName: fmt.Sprintf("%s.%s", trigger.TableName, trigger.Name),
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, `"Trigger-Name"`)
}

func TestDDLBuilder_AddTriggerWithQualifiedObjectName(t *testing.T) {
	t.Parallel()

	trigger := &schema.Trigger{
		Schema:         "app",
		Name:           "set_updated_at",
		TableName:      "users",
		Timing:         "BEFORE",
		Events:         []string{"UPDATE"},
		ForEachRow:     true,
		FunctionSchema: schema.DefaultSchema,
		FunctionName:   "update_updated_at_column",
	}

	current := &schema.Database{}
	desired := &schema.Database{Triggers: []schema.Trigger{*trigger}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeAddTrigger,
				ObjectName: "app.users.set_updated_at",
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "CREATE TRIGGER")
	assert.Contains(t, stmt.SQL, "set_updated_at")
	assert.Contains(t, stmt.SQL, "ON app.users")
}
