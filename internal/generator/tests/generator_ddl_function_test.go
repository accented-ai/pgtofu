package generator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDDLBuilder_FunctionOperations(t *testing.T) { //nolint:maintidx
	t.Parallel()

	tests := []struct {
		name           string
		changeType     differ.ChangeType
		function       *schema.Function
		wantSQL        []string
		wantUnsafe     bool
		wantRequiresTx bool
	}{
		{
			name:       "add function with plpgsql",
			changeType: differ.ChangeTypeAddFunction,
			function: &schema.Function{
				Schema:        schema.DefaultSchema,
				Name:          "update_timestamp",
				ArgumentTypes: []string{},
				ReturnType:    "trigger",
				Language:      "plpgsql",
				Body:          "$$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$",
			},
			wantSQL: []string{
				"CREATE OR REPLACE FUNCTION",
				"public.UPDATE_TIMESTAMP",
				"RETURNS trigger",
				"LANGUAGE plpgsql",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add function with arguments",
			changeType: differ.ChangeTypeAddFunction,
			function: &schema.Function{
				Schema:        schema.DefaultSchema,
				Name:          "calculate_total",
				ArgumentTypes: []string{"numeric", "numeric"},
				ArgumentNames: []string{"price", "tax"},
				ReturnType:    "numeric",
				Language:      "sql",
				Body:          "$$ SELECT price + tax; $$",
			},
			wantSQL: []string{
				"CREATE OR REPLACE FUNCTION",
				"public.CALCULATE_TOTAL",
				"price",
				"tax",
				"RETURNS numeric",
			},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add function with IMMUTABLE volatility",
			changeType: differ.ChangeTypeAddFunction,
			function: &schema.Function{
				Schema:        schema.DefaultSchema,
				Name:          "get_constant",
				ArgumentTypes: []string{},
				ReturnType:    "text",
				Language:      "sql",
				Volatility:    "IMMUTABLE",
				Body:          "$$ SELECT 'constant'; $$",
			},
			wantSQL:        []string{"CREATE OR REPLACE FUNCTION", "IMMUTABLE"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add function with STABLE volatility",
			changeType: differ.ChangeTypeAddFunction,
			function: &schema.Function{
				Schema:        schema.DefaultSchema,
				Name:          "get_current_user",
				ArgumentTypes: []string{},
				ReturnType:    "text",
				Language:      "sql",
				Volatility:    "STABLE",
				Body:          "$$ SELECT current_user; $$",
			},
			wantSQL:        []string{"CREATE OR REPLACE FUNCTION", "STABLE"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add function with SECURITY DEFINER",
			changeType: differ.ChangeTypeAddFunction,
			function: &schema.Function{
				Schema:            schema.DefaultSchema,
				Name:              "admin_function",
				ArgumentTypes:     []string{},
				ReturnType:        "void",
				Language:          "plpgsql",
				IsSecurityDefiner: true,
				Body:              "$$ BEGIN PERFORM admin_task(); END; $$",
			},
			wantSQL:        []string{"CREATE OR REPLACE FUNCTION", "SECURITY DEFINER"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add function with STRICT",
			changeType: differ.ChangeTypeAddFunction,
			function: &schema.Function{
				Schema:        schema.DefaultSchema,
				Name:          "strict_function",
				ArgumentTypes: []string{"text"},
				ReturnType:    "text",
				Language:      "sql",
				IsStrict:      true,
				Body:          "$$ SELECT $1; $$",
			},
			wantSQL:        []string{"CREATE OR REPLACE FUNCTION", "STRICT"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add function with OUT parameters",
			changeType: differ.ChangeTypeAddFunction,
			function: &schema.Function{
				Schema:        schema.DefaultSchema,
				Name:          "get_user_info",
				ArgumentTypes: []string{"bigint"},
				ArgumentNames: []string{"user_id"},
				ArgumentModes: []string{"OUT"},
				ReturnType:    "record",
				Language:      "plpgsql",
				Body:          "$$ BEGIN SELECT * INTO user_info FROM users WHERE id = user_id; END; $$",
			},
			wantSQL:        []string{"CREATE OR REPLACE FUNCTION", "public.GET_USER_INFO"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "add function with comment",
			changeType: differ.ChangeTypeAddFunction,
			function: &schema.Function{
				Schema:        schema.DefaultSchema,
				Name:          "commented_function",
				ArgumentTypes: []string{},
				ReturnType:    "void",
				Language:      "plpgsql",
				Comment:       "This function does something useful",
				Body:          "$$ BEGIN NULL; END; $$",
			},
			wantSQL:        []string{"CREATE OR REPLACE FUNCTION", "public.COMMENTED_FUNCTION"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "drop function",
			changeType: differ.ChangeTypeDropFunction,
			function: &schema.Function{
				Schema:        schema.DefaultSchema,
				Name:          "old_function",
				ArgumentTypes: []string{},
				ReturnType:    "void",
				Language:      "plpgsql",
				Body:          "$$ BEGIN NULL; END; $$",
			},
			wantSQL: []string{
				"DROP FUNCTION",
				"IF EXISTS",
				"public.old_function",
				"CASCADE",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "drop function with arguments",
			changeType: differ.ChangeTypeDropFunction,
			function: &schema.Function{
				Schema:        schema.DefaultSchema,
				Name:          "overloaded_function",
				ArgumentTypes: []string{"text"},
				ReturnType:    "text",
				Language:      "sql",
				Body:          "$$ SELECT $1; $$",
			},
			wantSQL: []string{
				"DROP FUNCTION",
				"IF EXISTS",
				"public.overloaded_function",
				"text",
			},
			wantUnsafe:     true,
			wantRequiresTx: true,
		},
		{
			name:       "modify function body",
			changeType: differ.ChangeTypeModifyFunction,
			function: &schema.Function{
				Schema:        schema.DefaultSchema,
				Name:          "updated_function",
				ArgumentTypes: []string{},
				ReturnType:    "void",
				Language:      "plpgsql",
				Body:          "$$ BEGIN PERFORM new_logic(); END; $$",
			},
			wantSQL:        []string{"CREATE OR REPLACE FUNCTION", "public.UPDATED_FUNCTION"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "modify function comment only",
			changeType: differ.ChangeTypeModifyFunction,
			function: &schema.Function{
				Schema:        schema.DefaultSchema,
				Name:          "function_with_comment",
				ArgumentTypes: []string{},
				ReturnType:    "void",
				Language:      "plpgsql",
				Comment:       "Updated comment",
				Body:          "$$ BEGIN NULL; END; $$",
			},
			wantSQL:        []string{"COMMENT ON FUNCTION", "public.FUNCTION_WITH_COMMENT"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
		{
			name:       "modify function with schema",
			changeType: differ.ChangeTypeAddFunction,
			function: &schema.Function{
				Schema:        "app",
				Name:          "schema_function",
				ArgumentTypes: []string{},
				ReturnType:    "void",
				Language:      "plpgsql",
				Body:          "$$ BEGIN NULL; END; $$",
			},
			wantSQL:        []string{"CREATE OR REPLACE FUNCTION", "app.SCHEMA_FUNCTION"},
			wantUnsafe:     false,
			wantRequiresTx: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var current, desired *schema.Database
			if tt.changeType == differ.ChangeTypeDropFunction {
				current = &schema.Database{Functions: []schema.Function{*tt.function}}
				desired = &schema.Database{}
			} else {
				current = &schema.Database{}
				desired = &schema.Database{Functions: []schema.Function{*tt.function}}
			}

			objectName := differ.FunctionKey(
				tt.function.Schema,
				tt.function.Name,
				tt.function.ArgumentTypes,
			)
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

func TestDDLBuilder_FunctionModifyWithCommentChange(t *testing.T) {
	t.Parallel()

	fn := &schema.Function{
		Schema:        schema.DefaultSchema,
		Name:          "test_function",
		ArgumentTypes: []string{},
		ReturnType:    "void",
		Language:      "plpgsql",
		Comment:       "New comment",
		Body:          "$$ BEGIN NULL; END; $$",
	}

	current := &schema.Database{
		Functions: []schema.Function{
			{
				Schema:        schema.DefaultSchema,
				Name:          "test_function",
				ArgumentTypes: []string{},
				ReturnType:    "void",
				Language:      "plpgsql",
				Comment:       "Old comment",
				Body:          "$$ BEGIN NULL; END; $$",
			},
		},
	}
	desired := &schema.Database{Functions: []schema.Function{*fn}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeModifyFunction,
				ObjectName: differ.FunctionKey(fn.Schema, fn.Name, fn.ArgumentTypes),
				Details: map[string]any{
					"old_comment": "Old comment",
					"new_comment": "New comment",
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "COMMENT ON FUNCTION public.TEST_FUNCTION")
	assert.Contains(t, stmt.SQL, "New comment")
	assert.NotContains(t, stmt.SQL, "CREATE OR REPLACE")
}

func TestDDLBuilder_FunctionDropComment(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Functions: []schema.Function{
			{
				Schema:        schema.DefaultSchema,
				Name:          "commented_function",
				ArgumentTypes: []string{},
				ReturnType:    "void",
				Language:      "plpgsql",
				Comment:       "Old comment",
				Body:          "$$ BEGIN NULL; END; $$",
			},
		},
	}
	desired := &schema.Database{
		Functions: []schema.Function{
			{
				Schema:        schema.DefaultSchema,
				Name:          "commented_function",
				ArgumentTypes: []string{},
				ReturnType:    "void",
				Language:      "plpgsql",
				Comment:       "",
				Body:          "$$ BEGIN NULL; END; $$",
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type: differ.ChangeTypeModifyFunction,
				ObjectName: differ.FunctionKey(
					schema.DefaultSchema,
					"commented_function",
					[]string{},
				),
				Details: map[string]any{
					"old_comment": "Old comment",
					"new_comment": "",
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "COMMENT ON FUNCTION")
	assert.Contains(t, stmt.SQL, "IS NULL")
}

func TestDDLBuilder_FunctionIdempotent(t *testing.T) {
	t.Parallel()

	fn := &schema.Function{
		Schema:        schema.DefaultSchema,
		Name:          "test_function",
		ArgumentTypes: []string{},
		ReturnType:    "void",
		Language:      "plpgsql",
		Body:          "$$ BEGIN NULL; END; $$",
	}

	current := &schema.Database{Functions: []schema.Function{*fn}}
	desired := &schema.Database{}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeDropFunction,
				ObjectName: differ.FunctionKey(fn.Schema, fn.Name, fn.ArgumentTypes),
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

func TestDDLBuilder_FunctionRevertModify(t *testing.T) {
	t.Parallel()

	currentFn := &schema.Function{
		Schema:        schema.DefaultSchema,
		Name:          "reverted_function",
		ArgumentTypes: []string{},
		ReturnType:    "void",
		Language:      "plpgsql",
		Body:          "$$ BEGIN OLD_LOGIC(); END; $$",
	}

	current := &schema.Database{Functions: []schema.Function{*currentFn}}
	desired := &schema.Database{
		Functions: []schema.Function{
			{
				Schema:        schema.DefaultSchema,
				Name:          "reverted_function",
				ArgumentTypes: []string{},
				ReturnType:    "void",
				Language:      "plpgsql",
				Body:          "$$ BEGIN NEW_LOGIC(); END; $$",
			},
		},
	}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type: differ.ChangeTypeModifyFunction,
				ObjectName: differ.FunctionKey(
					currentFn.Schema,
					currentFn.Name,
					currentFn.ArgumentTypes,
				),
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "CREATE OR REPLACE FUNCTION")
	assert.Contains(t, stmt.SQL, "OLD_LOGIC")
	assert.NotContains(t, stmt.SQL, "NEW_LOGIC")
}

func TestDDLBuilder_FunctionWithSpecialCharactersInComment(t *testing.T) {
	t.Parallel()

	fn := &schema.Function{
		Schema:        schema.DefaultSchema,
		Name:          "special_function",
		ArgumentTypes: []string{},
		ReturnType:    "void",
		Language:      "plpgsql",
		Comment:       "Function's comment with 'quotes' and \"double quotes\"",
		Body:          "$$ BEGIN NULL; END; $$",
	}

	current := &schema.Database{
		Functions: []schema.Function{
			{
				Schema:        schema.DefaultSchema,
				Name:          "special_function",
				ArgumentTypes: []string{},
				ReturnType:    "void",
				Language:      "plpgsql",
				Comment:       "Old comment",
				Body:          "$$ BEGIN NULL; END; $$",
			},
		},
	}
	desired := &schema.Database{Functions: []schema.Function{*fn}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeModifyFunction,
				ObjectName: differ.FunctionKey(fn.Schema, fn.Name, fn.ArgumentTypes),
				Details: map[string]any{
					"old_comment": "Old comment",
					"new_comment": fn.Comment,
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildUpStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "COMMENT ON FUNCTION")
	quotesCount := strings.Count(stmt.SQL, "'")
	assert.GreaterOrEqual(t, quotesCount, 2, "SQL should properly escape single quotes")
}

func TestDDLBuilder_FunctionDownForAddedFunction(t *testing.T) {
	t.Parallel()

	fn := &schema.Function{
		Schema:        schema.DefaultSchema,
		Name:          "validate_content_hierarchy",
		ArgumentTypes: []string{},
		ReturnType:    "trigger",
		Language:      "plpgsql",
		Body:          "$$ BEGIN RETURN NEW; END; $$",
	}

	current := &schema.Database{}
	desired := &schema.Database{Functions: []schema.Function{*fn}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeAddFunction,
				ObjectName: differ.FunctionKey(fn.Schema, fn.Name, fn.ArgumentTypes),
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "DROP FUNCTION")
	assert.Contains(t, stmt.SQL, "IF EXISTS")
	assert.Contains(t, stmt.SQL, fn.Name)
}

func TestDDLBuilder_FunctionRevertNewComment(t *testing.T) {
	t.Parallel()

	fn := &schema.Function{
		Schema:        schema.DefaultSchema,
		Name:          "update_updated_at_column",
		ArgumentTypes: []string{},
		ReturnType:    "trigger",
		Language:      "plpgsql",
		Comment:       "Automatically sets updated_at",
		Body:          "$$ BEGIN RETURN NEW; END; $$",
	}

	current := &schema.Database{Functions: []schema.Function{{
		Schema:        schema.DefaultSchema,
		Name:          "update_updated_at_column",
		ArgumentTypes: []string{},
		ReturnType:    "trigger",
		Language:      "plpgsql",
		Comment:       "",
		Body:          "$$ BEGIN RETURN NEW; END; $$",
	}}}
	desired := &schema.Database{Functions: []schema.Function{*fn}}

	result := &differ.DiffResult{
		Current: current,
		Desired: desired,
		Changes: []differ.Change{
			{
				Type:       differ.ChangeTypeModifyFunction,
				ObjectName: differ.FunctionKey(fn.Schema, fn.Name, fn.ArgumentTypes),
				Details: map[string]any{
					"old_comment": "",
					"new_comment": fn.Comment,
				},
			},
		},
	}

	builder := generator.NewDDLBuilder(result, true)
	stmt, err := builder.BuildDownStatement(result.Changes[0])

	require.NoError(t, err)
	assert.Contains(t, stmt.SQL, "COMMENT ON FUNCTION")
	assert.Contains(t, stmt.SQL, "IS NULL")
}

func TestDownSkipsFunctionCommentWhenDroppingFunction(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Functions: []schema.Function{
			{
				Schema:        schema.DefaultSchema,
				Name:          "f",
				Language:      "sql",
				Definition:    "SELECT 1",
				ArgumentTypes: nil,
				Comment:       "hello",
			},
		},
	}
	current := &schema.Database{}

	res := &differ.DiffResult{Current: current, Desired: desired}
	fn := &desired.Functions[0]
	fnKey := differ.FunctionKey(fn.Schema, fn.Name, fn.ArgumentTypes)
	res.Changes = append(
		res.Changes,
		differ.Change{
			Type:        differ.ChangeTypeAddFunction,
			ObjectName:  fnKey,
			Description: "Add function: public.f()",
		},
		differ.Change{
			Type:       differ.ChangeTypeModifyFunction,
			ObjectName: fnKey,
			Details:    map[string]any{"old_comment": "", "new_comment": "hello"},
		},
	)

	opts := testOptions()
	opts.GenerateDownMigrations = true
	g := generator.New(opts)

	gen, err := g.Generate(res)
	require.NoError(t, err)
	require.Len(t, gen.Migrations, 1)
	require.NotNil(t, gen.Migrations[0].DownFile)
	out := gen.Migrations[0].DownFile.Content

	if strings.Contains(out, "COMMENT ON FUNCTION public.f() IS NULL") {
		t.Fatalf(
			"down migration should not contain function comment revert when function is dropped; got:\n%s",
			out,
		)
	}

	if !strings.Contains(out, "DROP FUNCTION") {
		t.Fatalf("down migration should drop the function; got:\n%s", out)
	}
}
