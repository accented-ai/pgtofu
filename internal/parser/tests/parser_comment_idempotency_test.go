package parser_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestCommentIdempotency(t *testing.T) {
	t.Parallel()

	sql := `
		CREATE TABLE app.users (
			id UUID PRIMARY KEY,
			email TEXT NOT NULL
		);

		COMMENT ON TABLE app.users IS
		'Stores user information and authentication details.';

		CREATE OR REPLACE FUNCTION update_updated_at_column()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = current_timestamp;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		COMMENT ON FUNCTION update_updated_at_column() IS
		'Automatically sets updated_at to current timestamp when a row is modified. '
		'Apply this trigger to any table with an updated_at column for automatic tracking.';
	`

	parsed := parseSQL(t, sql)

	extracted := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "app",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{Name: "email", DataType: "text", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:       "users_pkey",
						Type:       schema.ConstraintPrimaryKey,
						Columns:    []string{"id"},
						Definition: "PRIMARY KEY (id)",
					},
				},
				Indexes: []schema.Index{
					{
						Schema:    "app",
						TableName: "users",
						Name:      "users_pkey",
						Columns:   []string{"id"},
						Type:      "btree",
						IsUnique:  true,
						IsPrimary: true,
					},
				},
				Comment: "Stores user information and authentication details.",
			},
		},
		Functions: []schema.Function{
			{
				Schema:     schema.DefaultSchema,
				Name:       "update_updated_at_column",
				ReturnType: "trigger",
				Language:   "plpgsql",
				Body: `
BEGIN
	NEW.updated_at = current_timestamp;
	RETURN NEW;
END;
`,
				Comment: "Automatically sets updated_at to current timestamp when a row is modified. " +
					"Apply this trigger to any table with an updated_at column for automatic tracking.",
			},
		},
	}

	if len(parsed.Tables) == 0 {
		t.Fatal("No tables parsed")
	}

	if len(extracted.Tables) == 0 {
		t.Fatal("No tables in extracted schema")
	}

	parsedTableComment := parsed.Tables[0].Comment

	extractedTableComment := extracted.Tables[0].Comment
	if parsedTableComment != extractedTableComment {
		t.Errorf(
			"Table comment mismatch:\n  Parsed:    %q\n  Extracted: %q",
			parsedTableComment,
			extractedTableComment,
		)
	}

	if len(parsed.Functions) == 0 {
		t.Fatal("No functions parsed")
	}

	if len(extracted.Functions) == 0 {
		t.Fatal("No functions in extracted schema")
	}

	parsedFuncComment := parsed.Functions[0].Comment

	extractedFuncComment := extracted.Functions[0].Comment
	if parsedFuncComment != extractedFuncComment {
		t.Errorf(
			"Function comment mismatch:\n  Parsed:    %q\n  Extracted: %q",
			parsedFuncComment,
			extractedFuncComment,
		)
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(extracted, parsed)
	if err != nil {
		t.Fatalf("Compare error = %v", err)
	}

	for _, change := range result.Changes {
		if change.Type == differ.ChangeTypeModifyTableComment {
			t.Errorf("Unexpected MODIFY_TABLE_COMMENT change: %s", change.Description)
		}
	}
}
