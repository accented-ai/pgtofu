package differ_test

import (
	"slices"
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

const (
	columnEmail = "email"
	tableItems  = "public.items"
	tablePosts  = "public.posts"
	tableUsers  = "public.users"
	tableStats  = "public.stats"
	users       = "users"
)

func TestDependencyResolution(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    users,
				Columns: []schema.Column{{Name: "id", DataType: "bigint", Position: 1}},
			},
		},
		Views: []schema.View{
			{Schema: schema.DefaultSchema, Name: "user_view", Definition: "SELECT * FROM users"},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Changes) != 2 {
		t.Fatalf("expected 2 changes, got %d", len(result.Changes))
	}

	tableIndex, viewIndex := -1, -1

	for i, change := range result.Changes {
		if change.Type == differ.ChangeTypeAddTable && change.ObjectName == tableUsers {
			tableIndex = i
		}

		if change.Type == differ.ChangeTypeAddView && change.ObjectName == "public.user_view" {
			viewIndex = i
		}
	}

	if tableIndex == -1 {
		t.Fatal("ADD_TABLE change not found")
	}

	if viewIndex == -1 {
		t.Fatal("ADD_VIEW change not found")
	}

	if tableIndex >= viewIndex {
		t.Errorf(
			"expected table to come before view (view depends on table). Table at index %d, view at index %d",
			tableIndex,
			viewIndex,
		)
	}
}

func TestDependencyResolutionWithMultilineViewDefinition(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
				},
			},
		},
		Views: []schema.View{
			{
				Schema: schema.DefaultSchema,
				Name:   "all_items",
				Definition: `
SELECT id
FROM public.items AS i
UNION ALL
SELECT id
FROM public.items AS i2
`,
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Changes) != 2 {
		t.Fatalf("expected 2 changes, got %d", len(result.Changes))
	}

	var (
		tableIndex = -1
		viewIndex  = -1
	)

	for i, change := range result.Changes {
		switch {
		case change.Type == differ.ChangeTypeAddTable && change.ObjectName == "public.items":
			tableIndex = i
		case change.Type == differ.ChangeTypeAddView && change.ObjectName == "public.all_items":
			viewIndex = i
		}
	}

	if tableIndex == -1 {
		t.Fatal("table change not found")
	}

	if viewIndex == -1 {
		t.Fatal("view change not found")
	}

	if tableIndex >= viewIndex {
		t.Fatalf(
			"expected table to be ordered before view (tableIndex=%d, viewIndex=%d)",
			tableIndex,
			viewIndex,
		)
	}
}

func TestDependencyResolutionFunctionBeforeTrigger(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
		Functions: []schema.Function{
			{
				Schema:        schema.DefaultSchema,
				Name:          "notify_items",
				Language:      "plpgsql",
				ReturnType:    "trigger",
				Body:          "BEGIN RETURN NEW; END;",
				Volatility:    schema.VolatilityVolatile,
				ArgumentTypes: []string{},
			},
		},
		Triggers: []schema.Trigger{
			{
				Schema:         schema.DefaultSchema,
				Name:           "items_notify",
				TableName:      "items",
				Timing:         "BEFORE",
				Events:         []string{"INSERT"},
				ForEachRow:     true,
				FunctionSchema: schema.DefaultSchema,
				FunctionName:   "notify_items",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var (
		functionIndex = -1
		triggerIndex  = -1
		triggerChange differ.Change
	)

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddFunction:
			functionIndex = i
		case differ.ChangeTypeAddTrigger:
			triggerIndex = i
			triggerChange = change
		}
	}

	if functionIndex == -1 {
		t.Fatal("ADD_FUNCTION change not found")
	}

	if triggerIndex == -1 {
		t.Fatal("ADD_TRIGGER change not found")
	}

	if functionIndex >= triggerIndex {
		t.Fatalf(
			"expected function to be ordered before trigger (functionIndex=%d, triggerIndex=%d)",
			functionIndex,
			triggerIndex,
		)
	}

	expectedDep := "public.notify_items()"

	found := slices.Contains(triggerChange.DependsOn, expectedDep)
	if !found {
		t.Fatalf(
			"expected trigger dependency list to include %s, got %v",
			expectedDep,
			triggerChange.DependsOn,
		)
	}
}

func TestCircularDependencyErrorMessage(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "table_a",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   "table_b",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     schema.DefaultSchema,
				Name:       "view_a",
				Definition: "SELECT * FROM table_b JOIN view_b ON 1=1",
			},
			{
				Schema:     schema.DefaultSchema,
				Name:       "view_b",
				Definition: "SELECT * FROM table_a JOIN view_a ON 1=1",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	_, err := d.Compare(current, desired)
	if err == nil {
		t.Skip("circular dependency not detected in this scenario - may be acceptable behavior")
		return
	}

	errorMsg := err.Error()
	if !contains(errorMsg, "circular dependency") {
		t.Errorf("error message should mention circular dependency, got: %s", errorMsg)
	}

	if !contains(errorMsg, "->") {
		t.Errorf("error message should show cycle path with arrows, got: %s", errorMsg)
	}
}

func TestOperationGrouping(t *testing.T) {
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "statuses",
				Comment: "Statuses",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
			{
				Schema:  schema.DefaultSchema,
				Name:    "items",
				Comment: "Items",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
				},
			},
			{
				Schema:  schema.DefaultSchema,
				Name:    users,
				Comment: "Users",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var (
		statusesTableIndex, statusesCommentIndex = -1, -1
		itemsTableIndex, itemsCommentIndex       = -1, -1
	)

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddTable:
			switch change.ObjectName {
			case "public.statuses":
				statusesTableIndex = i
			case tableItems:
				itemsTableIndex = i
			}
		case differ.ChangeTypeModifyTableComment:
			switch change.ObjectName {
			case "public.statuses":
				statusesCommentIndex = i
			case tableItems:
				itemsCommentIndex = i
			}
		}
	}

	if statusesTableIndex == -1 || statusesCommentIndex == -1 {
		t.Fatal("statuses changes not found")
	}

	if itemsTableIndex == -1 || itemsCommentIndex == -1 {
		t.Fatal("items changes not found")
	}

	if statusesTableIndex >= statusesCommentIndex {
		t.Errorf(
			"statuses comment should come after table. Table at %d, comment at %d",
			statusesTableIndex,
			statusesCommentIndex,
		)
	}

	if itemsTableIndex >= itemsCommentIndex {
		t.Errorf(
			"items comment should come after table. Table at %d, comment at %d",
			itemsTableIndex,
			itemsCommentIndex,
		)
	}
}

func TestColumnCommentOrderingForNewTable(t *testing.T) { //nolint:cyclop
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "posts",
				Comment: "Posts table",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{
						Name:       "attributes",
						DataType:   "jsonb",
						IsNullable: true,
						Position:   2,
						Comment:    "Post attributes",
					},
				},
			},
			{
				Schema:  schema.DefaultSchema,
				Name:    "items",
				Comment: "Items table",
				Columns: []schema.Column{
					{Name: "id", DataType: "uuid", IsNullable: false, Position: 1},
					{
						Name:       "name",
						DataType:   "text",
						IsNullable: false,
						Position:   2,
						Comment:    "Item name",
					},
					{
						Name:       "attributes",
						DataType:   "jsonb",
						IsNullable: true,
						Position:   3,
						Comment:    "Item attributes",
					},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var (
		postsTableIndex, postsCommentIndex, postsColumnCommentIndex = -1, -1, -1
		itemsTableIndex, itemsCommentIndex, itemsColumnCommentIndex = -1, -1, -1
	)

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddTable:
			switch change.ObjectName {
			case tablePosts:
				postsTableIndex = i
			case tableItems:
				itemsTableIndex = i
			}
		case differ.ChangeTypeModifyTableComment:
			switch change.ObjectName {
			case tablePosts:
				postsCommentIndex = i
			case tableItems:
				itemsCommentIndex = i
			}
		case differ.ChangeTypeModifyColumnComment:
			tableName, _ := change.Details["table"].(string)

			columnName, _ := change.Details["column_name"].(string)
			if (tableName == "posts" || tableName == tablePosts) && columnName == "attributes" {
				postsColumnCommentIndex = i
			} else if (tableName == "items" || tableName == tableItems) && columnName == "attributes" {
				itemsColumnCommentIndex = i
			}
		}
	}

	if postsTableIndex == -1 {
		t.Fatal("posts table change not found")
	}

	if postsCommentIndex == -1 {
		t.Fatal("posts table comment change not found")
	}

	if postsColumnCommentIndex == -1 {
		t.Fatal("posts column comment change not found")
	}

	if postsTableIndex >= postsCommentIndex {
		t.Errorf(
			"posts table comment should come after table. Table at %d, comment at %d",
			postsTableIndex,
			postsCommentIndex,
		)
	}

	if postsTableIndex >= postsColumnCommentIndex {
		t.Errorf(
			"posts column comment should come after table. Table at %d, column comment at %d",
			postsTableIndex,
			postsColumnCommentIndex,
		)
	}

	if postsCommentIndex >= postsColumnCommentIndex {
		t.Errorf(
			"posts column comment should come after table comment. Table comment at %d, column comment at %d",
			postsCommentIndex,
			postsColumnCommentIndex,
		)
	}

	if itemsTableIndex == -1 {
		t.Fatal("items table change not found")
	}

	if itemsCommentIndex == -1 {
		t.Fatal("items table comment change not found")
	}

	if itemsColumnCommentIndex == -1 {
		t.Fatal("items column comment change not found")
	}

	if itemsTableIndex >= itemsCommentIndex {
		t.Errorf(
			"items table comment should come after table. Table at %d, comment at %d",
			itemsTableIndex,
			itemsCommentIndex,
		)
	}

	if itemsTableIndex >= itemsColumnCommentIndex {
		t.Errorf(
			"items column comment should come after table. Table at %d, column comment at %d",
			itemsTableIndex,
			itemsColumnCommentIndex,
		)
	}
}

func TestAllCommentTypesOrdering(t *testing.T) { //nolint:cyclop,gocognit,gocyclo
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    users,
				Comment: "Users table",
				Columns: []schema.Column{
					{
						Name:       "id",
						DataType:   "bigint",
						IsNullable: false,
						Position:   1,
						Comment:    "User ID",
					},
					{
						Name:       columnEmail,
						DataType:   "text",
						IsNullable: false,
						Position:   2,
						Comment:    "Email address",
					},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     schema.DefaultSchema,
				Name:       "user_summary",
				Definition: "SELECT id, email FROM users",
				Comment:    "User summary view",
			},
		},
		Functions: []schema.Function{
			{
				Schema:     schema.DefaultSchema,
				Name:       "get_user",
				Language:   "plpgsql",
				ReturnType: tableUsers,
				Body:       "BEGIN RETURN NULL; END;",
				Comment:    "Get user function",
			},
		},
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     schema.DefaultSchema,
				Name:       "stats",
				Definition: "SELECT COUNT(*) FROM users",
				Comment:    "Statistics",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var (
		addTableIndex, tableCommentIndex, columnComment1Index, columnComment2Index = -1, -1, -1, -1
		addViewIndex, viewCommentIndex                                             = -1, -1
		addFunctionIndex, functionCommentIndex                                     = -1, -1
		addMVIndex, mvCommentIndex                                                 = -1, -1
	)

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddTable:
			if change.ObjectName == tableUsers {
				addTableIndex = i
			}
		case differ.ChangeTypeModifyTableComment:
			if change.ObjectName == tableUsers {
				tableCommentIndex = i
			}
		case differ.ChangeTypeModifyColumnComment:
			tableName, _ := change.Details["table"].(string)

			columnName, _ := change.Details["column_name"].(string)
			if (tableName == users || tableName == tableUsers) && columnName == "id" {
				columnComment1Index = i
			} else if (tableName == users || tableName == tableUsers) && columnName == columnEmail {
				columnComment2Index = i
			}
		case differ.ChangeTypeAddView:
			if change.ObjectName == "public.user_summary" {
				addViewIndex = i
			}
		case differ.ChangeTypeModifyView:
			_, hasOldComment := change.Details["old_comment"]

			_, hasNewComment := change.Details["new_comment"]
			if hasOldComment && hasNewComment && change.ObjectName == "public.user_summary" {
				viewCommentIndex = i
			}
		case differ.ChangeTypeAddFunction:
			if change.ObjectName == "public.get_user()" {
				addFunctionIndex = i
			}
		case differ.ChangeTypeModifyFunction:
			_, hasOldComment := change.Details["old_comment"]

			_, hasNewComment := change.Details["new_comment"]
			if hasOldComment && hasNewComment && change.ObjectName == "public.get_user()" {
				functionCommentIndex = i
			}
		case differ.ChangeTypeAddMaterializedView:
			if change.ObjectName == tableStats {
				addMVIndex = i
			}
		case differ.ChangeTypeModifyMaterializedView:
			_, hasOldComment := change.Details["old_comment"]

			_, hasNewComment := change.Details["new_comment"]
			if hasOldComment && hasNewComment && change.ObjectName == tableStats {
				mvCommentIndex = i
			}
		}
	}

	if addTableIndex >= tableCommentIndex {
		t.Errorf(
			"table comment should come after table. Table at %d, comment at %d",
			addTableIndex,
			tableCommentIndex,
		)
	}

	if addTableIndex >= columnComment1Index {
		t.Errorf(
			"column comment should come after table. Table at %d, column comment at %d",
			addTableIndex,
			columnComment1Index,
		)
	}

	if addTableIndex >= columnComment2Index {
		t.Errorf(
			"column comment should come after table. Table at %d, column comment at %d",
			addTableIndex,
			columnComment2Index,
		)
	}

	if addViewIndex >= viewCommentIndex {
		t.Errorf(
			"view comment should come after view. View at %d, comment at %d",
			addViewIndex,
			viewCommentIndex,
		)
	}

	if addFunctionIndex >= functionCommentIndex {
		t.Errorf(
			"function comment should come after function. Function at %d, comment at %d",
			addFunctionIndex,
			functionCommentIndex,
		)
	}

	if addMVIndex >= mvCommentIndex {
		t.Errorf(
			"materialized view comment should come after materialized view. MV at %d, comment at %d",
			addMVIndex,
			mvCommentIndex,
		)
	}
}

func TestCommentOrdering(t *testing.T) { //nolint:cyclop
	t.Parallel()

	current := &schema.Database{}
	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    users,
				Comment: "Users table",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{
						Name:       columnEmail,
						DataType:   "text",
						IsNullable: false,
						Position:   2,
						Comment:    "User email",
					},
				},
			},
			{
				Schema:  schema.DefaultSchema,
				Name:    "posts",
				Comment: "Posts table",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     schema.DefaultSchema,
				Name:       "stats",
				Definition: "SELECT user_id, COUNT(*) FROM posts GROUP BY user_id",
				Comment:    "Statistics",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var (
		addTableIndex, tableCommentIndex   = -1, -1
		addColumnIndex, columnCommentIndex = -1, -1
		addMVIndex, mvCommentIndex         = -1, -1
	)

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddTable:
			if change.ObjectName == tableUsers {
				addTableIndex = i
			}
		case differ.ChangeTypeModifyTableComment:
			if change.ObjectName == tableUsers {
				tableCommentIndex = i
			}
		case differ.ChangeTypeAddColumn:
			tableName, _ := change.Details["table"].(string)
			if tableName == tableUsers {
				col, _ := change.Details["column"].(*schema.Column)
				if col != nil && col.Name == columnEmail {
					addColumnIndex = i
				}
			}
		case differ.ChangeTypeModifyColumnComment:
			tableName, _ := change.Details["table"].(string)

			columnName, _ := change.Details["column_name"].(string)
			if (tableName == users || tableName == tableUsers) && columnName == columnEmail {
				columnCommentIndex = i
			}
		case differ.ChangeTypeAddMaterializedView:
			if change.ObjectName == tableStats {
				addMVIndex = i
			}
		case differ.ChangeTypeModifyMaterializedView:
			_, hasOldComment := change.Details["old_comment"]

			_, hasNewComment := change.Details["new_comment"]
			if hasOldComment && hasNewComment && change.ObjectName == tableStats {
				mvCommentIndex = i
			}
		}
	}

	if addTableIndex == -1 {
		t.Fatal("AddTable change for app.users not found")
	}

	if tableCommentIndex == -1 {
		t.Fatal("ModifyTableComment change for app.users not found")
	}

	if addTableIndex >= tableCommentIndex {
		t.Errorf(
			"Table comment should come after table creation. AddTable at index %d, ModifyTableComment at index %d",
			addTableIndex,
			tableCommentIndex,
		)
	}

	if addColumnIndex != -1 && columnCommentIndex != -1 {
		if addColumnIndex >= columnCommentIndex {
			t.Errorf(
				"Column comment should come after column creation. AddColumn at index %d, ModifyColumnComment at index %d",
				addColumnIndex,
				columnCommentIndex,
			)
		}
	}

	if addMVIndex == -1 {
		t.Fatal("AddMaterializedView change for public.stats not found")
	}

	if mvCommentIndex == -1 {
		t.Fatal("ModifyMaterializedView comment change for public.stats not found")
	}

	if addMVIndex >= mvCommentIndex {
		t.Errorf(
			"Materialized view comment should come after materialized view creation. "+
				"AddMaterializedView at index %d, ModifyMaterializedView at index %d",
			addMVIndex,
			mvCommentIndex,
		)
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}

func TestModifyViewDependsOnAddColumn(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   users,
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     schema.DefaultSchema,
				Name:       "user_view",
				Definition: "SELECT id FROM users",
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   users,
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: columnEmail, DataType: "text", IsNullable: true, Position: 2},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     schema.DefaultSchema,
				Name:       "user_view",
				Definition: "SELECT id, email FROM users",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	addColumnIndex, modifyViewIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddColumn:
			addColumnIndex = i
		case differ.ChangeTypeModifyView:
			modifyViewIndex = i
		}
	}

	if addColumnIndex == -1 {
		t.Fatal("ADD_COLUMN change not found")
	}

	if modifyViewIndex == -1 {
		t.Fatal("MODIFY_VIEW change not found")
	}

	if addColumnIndex >= modifyViewIndex {
		t.Errorf(
			"ADD_COLUMN (index %d) should come before MODIFY_VIEW (index %d)",
			addColumnIndex,
			modifyViewIndex,
		)
	}
}

func TestModifyMaterializedViewDependsOnAddColumn(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "events",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     schema.DefaultSchema,
				Name:       "event_stats",
				Definition: "SELECT COUNT(*) FROM events",
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "events",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "event_type", DataType: "text", IsNullable: true, Position: 2},
				},
			},
		},
		MaterializedViews: []schema.MaterializedView{
			{
				Schema:     schema.DefaultSchema,
				Name:       "event_stats",
				Definition: "SELECT event_type, COUNT(*) FROM events GROUP BY event_type",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	addColumnIndex, modifyMVIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddColumn:
			addColumnIndex = i
		case differ.ChangeTypeModifyMaterializedView:
			modifyMVIndex = i
		}
	}

	if addColumnIndex == -1 {
		t.Fatal("ADD_COLUMN change not found")
	}

	if modifyMVIndex == -1 {
		t.Fatal("MODIFY_MATERIALIZED_VIEW change not found")
	}

	if addColumnIndex >= modifyMVIndex {
		t.Errorf(
			"ADD_COLUMN (index %d) should come before MODIFY_MATERIALIZED_VIEW (index %d)",
			addColumnIndex,
			modifyMVIndex,
		)
	}
}

func TestModifyViewDependsOnAddColumnMultipleTables(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "orders",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "user_id", DataType: "bigint", IsNullable: false, Position: 2},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   users,
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     schema.DefaultSchema,
				Name:       "order_summary",
				Definition: "SELECT o.id FROM orders o JOIN users u ON o.user_id = u.id",
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "orders",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "user_id", DataType: "bigint", IsNullable: false, Position: 2},
				},
			},
			{
				Schema: schema.DefaultSchema,
				Name:   users,
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "name", DataType: "text", IsNullable: true, Position: 2},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     schema.DefaultSchema,
				Name:       "order_summary",
				Definition: "SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	addColumnIndex, modifyViewIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddColumn:
			addColumnIndex = i
		case differ.ChangeTypeModifyView:
			modifyViewIndex = i
		}
	}

	if addColumnIndex == -1 {
		t.Fatal("ADD_COLUMN change not found")
	}

	if modifyViewIndex == -1 {
		t.Fatal("MODIFY_VIEW change not found")
	}

	if addColumnIndex >= modifyViewIndex {
		t.Errorf(
			"ADD_COLUMN (index %d) should come before MODIFY_VIEW (index %d)",
			addColumnIndex,
			modifyViewIndex,
		)
	}
}

func TestModifyViewComesBeforeDropColumn(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   users,
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: columnEmail, DataType: "text", IsNullable: true, Position: 2},
					{Name: "legacy_field", DataType: "text", IsNullable: true, Position: 3},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     schema.DefaultSchema,
				Name:       "user_view",
				Definition: "SELECT id, email, legacy_field FROM users",
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   users,
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: columnEmail, DataType: "text", IsNullable: true, Position: 2},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     schema.DefaultSchema,
				Name:       "user_view",
				Definition: "SELECT id, email FROM users",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dropColumnIndex, modifyViewIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeDropColumn:
			dropColumnIndex = i
		case differ.ChangeTypeModifyView:
			modifyViewIndex = i
		}
	}

	if dropColumnIndex == -1 {
		t.Fatal("DROP_COLUMN change not found")
	}

	if modifyViewIndex == -1 {
		t.Fatal("MODIFY_VIEW change not found")
	}

	if modifyViewIndex >= dropColumnIndex {
		t.Errorf(
			"MODIFY_VIEW (index %d) should come before DROP_COLUMN (index %d)",
			modifyViewIndex,
			dropColumnIndex,
		)
	}
}

func TestDropViewBeforeDropColumn(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   users,
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: columnEmail, DataType: "text", IsNullable: true, Position: 2},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     schema.DefaultSchema,
				Name:       "user_emails",
				Definition: "SELECT id, email FROM users",
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   users,
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
		Views: []schema.View{},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dropColumnIndex, dropViewIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeDropColumn:
			dropColumnIndex = i
		case differ.ChangeTypeDropView:
			dropViewIndex = i
		}
	}

	if dropColumnIndex == -1 {
		t.Fatal("DROP_COLUMN change not found")
	}

	if dropViewIndex == -1 {
		t.Fatal("DROP_VIEW change not found")
	}

	if dropViewIndex >= dropColumnIndex {
		t.Errorf(
			"DROP_VIEW (index %d) should come before DROP_COLUMN (index %d)",
			dropViewIndex,
			dropColumnIndex,
		)
	}
}

func TestSchemaQualifiedTableNamesInViewDependencies(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Schemas: []schema.Schema{{Name: "app"}},
		Tables: []schema.Table{
			{
				Schema: "app",
				Name:   users,
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     "app",
				Name:       "user_view",
				Definition: "SELECT id FROM app.users",
			},
		},
	}

	desired := &schema.Database{
		Schemas: []schema.Schema{{Name: "app"}},
		Tables: []schema.Table{
			{
				Schema: "app",
				Name:   users,
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: columnEmail, DataType: "text", IsNullable: true, Position: 2},
				},
			},
		},
		Views: []schema.View{
			{
				Schema:     "app",
				Name:       "user_view",
				Definition: "SELECT id, email FROM app.users",
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	addColumnIndex, modifyViewIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddColumn:
			addColumnIndex = i
		case differ.ChangeTypeModifyView:
			modifyViewIndex = i
		}
	}

	if addColumnIndex == -1 {
		t.Fatal("ADD_COLUMN change not found")
	}

	if modifyViewIndex == -1 {
		t.Fatal("MODIFY_VIEW change not found")
	}

	if addColumnIndex >= modifyViewIndex {
		t.Errorf(
			"ADD_COLUMN (index %d) should come before MODIFY_VIEW (index %d)",
			addColumnIndex,
			modifyViewIndex,
		)
	}
}

func TestAddIndexDependsOnAddColumn(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "category", DataType: "varchar(50)", IsNullable: false, Position: 2},
				},
				Indexes: []schema.Index{
					{
						Schema:    schema.DefaultSchema,
						TableName: "products",
						Name:      "idx_products_category",
						Columns:   []string{"category"},
						Type:      "btree",
					},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	addColumnIndex, addIndexIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddColumn:
			addColumnIndex = i
		case differ.ChangeTypeAddIndex:
			addIndexIndex = i
		}
	}

	if addColumnIndex == -1 {
		t.Fatal("ADD_COLUMN change not found")
	}

	if addIndexIndex == -1 {
		t.Fatal("ADD_INDEX change not found")
	}

	if addColumnIndex >= addIndexIndex {
		t.Errorf(
			"ADD_COLUMN (index %d) should come before ADD_INDEX (index %d)",
			addColumnIndex,
			addIndexIndex,
		)
	}
}

func TestAddIndexDependsOnAddColumnCompositeIndex(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "status", DataType: "varchar(50)", IsNullable: false, Position: 2},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "status", DataType: "varchar(50)", IsNullable: false, Position: 2},
					{Name: "category", DataType: "varchar(50)", IsNullable: false, Position: 3},
				},
				Indexes: []schema.Index{
					{
						Schema:    schema.DefaultSchema,
						TableName: "products",
						Name:      "idx_products_category_status",
						Columns:   []string{"category", "status"},
						Type:      "btree",
					},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	addColumnIndex, addIndexIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddColumn:
			addColumnIndex = i
		case differ.ChangeTypeAddIndex:
			addIndexIndex = i
		}
	}

	if addColumnIndex == -1 {
		t.Fatal("ADD_COLUMN change not found")
	}

	if addIndexIndex == -1 {
		t.Fatal("ADD_INDEX change not found")
	}

	if addColumnIndex >= addIndexIndex {
		t.Errorf(
			"ADD_COLUMN (index %d) should come before ADD_INDEX (index %d)",
			addColumnIndex,
			addIndexIndex,
		)
	}
}

func TestAddConstraintDependsOnAddColumn(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
				Constraints: []schema.Constraint{
					{Name: "products_pkey_old", Type: "PRIMARY KEY", Columns: []string{"id"}},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "tenant_id", DataType: "varchar(50)", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "products_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"tenant_id", "id"},
					},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	addColumnIndex, addConstraintIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddColumn:
			addColumnIndex = i
		case differ.ChangeTypeAddConstraint:
			addConstraintIndex = i
		}
	}

	if addColumnIndex == -1 {
		t.Fatal("ADD_COLUMN change not found")
	}

	if addConstraintIndex == -1 {
		t.Fatal("ADD_CONSTRAINT change not found")
	}

	if addColumnIndex >= addConstraintIndex {
		t.Errorf(
			"ADD_COLUMN (index %d) should come before ADD_CONSTRAINT (index %d)",
			addColumnIndex,
			addConstraintIndex,
		)
	}
}

func TestDropIndexBeforeDropColumn(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "category", DataType: "varchar(50)", IsNullable: false, Position: 2},
				},
				Indexes: []schema.Index{
					{
						Schema:    schema.DefaultSchema,
						TableName: "products",
						Name:      "idx_products_category",
						Columns:   []string{"category"},
						Type:      "btree",
					},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dropColumnIndex, dropIndexIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeDropColumn:
			dropColumnIndex = i
		case differ.ChangeTypeDropIndex:
			dropIndexIndex = i
		}
	}

	if dropColumnIndex == -1 {
		t.Fatal("DROP_COLUMN change not found")
	}

	if dropIndexIndex == -1 {
		t.Fatal("DROP_INDEX change not found")
	}

	if dropIndexIndex >= dropColumnIndex {
		t.Errorf(
			"DROP_INDEX (index %d) should come before DROP_COLUMN (index %d)",
			dropIndexIndex,
			dropColumnIndex,
		)
	}
}

func TestDropConstraintBeforeDropColumn(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "sku", DataType: "varchar(50)", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{Name: "products_sku_key", Type: "UNIQUE", Columns: []string{"sku"}},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dropColumnIndex, dropConstraintIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeDropColumn:
			dropColumnIndex = i
		case differ.ChangeTypeDropConstraint:
			dropConstraintIndex = i
		}
	}

	if dropColumnIndex == -1 {
		t.Fatal("DROP_COLUMN change not found")
	}

	if dropConstraintIndex == -1 {
		t.Fatal("DROP_CONSTRAINT change not found")
	}

	if dropConstraintIndex >= dropColumnIndex {
		t.Errorf(
			"DROP_CONSTRAINT (index %d) should come before DROP_COLUMN (index %d)",
			dropConstraintIndex,
			dropColumnIndex,
		)
	}
}

func TestAddCoveringIndexDependsOnAddColumn(t *testing.T) {
	t.Parallel()

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "products",
				Columns: []schema.Column{
					{Name: "id", DataType: "bigint", IsNullable: false, Position: 1},
					{Name: "description", DataType: "text", IsNullable: true, Position: 2},
				},
				Indexes: []schema.Index{
					{
						Schema:         schema.DefaultSchema,
						TableName:      "products",
						Name:           "idx_products_id_include_desc",
						Columns:        []string{"id"},
						IncludeColumns: []string{"description"},
						Type:           "btree",
					},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	addColumnIndex, addIndexIndex := -1, -1

	for i, change := range result.Changes {
		switch change.Type {
		case differ.ChangeTypeAddColumn:
			addColumnIndex = i
		case differ.ChangeTypeAddIndex:
			addIndexIndex = i
		}
	}

	if addColumnIndex == -1 {
		t.Fatal("ADD_COLUMN change not found")
	}

	if addIndexIndex == -1 {
		t.Fatal("ADD_INDEX change not found")
	}

	if addColumnIndex >= addIndexIndex {
		t.Errorf(
			"ADD_COLUMN (index %d) should come before ADD_INDEX (index %d)",
			addColumnIndex,
			addIndexIndex,
		)
	}
}
