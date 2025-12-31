package differ_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_CommentNormalization_MultilineComments(t *testing.T) {
	t.Parallel()

	extractedComment := "Order status values:" +
		"- pending: Order has been placed but not processed" +
		"- processing: Order is currently being fulfilled" +
		"- shipped: Order has been sent to customer" +
		"- delivered: Order has reached the customer" +
		"- cancelled: Order was cancelled before completion"

	desiredComment := "Order status values:\n" +
		"- pending: Order has been placed but not processed\n" +
		"- processing: Order is currently being fulfilled\n" +
		"- shipped: Order has been sent to customer\n" +
		"- delivered: Order has reached the customer\n" +
		"- cancelled: Order was cancelled before completion"

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "orders",
				Columns: []schema.Column{
					{
						Name:     "status",
						DataType: "text",
						Position: 1,
						Comment:  extractedComment,
					},
				},
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: schema.DefaultSchema,
				Name:   "orders",
				Columns: []schema.Column{
					{
						Name:     "status",
						DataType: "text",
						Position: 1,
						Comment:  desiredComment,
					},
				},
			},
		},
	}

	d := differ.New(differ.DefaultOptions())
	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	commentChanges := result.GetChangesByType(differ.ChangeTypeModifyColumnComment)
	assert.Empty(
		t,
		commentChanges,
		"multiline comments should normalize to the same value and not trigger changes",
	)
}

func TestDiffer_CommentNormalization_TableComments(t *testing.T) {
	t.Parallel()

	extractedComment := "Stores customer orderswith status trackingand payment information."
	desiredComment := "Stores customer orders\nwith status tracking\nand payment information."

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "orders",
				Comment: extractedComment,
			},
		},
	}

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema:  schema.DefaultSchema,
				Name:    "orders",
				Comment: desiredComment,
			},
		},
	}

	d := differ.New(differ.DefaultOptions())
	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	commentChanges := result.GetChangesByType(differ.ChangeTypeModifyTableComment)
	assert.Empty(t, commentChanges, "multiline table comments should normalize to the same value")
}

func TestDiffer_CommentNormalization_FunctionComments(t *testing.T) {
	t.Parallel()

	extractedComment := "Updates the updated_at columnto current timestamp.Used by triggers." //cspell:disable-line
	desiredComment := "Updates the updated_at column\nto current timestamp.\nUsed by triggers."

	current := &schema.Database{
		Functions: []schema.Function{
			{
				Schema:        schema.DefaultSchema,
				Name:          "update_timestamp",
				ArgumentTypes: []string{},
				ReturnType:    "trigger",
				Language:      "plpgsql",
				Comment:       extractedComment,
			},
		},
	}

	desired := &schema.Database{
		Functions: []schema.Function{
			{
				Schema:        schema.DefaultSchema,
				Name:          "update_timestamp",
				ArgumentTypes: []string{},
				ReturnType:    "trigger",
				Language:      "plpgsql",
				Comment:       desiredComment,
			},
		},
	}

	d := differ.New(differ.DefaultOptions())
	result, err := d.Compare(current, desired)
	require.NoError(t, err)

	commentChanges := result.GetChangesByType(differ.ChangeTypeModifyFunction)
	hasCommentOnlyChange := false

	for _, change := range commentChanges {
		if change.Description == "Modify function comment: public.update_timestamp()" {
			hasCommentOnlyChange = true
			break
		}
	}

	assert.False(
		t,
		hasCommentOnlyChange,
		"multiline function comments should normalize to the same value",
	)
}
