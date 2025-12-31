package differ_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestDiffer_SerialSequence_Idempotency(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "onboarding",
				Name:   "onboarding_steps",
				Columns: []schema.Column{
					{
						Name:       "id",
						DataType:   "integer",
						IsNullable: false,
						Position:   1,
						Default:    "nextval('onboarding.onboarding_steps_id_seq'::regclass)",
					},
					{Name: "label", DataType: "text", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "onboarding_steps_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"id"},
					},
				},
			},
		},
		Sequences: []schema.Sequence{},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "onboarding",
				Name:   "onboarding_steps",
				Columns: []schema.Column{
					{
						Name:       "id",
						DataType:   "integer",
						IsNullable: false,
						Position:   1,
						Default:    "nextval('onboarding.onboarding_steps_id_seq'::regclass)",
					},
					{Name: "label", DataType: "text", IsNullable: false, Position: 2},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "onboarding_steps_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"id"},
					},
				},
			},
		},
		Sequences: []schema.Sequence{},
	}

	d := differ.New(nil)

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	if len(result.Changes) != 0 {
		t.Errorf("Expected no changes for SERIAL sequence, got %d changes:", len(result.Changes))

		for _, change := range result.Changes {
			t.Errorf("  [%s] %s: %s", change.Severity, change.Type, change.Description)
		}
	}
}

func TestDiffer_ExplicitSequence_NotFiltered(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "orders",
				Columns: []schema.Column{
					{
						Name:       "order_number",
						DataType:   "bigint",
						IsNullable: false,
						Position:   1,
						Default:    "nextval('public.order_number_seq'::regclass)",
					},
				},
			},
		},
		Sequences: []schema.Sequence{
			{
				Schema:     "public",
				Name:       "order_number_seq",
				DataType:   "bigint",
				StartValue: 1000,
				Increment:  1,
			},
		},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "orders",
				Columns: []schema.Column{
					{
						Name:       "order_number",
						DataType:   "bigint",
						IsNullable: false,
						Position:   1,
						Default:    "nextval('public.order_number_seq'::regclass)",
					},
				},
			},
		},
		Sequences: []schema.Sequence{},
	}

	d := differ.New(nil)

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	foundAddSequence := false

	for _, change := range result.Changes {
		if change.Type == differ.ChangeTypeAddSequence {
			foundAddSequence = true
			break
		}
	}

	if !foundAddSequence {
		t.Error("Expected ADD_SEQUENCE change for explicit sequence")
	}
}

func TestDiffer_BigSerialSequence_Idempotency(t *testing.T) {
	t.Parallel()

	desired := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{
						Name:       "id",
						DataType:   "bigint",
						IsNullable: false,
						Position:   1,
						Default:    "nextval('public.users_id_seq'::regclass)",
					},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "users_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"id"},
					},
				},
			},
		},
		Sequences: []schema.Sequence{},
	}

	current := &schema.Database{
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{
						Name:       "id",
						DataType:   "bigint",
						IsNullable: false,
						Position:   1,
						Default:    "nextval('public.users_id_seq'::regclass)",
					},
				},
				Constraints: []schema.Constraint{
					{
						Name:    "users_pkey",
						Type:    "PRIMARY KEY",
						Columns: []string{"id"},
					},
				},
			},
		},
		Sequences: []schema.Sequence{},
	}

	d := differ.New(nil)

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	if len(result.Changes) != 0 {
		t.Errorf("Expected no changes for BIGSERIAL sequence, got %d changes:", len(result.Changes))

		for _, change := range result.Changes {
			t.Errorf("  [%s] %s: %s", change.Severity, change.Type, change.Description)
		}
	}
}
