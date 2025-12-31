package parser_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestIdempotencyConstraintNames(t *testing.T) { //nolint:cyclop,gocognit,gocyclo,maintidx
	t.Parallel()

	sql := `
		CREATE TABLE app.statuses (
			label TEXT NOT NULL PRIMARY KEY,
			description TEXT NOT NULL
		);

		CREATE TABLE app.users (
			id UUID DEFAULT UUID_GENERATE_V4() NOT NULL PRIMARY KEY,
			username TEXT NOT NULL UNIQUE,
			email TEXT NOT NULL UNIQUE,
			status TEXT NOT NULL REFERENCES app.statuses (label) DEFAULT 'new',
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`

	db := parseSQL(t, sql)

	if len(db.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(db.Tables))
	}

	statuses := db.GetTable("app", "statuses")
	if statuses == nil {
		t.Fatal("statuses table not found")
	}

	if len(statuses.Constraints) != 1 {
		t.Fatalf("statuses: expected 1 constraint, got %d", len(statuses.Constraints))
	}

	pkConstraint := statuses.Constraints[0]
	if pkConstraint.Name != "statuses_pkey" {
		t.Errorf("PRIMARY KEY constraint name = %v, want statuses_pkey", pkConstraint.Name)
	}

	if pkConstraint.Type != "PRIMARY KEY" {
		t.Errorf("constraint type = %v, want PRIMARY KEY", pkConstraint.Type)
	}

	if pkConstraint.Definition != "PRIMARY KEY (label)" {
		t.Errorf("constraint definition = %v, want PRIMARY KEY (label)", pkConstraint.Definition)
	}

	if len(statuses.Indexes) != 1 {
		t.Fatalf("statuses: expected 1 index, got %d", len(statuses.Indexes))
	}

	pkIndex := statuses.Indexes[0]
	if pkIndex.Name != "statuses_pkey" {
		t.Errorf("PRIMARY KEY index name = %v, want statuses_pkey", pkIndex.Name)
	}

	if !pkIndex.IsPrimary {
		t.Error("index IsPrimary = false, want true")
	}

	if !pkIndex.IsUnique {
		t.Error("index IsUnique = false, want true")
	}

	users := db.GetTable("app", "users")
	if users == nil {
		t.Fatal("users table not found")
	}

	expectedConstraints := map[string]struct {
		constraintType string
		columns        []string
		definition     string
	}{
		"users_pkey": {
			constraintType: "PRIMARY KEY",
			columns:        []string{"id"},
			definition:     "PRIMARY KEY (id)",
		},
		"users_username_key": {
			constraintType: "UNIQUE",
			columns:        []string{"username"},
			definition:     "UNIQUE (username)",
		},
		"users_email_key": {
			constraintType: "UNIQUE",
			columns:        []string{"email"},
			definition:     "UNIQUE (email)",
		},
		"users_status_fkey": {
			constraintType: schema.ForeignKey,
			columns:        []string{"status"},
			definition:     "FOREIGN KEY (status) REFERENCES app.statuses(label)",
		},
	}

	if len(users.Constraints) != len(expectedConstraints) {
		t.Fatalf(
			"users: expected %d constraints, got %d",
			len(expectedConstraints),
			len(users.Constraints),
		)
	}

	for _, constraint := range users.Constraints {
		expected, ok := expectedConstraints[constraint.Name]
		if !ok {
			t.Errorf("unexpected constraint: %s", constraint.Name)
			continue
		}

		if constraint.Type != expected.constraintType {
			t.Errorf(
				"constraint %s: type = %v, want %v",
				constraint.Name,
				constraint.Type,
				expected.constraintType,
			)
		}

		if len(constraint.Columns) != len(expected.columns) {
			t.Errorf(
				"constraint %s: columns = %v, want %v",
				constraint.Name,
				constraint.Columns,
				expected.columns,
			)
		} else {
			for i, col := range constraint.Columns {
				if col != expected.columns[i] {
					t.Errorf(
						"constraint %s: column[%d] = %v, want %v",
						constraint.Name,
						i,
						col,
						expected.columns[i],
					)
				}
			}
		}

		if constraint.Definition != expected.definition {
			t.Errorf(
				"constraint %s: definition = %v, want %v",
				constraint.Name,
				constraint.Definition,
				expected.definition,
			)
		}

		if constraint.Type == schema.ForeignKey {
			if constraint.OnDelete != schema.NoAction {
				t.Errorf(
					"constraint %s: OnDelete = %v, want NO ACTION",
					constraint.Name,
					constraint.OnDelete,
				)
			}

			if constraint.OnUpdate != schema.NoAction {
				t.Errorf(
					"constraint %s: OnUpdate = %v, want NO ACTION",
					constraint.Name,
					constraint.OnUpdate,
				)
			}

			if constraint.ReferencedSchema != "app" {
				t.Errorf(
					"constraint %s: ReferencedSchema = %v, want app",
					constraint.Name,
					constraint.ReferencedSchema,
				)
			}

			if constraint.ReferencedTable != "statuses" {
				t.Errorf(
					"constraint %s: ReferencedTable = %v, want statuses",
					constraint.Name,
					constraint.ReferencedTable,
				)
			}
		}
	}

	expectedIndexes := map[string]struct {
		isPrimary bool
		isUnique  bool
		columns   []string
	}{
		"users_pkey": {
			isPrimary: true,
			isUnique:  true,
			columns:   []string{"id"},
		},
		"users_username_key": {
			isPrimary: false,
			isUnique:  true,
			columns:   []string{"username"},
		},
		"users_email_key": {
			isPrimary: false,
			isUnique:  true,
			columns:   []string{"email"},
		},
	}

	if len(users.Indexes) != len(expectedIndexes) {
		t.Fatalf("users: expected %d indexes, got %d", len(expectedIndexes), len(users.Indexes))
	}

	for _, index := range users.Indexes {
		expected, ok := expectedIndexes[index.Name]
		if !ok {
			t.Errorf("unexpected index: %s", index.Name)
			continue
		}

		if index.IsPrimary != expected.isPrimary {
			t.Errorf(
				"index %s: IsPrimary = %v, want %v",
				index.Name,
				index.IsPrimary,
				expected.isPrimary,
			)
		}

		if index.IsUnique != expected.isUnique {
			t.Errorf(
				"index %s: IsUnique = %v, want %v",
				index.Name,
				index.IsUnique,
				expected.isUnique,
			)
		}

		if index.Type != "btree" {
			t.Errorf("index %s: Type = %v, want btree", index.Name, index.Type)
		}

		if len(index.Columns) != len(expected.columns) {
			t.Errorf("index %s: columns = %v, want %v", index.Name, index.Columns, expected.columns)
		}
	}
}
