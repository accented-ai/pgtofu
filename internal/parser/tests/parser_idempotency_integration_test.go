package parser_test

import (
	"testing"

	"github.com/accented-ai/pgtofu/internal/differ"
)

func TestIdempotencyIntegration(t *testing.T) {
	t.Parallel()

	sql := `
		CREATE SCHEMA IF NOT EXISTS app;

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

	current := parseSQL(t, sql)
	desired := parseSQL(t, sql)

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(current, desired)
	if err != nil {
		t.Fatalf("Compare error = %v", err)
	}

	if len(result.Changes) > 0 {
		t.Errorf("Expected no changes, but got %d changes:", len(result.Changes))

		for _, change := range result.Changes {
			t.Logf("  [%s] %s: %s", change.Severity, change.Type, change.Description)
		}
	}

	if result.HasBreakingChanges() {
		t.Error("Expected no breaking changes, but HasBreakingChanges() returned true")
	}
}

func TestIdempotencyWithExplicitConstraintNames(t *testing.T) {
	t.Parallel()

	sqlWithExplicitNames := `
		CREATE TABLE app.users (
			id UUID DEFAULT UUID_GENERATE_V4() NOT NULL,
			email TEXT NOT NULL,
			CONSTRAINT users_pkey PRIMARY KEY (id),
			CONSTRAINT users_email_key UNIQUE (email)
		);
	`

	sqlWithInlineConstraints := `
		CREATE TABLE app.users (
			id UUID DEFAULT UUID_GENERATE_V4() NOT NULL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE
		);
	`

	dbExplicit := parseSQL(t, sqlWithExplicitNames)
	dbInline := parseSQL(t, sqlWithInlineConstraints)

	d := differ.New(differ.DefaultOptions())

	result, err := d.Compare(dbExplicit, dbInline)
	if err != nil {
		t.Fatalf("Compare error = %v", err)
	}

	if len(result.Changes) > 0 {
		t.Errorf(
			"Expected no changes between explicit and inline constraints, but got %d changes:",
			len(result.Changes),
		)

		for _, change := range result.Changes {
			t.Logf("  [%s] %s: %s", change.Severity, change.Type, change.Description)
		}
	}
}
