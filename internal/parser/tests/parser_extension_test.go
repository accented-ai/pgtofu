package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestParseCreateExtension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sql           string
		wantExtension string
		wantSchema    string
		wantVersion   string
		shouldHaveErr bool
	}{
		{
			name:          "simple extension",
			sql:           `CREATE EXTENSION pg_trgm;`,
			wantExtension: "pg_trgm",
			wantSchema:    "",
		},
		{
			name:          "extension with IF NOT EXISTS",
			sql:           `CREATE EXTENSION IF NOT EXISTS pg_trgm;`,
			wantExtension: "pg_trgm",
			wantSchema:    "",
		},
		{
			name:          "quoted extension name",
			sql:           `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`,
			wantExtension: "uuid-ossp",
			wantSchema:    "",
		},
		{
			name:          "extension with schema",
			sql:           `CREATE EXTENSION pg_trgm SCHEMA extensions;`,
			wantExtension: "pg_trgm",
			wantSchema:    "extensions",
		},
		{
			name:          "extension with WITH SCHEMA clause",
			sql:           `CREATE EXTENSION pg_trgm WITH SCHEMA public;`,
			wantExtension: "pg_trgm",
			wantSchema:    "public",
		},
		{
			name:          "extension with equals schema clause",
			sql:           `CREATE EXTENSION pg_trgm WITH SCHEMA = "Extensions";`,
			wantExtension: "pg_trgm",
			wantSchema:    "extensions",
		},
		{
			name:          "extension with version",
			sql:           `CREATE EXTENSION postgis VERSION '3.3.0';`,
			wantExtension: "postgis",
			wantVersion:   "3.3.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := parser.New()
			db := &schema.Database{}

			err := p.ParseSQL(tt.sql, db)

			if tt.shouldHaveErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.wantExtension != "" {
				require.Len(t, db.Extensions, 1, "should have exactly one extension")
				ext := db.Extensions[0]
				assert.Equal(t, tt.wantExtension, ext.Name)

				if tt.wantSchema != "" {
					assert.Equal(t, tt.wantSchema, ext.Schema)
				}

				if tt.wantVersion != "" {
					assert.Equal(t, tt.wantVersion, ext.Version)
				}
			}
		})
	}
}

func TestParseCreateSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sql           string
		wantSchema    string
		shouldHaveErr bool
	}{
		{
			name:       "simple schema",
			sql:        `CREATE SCHEMA app;`,
			wantSchema: "app",
		},
		{
			name:       "schema with IF NOT EXISTS",
			sql:        `CREATE SCHEMA IF NOT EXISTS app;`,
			wantSchema: "app",
		},
		{
			name:       "quoted schema name",
			sql:        `CREATE SCHEMA IF NOT EXISTS "my-schema";`,
			wantSchema: "my-schema",
		},
		{
			name:       "multiple schemas",
			sql:        `CREATE SCHEMA app; CREATE SCHEMA shop;`,
			wantSchema: "shop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := parser.New()
			db := &schema.Database{}

			err := p.ParseSQL(tt.sql, db)

			if tt.shouldHaveErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Empty(t, p.GetWarnings(), "should not have warnings for CREATE SCHEMA")

			if tt.wantSchema != "" {
				found := false

				for _, sch := range db.Schemas {
					if sch.Name == tt.wantSchema {
						found = true
						break
					}
				}

				assert.True(t, found, "schema %s should be stored", tt.wantSchema)
			}
		})
	}
}
