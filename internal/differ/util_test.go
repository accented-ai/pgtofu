package differ //nolint:testpackage // testing internal function

import (
	"testing"
)

func TestNormalizeExpression_InClauseVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple IN clause from SQL",
			input:    "CHECK (status IN ('pending', 'processing', 'completed'))",
			expected: "status in ('pending', 'processing', 'completed')",
		},
		{
			name: "PostgreSQL ANY ARRAY format",
			input: "CHECK (((status)::text = ANY " +
				"(ARRAY['pending'::text, 'processing'::text, 'completed'::text])))",
			expected: "status in ('pending', 'processing', 'completed')",
		},
		{
			name: "VARCHAR column with text cast",
			input: "CHECK (((category)::text = ANY " +
				"(ARRAY['alpha'::text, 'beta'::text, 'gamma'::text])))",
			expected: "category in ('alpha', 'beta', 'gamma')",
		},
		{
			name:     "simple IN from SQL file",
			input:    "CHECK (category IN ('alpha', 'beta', 'gamma'))",
			expected: "category in ('alpha', 'beta', 'gamma')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := normalizeExpression(tt.input)
			if result != tt.expected {
				t.Errorf(
					"normalizeExpression() mismatch:\n  input:    %q\n  got:      %q\n  expected: %q",
					tt.input,
					result,
					tt.expected,
				)
			}
		})
	}
}

func TestNormalizeExpression_ComparisonParentheses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fromSQL      string
		fromPostgres string
	}{
		{
			name:         "AND expression with parenthesized comparisons",
			fromSQL:      "CHECK (low <= high AND open >= low)",
			fromPostgres: "CHECK (((low <= high) AND (open >= low)))",
		},
		{
			name:         "OR expression with parenthesized comparisons",
			fromSQL:      "CHECK (status = 'urgent' OR priority > 5)",
			fromPostgres: "CHECK (((status = 'urgent') OR (priority > 5)))",
		},
		{
			name:         "mixed AND/OR with IS NOT NULL",
			fromSQL:      "CHECK (is_active = true OR (end_time IS NOT NULL AND end_time > start_time))",
			fromPostgres: "CHECK (((is_active = true) OR ((end_time IS NOT NULL) AND (end_time > start_time))))",
		},
		{
			name:         "multiple comparisons in chain",
			fromSQL:      "CHECK (a <= b AND b <= c AND c <= d)",
			fromPostgres: "CHECK (((a <= b) AND (b <= c) AND (c <= d)))",
		},
		{
			name:         "arithmetic with ABS function and type casts",
			fromSQL:      "CHECK (a >= 0 AND a <= 1 AND ABS((x + y + z) - 1.0) < 0.01)",
			fromPostgres: "CHECK (((a >= (0)::numeric) AND (a <= (1)::numeric) AND (abs((((x + y) + z) - 1.0)) < 0.01)))",
		},
		{
			name:         "chained addition in function call",
			fromSQL:      "CHECK (total = (a + b + c))",
			fromPostgres: "CHECK ((total = ((a + b) + c)))",
		},
		{
			name:         "nested arithmetic operations",
			fromSQL:      "CHECK (result = (a + b - c + d))",
			fromPostgres: "CHECK ((result = (((a + b) - c) + d)))",
		},
		{
			name:         "BETWEEN with OR prefix",
			fromSQL:      "CHECK (score IS NULL OR score BETWEEN 8 AND 24)",
			fromPostgres: "CHECK (((score IS NULL) OR ((score >= 8) AND (score <= 24))))",
		},
		{
			name:         "OR expression with unary NOT term",
			fromSQL:      "CHECK ((mode = 'active') OR NOT is_enabled)",
			fromPostgres: "CHECK (((mode = 'active'::text) OR (NOT is_enabled)))",
		},
		{
			name:         "AND expression with unary NOT term",
			fromSQL:      "CHECK ((mode <> 'active') AND NOT is_enabled)",
			fromPostgres: "CHECK (((mode <> 'active'::text) AND (NOT is_enabled)))",
		},
		{
			name:         "unary NOT with redundant simple parentheses",
			fromSQL:      "CHECK (NOT is_enabled OR mode = 'manual')",
			fromPostgres: "CHECK (((NOT (is_enabled)) OR (mode = 'manual'::text)))",
		},
		{
			name:    "unary NOT preserving compound expression parentheses",
			fromSQL: "CHECK (NOT (is_enabled AND is_ready) OR mode = 'manual')",
			fromPostgres: "CHECK (((NOT ((is_enabled) AND (is_ready))) " +
				"OR (mode = 'manual'::text)))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			normalizedSQL := normalizeExpression(tt.fromSQL)
			normalizedPG := normalizeExpression(tt.fromPostgres)

			if normalizedSQL != normalizedPG {
				t.Errorf(
					"Normalized expressions don't match:\n  SQL:      %q\n  Postgres: %q",
					normalizedSQL, normalizedPG,
				)
			}
		})
	}
}

func TestNormalizeExpression_CompareEquivalentConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fromSQL      string
		fromPostgres string
	}{
		{
			name:    "status IN clause",
			fromSQL: "CHECK (status IN ('pending', 'processing', 'completed'))",
			fromPostgres: "CHECK (((status)::text = ANY " +
				"(ARRAY['pending'::text, 'processing'::text, 'completed'::text])))",
		},
		{
			name:         "quoted identifier (reserved keyword)",
			fromSQL:      "CHECK (position IN (0, 1, 2))",
			fromPostgres: `CHECK (("position" = ANY (ARRAY[0, 1, 2])))`,
		},
		{
			name:         "partial index WHERE clause with type cast",
			fromSQL:      "kind = 'active'",
			fromPostgres: "((kind)::text = 'active'::text)",
		},
		{
			name:    "category IN clause with five values",
			fromSQL: "CHECK (category IN ('type_a', 'type_b', 'type_c', 'type_d', 'type_e'))",
			fromPostgres: "CHECK (((category)::text = ANY (ARRAY['type_a'::text, " +
				"'type_b'::text, 'type_c'::text, 'type_d'::text, 'type_e'::text])))",
		},
		{
			name:    "character varying cast",
			fromSQL: "CHECK (status IN ('ACTIVE', 'PAUSED'))",
			fromPostgres: "CHECK (((status)::character varying = ANY " +
				"(ARRAY['ACTIVE'::character varying, 'PAUSED'::character varying])))",
		},
		{
			name:         "no spaces in array",
			fromSQL:      "CHECK (status IN ('a', 'b', 'c'))",
			fromPostgres: "CHECK (((status)::text = ANY (ARRAY['a'::text,'b'::text,'c'::text])))",
		},
		{
			name:         "array literal format",
			fromSQL:      "CHECK (status IN ('a', 'b', 'c'))",
			fromPostgres: "CHECK ((status = ANY ('{a,b,c}'::text[])))",
		},
		{
			name:    "array with text cast on column and text[] cast on array",
			fromSQL: "CHECK (category IN ('a', 'b', 'c', 'd', 'e'))",
			fromPostgres: "CHECK (((category)::text = ANY ((ARRAY['a'::character varying, " +
				"'b'::character varying, 'c'::character varying, 'd'::character varying, " +
				"'e'::character varying])::text[])))",
		},
		{
			name:    "NOT IN converted to <> ALL ARRAY by PostgreSQL",
			fromSQL: "CHECK (status NOT IN ('needs_changes', 'approved', 'published', 'rejected'))",
			fromPostgres: "CHECK (((status)::text <> ALL " +
				"(ARRAY['needs_changes'::text, 'approved'::text, 'published'::text, 'rejected'::text])))",
		},
		{
			name: "NOT IN with OR clause",
			fromSQL: "CHECK (status NOT IN ('needs_changes', 'approved', 'published', 'rejected') " +
				"OR (reviewed_by IS NOT NULL AND reviewed_at IS NOT NULL))",
			fromPostgres: "CHECK (((status <> ALL " +
				"(ARRAY['needs_changes'::text, 'approved'::text, 'published'::text, 'rejected'::text])) " +
				"OR ((reviewed_by IS NOT NULL) AND (reviewed_at IS NOT NULL))))",
		},
		{
			name:         "NOT IN with array literal format",
			fromSQL:      "CHECK (status NOT IN ('a', 'b', 'c'))",
			fromPostgres: "CHECK ((status <> ALL ('{a,b,c}'::text[])))",
		},
		{
			name:         "single element IN normalized to equality by PostgreSQL",
			fromSQL:      "CHECK (mode IN ('batch'))",
			fromPostgres: "CHECK ((mode = 'batch'::text))",
		},
		{
			name: "complex AND/OR with IN clauses and equality",
			fromSQL: "CHECK ((status IN ('pending', 'active') " +
				"AND reason = '') OR " +
				"(status IN ('skipped', 'failed') " +
				"AND reason <> ''))",
			fromPostgres: "CHECK (((status = ANY " +
				"(ARRAY['pending'::text, 'active'::text])) AND " +
				"(reason = ''::text)) OR " +
				"((status = ANY " +
				"(ARRAY['skipped'::text, 'failed'::text])) AND " +
				"(reason <> ''::text)))",
		},
		{
			name: "JSON accessor IN within OR",
			fromSQL: "CHECK (result_payload IS NULL OR " +
				"(JSONB_TYPEOF(result_payload) = 'object' AND " +
				"result_payload ->> 'schema_version' IN ('result.v1', 'result.v2')))",
			fromPostgres: "CHECK (((result_payload IS NULL) OR " +
				"((jsonb_typeof(result_payload) = 'object'::text) AND " +
				"((result_payload ->> 'schema_version'::text) = ANY " +
				"(ARRAY['result.v1'::text, 'result.v2'::text])))))",
		},
		{
			name:         "LIKE rendered as ~~ operator by PostgreSQL",
			fromSQL:      "CHECK (slug = '' OR slug LIKE 'a%')",
			fromPostgres: "CHECK (((slug = ''::text) OR (slug ~~ 'a%'::text)))",
		},
		{
			name:         "NOT LIKE rendered as !~~ operator by PostgreSQL",
			fromSQL:      "CHECK (code NOT LIKE 'a%')",
			fromPostgres: "CHECK ((code !~~ 'a%'::text))",
		},
		{
			name:         "ILIKE rendered as ~~* operator by PostgreSQL",
			fromSQL:      "CHECK (label ILIKE 'a%')",
			fromPostgres: "CHECK ((label ~~* 'a%'::text))",
		},
		{
			name:         "NOT ILIKE rendered as !~~* operator by PostgreSQL",
			fromSQL:      "CHECK (label NOT ILIKE 'a%')",
			fromPostgres: "CHECK ((label !~~* 'a%'::text))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			normalizedSQL := normalizeExpression(tt.fromSQL)
			normalizedPG := normalizeExpression(tt.fromPostgres)

			if normalizedSQL != normalizedPG {
				t.Errorf(
					"Normalized expressions don't match:\n  SQL:      %q\n  Postgres: %q",
					normalizedSQL, normalizedPG,
				)
			}
		})
	}
}

func TestNormalizeExpression_PostgresCanonicalCheckForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fromSQL      string
		fromPostgres string
	}{
		{
			name: "JSONB equality guarded by IS TRUE",
			fromSQL: "CHECK ((JSONB_TYPEOF(document) = 'object' " +
				"AND document -> 'enabled' = 'true'::JSONB " +
				"AND document = source_text::JSONB) IS TRUE)",
			fromPostgres: "CHECK ((((jsonb_typeof(document) = 'object'::text) AND " +
				"((document -> 'enabled'::text) = 'true') AND " +
				"(document = (source_text)::jsonb)) IS TRUE))",
		},
		{
			name: "public digest over converted text",
			fromSQL: "CHECK (content_hash ~ '^[0-9a-f]{64}$' AND " +
				"content_hash = ENCODE(PUBLIC.DIGEST(CONVERT_TO(" +
				"source_text, 'UTF8'), 'sha256'), 'hex'))",
			fromPostgres: "CHECK (((content_hash ~ '^[0-9a-f]{64}$'::text) AND " +
				"(content_hash = encode(digest(convert_to(source_text, 'UTF8'::name), " +
				"'sha256'::text), 'hex'::text))))",
		},
		{
			name: "UUID v5 over a concatenated value",
			fromSQL: "CHECK (id = UUID_GENERATE_V5(" +
				"'00000000-0000-0000-0000-000000000000'::UUID, " +
				"'document:' || content_hash))",
			fromPostgres: "CHECK ((id = uuid_generate_v5(" +
				"'00000000-0000-0000-0000-000000000000', " +
				"('document:'::text || content_hash))))",
		},
		{
			name: "nested JSONB accessors guarded by IS TRUE",
			fromSQL: "CHECK ((JSONB_TYPEOF(document) = 'object' " +
				"AND document ->> 'version' = 'v1' " +
				"AND document -> 'enabled' = 'true'::JSONB " +
				"AND JSONB_TYPEOF(document -> 'items') = 'array' " +
				"AND JSONB_ARRAY_LENGTH(document -> 'items') > 0 " +
				"AND JSONB_TYPEOF(document -> 'metadata') = 'array' " +
				"AND document = source_text::JSONB) IS TRUE)",
			fromPostgres: "CHECK ((((jsonb_typeof(document) = 'object'::text) AND " +
				"((document ->> 'version'::text) = 'v1'::text) AND " +
				"((document -> 'enabled'::text) = 'true'::jsonb) AND " +
				"(jsonb_typeof((document -> 'items'::text)) = 'array'::text) AND " +
				"(jsonb_array_length((document -> 'items'::text)) > 0) AND " +
				"(jsonb_typeof((document -> 'metadata'::text)) = 'array'::text) " +
				"AND (document = (source_text)::jsonb)) IS TRUE))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			normalizedSQL := normalizeExpression(tt.fromSQL)
			normalizedPostgres := normalizeExpression(tt.fromPostgres)

			if normalizedSQL != normalizedPostgres {
				t.Errorf(
					"normalized expressions do not match:\n  SQL:      %q\n  Postgres: %q",
					normalizedSQL,
					normalizedPostgres,
				)
			}
		})
	}
}

func TestNormalizeExpression_NullableRegexChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fromSQL      string
		fromPostgres string
	}{
		{
			name:    "nullable content hash",
			fromSQL: "CHECK (content_hash IS NULL OR content_hash ~ '^[0-9a-f]{64}$')",
			fromPostgres: "CHECK (((content_hash IS NULL) OR " +
				"(content_hash ~ '^[0-9a-f]{64}$'::text)))",
		},
		{
			name: "nullable slug",
			fromSQL: "CHECK (slug IS NULL OR " +
				"slug ~ '^[a-z0-9][a-z0-9-]{2,29}$')",
			fromPostgres: "CHECK (((slug IS NULL) OR " +
				"(slug ~ '^[a-z0-9][a-z0-9-]{2,29}$'::text)))",
		},
		{
			name:    "nullable four-digit code",
			fromSQL: "CHECK (optional_code IS NULL OR optional_code ~ '^[0-9]{4}$')",
			fromPostgres: "CHECK (((optional_code IS NULL) OR " +
				"(optional_code ~ '^[0-9]{4}$'::text)))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			normalizedSQL := normalizeExpression(tt.fromSQL)
			normalizedPostgres := normalizeExpression(tt.fromPostgres)

			if normalizedSQL != normalizedPostgres {
				t.Errorf(
					"normalized expressions do not match:\n  SQL:      %q\n  Postgres: %q",
					normalizedSQL,
					normalizedPostgres,
				)
			}
		})
	}
}

func TestNormalizeDefaultFunctionQualifiers(t *testing.T) {
	t.Parallel()

	expression := "public.digest(payload) = secure.digest(payload) " +
		"AND note = 'public.digest(payload)'"

	if got, want := normalizeDefaultFunctionQualifiers(expression),
		"digest(payload) = secure.digest(payload) AND note = 'public.digest(payload)'"; got != want {
		t.Errorf("normalizeDefaultFunctionQualifiers() = %q, want %q", got, want)
	}
}

func TestRemoveTypeCastsPreservesLiteralsAndCustomTypes(t *testing.T) {
	t.Parallel()

	expression := "id::uuid = owner_id::namespace_id " +
		"AND payload::jsonb = source::jsonb_document AND note = 'value::uuid'"
	want := "id = owner_id::namespace_id " +
		"AND payload = source::jsonb_document AND note = 'value::uuid'"

	if got := removeTypeCasts(expression); got != want {
		t.Errorf("removeTypeCasts() = %q, want %q", got, want)
	}
}
