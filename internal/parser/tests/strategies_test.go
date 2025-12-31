package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func TestRegistryIncludesAllBuiltinParsers(t *testing.T) {
	t.Parallel()

	registry := parser.NewParserRegistry()

	expected := []parser.StatementType{
		parser.StmtCreateTable,
		parser.StmtCreateIndex,
		parser.StmtCreateView,
		parser.StmtCreateMaterializedView,
		parser.StmtCreateFunction,
		parser.StmtCreateTrigger,
		parser.StmtCreateExtension,
		parser.StmtCreateSchema,
		parser.StmtCreateType,
		parser.StmtCreateSequence,
		parser.StmtAlterTable,
		parser.StmtSelectCreateHypertable,
		parser.StmtSelectAddCompressionPolicy,
		parser.StmtSelectAddRetentionPolicy,
		parser.StmtSelectAddContinuousAggregatePolicy,
		parser.StmtComment,
		parser.StmtDoBlock,
	}

	for _, stmtType := range expected {
		require.NotNilf(t, registry.Get(stmtType), "expected parser for %v", stmtType)
	}
}

func TestExtensionParserParse(t *testing.T) {
	t.Parallel()

	sql := `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA extensions VERSION '1.2.3';`

	tokens, err := parser.NewLexer(sql).Tokenize()
	require.NoError(t, err)

	if len(tokens) > 0 && tokens[len(tokens)-1].Type == parser.TokenEOF {
		tokens = tokens[:len(tokens)-1]
	}

	stmt := parser.Statement{
		Type:   parser.StmtCreateExtension,
		SQL:    sql,
		Tokens: tokens,
	}

	root := parser.New()
	db := &schema.Database{}

	p := parser.NewExtensionParser()
	err = p.Parse(root, stmt, db)
	require.NoError(t, err)

	require.Len(t, db.Extensions, 1)
	ext := db.Extensions[0]
	require.Equal(t, "uuid-ossp", ext.Name)
	require.Equal(t, "extensions", ext.Schema)
	require.Equal(t, "1.2.3", ext.Version)
}

func TestSchemaParserParse(t *testing.T) {
	t.Parallel()

	sql := `CREATE SCHEMA IF NOT EXISTS "My-App";`

	tokens, err := parser.NewLexer(sql).Tokenize()
	require.NoError(t, err)

	if len(tokens) > 0 && tokens[len(tokens)-1].Type == parser.TokenEOF {
		tokens = tokens[:len(tokens)-1]
	}

	stmt := parser.Statement{
		Type:   parser.StmtCreateSchema,
		SQL:    sql,
		Tokens: tokens,
	}

	p := parser.NewSchemaParser()
	db := &schema.Database{}

	require.NoError(t, p.Parse(parser.New(), stmt, db))
	require.Len(t, db.Schemas, 1)
	require.Equal(t, "my-app", db.Schemas[0].Name)
	require.NoError(t, p.Parse(parser.New(), stmt, db))
	require.Len(t, db.Schemas, 1)
}

func TestTypeParserParseEnum(t *testing.T) {
	t.Parallel()

	sql := `CREATE TYPE public.status AS ENUM ('pending', 'approved');`

	tokens, err := parser.NewLexer(sql).Tokenize()
	require.NoError(t, err)

	if len(tokens) > 0 && tokens[len(tokens)-1].Type == parser.TokenEOF {
		tokens = tokens[:len(tokens)-1]
	}

	stmt := parser.Statement{
		Type:   parser.StmtCreateType,
		SQL:    sql,
		Tokens: tokens,
	}

	p := parser.NewTypeParser()
	db := &schema.Database{}

	require.NoError(t, p.Parse(parser.New(), stmt, db))
	require.Len(t, db.CustomTypes, 1)

	customType := db.CustomTypes[0]
	require.Equal(t, "public", customType.Schema)
	require.Equal(t, "status", customType.Name)
	require.Equal(t, "enum", customType.Type)
	require.Equal(t, []string{"pending", "approved"}, customType.Values)
	require.Equal(t, sql, customType.Definition)
}

func TestSequenceParserParse(t *testing.T) {
	t.Parallel()

	sql := `CREATE SEQUENCE public.order_seq;`

	tokens, err := parser.NewLexer(sql).Tokenize()
	require.NoError(t, err)

	if len(tokens) > 0 && tokens[len(tokens)-1].Type == parser.TokenEOF {
		tokens = tokens[:len(tokens)-1]
	}

	stmt := parser.Statement{
		Type:   parser.StmtCreateSequence,
		SQL:    sql,
		Tokens: tokens,
	}

	p := parser.NewSequenceParser()
	db := &schema.Database{}

	require.NoError(t, p.Parse(parser.New(), stmt, db))
	require.Len(t, db.Sequences, 1)

	seq := db.Sequences[0]
	require.Equal(t, "public", seq.Schema)
	require.Equal(t, "order_seq", seq.Name)
	require.Equal(t, "bigint", seq.DataType)
	require.EqualValues(t, 1, seq.Increment)
}
