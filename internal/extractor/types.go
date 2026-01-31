package extractor

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

func (e *Extractor) extractExtensions(ctx context.Context) ([]schema.Extension, error) {
	query := e.queries.extensionsQuery()

	var extensions []schema.Extension

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var ext schema.Extension

		if err := rows.Scan(
			&ext.Name,
			&ext.Schema,
			&ext.Version,
			scanner.String("comment"),
		); err != nil {
			return util.WrapError("scan extension", err)
		}

		ext.Comment = scanner.GetString("comment")
		extensions = append(extensions, ext)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch extensions", err)
	}

	return extensions, nil
}

func (e *Extractor) extractCustomTypes(ctx context.Context) ([]schema.CustomType, error) {
	query := e.queries.customTypesQuery()

	var customTypes []schema.CustomType

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var ct schema.CustomType

		if err := rows.Scan(
			&ct.Schema,
			&ct.Name,
			&ct.Type,
			scanner.String("comment"),
			&ct.Definition,
		); err != nil {
			return util.WrapError("scan custom type", err)
		}

		ct.Comment = scanner.GetString("comment")

		if ct.Type == "enum" {
			values, err := e.extractEnumValues(ctx, ct.Schema, ct.Name)
			if err == nil {
				ct.Values = values
			}
		}

		customTypes = append(customTypes, ct)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch custom types", err)
	}

	return customTypes, nil
}

func (e *Extractor) extractEnumValues(
	ctx context.Context,
	schemaName, typeName string,
) ([]string, error) {
	var values []string

	err := e.queryHelper.FetchAll(ctx, queryEnumValues, func(rows pgx.Rows) error {
		var value string
		if err := rows.Scan(&value); err != nil {
			return util.WrapError("scan enum value", err)
		}

		values = append(values, value)

		return nil
	}, schemaName, typeName)
	if err != nil {
		return nil, util.WrapError("fetch enum values", err)
	}

	return values, nil
}

func (e *Extractor) extractSequences(ctx context.Context) ([]schema.Sequence, error) {
	query := e.queries.sequencesQuery()

	var sequences []schema.Sequence

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var seq schema.Sequence

		if err := rows.Scan(
			&seq.Schema,
			&seq.Name,
			&seq.DataType,
			&seq.StartValue,
			&seq.MinValue,
			&seq.MaxValue,
			&seq.Increment,
			&seq.CacheSize,
			&seq.IsCyclic,
			scanner.String("ownedByTable"),
			scanner.String("ownedByColumn"),
		); err != nil {
			return util.WrapError("scan sequence", err)
		}

		seq.OwnedByTable = scanner.GetString("ownedByTable")
		seq.OwnedByColumn = scanner.GetString("ownedByColumn")

		if seq.OwnedByTable != "" && seq.OwnedByColumn != "" {
			return nil
		}

		sequences = append(sequences, seq)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch sequences", err)
	}

	return sequences, nil
}

func (e *Extractor) extractSchemas(ctx context.Context) ([]schema.Schema, error) {
	query := e.queries.schemasQuery()

	var schemas []schema.Schema

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		var sch schema.Schema
		if err := rows.Scan(&sch.Name); err != nil {
			return util.WrapError("scan schema", err)
		}

		schemas = append(schemas, sch)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch schemas", err)
	}

	return schemas, nil
}
