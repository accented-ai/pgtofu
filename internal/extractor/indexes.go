package extractor

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

func (e *Extractor) extractIndexes(ctx context.Context, table *schema.Table) error {
	dimensionColumns := e.getHypertableDimensionColumns(ctx, table.Schema, table.Name)

	var indexes []schema.Index

	err := e.queryHelper.FetchAll(ctx, queryIndexes, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var idx schema.Index

		if err := rows.Scan(
			&idx.Schema,
			&idx.Name,
			&idx.TableName,
			&idx.IsUnique,
			&idx.IsPrimary,
			&idx.Type,
			scanner.String("where"),
			&idx.Definition,
			scanner.String("tablespace"),
		); err != nil {
			return util.WrapError("scan index", err)
		}

		idx.Where = scanner.GetString("where")
		idx.Tablespace = scanner.GetString("tablespace")

		columns, includeColumns := parseIndexDefinition(idx.Definition)
		idx.Columns = columns
		idx.IncludeColumns = includeColumns

		storageParams, err := e.extractIndexStorageParams(ctx, idx.Schema, idx.Name)
		if err == nil && len(storageParams) > 0 {
			idx.StorageParams = storageParams
		}

		if e.isTimescaleDBManagedIndex(idx.Name, table.Name, dimensionColumns) {
			return nil
		}

		indexes = append(indexes, idx)

		return nil
	}, table.Schema, table.Name)
	if err != nil {
		return util.WrapError("fetch indexes", err)
	}

	table.Indexes = indexes

	return nil
}

func (e *Extractor) extractIndexStorageParams(
	ctx context.Context,
	schemaName, indexName string,
) (map[string]string, error) {
	params := make(map[string]string)

	err := e.queryHelper.FetchAll(ctx, queryIndexStorageParams, func(rows pgx.Rows) error {
		var option string
		if err := rows.Scan(&option); err != nil {
			return util.WrapError("scan storage param", err)
		}

		if key, value, ok := strings.Cut(option, "="); ok {
			params[key] = value
		}

		return nil
	}, schemaName, indexName)
	if err != nil {
		return nil, util.WrapError("fetch storage params", err)
	}

	return params, nil
}

func parseIndexDefinition(definition string) ([]string, []string) {
	var columns, includeColumns []string

	start := strings.Index(definition, "(")
	if start == -1 {
		return columns, includeColumns
	}

	depth := 0

	end := start
	for i := start; i < len(definition); i++ {
		switch definition[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				end = i
			}
		}

		if depth == 0 && definition[i] == ')' {
			break
		}
	}

	if end <= start {
		return columns, includeColumns
	}

	columnList := definition[start+1 : end]

	if includeIdx := strings.Index(definition[end:], "INCLUDE"); includeIdx != -1 {
		includeStart := end + includeIdx
		if parenStart := strings.Index(definition[includeStart:], "("); parenStart != -1 {
			parenStart += includeStart
			if parenEnd := strings.Index(definition[parenStart:], ")"); parenEnd != -1 {
				parenEnd += parenStart
				includeList := definition[parenStart+1 : parenEnd]
				includeColumns = parseColumnList(includeList)
			}
		}
	}

	columns = parseColumnList(columnList)

	return columns, includeColumns
}

func parseColumnList(columnList string) []string {
	var (
		columns []string
		current strings.Builder
	)

	depth := 0
	inString := false

	for _, ch := range columnList {
		switch ch {
		case '\'':
			inString = !inString

			current.WriteRune(ch)
		case '(':
			if !inString {
				depth++
			}

			current.WriteRune(ch)
		case ')':
			if !inString {
				depth--
			}

			current.WriteRune(ch)
		case ',':
			if !inString && depth == 0 {
				if col := strings.TrimSpace(current.String()); col != "" {
					columns = append(columns, col)
				}

				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if col := strings.TrimSpace(current.String()); col != "" {
		columns = append(columns, col)
	}

	return columns
}

func (e *Extractor) getHypertableDimensionColumns(
	ctx context.Context,
	schemaName, tableName string,
) []string {
	if !e.hasTimescaleDB {
		return nil
	}

	var columns []string

	_ = e.queryHelper.FetchAll(ctx, queryHypertableDimensions, func(rows pgx.Rows) error {
		var column string
		if err := rows.Scan(&column); err != nil {
			return util.WrapError("scan dimension column", err)
		}

		columns = append(columns, column)

		return nil
	}, schemaName, tableName)

	return columns
}

func (e *Extractor) isTimescaleDBManagedIndex(
	indexName, tableName string,
	dimensionColumns []string,
) bool {
	if len(dimensionColumns) == 0 {
		return false
	}

	for _, column := range dimensionColumns {
		if indexName == tableName+"_"+column+"_idx" {
			return true
		}
	}

	return false
}
