package extractor

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

func (e *Extractor) extractTables(ctx context.Context) ([]schema.Table, error) {
	query := e.queries.tablesQuery()

	var tables []schema.Table

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		scanner := NewNullScanner()

		var table schema.Table

		if err := rows.Scan(
			&table.Schema,
			&table.Name,
			scanner.String("comment"),
			scanner.String("owner"),
			scanner.String("tablespace"),
		); err != nil {
			return util.WrapError("scan table", err)
		}

		table.Comment = scanner.GetString("comment")
		table.Owner = scanner.GetString("owner")
		table.Tablespace = scanner.GetString("tablespace")

		tables = append(tables, table)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch tables", err)
	}

	for i := range tables {
		if err := ctx.Err(); err != nil {
			return nil, err //nolint:wrapcheck
		}

		if err := e.enrichTable(ctx, &tables[i]); err != nil {
			return nil, err
		}
	}

	return tables, nil
}

func (e *Extractor) enrichTable(ctx context.Context, table *schema.Table) error {
	enrichers := []func(context.Context, *schema.Table) error{
		e.enrichColumns,
		e.enrichConstraints,
		e.enrichIndexes,
		e.enrichPartitions,
	}

	for _, enrich := range enrichers {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck
		}

		if err := enrich(ctx, table); err != nil {
			return util.WrapError("enrich table "+table.QualifiedName(), err)
		}
	}

	table.Sort()

	return nil
}

func (e *Extractor) enrichColumns(ctx context.Context, table *schema.Table) error {
	return e.extractColumns(ctx, table)
}

func (e *Extractor) enrichConstraints(ctx context.Context, table *schema.Table) error {
	return e.extractConstraints(ctx, table)
}

func (e *Extractor) enrichIndexes(ctx context.Context, table *schema.Table) error {
	return e.extractIndexes(ctx, table)
}

func (e *Extractor) enrichPartitions(ctx context.Context, table *schema.Table) error {
	return e.extractPartitionInfo(ctx, table)
}

func (e *Extractor) extractColumns(ctx context.Context, table *schema.Table) error {
	var columns []schema.Column

	err := e.queryHelper.FetchAll(ctx, queryColumns, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var col schema.Column

		if err := rows.Scan(
			&col.Name,
			&col.DataType,
			&col.IsNullable,
			scanner.String("default"),
			scanner.String("comment"),
			&col.Position,
			scanner.Int32("charMaxLength"),
			scanner.Int32("numericPrecision"),
			scanner.Int32("numericScale"),
			scanner.String("udtName"),
			&col.IsIdentity,
			scanner.String("identityGen"),
			&col.IsGenerated,
			scanner.String("generationExpr"),
		); err != nil {
			return util.WrapError("scan column", err)
		}

		col.Default = scanner.GetString("default")
		col.Comment = scanner.GetString("comment")
		col.IdentityGeneration = scanner.GetString("identityGen")
		col.GenerationExpression = scanner.GetString("generationExpr")

		if maxLength := scanner.GetInt("charMaxLength"); maxLength != nil {
			col.MaxLength = maxLength
		}

		if precision := scanner.GetInt("numericPrecision"); precision != nil &&
			isNumericType(col.DataType) {
			col.Precision = precision
		}

		if scale := scanner.GetInt("numericScale"); scale != nil && isNumericType(col.DataType) {
			col.Scale = scale
		}

		udtName := scanner.GetString("udtName")
		if udtName != "" && (col.DataType == "ARRAY" || (len(udtName) > 0 && udtName[0] == '_')) {
			col.IsArray = true

			elementType := extractArrayElementType(udtName)
			if elementType != "" {
				col.DataType = normalizeArrayElementType(elementType)
			}
		}

		columns = append(columns, col)

		return nil
	}, table.Schema, table.Name)
	if err != nil {
		return util.WrapError("fetch columns", err)
	}

	table.Columns = columns

	return nil
}

func (e *Extractor) extractConstraints(ctx context.Context, table *schema.Table) error {
	var constraints []schema.Constraint

	err := e.queryHelper.FetchAll(ctx, queryConstraints, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var (
			c              schema.Constraint
			constraintType *string
			refColumns     []string
		)

		if err := rows.Scan(
			&c.Name,
			&constraintType,
			&c.Columns,
			&c.Definition,
			scanner.String("refSchema"),
			scanner.String("refTable"),
			&refColumns,
			scanner.String("onUpdate"),
			scanner.String("onDelete"),
			&c.IsDeferrable,
			&c.InitiallyDeferred,
		); err != nil {
			return util.WrapError("scan constraint", err)
		}

		if constraintType == nil {
			return nil
		}

		c.Type = *constraintType

		if c.Type == schema.ConstraintForeignKey {
			c.ReferencedSchema = scanner.GetString("refSchema")
			c.ReferencedTable = scanner.GetString("refTable")
			c.OnUpdate = scanner.GetString("onUpdate")
			c.OnDelete = scanner.GetString("onDelete")
			c.ReferencedColumns = refColumns
		}

		if c.Type == schema.ConstraintCheck {
			c.CheckExpression = c.Definition
		}

		constraints = append(constraints, c)

		return nil
	}, table.Schema, table.Name)
	if err != nil {
		return util.WrapError("fetch constraints", err)
	}

	table.Constraints = constraints

	return nil
}

func isNumericType(dataType string) bool {
	dt := strings.ToLower(dataType)
	return dt == "numeric" || dt == "decimal"
}

func extractArrayElementType(udtName string) string {
	if len(udtName) == 0 || udtName[0] != '_' {
		return ""
	}

	return udtName[1:]
}

func normalizeArrayElementType(elementType string) string {
	dt := strings.ToLower(strings.TrimSpace(elementType))

	aliases := map[string]string{
		"int":               "integer",
		"int2":              "smallint",
		"int4":              "integer",
		"int8":              "bigint",
		"float":             "double precision",
		"float4":            "real",
		"float8":            "double precision",
		"serial":            "integer",
		"bigserial":         "bigint",
		"bool":              "boolean",
		"character varying": "varchar",
		"character":         "char",
		"decimal":           "numeric",
		"timestamp":         "timestamp without time zone",
		"timestamptz":       "timestamp with time zone",
		"time":              "time without time zone",
		"timetz":            "time with time zone",
	}

	if normalized, exists := aliases[dt]; exists {
		return normalized
	}

	return dt
}

func (e *Extractor) extractPartitionInfo(ctx context.Context, table *schema.Table) error {
	var (
		partitionStrategy string
		columns           []string
	)

	err := e.queryHelper.FetchOne(ctx, queryPartitionInfo, func(row pgx.Row) error {
		var start *string
		if err := row.Scan(&start, &columns); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}

			return util.WrapError("scan partition info", err)
		}

		if start != nil && *start != "" {
			switch *start {
			case "h":
				partitionStrategy = "HASH"
			case "r":
				partitionStrategy = "RANGE"
			case "l":
				partitionStrategy = "LIST"
			}
		}

		return nil
	}, table.Schema, table.Name)
	if err != nil {
		if err.Error() == "scan row: no rows in result set" {
			return nil
		}

		return util.WrapError("fetch partition info", err)
	}

	if partitionStrategy == "" {
		return nil
	}

	table.PartitionStrategy = &schema.PartitionStrategy{
		Type:    partitionStrategy,
		Columns: columns,
	}

	if err := e.extractPartitions(ctx, table); err != nil {
		return util.WrapError("extract partitions", err)
	}

	return nil
}

func (e *Extractor) extractPartitions(ctx context.Context, table *schema.Table) error {
	var partitions []schema.Partition

	err := e.queryHelper.FetchAll(ctx, queryPartitions, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var partition schema.Partition

		if err := rows.Scan(&partition.Name, scanner.String("def")); err != nil {
			return util.WrapError("scan partition", err)
		}

		partition.Definition = scanner.GetString("def")

		partitions = append(partitions, partition)

		return nil
	}, table.Schema, table.Name)
	if err != nil {
		return util.WrapError("fetch partitions", err)
	}

	if table.PartitionStrategy != nil {
		table.PartitionStrategy.Partitions = partitions
	}

	return nil
}
