package extractor

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

func (e *Extractor) extractHypertables(ctx context.Context) ([]schema.Hypertable, error) {
	var hypertables []schema.Hypertable

	err := e.queryHelper.FetchAll(ctx, queryHypertables, func(rows pgx.Rows) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var ht schema.Hypertable

		if err := rows.Scan(
			&ht.Schema,
			&ht.TableName,
			&ht.TimeColumnName,
			&ht.TimeColumnType,
			&ht.PartitionInterval,
			&ht.NumDimensions,
		); err != nil {
			return util.WrapError("scan hypertable", err)
		}

		if err := e.enrichHypertable(ctx, &ht); err != nil {
			return util.WrapError("enrich hypertable "+ht.QualifiedTableName(), err)
		}

		hypertables = append(hypertables, ht)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch hypertables", err)
	}

	return hypertables, nil
}

func (e *Extractor) enrichHypertable(ctx context.Context, ht *schema.Hypertable) error {
	if err := e.enrichCompression(ctx, ht); err != nil {
		return err
	}

	if err := e.enrichSpacePartitions(ctx, ht); err != nil {
		return err
	}

	if err := e.enrichRetentionPolicy(ctx, ht); err != nil {
		return err
	}

	return nil
}

func (e *Extractor) enrichCompression(ctx context.Context, ht *schema.Hypertable) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck
	}

	ht.CompressionEnabled = e.isCompressionEnabled(ctx, ht.Schema, ht.TableName)

	if ht.CompressionEnabled {
		compressionSettings, err := e.extractCompressionSettings(ctx, ht.Schema, ht.TableName)
		if err != nil {
			return util.WrapError("extract compression settings", err)
		}

		ht.CompressionSettings = compressionSettings
	}

	return nil
}

func (e *Extractor) enrichSpacePartitions(ctx context.Context, ht *schema.Hypertable) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck
	}

	if ht.NumDimensions > 1 {
		spaceColumns, err := e.extractSpacePartitionColumns(ctx, ht.Schema, ht.TableName)
		if err != nil {
			return util.WrapError("extract space partition columns", err)
		}

		if len(spaceColumns) > 0 {
			ht.SpacePartitions = len(spaceColumns)
			ht.SpaceColumns = spaceColumns
		}
	}

	return nil
}

func (e *Extractor) enrichRetentionPolicy(ctx context.Context, ht *schema.Hypertable) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck
	}

	retentionPolicy, err := e.extractRetentionPolicy(ctx, ht.Schema, ht.TableName)
	if err != nil {
		return util.WrapError("extract retention policy", err)
	}

	if retentionPolicy != nil {
		ht.RetentionPolicy = retentionPolicy
	}

	return nil
}

func (e *Extractor) isCompressionEnabled(ctx context.Context, schemaName, tableName string) bool {
	var enabled bool
	if err := e.pool.QueryRow(ctx, queryCompressionEnabled, schemaName, tableName).Scan(&enabled); err != nil {
		return false
	}

	return enabled
}

func (e *Extractor) extractSpacePartitionColumns(
	ctx context.Context,
	schemaName, tableName string,
) ([]string, error) {
	var columns []string

	err := e.queryHelper.FetchAll(ctx, querySpacePartitionColumns, func(rows pgx.Rows) error {
		var column string
		if err := rows.Scan(&column); err != nil {
			return util.WrapError("scan space partition column", err)
		}

		columns = append(columns, column)

		return nil
	}, schemaName, tableName)
	if err != nil {
		return nil, util.WrapError("fetch space partition columns", err)
	}

	return columns, nil
}

func (e *Extractor) extractCompressionSettings(
	ctx context.Context,
	schemaName, tableName string,
) (*schema.CompressionSettings, error) {
	var segmentBy, orderBy *string

	err := e.queryHelper.FetchOne(ctx, queryCompressionSettings, func(row pgx.Row) error {
		return row.Scan(&segmentBy, &orderBy)
	}, schemaName, tableName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil //nolint:nilnil
		}

		return nil, util.WrapError("fetch compression settings", err)
	}

	settings := &schema.CompressionSettings{}

	if segmentBy != nil && *segmentBy != "" {
		settings.SegmentByColumns = parseColumnList(*segmentBy)
	}

	if orderBy != nil && *orderBy != "" {
		settings.OrderByColumns = parseOrderByColumns(*orderBy)
	}

	return settings, nil
}

func (e *Extractor) extractRetentionPolicy(
	ctx context.Context,
	schemaName, tableName string,
) (*schema.RetentionPolicy, error) {
	scanner := NewNullScanner()

	var policy schema.RetentionPolicy

	err := e.queryHelper.FetchOne(ctx, queryRetentionPolicy, func(row pgx.Row) error {
		return row.Scan(&policy.DropAfter, scanner.String("scheduleInterval"))
	}, schemaName, tableName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil //nolint:nilnil
		}

		return nil, util.WrapError("fetch retention policy", err)
	}

	policy.ScheduleInterval = scanner.GetString("scheduleInterval")

	return &policy, nil
}

func (e *Extractor) extractContinuousAggregates(
	ctx context.Context,
) ([]schema.ContinuousAggregate, error) {
	query := e.queries.continuousAggregatesQuery()

	var aggregates []schema.ContinuousAggregate

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var (
			ca                  schema.ContinuousAggregate
			finalized           *bool
			matHypertableSchema string
			matHypertableName   string
		)

		if err := rows.Scan(
			&ca.Schema,
			&ca.ViewName,
			&ca.HypertableSchema,
			&ca.HypertableName,
			&ca.Query,
			&ca.Materialized,
			&finalized,
			scanner.String("comment"),
			&matHypertableSchema,
			&matHypertableName,
		); err != nil {
			return util.WrapError("scan continuous aggregate", err)
		}

		ca.WithData = true

		if finalized != nil {
			ca.Finalized = *finalized
		}

		ca.Comment = scanner.GetString("comment")

		refreshPolicy, err := e.extractRefreshPolicy(ctx, ca.Schema, ca.ViewName)
		if err == nil && refreshPolicy != nil {
			ca.RefreshPolicy = refreshPolicy
		}

		indexes, err := e.extractCAIndexes(
			ctx, matHypertableSchema, matHypertableName, ca.Schema, ca.ViewName,
		)
		if err == nil {
			ca.Indexes = indexes
		}

		aggregates = append(aggregates, ca)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch continuous aggregates", err)
	}

	return aggregates, nil
}

func (e *Extractor) extractRefreshPolicy(
	ctx context.Context,
	schemaName, viewName string,
) (*schema.RefreshPolicy, error) {
	scanner := NewNullScanner()

	var policy schema.RefreshPolicy

	err := e.queryHelper.FetchOne(ctx, queryRefreshPolicy, func(row pgx.Row) error {
		return row.Scan(
			scanner.String("startOffset"),
			scanner.String("endOffset"),
			&policy.ScheduleInterval,
		)
	}, schemaName, viewName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil //nolint:nilnil
		}

		return nil, util.WrapError("fetch refresh policy", err)
	}

	policy.StartOffset = scanner.GetString("startOffset")
	policy.EndOffset = scanner.GetString("endOffset")

	return &policy, nil
}

func (e *Extractor) extractCAIndexes(
	ctx context.Context,
	matSchema, matTableName, caSchema, viewName string,
) ([]schema.Index, error) {
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
			return util.WrapError("scan CA index", err)
		}

		if strings.HasPrefix(idx.Name, "_materialized_hypertable_") {
			return nil
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

		idx.TableName = viewName
		idx.Definition = strings.Replace(
			idx.Definition,
			matSchema+"."+matTableName,
			caSchema+"."+viewName,
			1,
		)
		idx.Schema = caSchema

		indexes = append(indexes, idx)

		return nil
	}, matSchema, matTableName)
	if err != nil {
		return nil, util.WrapError("fetch CA indexes", err)
	}

	return indexes, nil
}

func parseOrderByColumns(orderBy string) []schema.OrderByColumn {
	var columns []schema.OrderByColumn //nolint:prealloc

	parts := strings.SplitSeq(orderBy, ",")
	for part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		col := schema.OrderByColumn{Direction: "ASC"}

		upperPart := strings.ToUpper(part)
		if strings.Contains(upperPart, "NULLS FIRST") {
			col.NullsOrder = "NULLS FIRST"
			part = strings.TrimSpace(strings.Replace(upperPart, "NULLS FIRST", "", 1))
		} else if strings.Contains(upperPart, "NULLS LAST") {
			col.NullsOrder = "NULLS LAST"
			part = strings.TrimSpace(strings.Replace(upperPart, "NULLS LAST", "", 1))
		}

		if strings.HasSuffix(strings.ToUpper(part), " DESC") {
			col.Direction = "DESC"
			part = strings.TrimSpace(part[:len(part)-5])
		} else if strings.HasSuffix(strings.ToUpper(part), " ASC") {
			col.Direction = "ASC"
			part = strings.TrimSpace(part[:len(part)-4])
		}

		col.Column = part
		columns = append(columns, col)
	}

	return columns
}
