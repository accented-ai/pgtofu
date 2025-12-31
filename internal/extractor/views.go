package extractor

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

func (e *Extractor) extractViews(ctx context.Context) ([]schema.View, error) {
	query := e.queries.viewsQuery()

	var views []schema.View

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var view schema.View

		if err := rows.Scan(
			&view.Schema,
			&view.Name,
			&view.Definition,
			scanner.String("comment"),
			scanner.String("owner"),
			scanner.String("checkOption"),
			&view.IsUpdatable,
		); err != nil {
			return util.WrapError("scan view", err)
		}

		view.Comment = scanner.GetString("comment")
		view.Owner = scanner.GetString("owner")
		view.CheckOption = scanner.GetString("checkOption")

		views = append(views, view)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch views", err)
	}

	return views, nil
}

func (e *Extractor) extractMaterializedViews(
	ctx context.Context,
) ([]schema.MaterializedView, error) {
	query := e.queries.materializedViewsQuery()

	var matViews []schema.MaterializedView

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var mv schema.MaterializedView

		if err := rows.Scan(
			&mv.Schema,
			&mv.Name,
			&mv.Definition,
			scanner.String("comment"),
			scanner.String("owner"),
			scanner.String("tablespace"),
			&mv.WithData,
		); err != nil {
			return util.WrapError("scan materialized view", err)
		}

		mv.Comment = scanner.GetString("comment")
		mv.Owner = scanner.GetString("owner")
		mv.Tablespace = scanner.GetString("tablespace")

		table := &schema.Table{Schema: mv.Schema, Name: mv.Name}
		if err := e.extractIndexes(ctx, table); err != nil {
			return util.WrapError("extract indexes", err)
		}

		mv.Indexes = table.Indexes
		matViews = append(matViews, mv)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch materialized views", err)
	}

	return matViews, nil
}
