package extractor

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
	"github.com/accented-ai/pgtofu/pkg/database"
)

type viewCatalogName struct {
	schema string
	name   string
}

func (e *Extractor) extractViews(ctx context.Context) ([]schema.View, error) {
	query := e.queries.viewsQuery()

	var views []schema.View

	bindings := make(map[viewCatalogName]*schema.CatalogViewDefinition)

	err := e.extractViewCatalog(ctx, query, bindings, func(rows pgx.Rows) error {
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
			&view.SecurityInvoker,
		); err != nil {
			return util.WrapError("scan view", err)
		}

		view.Comment = scanner.GetString("comment")
		view.Owner = scanner.GetString("owner")
		view.CheckOption = scanner.GetString("checkOption")
		view.CatalogDefinition = &schema.CatalogViewDefinition{Original: view.Definition}
		bindings[viewCatalogName{schema: view.Schema, name: view.Name}] = view.CatalogDefinition

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

	bindings := make(map[viewCatalogName]*schema.CatalogViewDefinition)

	err := e.extractViewCatalog(ctx, query, bindings, func(rows pgx.Rows) error {
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
		mv.CatalogDefinition = &schema.CatalogViewDefinition{Original: mv.Definition}
		bindings[viewCatalogName{schema: mv.Schema, name: mv.Name}] = mv.CatalogDefinition

		matViews = append(matViews, mv)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch materialized views", err)
	}

	for index := range matViews {
		view := &matViews[index]
		table := &schema.Table{Schema: view.Schema, Name: view.Name}

		if err := e.extractIndexes(ctx, table); err != nil {
			return nil, util.WrapError("extract indexes", err)
		}

		view.Indexes = table.Indexes
	}

	return matViews, nil
}

func (e *Extractor) extractViewCatalog(
	ctx context.Context,
	query string,
	bindings map[viewCatalogName]*schema.CatalogViewDefinition,
	scan func(pgx.Rows) error,
) error {
	err := pgx.BeginTxFunc(ctx, e.pool, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	}, func(tx pgx.Tx) error {
		helper := database.NewQueryHelper(tx)
		if err := helper.FetchAll(ctx, query, scan); err != nil {
			return util.WrapError("fetch view presentation", err)
		}

		if len(bindings) == 0 {
			return nil
		}

		if _, err := tx.Exec(ctx, "SET LOCAL search_path = pg_catalog, pg_temp"); err != nil {
			return util.WrapError("set view catalog search path", err)
		}

		schemas := make([]string, 0, len(bindings))

		names := make([]string, 0, len(bindings))
		for name := range bindings {
			schemas = append(schemas, name.schema)
			names = append(names, name.name)
		}

		err := helper.FetchAll(
			ctx,
			`SELECT n.nspname, c.relname, pg_catalog.pg_get_viewdef(c.oid)
			FROM ROWS FROM (pg_catalog.unnest($1::text[]), pg_catalog.unnest($2::text[]))
				AS selected(schema_name, view_name)
			JOIN pg_catalog.pg_namespace n ON n.nspname = selected.schema_name
			JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = selected.view_name`,
			func(rows pgx.Rows) error {
				var schemaName, name, definition string
				if err := rows.Scan(&schemaName, &name, &definition); err != nil {
					return util.WrapError("scan catalog view", err)
				}

				bindings[viewCatalogName{schema: schemaName, name: name}].Definition = definition

				return nil
			},
			schemas,
			names,
		)

		return util.WrapError("fetch catalog view definitions", err)
	})

	return util.WrapError("extract view catalog", err)
}
