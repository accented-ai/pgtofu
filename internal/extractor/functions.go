package extractor

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

func (e *Extractor) extractFunctions(ctx context.Context) ([]schema.Function, error) {
	query := e.queries.functionsQuery()

	var functions []schema.Function

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var fn schema.Function

		if err := rows.Scan(
			&fn.Schema,
			&fn.Name,
			&fn.ArgumentTypes,
			&fn.ArgumentNames,
			&fn.ArgumentModes,
			scanner.String("returnType"),
			&fn.Language,
			&fn.Body,
			&fn.IsAggregate,
			&fn.IsWindow,
			&fn.IsStrict,
			&fn.IsSecurityDefiner,
			&fn.Volatility,
			&fn.ParallelSafety,
			scanner.String("comment"),
			scanner.String("owner"),
			&fn.Definition,
		); err != nil {
			return util.WrapError("scan function", err)
		}

		fn.ReturnType = scanner.GetString("returnType")
		fn.Comment = scanner.GetString("comment")
		fn.Owner = scanner.GetString("owner")

		functions = append(functions, fn)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch functions", err)
	}

	return functions, nil
}

func (e *Extractor) extractTriggers(ctx context.Context) ([]schema.Trigger, error) {
	query := e.queries.triggersQuery()

	var triggers []schema.Trigger

	err := e.queryHelper.FetchAll(ctx, query, func(rows pgx.Rows) error {
		scanner := NewNullScanner()

		var trig schema.Trigger

		if err := rows.Scan(
			&trig.Schema,
			&trig.Name,
			&trig.TableName,
			&trig.Timing,
			&trig.Events,
			&trig.UpdateColumns,
			&trig.ForEachRow,
			&trig.IsConstraint,
			&trig.IsDeferrable,
			&trig.InitiallyDeferred,
			&trig.ReferencedTableSchema,
			&trig.ReferencedTableName,
			&trig.Definition,
			&trig.FunctionSchema,
			&trig.FunctionName,
			scanner.String("comment"),
		); err != nil {
			return util.WrapError("scan trigger", err)
		}

		trig.Comment = scanner.GetString("comment")

		parsed := &schema.Database{}
		if err := parser.New().ParseSQL(trig.Definition, parsed); err != nil {
			return util.WrapError("parse extracted trigger definition", err)
		}

		if len(parsed.Triggers) == 1 {
			trig.WhenCondition = parsed.Triggers[0].WhenCondition
		}

		triggers = append(triggers, trig)

		return nil
	})
	if err != nil {
		return nil, util.WrapError("fetch triggers", err)
	}

	return triggers, nil
}
