package database

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/accented-ai/pgtofu/internal/util"
)

type QueryHelper struct {
	pool *Pool
}

func NewQueryHelper(pool *Pool) *QueryHelper {
	return &QueryHelper{pool: pool}
}

func (qh *QueryHelper) FetchAll(
	ctx context.Context,
	query string,
	scanFunc func(pgx.Rows) error,
	args ...any,
) error {
	rows, err := qh.pool.Query(ctx, query, args...)
	if err != nil {
		return util.WrapError("execute query", err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := scanFunc(rows); err != nil {
			return util.WrapError("scan row", err)
		}
	}

	if err := rows.Err(); err != nil {
		return util.WrapError("iterate rows", err)
	}

	return nil
}

func (qh *QueryHelper) FetchOne(
	ctx context.Context,
	query string,
	scanFunc func(pgx.Row) error,
	args ...any,
) error {
	row := qh.pool.QueryRow(ctx, query, args...)
	if err := scanFunc(row); err != nil {
		return util.WrapError("scan row", err)
	}

	return nil
}
