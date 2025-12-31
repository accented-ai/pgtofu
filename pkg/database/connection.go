package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/accented-ai/pgtofu/internal/util"
)

type Pool struct {
	pool *pgxpool.Pool
}

func NewPoolFromURL(ctx context.Context, url string) (*Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, util.WrapError("parse pool config", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, util.WrapError("create connection pool", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, util.WrapError("ping database", err)
	}

	return &Pool{pool: pool}, nil
}

func (p *Pool) Close() {
	p.pool.Close()
}

func (p *Pool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.pool.Query(ctx, sql, args...) //nolint:wrapcheck
}

func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.pool.QueryRow(ctx, sql, args...)
}

func (p *Pool) HasExtension(ctx context.Context, name string) (bool, error) {
	var exists bool

	query := "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1)"

	err := p.pool.QueryRow(ctx, query, name).Scan(&exists)
	if err != nil {
		return false, util.WrapError(fmt.Sprintf("check extension %q", name), err)
	}

	return exists, nil
}

func (p *Pool) HasTimescaleDB(ctx context.Context) (bool, error) {
	return p.HasExtension(ctx, "timescaledb")
}

func (p *Pool) TimescaleDBVersion(ctx context.Context) (string, error) {
	hasTimescale, err := p.HasTimescaleDB(ctx)
	if err != nil {
		return "", err
	}

	if !hasTimescale {
		return "", errors.New("timescaledb extension not installed")
	}

	var version string

	query := "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"

	err = p.pool.QueryRow(ctx, query).Scan(&version)
	if err != nil {
		return "", util.WrapError("get timescaledb version", err)
	}

	return version, nil
}

func (p *Pool) CurrentDatabase(ctx context.Context) (string, error) {
	var dbName string

	err := p.pool.QueryRow(ctx, "SELECT current_database()").Scan(&dbName)
	if err != nil {
		return "", util.WrapError("get current database", err)
	}

	return dbName, nil
}
