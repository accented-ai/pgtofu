package extractor

import (
	"context"
	"errors"
	"time"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
	"github.com/accented-ai/pgtofu/pkg/database"
)

var systemSchemas = []string{ //nolint:gochecknoglobals
	"_timescaledb_cache",
	"_timescaledb_catalog",
	"_timescaledb_config",
	"_timescaledb_debug",
	"_timescaledb_functions",
	"_timescaledb_internal",
	"hdb_catalog",
	"information_schema",
	"pg_catalog",
	"pg_toast",
	"timescaledb_experimental",
	"timescaledb_information",
	"timescaledb_internal",
}

type Options struct {
	ExcludeSchemas      []string
	ExcludeExtensions   []string
	IncludeSystemTables bool
}

type Extractor struct {
	pool           *database.Pool
	queryHelper    *database.QueryHelper
	hasTimescaleDB bool
	opts           Options
	queries        *queryBuilder
}

func New(ctx context.Context, pool *database.Pool, opts Options) (*Extractor, error) {
	if pool == nil {
		return nil, errors.New("pool cannot be nil")
	}

	hasTimescaleDB, err := pool.HasTimescaleDB(ctx)
	if err != nil {
		return nil, util.WrapError("check timescaledb", err)
	}

	if opts.ExcludeSchemas == nil {
		opts.ExcludeSchemas = systemSchemas
	} else {
		opts.ExcludeSchemas = append(opts.ExcludeSchemas, systemSchemas...)
	}

	if opts.ExcludeExtensions == nil {
		opts.ExcludeExtensions = []string{"plpgsql"}
	}

	return &Extractor{
		pool:           pool,
		queryHelper:    database.NewQueryHelper(pool),
		hasTimescaleDB: hasTimescaleDB,
		opts:           opts,
		queries: &queryBuilder{
			excludeSchemas:      opts.ExcludeSchemas,
			excludeExtensions:   opts.ExcludeExtensions,
			includeSystemTables: opts.IncludeSystemTables,
			hasTimescaleDB:      hasTimescaleDB,
		},
	}, nil
}

func (e *Extractor) Extract(ctx context.Context) (*schema.Database, error) {
	dbName, err := e.pool.CurrentDatabase(ctx)
	if err != nil {
		return nil, util.WrapError("get database name", err)
	}

	db := &schema.Database{
		Version:      schema.SchemaVersion,
		DatabaseName: dbName,
		ExtractedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	extractors := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"schemas", func(ctx context.Context) error {
			schemas, err := e.extractSchemas(ctx)
			if err != nil {
				return err
			}

			db.Schemas = schemas

			return nil
		}},
		{"extensions", func(ctx context.Context) error {
			extensions, err := e.extractExtensions(ctx)
			if err != nil {
				return err
			}

			db.Extensions = extensions

			return nil
		}},
		{"custom types", func(ctx context.Context) error {
			types, err := e.extractCustomTypes(ctx)
			if err != nil {
				return err
			}

			db.CustomTypes = types

			return nil
		}},
		{"sequences", func(ctx context.Context) error {
			sequences, err := e.extractSequences(ctx)
			if err != nil {
				return err
			}

			db.Sequences = sequences

			return nil
		}},
		{"tables", func(ctx context.Context) error {
			tables, err := e.extractTables(ctx)
			if err != nil {
				return err
			}

			db.Tables = tables

			return nil
		}},
		{"views", func(ctx context.Context) error {
			views, err := e.extractViews(ctx)
			if err != nil {
				return err
			}

			db.Views = views

			return nil
		}},
		{"materialized views", func(ctx context.Context) error {
			matViews, err := e.extractMaterializedViews(ctx)
			if err != nil {
				return err
			}

			db.MaterializedViews = matViews

			return nil
		}},
		{"functions", func(ctx context.Context) error {
			functions, err := e.extractFunctions(ctx)
			if err != nil {
				return err
			}

			db.Functions = functions

			return nil
		}},
		{"triggers", func(ctx context.Context) error {
			triggers, err := e.extractTriggers(ctx)
			if err != nil {
				return err
			}

			db.Triggers = triggers

			return nil
		}},
	}

	for _, extractor := range extractors {
		if err := ctx.Err(); err != nil {
			return nil, err //nolint:wrapcheck
		}

		if err := extractor.fn(ctx); err != nil {
			return nil, util.WrapError("extract "+extractor.name, err)
		}
	}

	if e.hasTimescaleDB {
		if err := e.extractTimescaleDBFeatures(ctx, db); err != nil {
			return nil, util.WrapError("extract timescaledb features", err)
		}
	}

	db.Sort()

	return db, nil
}

func (e *Extractor) extractTimescaleDBFeatures(ctx context.Context, db *schema.Database) error {
	hypertables, err := e.extractHypertables(ctx)
	if err != nil {
		return util.WrapError("extract hypertables", err)
	}

	db.Hypertables = hypertables

	continuousAggregates, err := e.extractContinuousAggregates(ctx)
	if err != nil {
		return util.WrapError("extract continuous aggregates", err)
	}

	db.ContinuousAggregates = continuousAggregates

	return nil
}
