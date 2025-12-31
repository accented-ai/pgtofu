package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/accented-ai/pgtofu/internal/extractor"
	"github.com/accented-ai/pgtofu/internal/util"
	"github.com/accented-ai/pgtofu/pkg/database"
)

type extractConfig struct {
	databaseURL   string
	output        string
	excludeSchema []string
}

func newExtractCommand(ctx context.Context) *cobra.Command {
	cfg := &extractConfig{}

	cmd := &cobra.Command{
		Use:   "extract",
		Short: "Extract database schema to JSON",
		Long: `Extract the current database schema and save it as a JSON file.
This JSON file represents the current state of your database and can be
used with the diff and generate commands.`,
		Example: `  # Extract to file
  pgtofu extract --database-url "$DATABASE_URL" --output current-schema.json

  # Extract to stdout
  pgtofu extract --database-url "$DATABASE_URL" --output -`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExtract(ctx, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.databaseURL, "database-url", os.Getenv("DATABASE_URL"),
		"PostgreSQL connection URL (or set DATABASE_URL env var)")
	cmd.Flags().StringVarP(&cfg.output, "output", "o", "schema.json",
		"Output file path (use '-' for stdout)")
	cmd.Flags().StringArrayVar(&cfg.excludeSchema, "exclude-schema", []string{},
		"Additional schemas to exclude (can be specified multiple times). "+
			"System schemas (pg_catalog, information_schema, hdb_catalog, etc.) are excluded by default.")

	cmd.MarkFlagRequired("database-url") //nolint:errcheck

	return cmd
}

func runExtract(ctx context.Context, cfg *extractConfig) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	pool, err := database.NewPoolFromURL(ctx, cfg.databaseURL)
	if err != nil {
		return util.WrapError("connect to database", err)
	}
	defer pool.Close()

	extractorOpts := extractor.Options{
		ExcludeSchemas: cfg.excludeSchema,
	}

	ext, err := extractor.New(ctx, pool, extractorOpts)
	if err != nil {
		return util.WrapError("create extractor", err)
	}

	dbName, _ := pool.CurrentDatabase(ctx)
	hasTimescale, _ := pool.HasTimescaleDB(ctx)

	timescaleInfo := ""

	if hasTimescale {
		if version, err := pool.TimescaleDBVersion(ctx); err == nil {
			timescaleInfo = " with TimescaleDB " + version
		}
	}

	fmt.Fprintf(os.Stderr, "Connected to database: %s%s\n", dbName, timescaleInfo)
	fmt.Fprintf(os.Stderr, "Extracting schema...\n")

	startTime := time.Now()

	dbSchema, err := ext.Extract(ctx)
	if err != nil {
		return util.WrapError("extract schema", err)
	}

	printExtractionSummary(dbSchema, time.Since(startTime), hasTimescale)

	jsonData, _ := json.MarshalIndent(dbSchema, "", "  ")

	if err := writeOutput(cfg.output, jsonData); err != nil {
		return err
	}

	if cfg.output != "-" {
		absPath, _ := filepath.Abs(cfg.output)
		fmt.Fprintf(os.Stderr, "\nSchema written to: %s\n", absPath)
	}

	return nil
}

func printExtractionSummary(db any, elapsed time.Duration, hasTimescale bool) {
	type summaryDB interface { //nolint:interfacebloat
		GetSchemas() int
		GetExtensions() int
		GetCustomTypes() int
		GetSequences() int
		GetTables() int
		GetViews() int
		GetMaterializedViews() int
		GetFunctions() int
		GetTriggers() int
		GetHypertables() int
		GetContinuousAggregates() int
	}

	fmt.Fprintf(os.Stderr, "\nExtraction complete in %v\n", elapsed.Round(time.Millisecond))
	fmt.Fprintf(os.Stderr, "Summary:\n")

	if s, ok := db.(summaryDB); ok {
		fmt.Fprintf(os.Stderr, "  Schemas:             %d\n", s.GetSchemas())
		fmt.Fprintf(os.Stderr, "  Extensions:          %d\n", s.GetExtensions())
		fmt.Fprintf(os.Stderr, "  Custom Types:        %d\n", s.GetCustomTypes())
		fmt.Fprintf(os.Stderr, "  Sequences:           %d\n", s.GetSequences())
		fmt.Fprintf(os.Stderr, "  Tables:              %d\n", s.GetTables())
		fmt.Fprintf(os.Stderr, "  Views:               %d\n", s.GetViews())
		fmt.Fprintf(os.Stderr, "  Materialized Views:  %d\n", s.GetMaterializedViews())
		fmt.Fprintf(os.Stderr, "  Functions:           %d\n", s.GetFunctions())
		fmt.Fprintf(os.Stderr, "  Triggers:            %d\n", s.GetTriggers())

		if hasTimescale {
			fmt.Fprintf(os.Stderr, "  Hypertables:         %d\n", s.GetHypertables())
			fmt.Fprintf(os.Stderr, "  Continuous Aggregates: %d\n", s.GetContinuousAggregates())
		}
	}
}
