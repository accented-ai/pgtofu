package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/generator"
	"github.com/accented-ai/pgtofu/internal/util"
)

type generateConfig struct {
	current      string
	desired      string
	outputDir    string
	preview      bool
	startVersion int
}

func newGenerateCommand() *cobra.Command {
	cfg := &generateConfig{}

	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate migration files from schema differences",
		Long: `Compare the current database schema with the desired schema and
generate golang-migrate compatible migration files.

Generated files follow the naming convention:
  {version}_{description}.up.sql
  {version}_{description}.down.sql`,
		Example: `  # Generate migrations
  pgtofu generate --current current-schema.json --desired ./schema

  # Preview without writing files
  pgtofu generate --current current-schema.json --desired ./schema --preview

  # Specify output directory and start version
  pgtofu generate --current current-schema.json --desired ./schema \
    --output-dir ./migrations --start-version 10`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGenerate(cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.current, "current", "",
		"Path to current schema JSON file (from extract)")
	cmd.Flags().StringVar(&cfg.desired, "desired", "",
		"Path to desired schema SQL file or directory")
	cmd.Flags().StringVar(&cfg.outputDir, "output-dir", "./migrations",
		"Output directory for migration files")
	cmd.Flags().BoolVar(&cfg.preview, "preview", false,
		"Preview migrations without writing files")
	cmd.Flags().IntVar(&cfg.startVersion, "start-version", 0,
		"Starting version number (0 = auto-detect)")

	cmd.MarkFlagRequired("current") //nolint:errcheck
	cmd.MarkFlagRequired("desired") //nolint:errcheck

	return cmd
}

func runGenerate(cfg *generateConfig) error {
	current, err := loadCurrentSchema(cfg.current)
	if err != nil {
		return err
	}

	desired, err := loadDesiredSchema(cfg.desired)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Comparing schemas...\n")

	d := differ.New(differ.DefaultOptions())

	diffResult, err := d.Compare(current, desired)
	if err != nil {
		return util.WrapError("compare schemas", err)
	}

	if !diffResult.HasChanges() {
		fmt.Fprintf(os.Stderr, "\nNo changes detected. No migrations generated.\n")
		return nil
	}

	fmt.Fprintf(os.Stderr, "Found %d changes\n", len(diffResult.Changes))

	opts := generator.DefaultOptions()
	opts.OutputDir = cfg.outputDir
	opts.PreviewMode = cfg.preview

	if cfg.startVersion > 0 {
		opts.StartVersion = cfg.startVersion
	} else {
		gen := generator.New(opts)
		if nextVersion, err := gen.GetNextMigrationVersion(); err == nil {
			opts.StartVersion = nextVersion
		}
	}

	gen := generator.New(opts)

	fmt.Fprintf(os.Stderr, "Generating migrations...\n")

	genResult, err := gen.Generate(diffResult)
	if err != nil {
		return util.WrapError("generate migrations", err)
	}

	fmt.Println(genResult.Summary())

	if !cfg.preview {
		absPath, _ := filepath.Abs(cfg.outputDir)
		fmt.Fprintf(os.Stderr, "\nMigrations written to: %s\n", absPath)
	}

	return nil
}
