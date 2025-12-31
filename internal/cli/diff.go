package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/util"
)

type diffConfig struct {
	current string
	desired string
}

func newDiffCommand() *cobra.Command {
	cfg := &diffConfig{}

	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Compare current schema with desired schema",
		Long: `Compare the current database schema (from extract) with the desired
schema (SQL files) and display the differences.

This command does not generate migration files. Use 'generate' for that.`,
		Example: `  # Compare schemas
  pgtofu diff --current current-schema.json --desired ./schema

  # Compare with single file
  pgtofu diff --current current-schema.json --desired schema.sql`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDiff(cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.current, "current", "",
		"Path to current schema JSON file (from extract)")
	cmd.Flags().StringVar(&cfg.desired, "desired", "",
		"Path to desired schema SQL file or directory")

	cmd.MarkFlagRequired("current") //nolint:errcheck
	cmd.MarkFlagRequired("desired") //nolint:errcheck

	return cmd
}

func runDiff(cfg *diffConfig) error {
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

	result, err := d.Compare(current, desired)
	if err != nil {
		return util.WrapError("compare schemas", err)
	}

	fmt.Println(result.Summary())

	if result.HasChanges() {
		fmt.Println("\nDetailed Changes:")

		for _, change := range result.Changes {
			fmt.Printf(
				"[%s] %s: %s\n",
				change.Severity,
				change.Type,
				change.Description,
			)
		}
	}

	if result.HasBreakingChanges() {
		fmt.Fprintf(os.Stderr, "\n⚠️  WARNING: Breaking changes detected!\n")
	}

	return nil
}
