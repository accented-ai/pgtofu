package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/accented-ai/pgtofu/internal/util"
)

func Execute(ctx context.Context) error {
	rootCmd := newRootCommand()
	rootCmd.AddCommand(
		newExtractCommand(ctx),
		newDiffCommand(),
	)

	return util.WrapError("execute command", rootCmd.ExecuteContext(ctx))
}

func newRootCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "pgtofu",
		Short: "PostgreSQL/TimescaleDB Schema Management Tool",
		Long: `pgtofu is a state-based database migration tool that enables declarative
schema management for PostgreSQL and TimescaleDB.

Define your desired schema in SQL files, and pgtofu automatically generates
safe, versioned migration files compatible with golang-migrate.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
}
