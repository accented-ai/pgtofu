package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/accented-ai/pgtofu/internal/util"
)

type BuildInfo struct {
	Version   string
	Commit    string
	BuildTime string
}

func Execute(ctx context.Context, info BuildInfo) error {
	rootCmd := newRootCommand()
	rootCmd.AddCommand(
		newExtractCommand(ctx),
		newDiffCommand(),
		newGenerateCommand(),
		newPartitionCommand(),
		newVersionCommand(info),
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

func newVersionCommand(info BuildInfo) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("pgtofu %s\n", info.Version)
			fmt.Printf("  commit:     %s\n", info.Commit)
			fmt.Printf("  built:      %s\n", info.BuildTime)
		},
	}
}
