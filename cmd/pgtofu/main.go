package main

import (
	"context"
	"fmt"
	"os"

	"github.com/accented-ai/pgtofu/internal/cli"
)

var (
	version   = "dev"
	commit    = "unknown" //nolint:gochecknoglobals
	buildTime = "unknown" //nolint:gochecknoglobals
)

func main() {
	ctx := context.Background()

	if err := cli.Execute(ctx, cli.BuildInfo{
		Version:   version,
		Commit:    commit,
		BuildTime: buildTime,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
