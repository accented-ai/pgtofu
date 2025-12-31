package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/accented-ai/pgtofu/internal/util"
)

func writeOutput(path string, data []byte) error {
	if path == "-" {
		fmt.Println(string(data))
		return nil
	}

	outputDir := filepath.Dir(path)
	if outputDir != "." && outputDir != "" {
		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			return util.WrapError("create output directory", err)
		}
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return util.WrapError("write output file", err)
	}

	return nil
}
