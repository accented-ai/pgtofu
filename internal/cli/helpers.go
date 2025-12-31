package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/accented-ai/pgtofu/internal/parser"
	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

func loadCurrentSchema(path string) (*schema.Database, error) {
	fmt.Fprintf(os.Stderr, "Loading current schema from: %s\n", path)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, util.WrapError("read current schema", err)
	}

	var db schema.Database
	if err := json.Unmarshal(data, &db); err != nil {
		return nil, util.WrapError("parse current schema", err)
	}

	return &db, nil
}

func loadDesiredSchema(path string) (*schema.Database, error) {
	fmt.Fprintf(os.Stderr, "Loading desired schema from: %s\n", path)

	info, err := os.Stat(path)
	if err != nil {
		return nil, util.WrapError("stat path", err)
	}

	p := parser.New()
	db := &schema.Database{
		Version:      schema.SchemaVersion,
		DatabaseName: "desired",
		Tables:       []schema.Table{},
	}

	if info.IsDir() {
		if err := parseDirectory(p, path, db); err != nil {
			return nil, err
		}
	} else {
		if err := p.ParseFile(path, db); err != nil {
			return nil, util.WrapError("parse file", err)
		}
	}

	db.Sort()

	if err := checkParserErrors(p); err != nil {
		return nil, err
	}

	displayParserWarnings(p)

	return db, nil
}

func parseDirectory(p *parser.Parser, path string, db *schema.Database) error {
	err := filepath.WalkDir(path, func(filePath string, d os.DirEntry, err error) error {
		if err != nil {
			return util.WrapError("walking directory", err)
		}

		if d.IsDir() {
			return nil
		}

		if filepath.Ext(d.Name()) != ".sql" {
			return nil
		}

		if err := p.ParseFileWithoutProcessingDeferred(filePath, db); err != nil {
			return util.WrapError("parse file "+filePath, err)
		}

		return nil
	})
	if err != nil {
		return util.WrapError("parse directory", err)
	}

	if err := p.ProcessDeferredPartitions(db); err != nil {
		return util.WrapError("processing deferred partitions", err)
	}

	return nil
}

func checkParserErrors(p *parser.Parser) error {
	errors := p.GetErrors()
	if len(errors) == 0 {
		return nil
	}

	fmt.Fprintf(os.Stderr, "\n⚠️  Parser Errors:\n")

	for _, err := range errors {
		fmt.Fprintf(os.Stderr, "  - %s\n", err.Error())
	}

	return fmt.Errorf("encountered %d parsing errors", len(errors))
}

func displayParserWarnings(p *parser.Parser) {
	warnings := p.GetWarnings()
	if len(warnings) == 0 {
		return
	}

	fmt.Fprintf(os.Stderr, "\n⚠️  Parser Warnings:\n")

	for _, w := range warnings {
		fmt.Fprintf(os.Stderr, "  - %s:%d: %s\n", w.File, w.Line, w.Message)
	}
}

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
