package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

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
	var sqlFiles []string

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

		sqlFiles = append(sqlFiles, filePath)

		return nil
	})
	if err != nil {
		return util.WrapError("parse directory", err)
	}

	sort.SliceStable(sqlFiles, func(i, j int) bool {
		iPolicy := isPolicyFile(path, sqlFiles[i])

		jPolicy := isPolicyFile(path, sqlFiles[j])
		if iPolicy != jPolicy {
			return !iPolicy
		}

		return sqlFiles[i] < sqlFiles[j]
	})

	for _, filePath := range sqlFiles {
		if err := p.ParseFileWithoutProcessingDeferred(filePath, db); err != nil {
			return util.WrapError("parse file "+filePath, err)
		}
	}

	if err := p.ProcessDeferredPartitions(db); err != nil {
		return util.WrapError("processing deferred partitions", err)
	}

	validatePartitions(db)

	return nil
}

func isPolicyFile(root, filePath string) bool {
	relative, err := filepath.Rel(root, filePath)
	if err != nil {
		return false
	}

	for _, directory := range strings.Split(filepath.Dir(relative), string(filepath.Separator)) {
		if directory == "policies" {
			return true
		}
	}

	return false
}

func validatePartitions(db *schema.Database) { //nolint:gocognit
	for i := range db.Tables {
		table := &db.Tables[i]
		if table.PartitionStrategy == nil {
			continue
		}

		if table.PartitionStrategy.Type == "HASH" { //nolint:nestif
			expectedCount := len(table.PartitionStrategy.Partitions)
			if expectedCount == 0 {
				continue
			}

			remainders := make(map[int]bool)

			for _, partition := range table.PartitionStrategy.Partitions {
				if strings.Contains(strings.ToUpper(partition.Definition), "REMAINDER") {
					remainderPattern := regexp.MustCompile(`REMAINDER\s+(\d+)`)
					if matches := remainderPattern.FindStringSubmatch(partition.Definition); len(
						matches,
					) > 1 {
						var remainder int
						if _, err := fmt.Sscanf(matches[1], "%d", &remainder); err == nil {
							remainders[remainder] = true
						}
					}
				}
			}

			if len(remainders) != expectedCount {
				fmt.Fprintf(
					os.Stderr,
					"⚠️  WARNING: Table %s.%s has %d partitions but only %d unique REMAINDER values found\n",
					table.Schema,
					table.Name,
					expectedCount,
					len(remainders),
				)
			}

			if len(remainders) < expectedCount {
				missing := []int{}

				for i := range expectedCount {
					if !remainders[i] {
						missing = append(missing, i)
					}
				}

				if len(missing) > 0 {
					fmt.Fprintf(
						os.Stderr,
						"⚠️  WARNING: Table %s.%s missing partitions for REMAINDER values: %v\n",
						table.Schema,
						table.Name,
						missing,
					)
				}
			}
		}
	}
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
