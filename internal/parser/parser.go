package parser

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

type Config struct {
	CaseSensitive bool
}

type parseContext struct {
	currentFile string
	errors      []ParseError
	warnings    []Warning
	deferred    []deferredPartition
}

type Parser struct {
	config     Config
	errors     []ParseError
	warnings   []Warning
	normalizer *IdentifierNormalizer
	registry   *ParserRegistry
	ctx        *parseContext
	deferred   []deferredPartition
}

type deferredPartition struct {
	parentSchema  string
	parentName    string
	partitionName string
	definition    string
}

type Warning struct {
	File    string
	Line    int
	Message string
}

type Result struct {
	Database *schema.Database
	Errors   []ParseError
	Warnings []Warning
}

func (r *Result) HasErrors() bool {
	return len(r.Errors) > 0
}

type Option func(*Parser)

func New(opts ...Option) *Parser {
	p := &Parser{
		config: Config{},
	}
	for _, opt := range opts {
		opt(p)
	}

	p.normalizer = NewIdentifierNormalizer(p.config.CaseSensitive)
	p.registry = NewParserRegistry()

	return p
}

func (p *Parser) runWithContext(file string, fn func() error) (*parseContext, error) {
	if p.ctx != nil {
		prevFile := p.ctx.currentFile
		p.ctx.currentFile = file
		err := fn()
		p.ctx.currentFile = prevFile
		p.errors = p.ctx.errors
		p.warnings = p.ctx.warnings
		p.deferred = p.ctx.deferred

		return p.ctx, err
	}

	ctx := &parseContext{
		currentFile: file,
	}
	p.ctx = ctx
	err := fn()
	p.errors = ctx.errors
	p.warnings = ctx.warnings
	p.deferred = ctx.deferred
	p.ctx = nil

	return ctx, err
}

func (p *Parser) ensureContext() *parseContext {
	if p.ctx == nil {
		p.ctx = &parseContext{
			errors:   append([]ParseError(nil), p.errors...),
			warnings: append([]Warning(nil), p.warnings...),
			deferred: append([]deferredPartition(nil), p.deferred...),
		}
	}

	return p.ctx
}

func (p *Parser) setCurrentFile(file string) {
	ctx := p.ensureContext()
	ctx.currentFile = file
}

func (p *Parser) getCurrentFile() string {
	if p.ctx != nil {
		return p.ctx.currentFile
	}

	return ""
}

func (p *Parser) GetErrors() []ParseError {
	return p.errors
}

func (p *Parser) GetWarnings() []Warning {
	return p.warnings
}

func (p *Parser) ParseDirectory(dirPath string) (*Result, error) {
	db := &schema.Database{
		Version: "1.0",
	}

	ctx, err := p.runWithContext("", func() error {
		return p.parseDirectoryContents(dirPath, db)
	})
	if err != nil {
		return nil, err
	}

	if err := p.ProcessDeferredPartitions(db); err != nil {
		return nil, util.WrapError("processing deferred partitions", err)
	}

	return &Result{
		Database: db,
		Errors:   ctx.errors,
		Warnings: ctx.warnings,
	}, nil
}

func (p *Parser) ParseFile(filePath string, db *schema.Database) error {
	_, err := p.runWithContext(filePath, func() error {
		file, err := os.Open(filePath)
		if err != nil {
			return util.WrapError("opening file", err)
		}
		defer file.Close()

		content, err := io.ReadAll(file)
		if err != nil {
			return util.WrapError("reading file", err)
		}

		return p.ParseSQL(string(content), db)
	})
	if err != nil {
		return err
	}

	return p.ProcessDeferredPartitions(db)
}

func (p *Parser) parseDirectoryContents(dirPath string, db *schema.Database) error {
	subdirs := []string{
		"extensions",
		"types",
		"tables",
		"indexes",
		"views",
		"functions",
		"timescaledb",
	}

	for _, subdir := range subdirs {
		subPath := filepath.Join(dirPath, subdir)
		if _, err := os.Stat(subPath); os.IsNotExist(err) {
			continue
		}

		if err := p.parseSubdirectory(subPath, db); err != nil {
			return util.WrapError("parsing "+subdir, err)
		}
	}

	return nil
}

func (p *Parser) ParseSQL(sql string, db *schema.Database) error {
	if p.ctx == nil {
		_, err := p.runWithContext(p.getCurrentFile(), func() error {
			return p.parseSQLInternal(sql, db)
		})

		return err
	}

	return p.parseSQLInternal(sql, db)
}

func (p *Parser) parseSQLInternal(sql string, db *schema.Database) error {
	statements, err := splitStatements(sql)
	if err != nil {
		return err
	}

	for _, stmt := range statements {
		p.recordParseError(stmt, p.parseStatement(stmt, db))
	}

	return nil
}

func (p *Parser) parseStatement(stmt Statement, db *schema.Database) error {
	sql := stmt.NormalizedSQL()
	if sql == "" {
		return nil
	}

	stmtType := stmt.Type
	if stmtType == StmtUnknown {
		stmtType = determineStatementType(stmt.Tokens, sql)
	}

	if handler := p.registry.Get(stmtType); handler != nil {
		return handler.Parse(p, stmt, db) //nolint:wrapcheck
	}

	p.addWarning(stmt.Line, "unsupported statement: "+truncate(sql, 50))

	return nil
}

func (p *Parser) parseSubdirectory(dirPath string, db *schema.Database) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return util.WrapError("reading directory", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".sql") {
			continue
		}

		filePath := filepath.Join(dirPath, entry.Name())
		if err := p.ParseFileWithoutProcessingDeferred(filePath, db); err != nil {
			return util.WrapError("parsing file "+entry.Name(), err)
		}
	}

	return nil
}

func (p *Parser) ParseFileWithoutProcessingDeferred(filePath string, db *schema.Database) error {
	p.setCurrentFile(filePath)

	file, err := os.Open(filePath)
	if err != nil {
		return util.WrapError("opening file", err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return util.WrapError("reading file", err)
	}

	return p.ParseSQL(string(content), db)
}

func (p *Parser) addError(line int, message, sql string) {
	ctx := p.ensureContext()

	ctx.errors = append(ctx.errors, ParseError{
		File:    ctx.currentFile,
		Line:    line,
		Message: message,
		SQL:     sql,
	})

	p.errors = ctx.errors
}

func (p *Parser) recordParseError(stmt Statement, err error) {
	if err == nil {
		return
	}

	ctx := p.ensureContext()

	var parseErr ParseError
	if errors.As(err, &parseErr) {
		if parseErr.File == "" {
			parseErr.File = ctx.currentFile
		}

		if parseErr.Line == 0 {
			parseErr.Line = stmt.Line
		}

		if parseErr.SQL == "" {
			parseErr.SQL = stmt.SQL
		}

		ctx.errors = append(ctx.errors, parseErr)
		p.errors = ctx.errors

		return
	}

	ctx.errors = append(ctx.errors, ParseError{
		File:    ctx.currentFile,
		Line:    stmt.Line,
		Message: err.Error(),
		SQL:     stmt.SQL,
		Cause:   err,
	})

	p.errors = ctx.errors
}

func (p *Parser) addWarning(line int, message string) {
	ctx := p.ensureContext()

	ctx.warnings = append(ctx.warnings, Warning{
		File:    ctx.currentFile,
		Line:    line,
		Message: message,
	})

	p.warnings = ctx.warnings
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[:n] + "..."
}

func (p *Parser) ProcessDeferredPartitions(db *schema.Database) error {
	createdCtx := false

	if p.ctx == nil {
		p.ensureContext()

		createdCtx = true
	}

	ctx := p.ctx
	for _, deferred := range ctx.deferred {
		parentTable := db.GetTable(deferred.parentSchema, deferred.parentName)
		if parentTable == nil {
			p.addError(
				0,
				fmt.Sprintf(
					"parent table %s.%s not found for partition %s",
					deferred.parentSchema,
					deferred.parentName,
					deferred.partitionName,
				),
				"",
			)

			continue
		}

		if parentTable.PartitionStrategy == nil {
			parentTable.PartitionStrategy = &schema.PartitionStrategy{}
		}

		partition := schema.Partition{
			Name:       deferred.partitionName,
			Definition: deferred.definition,
		}

		parentTable.PartitionStrategy.Partitions = append(
			parentTable.PartitionStrategy.Partitions,
			partition,
		)
	}

	ctx.deferred = nil
	p.deferred = nil
	p.errors = ctx.errors
	p.warnings = ctx.warnings

	if createdCtx {
		p.ctx = nil
	}

	return nil
}
