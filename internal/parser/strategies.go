package parser

import (
	"errors"
	"regexp"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type StatementParser interface {
	StatementTypes() []StatementType
	Parse(root *Parser, stmt Statement, db *schema.Database) error
}

type ParserRegistry struct {
	parsers map[StatementType]StatementParser
}

func NewParserRegistry() *ParserRegistry {
	r := &ParserRegistry{
		parsers: make(map[StatementType]StatementParser),
	}

	r.Register(NewTableParser())
	r.Register(NewIndexParser())
	r.Register(NewViewParser())
	r.Register(NewMaterializedViewParser())
	r.Register(NewFunctionParser())
	r.Register(NewTriggerParser())
	r.Register(NewExtensionParser())
	r.Register(NewSchemaParser())
	r.Register(NewTypeParser())
	r.Register(NewSequenceParser())
	r.Register(NewAlterTableParser())
	r.Register(NewHypertableParser())
	r.Register(NewCompressionPolicyParser())
	r.Register(NewRetentionPolicyParser())
	r.Register(NewContinuousAggregatePolicyParser())
	r.Register(NewCommentParser())
	r.Register(NewDoBlockParser())

	return r
}

func (r *ParserRegistry) Register(parser StatementParser) {
	for _, stmtType := range parser.StatementTypes() {
		r.parsers[stmtType] = parser
	}
}

func (r *ParserRegistry) Get(stmtType StatementType) StatementParser {
	return r.parsers[stmtType]
}

type ExtensionParser struct {
	namePattern    *regexp.Regexp
	schemaPattern  *regexp.Regexp
	versionPattern *regexp.Regexp
}

func NewExtensionParser() *ExtensionParser {
	return &ExtensionParser{
		namePattern: regexp.MustCompile(
			`(?i)CREATE\s+EXTENSION\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*|"[^"]*")`,
		),
		schemaPattern: regexp.MustCompile(
			`(?i)(?:WITH\s+)?SCHEMA\s*(?:=\s*)?([a-zA-Z_][a-zA-Z0-9_]*|"[^"]*")`,
		),
		versionPattern: regexp.MustCompile(
			`(?i)VERSION\s+'?([^'\s]+)'?`,
		),
	}
}

func (p *ExtensionParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreateExtension}
}

func (p *ExtensionParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	sql := stmt.NormalizedSQL()

	matches := p.namePattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return errors.New("cannot extract extension name")
	}

	name := unquote(matches[1])
	ext := schema.Extension{Name: name}

	if schemaMatch := p.schemaPattern.FindStringSubmatch(sql); len(schemaMatch) > 1 {
		ext.Schema = schema.NormalizeIdentifier(unquote(schemaMatch[1]))
	}

	if versionMatch := p.versionPattern.FindStringSubmatch(sql); len(versionMatch) > 1 {
		ext.Version = versionMatch[1]
	}

	db.Extensions = append(db.Extensions, ext)

	return nil
}

type SchemaParser struct {
	namePattern *regexp.Regexp
}

func NewSchemaParser() *SchemaParser {
	return &SchemaParser{
		namePattern: regexp.MustCompile(
			`(?i)CREATE\s+SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*|"[^"]*")`,
		),
	}
}

func (p *SchemaParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreateSchema}
}

func (p *SchemaParser) Parse(_ *Parser, stmt Statement, db *schema.Database) error {
	sql := stmt.NormalizedSQL()

	matches := p.namePattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return errors.New("cannot extract schema name")
	}

	name := schema.NormalizeIdentifier(unquote(matches[1]))
	newSchema := schema.Schema{Name: name}

	for i := range db.Schemas {
		if db.Schemas[i].Name == name {
			db.Schemas[i] = newSchema
			return nil
		}
	}

	db.Schemas = append(db.Schemas, newSchema)

	return nil
}

type TypeParser struct {
	definitionPattern *regexp.Regexp
}

func NewTypeParser() *TypeParser {
	return &TypeParser{
		definitionPattern: regexp.MustCompile(
			`(?i)CREATE\s+TYPE\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s+AS\s+(\w+)`,
		),
	}
}

func (p *TypeParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreateType}
}

func (p *TypeParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	sql := stmt.NormalizedSQL()

	matches := p.definitionPattern.FindStringSubmatch(sql)
	if len(matches) < 3 {
		return errors.New("cannot extract type definition")
	}

	schemaName, typeName := root.splitSchemaTable(matches[1])

	customType := schema.CustomType{
		Schema:     schemaName,
		Name:       typeName,
		Type:       strings.ToLower(matches[2]),
		Definition: sql,
	}

	if strings.EqualFold(matches[2], "ENUM") {
		if values := extractParens(sql); values != "" {
			for _, val := range splitByComma(values) {
				customType.Values = append(customType.Values, unquote(val))
			}
		}
	}

	db.CustomTypes = append(db.CustomTypes, customType)

	return nil
}

type SequenceParser struct {
	namePattern *regexp.Regexp
}

func NewSequenceParser() *SequenceParser {
	return &SequenceParser{
		namePattern: regexp.MustCompile(
			`(?i)CREATE\s+SEQUENCE\s+([a-zA-Z_][a-zA-Z0-9_.]*|"[^"]*"(?:\."[^"]*")?)`,
		),
	}
}

func (p *SequenceParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreateSequence}
}

func (p *SequenceParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	sql := stmt.NormalizedSQL()

	matches := p.namePattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return errors.New("cannot extract sequence name")
	}

	schemaName, sequenceName := root.splitSchemaTable(matches[1])

	sequence := schema.Sequence{
		Schema:    schemaName,
		Name:      sequenceName,
		DataType:  "bigint",
		Increment: 1,
	}

	db.Sequences = append(db.Sequences, sequence)

	return nil
}

type TableParser struct{}

func NewTableParser() *TableParser {
	return &TableParser{}
}

func (p *TableParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreateTable}
}

func (p *TableParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseCreateTable(stmt.NormalizedSQL(), db)
}

type IndexParser struct{}

func NewIndexParser() *IndexParser {
	return &IndexParser{}
}

func (p *IndexParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreateIndex}
}

func (p *IndexParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseCreateIndex(stmt.NormalizedSQL(), db)
}

type ViewParser struct{}

func NewViewParser() *ViewParser {
	return &ViewParser{}
}

func (p *ViewParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreateView}
}

func (p *ViewParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseCreateView(stmt.NormalizedSQL(), db)
}

type MaterializedViewParser struct{}

func NewMaterializedViewParser() *MaterializedViewParser {
	return &MaterializedViewParser{}
}

func (p *MaterializedViewParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreateMaterializedView}
}

func (p *MaterializedViewParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseCreateMaterializedView(stmt.NormalizedSQL(), db)
}

type FunctionParser struct{}

func NewFunctionParser() *FunctionParser {
	return &FunctionParser{}
}

func (p *FunctionParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreateFunction}
}

func (p *FunctionParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseCreateFunction(stmt.NormalizedSQL(), db)
}

type TriggerParser struct{}

func NewTriggerParser() *TriggerParser {
	return &TriggerParser{}
}

func (p *TriggerParser) StatementTypes() []StatementType {
	return []StatementType{StmtCreateTrigger}
}

func (p *TriggerParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseCreateTrigger(stmt.NormalizedSQL(), db)
}

type AlterTableParser struct{}

func NewAlterTableParser() *AlterTableParser {
	return &AlterTableParser{}
}

func (p *AlterTableParser) StatementTypes() []StatementType {
	return []StatementType{StmtAlterTable}
}

func (p *AlterTableParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseAlterTable(stmt.NormalizedSQL(), db)
}

type HypertableParser struct{}

func NewHypertableParser() *HypertableParser {
	return &HypertableParser{}
}

func (p *HypertableParser) StatementTypes() []StatementType {
	return []StatementType{StmtSelectCreateHypertable}
}

func (p *HypertableParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseCreateHypertable(stmt.NormalizedSQL(), db)
}

type CompressionPolicyParser struct{}

func NewCompressionPolicyParser() *CompressionPolicyParser {
	return &CompressionPolicyParser{}
}

func (p *CompressionPolicyParser) StatementTypes() []StatementType {
	return []StatementType{StmtSelectAddCompressionPolicy}
}

func (p *CompressionPolicyParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseCompressionPolicy(stmt.NormalizedSQL(), db)
}

type RetentionPolicyParser struct{}

func NewRetentionPolicyParser() *RetentionPolicyParser {
	return &RetentionPolicyParser{}
}

func (p *RetentionPolicyParser) StatementTypes() []StatementType {
	return []StatementType{StmtSelectAddRetentionPolicy}
}

func (p *RetentionPolicyParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseRetentionPolicy(stmt.NormalizedSQL(), db)
}

type ContinuousAggregatePolicyParser struct{}

func NewContinuousAggregatePolicyParser() *ContinuousAggregatePolicyParser {
	return &ContinuousAggregatePolicyParser{}
}

func (p *ContinuousAggregatePolicyParser) StatementTypes() []StatementType {
	return []StatementType{StmtSelectAddContinuousAggregatePolicy}
}

func (p *ContinuousAggregatePolicyParser) Parse(
	root *Parser,
	stmt Statement,
	db *schema.Database,
) error {
	return root.parseContinuousAggregatePolicy(stmt.NormalizedSQL(), db)
}

type CommentParser struct{}

func NewCommentParser() *CommentParser {
	return &CommentParser{}
}

func (p *CommentParser) StatementTypes() []StatementType {
	return []StatementType{StmtComment}
}

func (p *CommentParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseComment(stmt.NormalizedSQL(), db)
}

type DoBlockParser struct{}

func NewDoBlockParser() *DoBlockParser {
	return &DoBlockParser{}
}

func (p *DoBlockParser) StatementTypes() []StatementType {
	return []StatementType{StmtDoBlock}
}

func (p *DoBlockParser) Parse(root *Parser, stmt Statement, db *schema.Database) error {
	return root.parseDoBlock(stmt.NormalizedSQL(), db)
}
