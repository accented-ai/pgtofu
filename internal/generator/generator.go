// Package generator provides functionality for generating PostgreSQL migration files
// from schema differences. It converts high-level change descriptions into
// executable SQL statements, organized into versioned migration files compatible
// with golang-migrate.
//
// # Architecture
//
// The package is organized into several key components:
//
//   - Generator: Orchestrates the migration generation process
//   - DDLBuilder: Constructs DDL statements for specific change types
//   - MigrationFile: Represents a single migration file (up or down)
//   - Options: Configuration for migration generation behavior
//
// The data flow is as follows:
//
//	Changes → DDLBuilder → DDLStatements → Generator → MigrationFiles
//
// # Usage
//
// Basic usage example:
//
//	opts := generator.DefaultOptions()
//	opts.OutputDir = "./migrations"
//
//	gen := generator.New(opts)
//	result, err := gen.Generate(diffResult)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	fmt.Println(result.Summary())
//
// # Migration Generation
//
// The generator groups changes by schema and dependency order, then creates
// versioned migration files. Each migration includes:
//
//   - Header comments with metadata and change descriptions
//   - SQL statements in dependency order
//   - Transaction control (configurable)
//   - Safety warnings for potentially dangerous operations
//
// # Options
//
// The generator supports several options to customize behavior:
//
//   - OutputDir: Directory for migration files
//   - StartVersion: Initial version number
//   - TransactionMode: Control transaction wrapping (auto/always/never)
//   - IncludeComments: Add descriptive comments to migrations
//   - Idempotent: Use IF EXISTS/IF NOT EXISTS clauses
//   - GenerateDownMigrations: Create rollback migrations
//   - MaxOperationsPerFile: Split large migrations into batches
//   - PreviewMode: Generate without writing files
//
// # Thread Safety
//
// Generator instances are NOT safe for concurrent use. Create separate
// instances for parallel migration generation.
package generator

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/graph"
	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

type Generator struct {
	Options *Options
}

func New(opts *Options) *Generator {
	if opts == nil {
		opts = DefaultOptions()
	}

	return &Generator{Options: opts}
}

func (g *Generator) Generate(result *differ.DiffResult) (*GenerateResult, error) {
	if result == nil {
		return nil, ErrNilDiffResult
	}

	if err := g.Options.Validate(); err != nil {
		return nil, util.WrapError("invalid options", err)
	}

	if !result.HasChanges() {
		return &GenerateResult{
			Migrations: []MigrationPair{},
			Warnings:   []string{"No changes detected, no migrations generated"},
		}, nil
	}

	genResult := &GenerateResult{
		Migrations: []MigrationPair{},
		Warnings:   []string{},
	}

	batches := g.GroupChangesBySchema(result.Changes)

	currentVersion := g.Options.StartVersion
	for i, batch := range batches {
		migration, warnings := g.generateMigration(currentVersion+i, batch, result)
		genResult.Migrations = append(genResult.Migrations, migration)
		genResult.Warnings = append(genResult.Warnings, warnings...)
	}

	if !g.Options.PreviewMode {
		if err := g.writeMigrationFiles(genResult); err != nil {
			return nil, util.WrapError("write migration files", err)
		}

		genResult.FilesGenerated = len(genResult.Migrations)
		if g.Options.GenerateDownMigrations {
			genResult.FilesGenerated *= 2
		}
	}

	return genResult, nil
}

func (g *Generator) GroupChangesBySchema(changes []differ.Change) [][]differ.Change {
	if len(changes) == 0 {
		return nil
	}

	var batches [][]differ.Change

	schemaCreationChanges, nonSchemaCreationChanges := g.separateSchemaCreation(changes)
	if len(schemaCreationChanges) > 0 {
		batches = append(batches, schemaCreationChanges)
	}

	extensionChanges, nonExtensionChanges := g.separateExtensions(nonSchemaCreationChanges)
	if len(extensionChanges) > 0 {
		batches = append(batches, extensionChanges)
	}

	schemaGroups := g.groupBySchema(nonExtensionChanges)
	orderedSchemas := g.orderSchemasByDependencies(schemaGroups, nonExtensionChanges)

	for _, schema := range orderedSchemas {
		schemaChanges := schemaGroups[schema]
		g.sortSchemaChanges(schemaChanges)
		batches = append(batches, g.splitIntoBatches(schemaChanges)...)
	}

	return batches
}

func (g *Generator) groupBySchema(changes []differ.Change) map[string][]differ.Change {
	schemaGroups := make(map[string][]differ.Change)

	for i := range changes {
		schema := string(extractSchema(&changes[i]))
		schemaGroups[schema] = append(schemaGroups[schema], changes[i])
	}

	return schemaGroups
}

func (g *Generator) orderSchemasByDependencies( //nolint:gocognit
	schemaGroups map[string][]differ.Change,
	allChanges []differ.Change,
) []string {
	dg := graph.NewDirectedGraph[string]()

	for schema := range schemaGroups {
		dg.AddNode(schema)
	}

	changeByObject := make(map[string]*differ.Change)

	for i := range allChanges {
		change := &allChanges[i]
		if change.ObjectName != "" {
			normalized := normalizeObjectName(change.ObjectName)
			changeByObject[normalized] = change
			changeByObject[change.ObjectName] = change
		}

		if change.Details != nil {
			table, hasTable, err := getOptionalTable(change.Details)
			if err == nil && hasTable &&
				change.Type == differ.ChangeTypeAddTable {
				qualifiedName := table.QualifiedName()
				if qualifiedName != "" {
					normalized := normalizeObjectName(qualifiedName)
					changeByObject[normalized] = change
					changeByObject[qualifiedName] = change
				}
			}
		}
	}

	for schemaName, changes := range schemaGroups {
		for i := range changes {
			change := &changes[i]

			for _, dep := range change.DependsOn {
				depNormalized := normalizeObjectName(dep)

				var (
					depChange *differ.Change
					exists    bool
				)

				if depChange, exists = changeByObject[depNormalized]; !exists {
					if depChange, exists = changeByObject[dep]; !exists {
						if !strings.Contains(dep, ".") {
							publicKey := fmt.Sprintf(
								"%s.%s",
								schema.DefaultSchema,
								strings.ToLower(dep),
							)
							depChange, exists = changeByObject[publicKey]
						}
					}
				}

				if exists {
					depSchema := string(extractSchema(depChange))
					if depSchema != schemaName && dg.HasNode(depSchema) {
						_ = dg.AddEdge(schemaName, depSchema)
					}
				}
			}
		}
	}

	ordered, err := dg.TopologicalSort()
	if err != nil {
		return g.getSchemaOrderFallback(schemaGroups)
	}

	return g.prioritizeSchemaCreation(ordered, schemaGroups)
}

func (g *Generator) prioritizeSchemaCreation(
	ordered []string,
	schemaGroups map[string][]differ.Change,
) []string {
	var (
		withSchemaCreation    []string
		withoutSchemaCreation []string
	)

	for _, schema := range ordered {
		hasSchemaCreation := false

		for _, change := range schemaGroups[schema] {
			if change.Type == differ.ChangeTypeAddSchema ||
				change.Type == differ.ChangeTypeDropSchema {
				hasSchemaCreation = true
				break
			}
		}

		if hasSchemaCreation {
			withSchemaCreation = append(withSchemaCreation, schema)
		} else {
			withoutSchemaCreation = append(withoutSchemaCreation, schema)
		}
	}

	return append(withSchemaCreation, withoutSchemaCreation...)
}

func (g *Generator) getSchemaOrderFallback(schemaGroups map[string][]differ.Change) []string {
	schemas := make([]string, 0, len(schemaGroups))
	for schema := range schemaGroups {
		schemas = append(schemas, schema)
	}

	if len(schemas) == 0 {
		return schemas
	}

	for i := range len(schemas) - 1 {
		for j := i + 1; j < len(schemas); j++ {
			if schemas[i] > schemas[j] {
				schemas[i], schemas[j] = schemas[j], schemas[i]
			}
		}
	}

	return g.prioritizeSchemaCreation(schemas, schemaGroups)
}

func (g *Generator) splitIntoBatches(changes []differ.Change) [][]differ.Change {
	if len(changes) == 0 {
		return nil
	}

	var ( //nolint:prealloc
		batches      [][]differ.Change
		currentBatch []differ.Change
	)

	objectsInBatch := make(map[string]bool)

	for i, change := range changes {
		currentBatch = append(currentBatch, change)

		if change.ObjectName != "" {
			objectsInBatch[normalizeObjectName(change.ObjectName)] = true
		}

		if len(currentBatch) >= g.Options.MaxOperationsPerFile {
			if i+1 < len(changes) && g.canSplitBetween(currentBatch, changes[i+1], objectsInBatch) {
				batches = append(batches, currentBatch)
				currentBatch = []differ.Change{}
				objectsInBatch = make(map[string]bool)
			}
		}
	}

	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

func (g *Generator) canSplitBetween(
	currentBatch []differ.Change,
	nextChange differ.Change,
	objectsInBatch map[string]bool,
) bool {
	if g.hasDependencyInBatch(nextChange, objectsInBatch) {
		return false
	}

	if g.sharesSameTable(currentBatch, nextChange) {
		return false
	}

	if g.isViewRecreation(currentBatch, nextChange) {
		return false
	}

	if g.isConstraintRecreation(currentBatch, nextChange) {
		return false
	}

	if g.isIndexOnNewColumn(currentBatch, nextChange) {
		return false
	}

	return true
}

func (g *Generator) hasDependencyInBatch(
	nextChange differ.Change,
	objectsInBatch map[string]bool,
) bool {
	for _, dep := range nextChange.DependsOn {
		depNormalized := normalizeObjectName(dep)
		if objectsInBatch[depNormalized] {
			return true
		}
	}

	return false
}

func (g *Generator) sharesSameTable(currentBatch []differ.Change, nextChange differ.Change) bool {
	nextTable := extractTableNameFromChange(&nextChange)
	if nextTable == "" {
		return false
	}

	for i := range currentBatch {
		currentTable := extractTableNameFromChange(&currentBatch[i])
		if currentTable == nextTable {
			return true
		}
	}

	return false
}

func (g *Generator) isViewRecreation(currentBatch []differ.Change, nextChange differ.Change) bool {
	if nextChange.Type != differ.ChangeTypeAddView &&
		nextChange.Type != differ.ChangeTypeAddMaterializedView {
		return false
	}

	nextViewName := normalizeObjectName(nextChange.ObjectName)

	for i := range currentBatch {
		ch := &currentBatch[i]
		if (ch.Type == differ.ChangeTypeDropView ||
			ch.Type == differ.ChangeTypeDropMaterializedView) &&
			normalizeObjectName(ch.ObjectName) == nextViewName {
			return true
		}
	}

	return false
}

func (g *Generator) isConstraintRecreation(
	currentBatch []differ.Change,
	nextChange differ.Change,
) bool {
	if nextChange.Type != differ.ChangeTypeAddConstraint {
		return false
	}

	nextTable := extractTableNameFromChange(&nextChange)
	if nextTable == "" {
		return false
	}

	for i := range currentBatch {
		ch := &currentBatch[i]
		if ch.Type != differ.ChangeTypeDropConstraint {
			continue
		}

		chTable := extractTableNameFromChange(ch)
		if nextTable == chTable {
			return true
		}
	}

	return false
}

func (g *Generator) isIndexOnNewColumn(
	currentBatch []differ.Change,
	nextChange differ.Change,
) bool {
	if nextChange.Type != differ.ChangeTypeAddIndex {
		return false
	}

	indexDetails, ok := nextChange.Details["index"]
	if !ok {
		return false
	}

	idx, ok := indexDetails.(interface{ GetColumns() []string })
	if !ok {
		return false
	}

	indexCols := idx.GetColumns()

	for i := range currentBatch {
		ch := &currentBatch[i]
		if ch.Type != differ.ChangeTypeAddColumn {
			continue
		}

		colName, _ := ch.Details["column_name"].(string)

		for _, icol := range indexCols {
			if strings.EqualFold(colName, icol) {
				return true
			}
		}
	}

	return false
}

func (g *Generator) sortSchemaChanges(changes []differ.Change) {
	tableFirstOrder := make(map[string]int)

	for _, change := range changes {
		tableName := extractTableNameFromChange(&change)
		if tableName != "" {
			if firstOrder, exists := tableFirstOrder[tableName]; !exists ||
				change.Order < firstOrder {
				tableFirstOrder[tableName] = change.Order
			}
		}
	}

	sort.Slice(changes, func(i, j int) bool {
		iIsSchema := changes[i].Type == differ.ChangeTypeAddSchema ||
			changes[i].Type == differ.ChangeTypeDropSchema
		jIsSchema := changes[j].Type == differ.ChangeTypeAddSchema ||
			changes[j].Type == differ.ChangeTypeDropSchema

		if iIsSchema && !jIsSchema {
			return true
		}

		if !iIsSchema && jIsSchema {
			return false
		}

		tableNameI := extractTableNameFromChange(&changes[i])
		tableNameJ := extractTableNameFromChange(&changes[j])

		if tableNameI != "" && tableNameJ != "" { //nolint:nestif
			if tableNameI == tableNameJ {
				priorityI := getChangePriorityForTable(changes[i].Type)

				priorityJ := getChangePriorityForTable(changes[j].Type)
				if priorityI != priorityJ {
					return priorityI < priorityJ
				}

				return changes[i].Order < changes[j].Order
			}

			orderI, hasI := tableFirstOrder[tableNameI]

			orderJ, hasJ := tableFirstOrder[tableNameJ]
			if hasI && hasJ {
				if orderI != orderJ {
					return orderI < orderJ
				}

				return tableNameI < tableNameJ
			}

			if hasI {
				return true
			}

			if hasJ {
				return false
			}

			return tableNameI < tableNameJ
		}

		return changes[i].Order < changes[j].Order
	})
}

func (g *Generator) separateExtensions(changes []differ.Change) ([]differ.Change, []differ.Change) {
	var (
		extensions []differ.Change
		other      []differ.Change
	)

	for _, change := range changes {
		if change.Type == differ.ChangeTypeAddExtension ||
			change.Type == differ.ChangeTypeDropExtension ||
			change.Type == differ.ChangeTypeModifyExtension {
			extensions = append(extensions, change)
		} else {
			other = append(other, change)
		}
	}

	return extensions, other
}

func (g *Generator) separateSchemaCreation(
	changes []differ.Change,
) ([]differ.Change, []differ.Change) {
	var (
		schemaCreation []differ.Change
		other          []differ.Change
	)

	for _, change := range changes {
		if change.Type == differ.ChangeTypeAddSchema || change.Type == differ.ChangeTypeDropSchema {
			schemaCreation = append(schemaCreation, change)
		} else {
			other = append(other, change)
		}
	}

	return schemaCreation, other
}

func (g *Generator) generateMigration(
	version int,
	changes []differ.Change,
	result *differ.DiffResult,
) (MigrationPair, []string) {
	var warnings []string

	description := GenerateMigrationName(changes)
	builder := NewDDLBuilder(result, g.Options.Idempotent)

	upStatements, upWarnings := g.buildUpStatements(changes, builder)
	warnings = append(warnings, upWarnings...)

	var (
		downStatements []DDLStatement
		downWarnings   []string
	)

	if g.Options.GenerateDownMigrations {
		downStatements, downWarnings = g.buildDownStatements(changes, builder)
		warnings = append(warnings, downWarnings...)
	}

	upFile := &MigrationFile{
		Version:     version,
		Description: description,
		Direction:   DirectionUp,
		FileName:    FormatMigrationFileName(version, description, DirectionUp),
		Content: g.formatMigrationContent(
			version,
			description,
			DirectionUp,
			upStatements,
			changes,
		),
	}

	var downFile *MigrationFile
	if g.Options.GenerateDownMigrations {
		downFile = &MigrationFile{
			Version:     version,
			Description: description,
			Direction:   DirectionDown,
			FileName:    FormatMigrationFileName(version, description, DirectionDown),
			Content: g.formatMigrationContent(
				version,
				description,
				DirectionDown,
				downStatements,
				changes,
			),
		}
	}

	return MigrationPair{
		Version:     version,
		Description: description,
		UpFile:      upFile,
		DownFile:    downFile,
	}, warnings
}

func (g *Generator) buildUpStatements(
	changes []differ.Change,
	builder *DDLBuilder,
) ([]DDLStatement, []string) {
	var ( //nolint:prealloc
		statements []DDLStatement
		warnings   []string
	)

	droppedTables := g.identifyDroppedTables(changes)

	for _, change := range changes {
		if g.shouldSkipHypertableChange(change, droppedTables) {
			continue
		}

		stmt, err := builder.BuildUpStatement(change)
		if err != nil {
			warnings = append(
				warnings,
				fmt.Sprintf("Failed to build UP statement for %s: %v", change.Description, err),
			)

			continue
		}

		statements = append(statements, stmt)

		if stmt.IsUnsafe {
			warnings = append(warnings, "Unsafe operation: "+stmt.Description)
		}
	}

	return statements, warnings
}

func (g *Generator) buildDownStatements(
	changes []differ.Change,
	builder *DDLBuilder,
) ([]DDLStatement, []string) {
	var (
		statements []DDLStatement
		warnings   []string
	)

	dropTargets := g.identifyDropTargets(changes)
	droppedTables := g.identifyDroppedTables(changes)

	for i := len(changes) - 1; i >= 0; i-- {
		change := changes[i]

		if g.shouldSkipCommentOnlyChange(change, dropTargets) {
			continue
		}

		if g.shouldSkipHypertableChange(change, droppedTables) {
			continue
		}

		stmt, err := builder.BuildDownStatement(change)
		if err != nil {
			warnings = append(
				warnings,
				fmt.Sprintf("Failed to build DOWN statement for %s: %v", change.Description, err),
			)
			statements = append(statements, DDLStatement{
				SQL:         "-- WARNING: Manual rollback required for: " + change.Description,
				Description: "Manual rollback required: " + change.Description,
				IsUnsafe:    true,
			})

			continue
		}

		statements = append(statements, stmt)

		if stmt.IsUnsafe {
			warnings = append(warnings, "Unsafe rollback operation: "+stmt.Description)
		}
	}

	return statements, warnings
}

func (g *Generator) identifyDropTargets(changes []differ.Change) map[string]bool {
	dropTargets := make(map[string]bool)

	for _, ch := range changes {
		switch ch.Type {
		case differ.ChangeTypeAddTable,
			differ.ChangeTypeAddView,
			differ.ChangeTypeAddMaterializedView,
			differ.ChangeTypeAddFunction,
			differ.ChangeTypeAddSequence,
			differ.ChangeTypeAddCustomType,
			differ.ChangeTypeAddExtension,
			differ.ChangeTypeAddIndex,
			differ.ChangeTypeAddTrigger:
			dropTargets[ch.ObjectName] = true
		}
	}

	return dropTargets
}

func (g *Generator) identifyDroppedTables(changes []differ.Change) map[string]bool {
	droppedTables := make(map[string]bool)

	for _, ch := range changes {
		if ch.Type == differ.ChangeTypeDropTable {
			droppedTables[ch.ObjectName] = true
		}
	}

	return droppedTables
}

func (g *Generator) shouldSkipHypertableChange(
	change differ.Change,
	droppedTables map[string]bool,
) bool {
	if !droppedTables[change.ObjectName] {
		return false
	}

	switch change.Type {
	case differ.ChangeTypeDropHypertable,
		differ.ChangeTypeDropCompressionPolicy,
		differ.ChangeTypeDropRetentionPolicy:
		return true
	}

	return false
}

func (g *Generator) shouldSkipCommentOnlyChange(
	change differ.Change,
	dropTargets map[string]bool,
) bool {
	if !dropTargets[change.ObjectName] {
		return false
	}

	if change.Type == differ.ChangeTypeModifyTableComment ||
		change.Type == differ.ChangeTypeModifyColumnComment {
		return true
	}

	if change.Type == differ.ChangeTypeModifyView ||
		change.Type == differ.ChangeTypeModifyFunction {
		return g.isCommentOnlyChange(change)
	}

	return false
}

func (g *Generator) isCommentOnlyChange(change differ.Change) bool {
	ok, err := isCommentChangeOnly(change)
	return err == nil && ok
}

func (g *Generator) formatMigrationContent(
	version int,
	description string,
	direction Direction,
	statements []DDLStatement,
	changes []differ.Change,
) string {
	var sb strings.Builder

	if g.Options.IncludeComments {
		header := &migrationHeader{
			Version:     version,
			Description: description,
			Direction:   direction,
			Generated:   time.Now(),
			Changes:     make([]string, 0, len(changes)),
		}

		for _, change := range changes {
			header.Changes = append(header.Changes, change.Description)
		}

		sb.WriteString(header.String())
	}

	useTransaction := g.ShouldUseTransaction(statements)

	if useTransaction {
		sb.WriteString("BEGIN;\n\n")
	}

	for i, stmt := range statements {
		if i > 0 {
			sb.WriteString("\n")
		}

		if g.Options.IncludeComments && stmt.Description != "" {
			sb.WriteString(fmt.Sprintf("-- %s\n", stmt.Description))
		}

		if stmt.IsUnsafe && g.Options.IncludeComments {
			sb.WriteString("-- WARNING: This operation is potentially unsafe\n")
		}

		sb.WriteString(stmt.SQL)

		trimmed := strings.TrimRight(stmt.SQL, " \t\n\r")
		if trimmed != "" && !strings.HasSuffix(trimmed, ";") {
			sb.WriteString(";")
		}

		sb.WriteString("\n")
	}

	if useTransaction {
		sb.WriteString("\nCOMMIT;\n")
	}

	return sb.String()
}

func (g *Generator) ShouldUseTransaction(statements []DDLStatement) bool {
	switch g.Options.TransactionMode {
	case TransactionModeAlways:
		return true
	case TransactionModeNever:
		return false
	case TransactionModeAuto:
		for _, stmt := range statements {
			if stmt.CannotUseTx {
				return false
			}
		}

		return true
	default:
		return true
	}
}

func (g *Generator) writeMigrationFiles(result *GenerateResult) error {
	if err := os.MkdirAll(g.Options.OutputDir, DefaultDirMode); err != nil {
		return util.WrapError("create output directory", err)
	}

	for _, migration := range result.Migrations {
		if migration.UpFile != nil {
			if err := g.writeMigrationFile(migration.UpFile); err != nil {
				return util.WrapError("write UP file", err)
			}
		}

		if migration.DownFile != nil {
			if err := g.writeMigrationFile(migration.DownFile); err != nil {
				return util.WrapError("write DOWN file", err)
			}
		}
	}

	return nil
}

func (g *Generator) writeMigrationFile(file *MigrationFile) error {
	filePath := filepath.Join(g.Options.OutputDir, file.FileName)

	if err := os.WriteFile(filePath, []byte(file.Content), DefaultFileMode); err != nil {
		return util.WrapError("write file "+filePath, err)
	}

	return nil
}

func (g *Generator) GetNextMigrationVersion() (int, error) {
	if _, err := os.Stat(g.Options.OutputDir); os.IsNotExist(err) {
		return g.Options.StartVersion, nil
	}

	entries, err := os.ReadDir(g.Options.OutputDir)
	if err != nil {
		return 0, util.WrapError("read directory", err)
	}

	maxVersion := g.Options.StartVersion - 1

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		version, _, _, err := ParseMigrationFileName(entry.Name())
		if err != nil {
			continue
		}

		if version > maxVersion {
			maxVersion = version
		}
	}

	return maxVersion + 1, nil
}
