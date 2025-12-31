package differ

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type ChangeSeverity string

const (
	SeveritySafe                  ChangeSeverity = "SAFE"
	SeverityPotentiallyBreaking   ChangeSeverity = "POTENTIALLY_BREAKING"
	SeverityBreaking              ChangeSeverity = "BREAKING"
	SeverityDataMigrationRequired ChangeSeverity = "DATA_MIGRATION_REQUIRED"
)

type ChangeType string

const (
	ChangeTypeAddSchema                 ChangeType = "ADD_SCHEMA"
	ChangeTypeDropSchema                ChangeType = "DROP_SCHEMA"
	ChangeTypeAddTable                  ChangeType = "ADD_TABLE"
	ChangeTypeDropTable                 ChangeType = "DROP_TABLE"
	ChangeTypeModifyTableComment        ChangeType = "MODIFY_TABLE_COMMENT"
	ChangeTypeAddView                   ChangeType = "ADD_VIEW"
	ChangeTypeDropView                  ChangeType = "DROP_VIEW"
	ChangeTypeModifyView                ChangeType = "MODIFY_VIEW"
	ChangeTypeAddMaterializedView       ChangeType = "ADD_MATERIALIZED_VIEW"
	ChangeTypeDropMaterializedView      ChangeType = "DROP_MATERIALIZED_VIEW"
	ChangeTypeModifyMaterializedView    ChangeType = "MODIFY_MATERIALIZED_VIEW"
	ChangeTypeAddFunction               ChangeType = "ADD_FUNCTION"
	ChangeTypeDropFunction              ChangeType = "DROP_FUNCTION"
	ChangeTypeModifyFunction            ChangeType = "MODIFY_FUNCTION"
	ChangeTypeAddTrigger                ChangeType = "ADD_TRIGGER"
	ChangeTypeDropTrigger               ChangeType = "DROP_TRIGGER"
	ChangeTypeModifyTrigger             ChangeType = "MODIFY_TRIGGER"
	ChangeTypeAddExtension              ChangeType = "ADD_EXTENSION"
	ChangeTypeDropExtension             ChangeType = "DROP_EXTENSION"
	ChangeTypeModifyExtension           ChangeType = "MODIFY_EXTENSION"
	ChangeTypeAddSequence               ChangeType = "ADD_SEQUENCE"
	ChangeTypeDropSequence              ChangeType = "DROP_SEQUENCE"
	ChangeTypeModifySequence            ChangeType = "MODIFY_SEQUENCE"
	ChangeTypeAddCustomType             ChangeType = "ADD_CUSTOM_TYPE"
	ChangeTypeDropCustomType            ChangeType = "DROP_CUSTOM_TYPE"
	ChangeTypeModifyCustomType          ChangeType = "MODIFY_CUSTOM_TYPE"
	ChangeTypeAddColumn                 ChangeType = "ADD_COLUMN"
	ChangeTypeDropColumn                ChangeType = "DROP_COLUMN"
	ChangeTypeModifyColumnType          ChangeType = "MODIFY_COLUMN_TYPE"
	ChangeTypeModifyColumnNullability   ChangeType = "MODIFY_COLUMN_NULLABILITY"
	ChangeTypeModifyColumnDefault       ChangeType = "MODIFY_COLUMN_DEFAULT"
	ChangeTypeModifyColumnComment       ChangeType = "MODIFY_COLUMN_COMMENT"
	ChangeTypeRenameColumn              ChangeType = "RENAME_COLUMN"
	ChangeTypeAddConstraint             ChangeType = "ADD_CONSTRAINT"
	ChangeTypeDropConstraint            ChangeType = "DROP_CONSTRAINT"
	ChangeTypeModifyConstraint          ChangeType = "MODIFY_CONSTRAINT"
	ChangeTypeAddIndex                  ChangeType = "ADD_INDEX"
	ChangeTypeDropIndex                 ChangeType = "DROP_INDEX"
	ChangeTypeModifyIndex               ChangeType = "MODIFY_INDEX"
	ChangeTypeAddHypertable             ChangeType = "ADD_HYPERTABLE"
	ChangeTypeDropHypertable            ChangeType = "DROP_HYPERTABLE"
	ChangeTypeModifyHypertable          ChangeType = "MODIFY_HYPERTABLE"
	ChangeTypeAddCompressionPolicy      ChangeType = "ADD_COMPRESSION_POLICY"
	ChangeTypeDropCompressionPolicy     ChangeType = "DROP_COMPRESSION_POLICY"
	ChangeTypeModifyCompressionPolicy   ChangeType = "MODIFY_COMPRESSION_POLICY"
	ChangeTypeAddRetentionPolicy        ChangeType = "ADD_RETENTION_POLICY"
	ChangeTypeDropRetentionPolicy       ChangeType = "DROP_RETENTION_POLICY"
	ChangeTypeModifyRetentionPolicy     ChangeType = "MODIFY_RETENTION_POLICY"
	ChangeTypeAddContinuousAggregate    ChangeType = "ADD_CONTINUOUS_AGGREGATE"
	ChangeTypeDropContinuousAggregate   ChangeType = "DROP_CONTINUOUS_AGGREGATE"
	ChangeTypeModifyContinuousAggregate ChangeType = "MODIFY_CONTINUOUS_AGGREGATE"
)

type Change struct {
	Type        ChangeType
	Severity    ChangeSeverity
	Description string
	ObjectType  string
	ObjectName  string
	Details     map[string]any
	DependsOn   []string
	Order       int
}

func (c *Change) String() string {
	return fmt.Sprintf("[%s] %s: %s", c.Severity, c.Type, c.Description)
}

type DiffResult struct {
	Current  *schema.Database
	Desired  *schema.Database
	Changes  []Change
	Warnings []string
	Stats    DiffStats
}

type DiffStats struct {
	TablesAdded        int
	TablesDropped      int
	TablesModified     int
	ColumnsAdded       int
	ColumnsDropped     int
	ColumnsModified    int
	IndexesAdded       int
	IndexesDropped     int
	ConstraintsAdded   int
	ConstraintsDropped int
	ViewsAdded         int
	ViewsDropped       int
	ViewsModified      int
	FunctionsAdded     int
	FunctionsDropped   int
	FunctionsModified  int
}

func (dr *DiffResult) HasChanges() bool {
	return len(dr.Changes) > 0
}

func (dr *DiffResult) HasBreakingChanges() bool {
	for _, change := range dr.Changes {
		if change.Severity == SeverityBreaking || change.Severity == SeverityDataMigrationRequired {
			return true
		}
	}

	return false
}

func (dr *DiffResult) GetChangesBySeverity(severity ChangeSeverity) []Change {
	var filtered []Change

	for _, change := range dr.Changes {
		if change.Severity == severity {
			filtered = append(filtered, change)
		}
	}

	return filtered
}

func (dr *DiffResult) GetChangesByType(changeType ChangeType) []Change {
	var filtered []Change

	for _, change := range dr.Changes {
		if change.Type == changeType {
			filtered = append(filtered, change)
		}
	}

	return filtered
}

func (dr *DiffResult) Summary() string {
	var sb strings.Builder
	sb.WriteString("Schema Diff Summary\n")
	sb.WriteString("===================\n\n")

	if !dr.HasChanges() {
		sb.WriteString("No changes detected.\n")
		return sb.String()
	}

	sb.WriteString(fmt.Sprintf("Total Changes: %d\n\n", len(dr.Changes)))

	if dr.Stats.TablesAdded > 0 || dr.Stats.TablesDropped > 0 || dr.Stats.TablesModified > 0 {
		sb.WriteString(fmt.Sprintf("Tables: +%d -%d ~%d\n",
			dr.Stats.TablesAdded, dr.Stats.TablesDropped, dr.Stats.TablesModified))
	}

	if dr.Stats.ColumnsAdded > 0 || dr.Stats.ColumnsDropped > 0 || dr.Stats.ColumnsModified > 0 {
		sb.WriteString(fmt.Sprintf("Columns: +%d -%d ~%d\n",
			dr.Stats.ColumnsAdded, dr.Stats.ColumnsDropped, dr.Stats.ColumnsModified))
	}

	if dr.Stats.IndexesAdded > 0 || dr.Stats.IndexesDropped > 0 {
		sb.WriteString(fmt.Sprintf("Indexes: +%d -%d\n",
			dr.Stats.IndexesAdded, dr.Stats.IndexesDropped))
	}

	if dr.Stats.ConstraintsAdded > 0 || dr.Stats.ConstraintsDropped > 0 {
		sb.WriteString(fmt.Sprintf("Constraints: +%d -%d\n",
			dr.Stats.ConstraintsAdded, dr.Stats.ConstraintsDropped))
	}

	if dr.Stats.ViewsAdded > 0 || dr.Stats.ViewsDropped > 0 || dr.Stats.ViewsModified > 0 {
		sb.WriteString(fmt.Sprintf("Views: +%d -%d ~%d\n",
			dr.Stats.ViewsAdded, dr.Stats.ViewsDropped, dr.Stats.ViewsModified))
	}

	if dr.Stats.FunctionsAdded > 0 || dr.Stats.FunctionsDropped > 0 ||
		dr.Stats.FunctionsModified > 0 {
		sb.WriteString(fmt.Sprintf("Functions: +%d -%d ~%d\n",
			dr.Stats.FunctionsAdded, dr.Stats.FunctionsDropped, dr.Stats.FunctionsModified))
	}

	sb.WriteString("\n")

	safe := dr.GetChangesBySeverity(SeveritySafe)
	potentiallyBreaking := dr.GetChangesBySeverity(SeverityPotentiallyBreaking)
	breaking := dr.GetChangesBySeverity(SeverityBreaking)
	dataMigration := dr.GetChangesBySeverity(SeverityDataMigrationRequired)

	sb.WriteString("Severity Breakdown:\n")
	sb.WriteString(fmt.Sprintf("  Safe: %d\n", len(safe)))
	sb.WriteString(fmt.Sprintf("  Potentially Breaking: %d\n", len(potentiallyBreaking)))
	sb.WriteString(fmt.Sprintf("  Breaking: %d\n", len(breaking)))
	sb.WriteString(fmt.Sprintf("  Data Migration Required: %d\n", len(dataMigration)))

	if len(dr.Warnings) > 0 {
		sb.WriteString(fmt.Sprintf("\nWarnings: %d\n", len(dr.Warnings)))
	}

	return sb.String()
}

func TableKey(schema, name string) string {
	return fmt.Sprintf("%s.%s", normalizeSchema(schema), strings.ToLower(name))
}

func ViewKey(schema, name string) string {
	return fmt.Sprintf("%s.%s", normalizeSchema(schema), strings.ToLower(name))
}

func FunctionKey(schema, name string, argTypes []string) string {
	return fmt.Sprintf("%s.%s(%s)",
		normalizeSchema(schema),
		strings.ToLower(name),
		strings.Join(argTypes, ","))
}

func IndexKey(schema, name string) string {
	return fmt.Sprintf("%s.%s", normalizeSchema(schema), strings.ToLower(name))
}

func normalizeSchema(s string) string {
	if s == "" {
		return schema.DefaultSchema
	}

	return strings.ToLower(s)
}
