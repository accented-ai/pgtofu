package differ

import (
	"fmt"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type TableComparator struct {
	options        *Options
	columnComp     *ColumnComparator
	constraintComp *ConstraintComparator
}

func NewTableComparator(opts *Options) *TableComparator {
	return &TableComparator{
		options:        opts,
		columnComp:     NewColumnComparator(opts),
		constraintComp: NewConstraintComparator(opts),
	}
}

func (tc *TableComparator) Compare(result *DiffResult) {
	currentMap := tc.buildMap(result.Current.Tables)
	desiredMap := tc.buildMap(result.Desired.Tables)

	tc.detectAddedTables(result, currentMap, desiredMap)
	tc.detectDroppedTables(result, currentMap, desiredMap)
	tc.detectModifiedTables(result, currentMap, desiredMap)
}

func (tc *TableComparator) buildMap(tables []schema.Table) map[string]*schema.Table {
	m := make(map[string]*schema.Table, len(tables))
	for i := range tables {
		key := TableKey(tables[i].Schema, tables[i].Name)
		m[key] = &tables[i]
	}

	return m
}

func (tc *TableComparator) detectAddedTables(
	result *DiffResult,
	currentMap, desiredMap map[string]*schema.Table,
) {
	for key, table := range desiredMap {
		if _, exists := currentMap[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeAddTable,
				Severity:    SeveritySafe,
				Description: "Add table: " + table.QualifiedName(),
				ObjectType:  "table",
				ObjectName:  key,
				Details:     map[string]any{"table": table},
				DependsOn:   getTableDependencies(table),
			})

			tc.addTableCommentChange(result, key, table, "")
			tc.addColumnCommentChanges(result, key, table)
		}
	}
}

func (tc *TableComparator) detectDroppedTables(
	result *DiffResult,
	currentMap, desiredMap map[string]*schema.Table,
) {
	for key, table := range currentMap {
		if _, exists := desiredMap[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeDropTable,
				Severity:    SeverityBreaking,
				Description: "Drop table: " + table.QualifiedName(),
				ObjectType:  "table",
				ObjectName:  key,
				Details:     map[string]any{"table": table},
			})
		}
	}
}

func (tc *TableComparator) detectModifiedTables(
	result *DiffResult,
	currentMap, desiredMap map[string]*schema.Table,
) {
	for key, desired := range desiredMap {
		current, exists := currentMap[key]
		if !exists {
			continue
		}

		tc.columnComp.Compare(result, key, current, current, desired)
		tc.constraintComp.Compare(result, current, desired)
		tc.compareTableComments(result, key, current, desired)
	}
}

func (tc *TableComparator) addTableCommentChange(
	result *DiffResult,
	key string,
	table *schema.Table,
	oldComment string,
) {
	if tc.options.IgnoreComments || table.Comment == "" {
		return
	}

	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeModifyTableComment,
		Severity:    SeveritySafe,
		Description: "Add table comment: " + table.QualifiedName(),
		ObjectType:  "table",
		ObjectName:  key,
		Details: map[string]any{
			"table":       table.QualifiedName(),
			"old_comment": oldComment,
			"new_comment": table.Comment,
		},
	})
}

func (tc *TableComparator) addColumnCommentChanges(
	result *DiffResult,
	key string,
	table *schema.Table,
) {
	if tc.options.IgnoreComments {
		return
	}

	for i := range table.Columns {
		col := &table.Columns[i]
		if col.Comment != "" {
			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeModifyColumnComment,
				Severity: SeveritySafe,
				Description: fmt.Sprintf(
					"Add column comment: %s.%s",
					table.QualifiedName(),
					col.Name,
				),
				ObjectType: "column",
				ObjectName: key,
				Details: map[string]any{
					"table":       table.QualifiedName(),
					"column_name": col.Name,
					"old_comment": "",
					"new_comment": col.Comment,
				},
			})
		}
	}
}

func (tc *TableComparator) compareTableComments(
	result *DiffResult,
	key string,
	current, desired *schema.Table,
) {
	if tc.options.IgnoreComments {
		return
	}

	currentComment := normalizeComment(current.Comment)
	desiredComment := normalizeComment(desired.Comment)

	if currentComment == desiredComment {
		return
	}

	severity := SeveritySafe
	if desired.Comment == "" {
		severity = SeverityPotentiallyBreaking
	}

	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeModifyTableComment,
		Severity:    severity,
		Description: "Modify table comment: " + desired.QualifiedName(),
		ObjectType:  "table",
		ObjectName:  key,
		Details: map[string]any{
			"table":       desired.QualifiedName(),
			"old_comment": current.Comment,
			"new_comment": desired.Comment,
		},
	})
}

func getTableDependencies(table *schema.Table) []string {
	var deps []string

	tableName := table.QualifiedName()

	for i := range table.Constraints {
		constraint := &table.Constraints[i]
		if constraint.IsForeignKey() && constraint.ReferencedTable != "" {
			refTable := constraint.QualifiedReferencedTable()
			if refTable != "" && refTable != tableName {
				deps = append(deps, refTable)
			}
		}
	}

	return deps
}
