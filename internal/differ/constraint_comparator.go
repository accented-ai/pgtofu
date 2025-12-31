package differ

import (
	"fmt"
	"sort"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

type ConstraintComparator struct {
	options *Options
}

func NewConstraintComparator(opts *Options) *ConstraintComparator {
	return &ConstraintComparator{options: opts}
}

func (cc *ConstraintComparator) Compare(result *DiffResult, current, desired *schema.Table) {
	tableKey := TableKey(current.Schema, current.Name)
	tableName := current.QualifiedName()

	currentConstraints := cc.buildConstraintMap(current.Constraints)
	desiredConstraints := cc.buildConstraintMap(desired.Constraints)

	cc.detectAddedConstraints(result, tableKey, tableName, currentConstraints, desiredConstraints)
	cc.detectDroppedConstraints(result, tableKey, tableName, currentConstraints, desiredConstraints)
	cc.detectModifiedConstraints(
		result,
		tableKey,
		tableName,
		currentConstraints,
		desiredConstraints,
	)
}

func (cc *ConstraintComparator) buildConstraintMap(
	constraints []schema.Constraint,
) map[string]*schema.Constraint {
	m := make(map[string]*schema.Constraint, len(constraints))
	for i := range constraints {
		key := cc.constraintKey(&constraints[i])
		m[key] = &constraints[i]
	}

	return m
}

func (cc *ConstraintComparator) constraintKey(constraint *schema.Constraint) string {
	if cc.options.IgnoreConstraintNames {
		return constraintStructureKey(constraint)
	}

	return strings.ToLower(constraint.Name)
}

func (cc *ConstraintComparator) detectAddedConstraints(
	result *DiffResult,
	tableKey, tableName string,
	currentConstraints, desiredConstraints map[string]*schema.Constraint,
) {
	for key, constraint := range desiredConstraints {
		if _, exists := currentConstraints[key]; !exists {
			severity := SeveritySafe
			if constraint.IsForeignKey() {
				severity = SeverityDataMigrationRequired
			}

			if constraint.Type == "CHECK" &&
				strings.Contains(strings.ToLower(constraint.Definition), "not null") {
				severity = SeverityDataMigrationRequired
			}

			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeAddConstraint,
				Severity: severity,
				Description: fmt.Sprintf(
					"Add %s constraint: %s on %s",
					constraint.Type,
					constraint.Name,
					tableName,
				),
				ObjectType: "constraint",
				ObjectName: tableKey,
				Details:    map[string]any{"table": tableName, "constraint": constraint},
				DependsOn:  getConstraintDependencies(constraint),
			})
		}
	}
}

func (cc *ConstraintComparator) detectDroppedConstraints(
	result *DiffResult,
	tableKey, tableName string,
	currentConstraints, desiredConstraints map[string]*schema.Constraint,
) {
	for key, constraint := range currentConstraints {
		if _, exists := desiredConstraints[key]; !exists {
			severity := SeverityPotentiallyBreaking
			if constraint.IsPrimaryKey() || constraint.IsUnique() {
				severity = SeverityBreaking
			}

			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeDropConstraint,
				Severity: severity,
				Description: fmt.Sprintf(
					"Drop %s constraint: %s from %s",
					constraint.Type,
					constraint.Name,
					tableName,
				),
				ObjectType: "constraint",
				ObjectName: tableKey,
				Details:    map[string]any{"table": tableName, "constraint": constraint},
			})
		}
	}
}

func (cc *ConstraintComparator) detectModifiedConstraints(
	result *DiffResult,
	tableKey, tableName string,
	currentConstraints, desiredConstraints map[string]*schema.Constraint,
) {
	for key, desiredConstraint := range desiredConstraints {
		currentConstraint, exists := currentConstraints[key]
		if !exists {
			continue
		}

		if !areConstraintsEqual(currentConstraint, desiredConstraint) {
			result.Changes = append(result.Changes, Change{
				Type:     ChangeTypeModifyConstraint,
				Severity: SeverityPotentiallyBreaking,
				Description: fmt.Sprintf(
					"Modify %s constraint: %s on %s",
					desiredConstraint.Type,
					desiredConstraint.Name,
					tableName,
				),
				ObjectType: "constraint",
				ObjectName: tableKey,
				Details: map[string]any{
					"table":   tableName,
					"current": currentConstraint,
					"desired": desiredConstraint,
				},
				DependsOn: getConstraintDependencies(desiredConstraint),
			})
		}
	}
}

func constraintStructureKey(constraint *schema.Constraint) string {
	var key strings.Builder
	key.WriteString(constraint.Type)
	key.WriteString(":")

	cols := make([]string, len(constraint.Columns))
	copy(cols, constraint.Columns)
	sort.Strings(cols)
	key.WriteString(strings.Join(cols, ","))

	if constraint.IsForeignKey() {
		key.WriteString("->")
		key.WriteString(constraint.QualifiedReferencedTable())
		key.WriteString("(")

		refCols := make([]string, len(constraint.ReferencedColumns))
		copy(refCols, constraint.ReferencedColumns)
		sort.Strings(refCols)
		key.WriteString(strings.Join(refCols, ","))
		key.WriteString(")")
	}

	if constraint.IsCheck() {
		key.WriteString(":")
		key.WriteString(normalizeExpression(constraint.CheckExpression))
	}

	return strings.ToLower(key.String())
}

func getConstraintDependencies(constraint *schema.Constraint) []string {
	var deps []string
	if constraint.IsForeignKey() && constraint.ReferencedTable != "" {
		deps = append(deps, constraint.QualifiedReferencedTable())
	}

	return deps
}

func areConstraintsEqual(c1, c2 *schema.Constraint) bool {
	if c1.Type != c2.Type {
		return false
	}

	if !c1.IsCheck() && !equalStringSlices(c1.Columns, c2.Columns) {
		return false
	}

	if c1.IsForeignKey() {
		ref1 := normalizeTableReference(c1.ReferencedSchema, c1.ReferencedTable)

		ref2 := normalizeTableReference(c2.ReferencedSchema, c2.ReferencedTable)
		if ref1 != ref2 {
			return false
		}

		if !equalStringSlices(c1.ReferencedColumns, c2.ReferencedColumns) {
			return false
		}

		if normalizeAction(c1.OnDelete) != normalizeAction(c2.OnDelete) {
			return false
		}

		if normalizeAction(c1.OnUpdate) != normalizeAction(c2.OnUpdate) {
			return false
		}
	}

	if c1.IsCheck() {
		if normalizeExpression(c1.CheckExpression) != normalizeExpression(c2.CheckExpression) {
			return false
		}
	}

	if c1.IsDeferrable != c2.IsDeferrable || c1.InitiallyDeferred != c2.InitiallyDeferred {
		return false
	}

	return true
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}

	return true
}

func normalizeTableReference(schemaName, table string) string {
	normalizedSchema := strings.ToLower(strings.TrimSpace(schemaName))
	normalizedTable := strings.ToLower(strings.TrimSpace(table))

	if normalizedSchema == "" || normalizedSchema == schema.DefaultSchema {
		return normalizedTable
	}

	return normalizedSchema + "." + normalizedTable
}

func normalizeAction(action string) string {
	normalized := strings.ToUpper(strings.TrimSpace(action))
	if normalized == "" || normalized == "NO ACTION" {
		return "NO ACTION"
	}

	return normalized
}
