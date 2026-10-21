package differ

import (
	"fmt"
	"slices"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func (d *Differ) prepareForeignKeyRecreations(result *DiffResult) {
	// Release the old referenced key before changing it, then acquire the new key.
	for i := range result.Current.Tables {
		current := &result.Current.Tables[i]

		desired := result.Desired.GetTable(current.Schema, current.Name)
		if desired == nil {
			continue
		}

		comparator := d.tableComp.constraintComp
		desiredConstraints := comparator.buildConstraintMap(desired.Constraints)

		for j := range current.Constraints {
			foreign := &current.Constraints[j]
			if !foreign.IsForeignKey() || !foreignKeyNeedsRecreation(foreign, result) {
				continue
			}

			next := desiredConstraints[comparator.constraintKey(foreign)]
			if next == nil || !next.IsForeignKey() {
				continue
			}

			if d.options.IgnoreConstraintNames {
				retained := *next
				retained.Name = foreign.Name
				next = &retained
			}

			result.Changes = slices.DeleteFunc(result.Changes, func(change Change) bool {
				constraint := constraintForDependency(&change, "current")

				return change.Type == ChangeTypeModifyConstraint &&
					change.Details["table"] == current.QualifiedName() &&
					constraint != nil && constraint.Name == foreign.Name
			})
			result.Changes = append(result.Changes,
				foreignKeyRecreation(ChangeTypeDropConstraint, current, foreign),
				foreignKeyRecreation(ChangeTypeAddConstraint, desired, next),
			)
		}
	}
}

func foreignKeyNeedsRecreation(foreign *schema.Constraint, result *DiffResult) bool {
	for i := range result.Changes {
		change := &result.Changes[i]
		switch change.Type {
		case ChangeTypeDropConstraint, ChangeTypeModifyConstraint,
			ChangeTypeDropIndex, ChangeTypeModifyIndex:
			if foreignKeyReferencesKey(foreign, change, "current") {
				return true
			}
		}
	}

	return false
}

func foreignKeyRecreation(
	changeType ChangeType,
	table *schema.Table,
	foreign *schema.Constraint,
) Change {
	return Change{
		Type:     changeType,
		Severity: SeverityPotentiallyBreaking,
		Description: fmt.Sprintf(
			"Recreate foreign key: %s on %s",
			foreign.Name,
			table.QualifiedName(),
		),
		ObjectType: "constraint",
		ObjectName: table.QualifiedName() + "." + foreign.Name,
		Details: map[string]any{
			"table": table.QualifiedName(), "constraint": foreign,
			"recreate": changeType == ChangeTypeDropConstraint,
		},
		DependsOn: getConstraintDependencies(foreign, nil),
	}
}

func addForeignKeyChangeDependencies(result *DiffResult) {
	for i := range result.Changes {
		change := &result.Changes[i]
		for j := range result.Changes {
			other := &result.Changes[j]
			if i == j || !foreignKeyChangeDependsOn(change, other) {
				continue
			}

			dependencies, name := &change.DependsOn, other.ObjectName
			if keyChangeDependsOnForeignRemoval(change, other) {
				dependencies, name = &other.RollbackDependsOn, change.ObjectName
			}

			if !slices.Contains(*dependencies, name) {
				*dependencies = append(*dependencies, name)
			}
		}
	}
}

func foreignKeyChangeDependsOn(change, other *Change) bool {
	if change.Type == ChangeTypeAddConstraint && other.Type == ChangeTypeDropConstraint &&
		change.ObjectName == other.ObjectName {
		recreate, _ := other.Details["recreate"].(bool)
		if recreate {
			return true
		}
	}

	switch change.Type {
	case ChangeTypeAddTable, ChangeTypeAddConstraint, ChangeTypeModifyConstraint:
		if createsReferencedKey(other.Type) && foreignKeysReferenceKey(change, other, "desired") {
			return true
		}
	}

	return keyChangeDependsOnForeignRemoval(change, other)
}

func keyChangeDependsOnForeignRemoval(change, other *Change) bool {
	switch change.Type {
	case ChangeTypeDropConstraint,
		ChangeTypeDropIndex,
		ChangeTypeModifyConstraint,
		ChangeTypeModifyIndex,
		ChangeTypeDropTable:
		if other.Type != ChangeTypeDropConstraint && other.Type != ChangeTypeDropTable &&
			other.Type != ChangeTypeModifyConstraint {
			return false
		}

		return foreignKeysReferenceKey(other, change, "current")
	default:
		return false
	}
}

func createsReferencedKey(changeType ChangeType) bool {
	switch changeType {
	case ChangeTypeAddConstraint, ChangeTypeModifyConstraint,
		ChangeTypeAddIndex, ChangeTypeModifyIndex:
		return true
	default:
		return false
	}
}

func foreignKeysReferenceKey(foreignChange, keyChange *Change, side string) bool {
	if foreignChange.Type == ChangeTypeAddTable || foreignChange.Type == ChangeTypeDropTable {
		table, ok := foreignChange.Details["table"].(*schema.Table)
		if !ok || table == nil {
			return false
		}

		for i := range table.Constraints {
			if foreignKeyReferencesKey(&table.Constraints[i], keyChange, side) {
				return true
			}
		}

		return false
	}

	return foreignKeyReferencesKey(constraintForDependency(foreignChange, side), keyChange, side)
}

func constraintForDependency(change *Change, side string) *schema.Constraint {
	constraint, _ := change.Details["constraint"].(*schema.Constraint)
	if constraint == nil {
		constraint, _ = change.Details[side].(*schema.Constraint)
	}

	return constraint
}

func foreignKeyReferencesKey(foreign *schema.Constraint, keyChange *Change, side string) bool {
	if foreign == nil || !foreign.IsForeignKey() {
		return false
	}

	if keyChange.Type == ChangeTypeDropTable {
		table, _ := keyChange.Details["table"].(*schema.Table)
		return table != nil && table.QualifiedName() == foreign.QualifiedReferencedTable()
	}

	var (
		table   string
		columns []string
		primary bool
	)

	if constraint := constraintForDependency(keyChange, side); constraint != nil {
		if (!constraint.IsPrimaryKey() && !constraint.IsUnique()) || constraint.IsDeferrable {
			return false
		}

		table, _ = keyChange.Details["table"].(string)
		columns, primary = constraint.Columns, constraint.IsPrimaryKey()
	} else {
		index, _ := keyChange.Details["index"].(*schema.Index)
		if index == nil {
			index, _ = keyChange.Details[side].(*schema.Index)
		}

		if index == nil || !index.IsUnique || index.IsPartial() || index.IsExpression() {
			return false
		}

		table, columns, primary = index.QualifiedTableName(), index.Columns, index.IsPrimary
	}

	if table != foreign.QualifiedReferencedTable() {
		return false
	}

	if len(foreign.ReferencedColumns) == 0 {
		return primary
	}

	return len(columns) == len(foreign.ReferencedColumns) &&
		slices.Equal(sortedCopy(columns), sortedCopy(foreign.ReferencedColumns))
}

func sortedCopy(values []string) []string {
	sorted := slices.Clone(values)
	slices.Sort(sorted)

	return sorted
}
