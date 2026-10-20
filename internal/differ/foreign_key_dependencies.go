package differ

import (
	"slices"

	"github.com/accented-ai/pgtofu/internal/schema"
)

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
