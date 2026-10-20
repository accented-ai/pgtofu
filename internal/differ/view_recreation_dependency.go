package differ

func isViewDrop(change *Change) bool {
	return change.Type == ChangeTypeDropView || change.Type == ChangeTypeDropMaterializedView
}

func viewRecreationDependsOn(change, other *Change) bool {
	if !isViewDrop(other) {
		return false
	}

	if isViewDrop(change) {
		return tableMatchesDependency(change.ObjectName, other.DependsOn)
	}

	return (change.Type == ChangeTypeAddView || change.Type == ChangeTypeAddMaterializedView) &&
		change.ObjectName == other.ObjectName
}
