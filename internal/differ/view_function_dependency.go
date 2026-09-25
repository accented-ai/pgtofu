package differ

import "github.com/accented-ai/pgtofu/internal/schema"

func addViewFunctionDependencies(result *DiffResult) {
	currentFunctions := buildFunctionMap(result.Current.Functions)
	desiredFunctions := buildFunctionMap(result.Desired.Functions)
	addedFunctions := findNewFunctions(currentFunctions, result.Desired.Functions)

	removedFunctions := findNewFunctions(desiredFunctions, result.Current.Functions)
	if len(addedFunctions) == 0 && len(removedFunctions) == 0 {
		return
	}

	currentViews := buildViewMap(result.Current.Views)
	desiredViews := buildViewMap(result.Desired.Views)
	currentMaterializedViews := buildMaterializedViewMap(result.Current.MaterializedViews)
	desiredMaterializedViews := buildMaterializedViewMap(result.Desired.MaterializedViews)

	for i := range result.Changes {
		change := &result.Changes[i]

		switch change.Type {
		case ChangeTypeAddView, ChangeTypeModifyView:
			if isCommentOnlyObjectChange(change, ChangeTypeModifyView) {
				continue
			}

			if view := desiredViews[change.ObjectName]; view != nil {
				change.DependsOn = appendViewFunctionDependencies(
					change.DependsOn, view.Definition, addedFunctions,
				)
			}

			if change.Type == ChangeTypeModifyView {
				if view := currentViews[change.ObjectName]; view != nil {
					change.RollbackDependsOn = appendViewFunctionDependencies(
						change.RollbackDependsOn, view.Definition, removedFunctions,
					)
				}
			}

		case ChangeTypeDropView:
			if view := currentViews[change.ObjectName]; view != nil {
				change.RollbackDependsOn = appendViewFunctionDependencies(
					change.RollbackDependsOn, view.Definition, removedFunctions,
				)
			}

		case ChangeTypeAddMaterializedView, ChangeTypeModifyMaterializedView:
			if isCommentOnlyObjectChange(change, ChangeTypeModifyMaterializedView) {
				continue
			}

			if view := desiredMaterializedViews[change.ObjectName]; view != nil {
				change.DependsOn = appendViewFunctionDependencies(
					change.DependsOn, view.Definition, addedFunctions,
				)
			}

			if change.Type == ChangeTypeModifyMaterializedView {
				if view := currentMaterializedViews[change.ObjectName]; view != nil {
					change.RollbackDependsOn = appendViewFunctionDependencies(
						change.RollbackDependsOn, view.Definition, removedFunctions,
					)
				}
			}

		case ChangeTypeDropMaterializedView:
			if view := currentMaterializedViews[change.ObjectName]; view != nil {
				change.RollbackDependsOn = appendViewFunctionDependencies(
					change.RollbackDependsOn, view.Definition, removedFunctions,
				)
			}
		}
	}
}

func appendViewFunctionDependencies(
	dependencies []string,
	definition string,
	functions []schema.Function,
) []string {
	if len(functions) == 0 {
		return dependencies
	}

	refs := extractFunctionCallReferences(definition)

	return dedupeDependencies(append(dependencies, resolveFunctionDependencies(refs, functions)...))
}
