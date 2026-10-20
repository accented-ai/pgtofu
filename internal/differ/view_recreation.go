package differ

import (
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

func (d *Differ) processViewRecreation(result *DiffResult) {
	affected := d.findTablesWithColumnTypeChanges(result.Changes)
	for _, change := range result.Changes {
		switch change.Type {
		case ChangeTypeDropView, ChangeTypeDropMaterializedView,
			ChangeTypeAddView, ChangeTypeAddMaterializedView:
			affected[change.ObjectName] = true
		case ChangeTypeModifyView:
			current, currentOK := change.Details["current"].(schema.View)

			desired, desiredOK := change.Details["desired"].(schema.View)
			if currentOK && desiredOK && viewOutputCountChanges(current, desired) {
				affected[change.ObjectName] = true
			}
		}
	}

	desiredViews := buildViewMap(result.Desired.Views)
	desiredMaterializedViews := buildMaterializedViewMap(result.Desired.MaterializedViews)

	// Rebuild surviving dependents explicitly so a cascading drop cannot lose
	// an unchanged view, its comment, or a materialized view's indexes.
	for changed := true; changed; {
		changed = false

		for _, view := range result.Current.Views {
			if markDependentView(affected, view.QualifiedName(), view.Definition) {
				changed = true
			}

			if desired := desiredViews[ViewKey(view.Schema, view.Name)]; desired != nil &&
				markDependentView(affected, view.QualifiedName(), desired.Definition) {
				changed = true
			}
		}

		for _, view := range result.Current.MaterializedViews {
			if markDependentView(affected, view.QualifiedName(), view.Definition) {
				changed = true
			}

			if desired := desiredMaterializedViews[ViewKey(view.Schema, view.Name)]; desired != nil &&
				markDependentView(affected, view.QualifiedName(), desired.Definition) {
				changed = true
			}
		}
	}

	d.recreateAffectedViews(result, affected)
	d.recreateAffectedMaterializedViews(result, affected)
}

func viewOutputCountChanges(current, desired schema.View) bool {
	currentColumns := postgresViewOutputNames(current.Definition)
	desiredColumns := postgresViewOutputNames(desired.Definition)

	return len(currentColumns) > 0 && len(desiredColumns) > 0 &&
		len(currentColumns) != len(desiredColumns)
}

func markDependentView(affected map[string]bool, name, definition string) bool {
	if affected[name] || !viewDependsOnAnyTable(extractViewDependencies(definition), affected) {
		return false
	}

	affected[name] = true

	return true
}

func (d *Differ) findTablesWithColumnTypeChanges(changes []Change) map[string]bool {
	tables := make(map[string]bool)

	for _, change := range changes {
		if change.Type == ChangeTypeModifyColumnType {
			tableName, _ := change.Details["table"].(string)
			if tableName != "" {
				tables[strings.ToLower(tableName)] = true
			}
		}
	}

	return tables
}

func (d *Differ) recreateAffectedViews(
	result *DiffResult,
	affected map[string]bool,
) {
	currentViews := buildViewMap(result.Current.Views)
	desiredViews := buildViewMap(result.Desired.Views)

	viewsWithChanges := make(map[string]int) // key -> index in Changes

	for i, change := range result.Changes {
		if isCommentOnlyObjectChange(&change, ChangeTypeModifyView) {
			continue
		}

		if change.Type == ChangeTypeModifyView || change.Type == ChangeTypeDropView {
			viewsWithChanges[change.ObjectName] = i
		}
	}

	for key, desiredView := range desiredViews {
		if !affected[key] {
			continue
		}

		if idx, hasChange := viewsWithChanges[key]; hasChange {
			if result.Changes[idx].Type == ChangeTypeModifyView {
				d.convertModifyViewToDropAdd(result, idx, key, desiredView)
			}
		} else {
			if currentView, exists := currentViews[key]; exists {
				d.addViewRecreationChanges(result, key, currentView, desiredView)
			}
		}
	}
}

func (d *Differ) recreateAffectedMaterializedViews(
	result *DiffResult,
	affected map[string]bool,
) {
	currentViews := buildMaterializedViewMap(result.Current.MaterializedViews)
	desiredViews := buildMaterializedViewMap(result.Desired.MaterializedViews)

	viewsWithChanges := make(map[string]int) // key -> index

	for i, change := range result.Changes {
		if isCommentOnlyObjectChange(&change, ChangeTypeModifyMaterializedView) {
			continue
		}

		if change.Type == ChangeTypeModifyMaterializedView ||
			change.Type == ChangeTypeDropMaterializedView {
			viewsWithChanges[change.ObjectName] = i
		}
	}

	for key, desiredView := range desiredViews {
		if !affected[key] {
			continue
		}

		if idx, hasChange := viewsWithChanges[key]; hasChange {
			if result.Changes[idx].Type == ChangeTypeModifyMaterializedView {
				d.convertModifyMaterializedViewToDropAdd(result, idx, key, desiredView)
			}
		} else {
			if currentView, exists := currentViews[key]; exists {
				d.addMaterializedViewRecreationChanges(result, key, currentView, desiredView)
			}
		}
	}
}

func viewDependsOnAnyTable(deps []string, tables map[string]bool) bool {
	for _, dep := range deps {
		if depMatchesTables(dep, tables) {
			return true
		}
	}

	return false
}

func depMatchesTables(dep string, tables map[string]bool) bool {
	depLower := strings.ToLower(dep)

	if tables[depLower] {
		return true
	}

	parts := strings.Split(depLower, ".")

	if len(parts) == 2 && (parts[0] == schema.DefaultSchema || parts[0] == "public") {
		if tables[parts[1]] {
			return true
		}
	}

	if len(parts) == 1 {
		if tables["public."+depLower] || tables[schema.DefaultSchema+"."+depLower] {
			return true
		}
	}

	return false
}

func (d *Differ) convertModifyViewToDropAdd(
	result *DiffResult,
	idx int,
	key string,
	desiredView *schema.View,
) {
	originalChange := result.Changes[idx]

	var currentView *schema.View
	if current, ok := originalChange.Details["current"].(schema.View); ok {
		currentView = &current
	} else {
		for i := range result.Current.Views {
			if ViewKey(result.Current.Views[i].Schema, result.Current.Views[i].Name) == key {
				currentView = &result.Current.Views[i]
				break
			}
		}
	}

	if currentView == nil {
		return
	}

	deps := extractViewDependencies(currentView.Definition)
	result.Changes[idx] = Change{
		Type:        ChangeTypeDropView,
		Severity:    SeverityPotentiallyBreaking,
		Description: "Drop view for recreation: " + desiredView.QualifiedName(),
		ObjectType:  "view",
		ObjectName:  key,
		Details: map[string]any{
			"view":              currentView,
			"original_change":   originalChange,
			"will_be_recreated": true,
		},
		DependsOn:         deps,
		RollbackDependsOn: deps,
	}

	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeAddView,
		Severity:    SeveritySafe,
		Description: "Recreate view: " + desiredView.QualifiedName(),
		ObjectType:  "view",
		ObjectName:  key,
		Details: map[string]any{
			"view":          *desiredView,
			"is_recreation": true,
		},
		DependsOn: extractViewDependencies(desiredView.Definition),
	})
}

func (d *Differ) addViewRecreationChanges(
	result *DiffResult,
	key string,
	currentView, desiredView *schema.View,
) {
	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeDropView,
		Severity:    SeverityPotentiallyBreaking,
		Description: "Drop view for recreation: " + currentView.QualifiedName(),
		ObjectType:  "view",
		ObjectName:  key,
		Details: map[string]any{
			"view":              *currentView,
			"will_be_recreated": true,
		},
		DependsOn:         extractViewDependencies(currentView.Definition),
		RollbackDependsOn: extractViewDependencies(currentView.Definition),
	})

	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeAddView,
		Severity:    SeveritySafe,
		Description: "Recreate view: " + desiredView.QualifiedName(),
		ObjectType:  "view",
		ObjectName:  key,
		Details: map[string]any{
			"view":          *desiredView,
			"is_recreation": true,
		},
		DependsOn: extractViewDependencies(desiredView.Definition),
	})
}

func (d *Differ) convertModifyMaterializedViewToDropAdd(
	result *DiffResult,
	idx int,
	key string,
	desiredView *schema.MaterializedView,
) {
	originalChange := result.Changes[idx]

	var currentView *schema.MaterializedView
	if current, ok := originalChange.Details["current"].(schema.MaterializedView); ok {
		currentView = &current
	} else if current, ok := originalChange.Details["current"].(*schema.MaterializedView); ok {
		currentView = current
	} else {
		for i := range result.Current.MaterializedViews {
			mv := &result.Current.MaterializedViews[i]
			if ViewKey(mv.Schema, mv.Name) == key {
				currentView = mv
				break
			}
		}
	}

	if currentView == nil {
		return
	}

	deps := extractViewDependencies(currentView.Definition)
	result.Changes[idx] = Change{
		Type:        ChangeTypeDropMaterializedView,
		Severity:    SeverityPotentiallyBreaking,
		Description: "Drop materialized view for recreation: " + desiredView.QualifiedName(),
		ObjectType:  "materialized_view",
		ObjectName:  key,
		Details: map[string]any{
			"view":              currentView,
			"original_change":   originalChange,
			"will_be_recreated": true,
		},
		DependsOn:         deps,
		RollbackDependsOn: deps,
	}

	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeAddMaterializedView,
		Severity:    SeveritySafe,
		Description: "Recreate materialized view: " + desiredView.QualifiedName(),
		ObjectType:  "materialized_view",
		ObjectName:  key,
		Details: map[string]any{
			"view":          *desiredView,
			"is_recreation": true,
		},
		DependsOn: extractViewDependencies(desiredView.Definition),
	})
}

func (d *Differ) addMaterializedViewRecreationChanges(
	result *DiffResult,
	key string,
	currentView, desiredView *schema.MaterializedView,
) {
	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeDropMaterializedView,
		Severity:    SeverityPotentiallyBreaking,
		Description: "Drop materialized view for recreation: " + currentView.QualifiedName(),
		ObjectType:  "materialized_view",
		ObjectName:  key,
		Details: map[string]any{
			"view":              *currentView,
			"will_be_recreated": true,
		},
		DependsOn:         extractViewDependencies(currentView.Definition),
		RollbackDependsOn: extractViewDependencies(currentView.Definition),
	})

	result.Changes = append(result.Changes, Change{
		Type:        ChangeTypeAddMaterializedView,
		Severity:    SeveritySafe,
		Description: "Recreate materialized view: " + desiredView.QualifiedName(),
		ObjectType:  "materialized_view",
		ObjectName:  key,
		Details: map[string]any{
			"view":          *desiredView,
			"is_recreation": true,
		},
		DependsOn: extractViewDependencies(desiredView.Definition),
	})
}
