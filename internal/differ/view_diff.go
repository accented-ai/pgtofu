package differ

import (
	"regexp"
	"strings"

	"github.com/accented-ai/pgtofu/internal/schema"
)

var viewDependencyPattern = regexp.MustCompile(
	`(?is)\b(?:from|join)\s+(?:lateral\s+)?(?:only\s+)?((?:"[^"]+"|[a-z0-9_]+)(?:\.(?:"[^"]+"|[a-z0-9_]+))?)`,
)

func (d *Differ) compareViews(result *DiffResult) {
	currentViews := buildViewMap(result.Current.Views)
	desiredViews := buildViewMap(result.Desired.Views)

	for key, view := range desiredViews {
		if _, exists := currentViews[key]; !exists {
			change := d.viewComp.CreateAddChange(key, *view)
			result.Changes = append(result.Changes, change)

			if !d.options.IgnoreComments && view.Comment != "" {
				result.Changes = append(
					result.Changes,
					d.viewComp.CreateCommentChange(key, *view, "", view.Comment),
				)
			}
		}
	}

	for key, view := range currentViews {
		if _, exists := desiredViews[key]; !exists {
			result.Changes = append(result.Changes, d.viewComp.CreateDropChange(key, *view))
		}
	}

	for key, desiredView := range desiredViews {
		if currentView, exists := currentViews[key]; exists {
			if !d.viewComp.AreEqual(*currentView, *desiredView) {
				change := d.viewComp.CreateModifyChange(key, *currentView, *desiredView)
				if change.Type != "" {
					result.Changes = append(result.Changes, change)
				}
			}

			if !d.options.IgnoreComments && currentView.Comment != desiredView.Comment {
				result.Changes = append(
					result.Changes,
					d.viewComp.CreateCommentChange(
						key,
						*desiredView,
						currentView.Comment,
						desiredView.Comment,
					),
				)
			}
		}
	}
}

func buildViewMap(views []schema.View) map[string]*schema.View {
	m := make(map[string]*schema.View, len(views))
	for i := range views {
		key := ViewKey(views[i].Schema, views[i].Name)
		m[key] = &views[i]
	}

	return m
}

func (d *Differ) compareMaterializedViews(result *DiffResult) {
	d.processMaterializedViewChanges(
		result,
		buildMaterializedViewMap(result.Current.MaterializedViews),
		buildMaterializedViewMap(result.Desired.MaterializedViews),
	)
}

func (d *Differ) processMaterializedViewChanges(
	result *DiffResult,
	currentViews map[string]*schema.MaterializedView,
	desiredViews map[string]*schema.MaterializedView,
) {
	for key, view := range desiredViews {
		if _, exists := currentViews[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeAddMaterializedView,
				Severity:    SeveritySafe,
				Description: "Add materialized view: " + view.QualifiedName(),
				ObjectType:  "materialized_view",
				ObjectName:  key,
				Details:     map[string]any{"view": view},
				DependsOn:   extractViewDependencies(view.Definition),
			})

			if !d.options.IgnoreComments && view.Comment != "" {
				result.Changes = append(result.Changes, Change{
					Type:        ChangeTypeModifyMaterializedView,
					Severity:    SeveritySafe,
					Description: "Add materialized view comment: " + view.QualifiedName(),
					ObjectType:  "materialized_view",
					ObjectName:  key,
					Details: map[string]any{
						"view":        view,
						"old_comment": "",
						"new_comment": view.Comment,
					},
				})
			}
		}
	}

	for key, view := range currentViews {
		if _, exists := desiredViews[key]; !exists {
			result.Changes = append(result.Changes, Change{
				Type:        ChangeTypeDropMaterializedView,
				Severity:    SeverityBreaking,
				Description: "Drop materialized view: " + view.QualifiedName(),
				ObjectType:  "materialized_view",
				ObjectName:  key,
				Details:     map[string]any{"view": view},
			})
		}
	}

	for key, desiredView := range desiredViews {
		if currentView, exists := currentViews[key]; exists {
			defEqual := NormalizeViewDefinition(
				currentView.Definition,
			) == NormalizeViewDefinition(
				desiredView.Definition,
			)
			currentComment := normalizeComment(currentView.Comment)
			desiredComment := normalizeComment(desiredView.Comment)
			commentEqual := currentComment == desiredComment

			if !defEqual {
				result.Changes = append(result.Changes, Change{
					Type:        ChangeTypeModifyMaterializedView,
					Severity:    SeverityPotentiallyBreaking,
					Description: "Modify materialized view: " + desiredView.QualifiedName(),
					ObjectType:  "materialized_view",
					ObjectName:  key,
					Details:     map[string]any{"current": currentView, "desired": desiredView},
					DependsOn:   extractViewDependencies(desiredView.Definition),
				})
			}

			if !d.options.IgnoreComments && !commentEqual {
				result.Changes = append(result.Changes, Change{
					Type:        ChangeTypeModifyMaterializedView,
					Severity:    SeveritySafe,
					Description: "Modify materialized view comment: " + desiredView.QualifiedName(),
					ObjectType:  "materialized_view",
					ObjectName:  key,
					Details: map[string]any{
						"view":        desiredView,
						"old_comment": currentView.Comment,
						"new_comment": desiredView.Comment,
					},
				})
			}
		}
	}
}

func buildMaterializedViewMap(views []schema.MaterializedView) map[string]*schema.MaterializedView {
	m := make(map[string]*schema.MaterializedView, len(views))
	for i := range views {
		key := ViewKey(views[i].Schema, views[i].Name)
		m[key] = &views[i]
	}

	return m
}

func extractViewDependencies(definition string) []string {
	matches := viewDependencyPattern.FindAllStringSubmatch(definition, -1)
	if len(matches) == 0 {
		return nil
	}

	deps := make([]string, 0, len(matches))
	seen := make(map[string]struct{})

	for _, match := range matches {
		if len(match) < 2 {
			continue
		}

		table := normalizeDependencyIdentifier(match[1])
		if table == "" || isReservedWord(table) {
			continue
		}

		if _, exists := seen[table]; exists {
			continue
		}

		seen[table] = struct{}{}
		deps = append(deps, table)
	}

	return deps
}

func normalizeDependencyIdentifier(identifier string) string {
	trimmed := strings.TrimSpace(identifier)

	trimmed = strings.Trim(trimmed, "();,")
	if trimmed == "" {
		return ""
	}

	parts := splitIdentifierParts(trimmed)
	if len(parts) == 0 {
		return ""
	}

	for i := range parts {
		part := strings.TrimSpace(parts[i])

		part = strings.Trim(part, `"`)
		if part == "" {
			return ""
		}

		parts[i] = part
	}

	return strings.ToLower(strings.Join(parts, "."))
}

func splitIdentifierParts(identifier string) []string {
	var (
		parts    []string
		current  strings.Builder
		inQuotes bool
	)

	for _, r := range identifier {
		switch r {
		case '"':
			inQuotes = !inQuotes

			current.WriteRune(r)
		case '.':
			if inQuotes {
				current.WriteRune(r)
				continue
			}

			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

var reservedWords = map[string]bool{ //nolint:gochecknoglobals
	"select": true, "where": true, "group": true, "order": true,
	"having": true, "limit": true, "offset": true, "union": true,
	"except": true, "intersect": true, "unnest": true, "generate_series": true,
	"values": true,
}

func isReservedWord(word string) bool {
	return reservedWords[strings.ToLower(word)]
}
