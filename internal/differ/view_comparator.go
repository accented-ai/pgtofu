package differ

import (
	"github.com/accented-ai/pgtofu/internal/schema"
)

type ViewComparator struct {
	options    *Options
	normalizer *viewNormalizer
}

func NewViewComparator(opts *Options) *ViewComparator {
	return &ViewComparator{
		options:    opts,
		normalizer: NewViewNormalizer(),
	}
}

func (vc *ViewComparator) AreEqual(current, desired schema.View) bool {
	if vc.options.IgnoreComments {
		return vc.normalizer.normalizeDefinition(
			current.Definition,
		) == vc.normalizer.normalizeDefinition(
			desired.Definition,
		) &&
			normalizeCheckOption(current.CheckOption) == normalizeCheckOption(desired.CheckOption)
	}

	currentComment := normalizeComment(current.Comment)
	desiredComment := normalizeComment(desired.Comment)

	return vc.normalizer.normalizeDefinition(
		current.Definition,
	) == vc.normalizer.normalizeDefinition(
		desired.Definition,
	) &&
		normalizeCheckOption(current.CheckOption) == normalizeCheckOption(desired.CheckOption) &&
		currentComment == desiredComment
}

func (vc *ViewComparator) CreateAddChange(key string, view schema.View) Change {
	return Change{
		Type:        ChangeTypeAddView,
		Severity:    SeveritySafe,
		Description: "Add view: " + view.QualifiedName(),
		ObjectType:  "view",
		ObjectName:  key,
		Details:     map[string]any{"view": view},
		DependsOn:   extractViewDependencies(view.Definition),
	}
}

func (vc *ViewComparator) CreateDropChange(key string, view schema.View) Change {
	return Change{
		Type:        ChangeTypeDropView,
		Severity:    SeverityBreaking,
		Description: "Drop view: " + view.QualifiedName(),
		ObjectType:  "view",
		ObjectName:  key,
		Details:     map[string]any{"view": view},
	}
}

func (vc *ViewComparator) CreateModifyChange(key string, current, desired schema.View) Change {
	defEqual := vc.normalizer.normalizeDefinition(
		current.Definition,
	) == vc.normalizer.normalizeDefinition(
		desired.Definition,
	)
	checkOptEqual := normalizeCheckOption(
		current.CheckOption,
	) == normalizeCheckOption(
		desired.CheckOption,
	)

	if !defEqual || !checkOptEqual {
		return Change{
			Type:        ChangeTypeModifyView,
			Severity:    SeverityPotentiallyBreaking,
			Description: "Modify view: " + desired.QualifiedName(),
			ObjectType:  "view",
			ObjectName:  key,
			Details:     map[string]any{"current": current, "desired": desired},
			DependsOn:   extractViewDependencies(desired.Definition),
		}
	}

	return Change{}
}

func (vc *ViewComparator) CreateCommentChange(
	key string,
	view schema.View,
	oldComment, newComment string,
) Change {
	return Change{
		Type:        ChangeTypeModifyView,
		Severity:    SeveritySafe,
		Description: "Modify view comment: " + view.QualifiedName(),
		ObjectType:  "view",
		ObjectName:  key,
		Details: map[string]any{
			"view":        view,
			"old_comment": oldComment,
			"new_comment": newComment,
		},
	}
}
