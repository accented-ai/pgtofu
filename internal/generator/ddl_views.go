package generator

import (
	"fmt"
	"strings"

	"github.com/accented-ai/pgtofu/internal/differ"
)

func (b *DDLBuilder) buildAddView(change differ.Change) (DDLStatement, error) {
	view := b.getView(change.ObjectName, b.result.Desired)
	if view == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddView",
			&change,
			wrapObjectNotFoundError(ErrViewNotFound, "view", change.ObjectName),
		)
	}

	definition, err := formatViewDefinition(view, false)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddView", &change, err)
	}

	var sb strings.Builder
	appendStatement(&sb, definition)

	if view.Comment != "" {
		commentSQL := buildCommentStatement(
			"VIEW",
			QualifiedName(view.Schema, view.Name),
			view.Comment,
			false,
		)
		appendStatement(&sb, commentSQL)
	}

	return DDLStatement{
		SQL:         sb.String(),
		Description: "Add view " + view.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildDropView(change differ.Change) (DDLStatement, error) {
	view := b.findView(change.ObjectName)
	if view == nil {
		return DDLStatement{}, fmt.Errorf("view not found: %s", change.ObjectName)
	}

	sql := fmt.Sprintf("DROP VIEW %s%s CASCADE;",
		b.ifExists(), QualifiedName(view.Schema, view.Name))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop view " + view.Name,
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildAddViewForDown(change differ.Change) (DDLStatement, error) {
	view := b.getView(change.ObjectName, b.result.Current)
	if view == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddViewForDown",
			&change,
			wrapObjectNotFoundError(ErrViewNotFound, "view", change.ObjectName),
		)
	}

	definition, err := formatViewDefinition(view, false)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddViewForDown", &change, err)
	}

	var sb strings.Builder
	appendStatement(&sb, definition)

	if view.Comment != "" {
		commentSQL := buildCommentStatement(
			"VIEW",
			QualifiedName(view.Schema, view.Name),
			view.Comment,
			false,
		)
		appendStatement(&sb, commentSQL)
	}

	return DDLStatement{
		SQL:         sb.String(),
		Description: "Add view " + view.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildModifyView(change differ.Change) (DDLStatement, error) {
	comment, err := extractCommentDetails(change)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyView", &change, err)
	}

	if comment.HasOld && comment.HasNew {
		view := b.getView(change.ObjectName, b.result.Desired)
		if view == nil {
			return DDLStatement{}, newGeneratorError(
				"buildModifyView",
				&change,
				wrapObjectNotFoundError(ErrViewNotFound, "view", change.ObjectName),
			)
		}

		sql := buildCommentStatement(
			"VIEW",
			QualifiedName(view.Schema, view.Name),
			comment.New,
			false,
		)

		return DDLStatement{
			SQL:         sql,
			Description: "Modify view comment " + view.Name,
			RequiresTx:  true,
		}, nil
	}

	view := b.getView(change.ObjectName, b.result.Desired)
	if view == nil {
		return DDLStatement{}, newGeneratorError(
			"buildModifyView",
			&change,
			wrapObjectNotFoundError(ErrViewNotFound, "view", change.ObjectName),
		)
	}

	definition, err := formatViewDefinition(view, true)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyView", &change, err)
	}

	var sb strings.Builder
	appendStatement(&sb, definition)

	if view.Comment != "" {
		commentSQL := buildCommentStatement(
			"VIEW",
			QualifiedName(view.Schema, view.Name),
			view.Comment,
			false,
		)
		appendStatement(&sb, commentSQL)
	}

	return DDLStatement{
		SQL:         sb.String(),
		Description: "Modify view " + view.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildRevertModifyView(change differ.Change) (DDLStatement, error) {
	view := b.getView(change.ObjectName, b.result.Current)
	if view == nil {
		comment, err := extractCommentDetails(change)
		if err != nil {
			return DDLStatement{}, newGeneratorError("buildRevertModifyView", &change, err)
		}

		if comment.HasOld && comment.HasNew && comment.Old == "" {
			desiredView := b.getView(change.ObjectName, b.result.Desired)
			if desiredView != nil {
				sql := buildCommentStatement(
					"VIEW",
					QualifiedName(desiredView.Schema, desiredView.Name),
					"",
					false,
				)

				return DDLStatement{
					SQL:         sql,
					Description: "Revert view comment " + desiredView.Name,
					RequiresTx:  true,
				}, nil
			}
		}

		return DDLStatement{}, newGeneratorError(
			"buildRevertModifyView",
			&change,
			wrapObjectNotFoundError(ErrViewNotFound, "view", change.ObjectName),
		)
	}

	definition, err := formatViewDefinition(view, true)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildRevertModifyView", &change, err)
	}

	return DDLStatement{
		SQL:         ensureStatementTerminated(definition),
		Description: "Revert view " + view.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildAddMaterializedView(change differ.Change) (DDLStatement, error) {
	mv := b.getMaterializedView(change.ObjectName, b.result.Desired)
	if mv == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddMaterializedView",
			&change,
			wrapObjectNotFoundError(
				ErrMaterializedViewNotFound,
				"materialized view",
				change.ObjectName,
			),
		)
	}

	definition, err := formatMaterializedViewDefinition(mv)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddMaterializedView", &change, err)
	}

	var sb strings.Builder
	appendStatement(&sb, definition)

	if mv.Comment != "" {
		commentSQL := buildCommentStatement(
			"MATERIALIZED VIEW",
			QualifiedName(mv.Schema, mv.Name),
			mv.Comment,
			false,
		)
		appendStatement(&sb, commentSQL)
	}

	return DDLStatement{
		SQL:         sb.String(),
		Description: "Add materialized view " + mv.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildDropMaterializedView(change differ.Change) (DDLStatement, error) {
	mv := b.findMaterializedView(change.ObjectName)
	if mv == nil {
		return DDLStatement{}, fmt.Errorf("materialized view not found: %s", change.ObjectName)
	}

	sql := fmt.Sprintf("DROP MATERIALIZED VIEW %s%s CASCADE;",
		b.ifExists(), QualifiedName(mv.Schema, mv.Name))

	return DDLStatement{
		SQL:         sql,
		Description: "Drop materialized view " + mv.Name,
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildAddMaterializedViewForDown(
	change differ.Change,
) (DDLStatement, error) {
	mv := b.getMaterializedView(change.ObjectName, b.result.Current)
	if mv == nil {
		return DDLStatement{}, newGeneratorError(
			"buildAddMaterializedViewForDown",
			&change,
			wrapObjectNotFoundError(
				ErrMaterializedViewNotFound,
				"materialized view",
				change.ObjectName,
			),
		)
	}

	definition, err := formatMaterializedViewDefinition(mv)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildAddMaterializedViewForDown", &change, err)
	}

	var sb strings.Builder
	appendStatement(&sb, definition)

	if mv.Comment != "" {
		commentSQL := buildCommentStatement(
			"MATERIALIZED VIEW",
			QualifiedName(mv.Schema, mv.Name),
			mv.Comment,
			false,
		)
		appendStatement(&sb, commentSQL)
	}

	return DDLStatement{
		SQL:         sb.String(),
		Description: "Add materialized view " + mv.Name,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildModifyMaterializedView(change differ.Change) (DDLStatement, error) {
	comment, err := extractCommentDetails(change)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyMaterializedView", &change, err)
	}

	if comment.HasOld && comment.HasNew {
		mv := b.getMaterializedView(change.ObjectName, b.result.Desired)
		if mv == nil {
			return DDLStatement{}, newGeneratorError(
				"buildModifyMaterializedView",
				&change,
				wrapObjectNotFoundError(
					ErrMaterializedViewNotFound,
					"materialized view",
					change.ObjectName,
				),
			)
		}

		sql := buildCommentStatement(
			"MATERIALIZED VIEW",
			QualifiedName(mv.Schema, mv.Name),
			comment.New,
			false,
		)

		return DDLStatement{
			SQL:         sql,
			Description: "Modify materialized view comment " + mv.Name,
			RequiresTx:  true,
		}, nil
	}

	mv := b.getMaterializedView(change.ObjectName, b.result.Desired)
	if mv == nil {
		return DDLStatement{}, newGeneratorError(
			"buildModifyMaterializedView",
			&change,
			wrapObjectNotFoundError(
				ErrMaterializedViewNotFound,
				"materialized view",
				change.ObjectName,
			),
		)
	}

	var sb strings.Builder

	dropStatement := fmt.Sprintf(
		"DROP MATERIALIZED VIEW %s%s;",
		b.ifExists(),
		QualifiedName(mv.Schema, mv.Name),
	)
	appendStatement(&sb, dropStatement)

	definition, err := formatMaterializedViewDefinition(mv)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildModifyMaterializedView", &change, err)
	}

	appendStatement(&sb, definition)

	if mv.Comment != "" {
		commentSQL := buildCommentStatement(
			"MATERIALIZED VIEW",
			QualifiedName(mv.Schema, mv.Name),
			mv.Comment,
			false,
		)
		appendStatement(&sb, commentSQL)
	}

	return DDLStatement{
		SQL:         sb.String(),
		Description: "Modify materialized view " + mv.Name,
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}

func (b *DDLBuilder) buildRevertModifyMaterializedView(change differ.Change) (DDLStatement, error) {
	mv := b.getMaterializedView(change.ObjectName, b.result.Current)
	if mv == nil {
		comment, err := extractCommentDetails(change)
		if err != nil {
			return DDLStatement{}, newGeneratorError(
				"buildRevertModifyMaterializedView", &change, err,
			)
		}

		if comment.HasOld && comment.HasNew && comment.Old == "" {
			desiredMV := b.getMaterializedView(change.ObjectName, b.result.Desired)
			if desiredMV != nil {
				sql := buildCommentStatement(
					"MATERIALIZED VIEW",
					QualifiedName(desiredMV.Schema, desiredMV.Name),
					"",
					false,
				)

				return DDLStatement{
					SQL:         sql,
					Description: "Revert materialized view comment " + desiredMV.Name,
					RequiresTx:  true,
				}, nil
			}
		}

		return DDLStatement{}, newGeneratorError(
			"buildRevertModifyMaterializedView",
			&change,
			wrapObjectNotFoundError(
				ErrMaterializedViewNotFound,
				"materialized view",
				change.ObjectName,
			),
		)
	}

	var sb strings.Builder

	dropStatement := fmt.Sprintf(
		"DROP MATERIALIZED VIEW %s%s;",
		b.ifExists(),
		QualifiedName(mv.Schema, mv.Name),
	)
	appendStatement(&sb, dropStatement)

	definition, err := formatMaterializedViewDefinition(mv)
	if err != nil {
		return DDLStatement{}, newGeneratorError("buildRevertModifyMaterializedView", &change, err)
	}

	appendStatement(&sb, definition)

	return DDLStatement{
		SQL:         sb.String(),
		Description: "Revert materialized view " + mv.Name,
		IsUnsafe:    true,
		RequiresTx:  true,
	}, nil
}
