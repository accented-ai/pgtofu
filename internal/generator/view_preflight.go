package generator

import (
	"fmt"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
)

func validateViewColumnReplacements(changes []differ.Change, generateDown bool) error {
	for _, change := range changes {
		if change.Type != differ.ChangeTypeModifyView {
			continue
		}

		current, currentOK := change.Details["current"].(schema.View)

		desired, desiredOK := change.Details["desired"].(schema.View)
		if !currentOK || !desiredOK {
			continue
		}

		currentColumns, currentKnown := viewOutputColumnNames(current.Definition)

		desiredColumns, desiredKnown := viewOutputColumnNames(desired.Definition)
		if !currentKnown || !desiredKnown {
			continue
		}

		if len(desiredColumns) < len(currentColumns) {
			return viewColumnRemovalError(change.ObjectName, "up")
		}

		if generateDown && len(currentColumns) < len(desiredColumns) {
			return viewColumnRemovalError(change.ObjectName, "down")
		}
	}

	return nil
}

func viewColumnRemovalError(viewName, direction string) error {
	const reason = "CREATE OR REPLACE VIEW cannot remove output columns; " +
		"rebuild the view and its dependents explicitly"

	return fmt.Errorf(
		"cannot generate %s migration for view %s: %s",
		direction,
		viewName,
		reason,
	)
}
