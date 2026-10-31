package differ

import "strings"

func (vn *viewNormalizer) catalogDefinitionsEqual(
	current, desired, currentBound, desiredBound string,
) bool {
	if currentBound == "" && desiredBound == "" {
		return vn.definitionsEqual(current, desired)
	}

	if currentBound != "" && desiredBound != "" {
		return vn.catalogASTsEqual(
			currentBound,
			desiredBound,
			catalogPostgresView,
			catalogPostgresView,
		)
	}

	// Catalog evidence must not fall back to text that erases binding differences.
	if vn.catalogASTsEqual(current, desired, sourcePostgresView, sourcePostgresView) {
		return true
	}

	if currentBound != "" {
		return vn.catalogASTsEqual(currentBound, desired, catalogPostgresView, sourcePostgresView)
	}

	return vn.catalogASTsEqual(current, desiredBound, sourcePostgresView, catalogPostgresView)
}

func (vn *viewNormalizer) catalogASTsEqual(
	current, desired string,
	currentMode, desiredMode postgresViewNormalization,
) bool {
	left, leftOK, _ := canonicalPostgresViewMode(current, vn.currentColumns, currentMode)

	right, rightOK, _ := canonicalPostgresViewMode(desired, vn.desiredColumns, desiredMode)
	if leftOK && rightOK {
		return left == right
	}

	return strings.TrimSpace(current) == strings.TrimSpace(desired)
}
