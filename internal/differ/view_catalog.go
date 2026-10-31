package differ

import (
	"reflect"
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

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

	left, leftOK, _ := canonicalPostgresViewWithCatalog(
		current, currentBound, vn.currentColumns, sourcePostgresView,
	)

	right, rightOK, _ := canonicalPostgresViewWithCatalog(
		desired, desiredBound, vn.desiredColumns, sourcePostgresView,
	)
	if leftOK && rightOK && left == right {
		return true
	}

	if currentBound != "" {
		return vn.catalogASTsEqual(currentBound, desired, catalogPostgresView, sourcePostgresView)
	}

	return vn.catalogASTsEqual(current, desiredBound, sourcePostgresView, catalogPostgresView)
}

// Only matching deparsed trees can attest casts in the unchanged presentation.
// Qualification can differ; every expression, literal, and type modifier must match.
func collectCatalogViewCasts(
	presentation, catalog protoreflect.Message,
	casts map[*pgquery.TypeCast]*pgquery.TypeCast,
) bool {
	if presentation.Descriptor() != catalog.Descriptor() {
		return false
	}

	nameField, names := catalogViewNames(presentation)

	_, catalogNames := catalogViewNames(catalog)
	if nameField != "" && !catalogViewNamesMatch(names, catalogNames) {
		return false
	}

	fields := presentation.Descriptor().Fields()
	for index := range fields.Len() {
		field := fields.Get(index)

		name := string(field.Name())
		if isPostgresLocationField(name) || name == nameField {
			continue
		}

		left, right := presentation.Get(field), catalog.Get(field)
		if _, relation := presentation.Interface().(*pgquery.RangeVar); relation &&
			name == "schemaname" {
			if left.String() != "" && left.String() != right.String() {
				return false
			}

			continue
		}

		if presentation.Has(field) != catalog.Has(field) {
			return false
		}

		if !presentation.Has(field) {
			continue
		}

		if field.IsList() {
			if left.List().Len() != right.List().Len() {
				return false
			}

			for item := range left.List().Len() {
				if !catalogViewValuesMatch(
					field,
					left.List().Get(item),
					right.List().Get(item),
					casts,
				) {
					return false
				}
			}
		} else if !catalogViewValuesMatch(field, left, right, casts) {
			return false
		}
	}

	if cast, ok := presentation.Interface().(*pgquery.TypeCast); ok {
		if bound, boundOK := catalog.Interface().(*pgquery.TypeCast); boundOK {
			casts[cast] = bound
		}
	}

	return true
}

func catalogViewValuesMatch(
	field protoreflect.FieldDescriptor,
	presentation, catalog protoreflect.Value,
	casts map[*pgquery.TypeCast]*pgquery.TypeCast,
) bool {
	if field.Kind() == protoreflect.MessageKind {
		return collectCatalogViewCasts(presentation.Message(), catalog.Message(), casts)
	}

	return reflect.DeepEqual(presentation.Interface(), catalog.Interface())
}

func catalogViewNames(message protoreflect.Message) (string, []*pgquery.Node) {
	switch node := message.Interface().(type) {
	case *pgquery.TypeName:
		return "names", node.GetNames()
	case *pgquery.FuncCall:
		return "funcname", node.GetFuncname()
	case *pgquery.A_Expr:
		return "name", node.GetName()
	default:
		return "", nil
	}
}

func catalogViewNamesMatch(presentation, catalog []*pgquery.Node) bool {
	if len(presentation) == 0 || len(catalog) == 0 {
		return len(presentation) == len(catalog)
	}

	if postgresNodeListLastString(presentation) != postgresNodeListLastString(catalog) {
		return false
	}

	if len(presentation) == 1 {
		return len(catalog) <= 2
	}

	return len(presentation) == 2 && (len(catalog) == 1 &&
		presentation[0].GetString_().GetSval() == "pg_catalog" || len(catalog) == 2 &&
		presentation[0].GetString_().GetSval() == catalog[0].GetString_().GetSval())
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
