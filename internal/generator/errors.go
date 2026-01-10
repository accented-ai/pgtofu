package generator

import (
	"errors"
	"fmt"

	"github.com/accented-ai/pgtofu/internal/differ"
	"github.com/accented-ai/pgtofu/internal/schema"
	"github.com/accented-ai/pgtofu/internal/util"
)

var (
	ErrNilDiffResult            = errors.New("diff result is nil")
	ErrInvalidChangeDetails     = errors.New("invalid change details")
	ErrTableNotFound            = errors.New("table not found")
	ErrViewNotFound             = errors.New("view not found")
	ErrMaterializedViewNotFound = errors.New("materialized view not found")
	ErrFunctionNotFound         = errors.New("function not found")
	ErrIndexNotFound            = errors.New("index not found")
	ErrConstraintNotFound       = errors.New("constraint not found")
	ErrSequenceNotFound         = errors.New("sequence not found")
	ErrCustomTypeNotFound       = errors.New("custom type not found")
	ErrExtensionNotFound        = errors.New("extension not found")
	ErrHypertableNotFound       = errors.New("hypertable not found")
	ErrTriggerNotFound          = errors.New("trigger not found")
	ErrUnsupportedChangeType    = errors.New("unsupported change type")
)

type GeneratorError struct {
	Op     string
	Change *differ.Change
	Err    error
}

func (e *GeneratorError) Error() string {
	if e.Change != nil {
		return fmt.Sprintf("generator.%s: failed for change %s (%s): %v",
			e.Op, e.Change.Type, e.Change.ObjectName, e.Err)
	}

	return fmt.Sprintf("generator.%s: %v", e.Op, e.Err)
}

func (e *GeneratorError) Unwrap() error {
	return e.Err
}

func newGeneratorError(op string, change *differ.Change, err error) *GeneratorError {
	return &GeneratorError{
		Op:     op,
		Change: change,
		Err:    err,
	}
}

var (
	errDetailMapNil    = errors.New("details map is nil")
	errDetailMissing   = errors.New("missing value")
	errDetailWrongType = errors.New("value has unexpected type")
)

func wrapDetailError(key DetailKey, err error) error {
	if err == nil {
		return nil
	}

	detailErr := util.WrapError(fmt.Sprintf("detail %q", key.String()), err)

	return fmt.Errorf("%w: %w", ErrInvalidChangeDetails, detailErr)
}

func wrapObjectNotFoundError(base error, objectType, name string) error {
	if base == nil {
		base = fmt.Errorf("%s not found", objectType)
	}

	return util.WrapError(fmt.Sprintf("%s %s", objectType, name), base)
}

func requireDetail[T any](details map[string]any, key DetailKey) (T, error) { //nolint:ireturn
	var zero T

	if details == nil {
		return zero, wrapDetailError(key, errDetailMapNil)
	}

	raw, ok := details[key.String()]
	if !ok {
		return zero, wrapDetailError(key, errDetailMissing)
	}

	value, ok := raw.(T)
	if !ok {
		expected := fmt.Sprintf("%T", zero)

		return zero, wrapDetailError(
			key,
			fmt.Errorf("%w: expected %s, got %T", errDetailWrongType, expected, raw),
		)
	}

	return value, nil
}

func optionalDetail[T any]( //nolint:ireturn
	details map[string]any,
	key DetailKey,
) (T, bool, error) {
	var zero T

	if details == nil {
		return zero, false, nil
	}

	raw, ok := details[key.String()]
	if !ok {
		return zero, false, nil
	}

	value, ok := raw.(T)
	if !ok {
		expected := fmt.Sprintf("%T", zero)

		return zero, true, wrapDetailError(
			key,
			fmt.Errorf("%w: expected %s, got %T", errDetailWrongType, expected, raw),
		)
	}

	return value, true, nil
}

func getDetailString(details map[string]any, key DetailKey) (string, error) {
	return requireDetail[string](details, key)
}

func getOptionalDetailString(details map[string]any, key DetailKey) (string, bool, error) {
	return optionalDetail[string](details, key)
}

func getDetailBool(details map[string]any, key DetailKey) (bool, error) {
	return requireDetail[bool](details, key)
}

func getDetailColumn(details map[string]any) (*schema.Column, error) {
	return requireDetail[*schema.Column](details, DetailKeyColumn)
}

func getDetailConstraint(details map[string]any) (*schema.Constraint, error) {
	return requireDetail[*schema.Constraint](details, DetailKeyConstraint)
}

func getCurrentConstraint(details map[string]any) (*schema.Constraint, error) {
	return requireDetail[*schema.Constraint](details, DetailKeyCurrent)
}

func getDesiredConstraint(details map[string]any) (*schema.Constraint, error) {
	return requireDetail[*schema.Constraint](details, DetailKeyDesired)
}

func getDetailIndex(details map[string]any) (*schema.Index, error) {
	return requireDetail[*schema.Index](details, DetailKeyIndex)
}

func getDetailPartition(details map[string]any) (*schema.Partition, error) {
	return requireDetail[*schema.Partition](details, DetailKeyPartition)
}

func getOptionalTable(details map[string]any) (*schema.Table, bool, error) {
	return optionalDetail[*schema.Table](details, DetailKeyTable)
}
