package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestWrapExtractSchemaErrorIncludesTimeoutGuidance(t *testing.T) {
	t.Parallel()

	err := wrapExtractSchemaError(
		fmt.Errorf("extract tables: %w", context.DeadlineExceeded),
		15*time.Minute,
	)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected wrapped error to match context deadline exceeded")
	}

	msg := err.Error()
	if !strings.Contains(msg, "extract schema timed out after 15m0s") {
		t.Fatalf("expected timeout in error message, got %q", msg)
	}

	if !strings.Contains(msg, "--timeout") {
		t.Fatalf("expected timeout guidance in error message, got %q", msg)
	}
}

func TestWrapExtractSchemaErrorKeepsRegularContext(t *testing.T) {
	t.Parallel()

	err := wrapExtractSchemaError(errors.New("extract tables: fetch tables"), 15*time.Minute)

	if err.Error() != "extract schema: extract tables: fetch tables" {
		t.Fatalf("unexpected error message: %q", err.Error())
	}
}
