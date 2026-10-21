package postgresparse_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/accented-ai/pgtofu/internal/postgresparse"
)

func TestConcurrentFirstUse(t *testing.T) {
	t.Parallel()

	const callers = 8

	start := make(chan struct{})
	failures := make(chan error, callers)

	var workers sync.WaitGroup
	for range callers {
		workers.Go(func() {
			<-start

			tree, err := postgresparse.Parse("SELECT 'scope'::text")
			if err != nil {
				failures <- err
				return
			}

			canonical, err := postgresparse.Deparse(tree)
			if err != nil {
				failures <- err
			} else if canonical != "SELECT 'scope'::text" {
				failures <- fmt.Errorf("unexpected SQL after round trip: %s", canonical)
			}
		})
	}

	close(start)
	workers.Wait()
	close(failures)

	for err := range failures {
		t.Error(err)
	}
}
