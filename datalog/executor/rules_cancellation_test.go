package executor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
)

// TestEvaluateRules_CancelledAtIteration cancels the context from the first
// iteration event of a closure; the fixpoint stops at the next iteration with
// the context's error.
func TestEvaluateRules_CancelledAtIteration(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(5))
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			iterations := 0
			opts := mode.PlannerOptions()
			opts.Handler = func(e annotations.Event) {
				if e.Name == annotations.RulesIteration {
					iterations++
					cancel()
				}
			}
			exec := NewExecutorWithOptions(matcher, nil, opts)
			_, err := exec.ExecuteWithRelations(NewContext().WithRules(rs).WithCancellation(ctx), q, nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("closure under a cancelled context returned %v, want the context's error", err)
			}
			if iterations > 2 {
				t.Errorf("observed %d iteration events after the cancellation, want at most 2", iterations)
			}
		})
	}
}
