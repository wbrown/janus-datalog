package executor

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestPlannerHandler_ConcurrentAnnotatedQueriesIsolated is a regression test for
// BUG_PLANNER_HANDLER_SHARED_STATE_RACE.
//
// An annotation handler must not be installed as mutable state on the planner.
// When it is, concurrent annotated queries overwrite or clear each other's
// handler, so a query's algebra-bridge events are dropped or delivered to another
// query's collector — and the handler field write/read is itself a data race.
//
// The handler is registered on the options each executor is constructed with, so
// isolation is per executor: each goroutine builds its own, carrying its own
// handler, over one shared matcher. That is what a concurrent caller wanting its
// own events does, and it is the arrangement this pins.
//
// optimizeViaAlgebra emits exactly one "algebra/bridge-begin" per planning call,
// and these executors use no plan cache, so every query plans independently. With
// correct per-query handler threading, each handler sees exactly one
// bridge-begin: its own. Under shared-handler state, the count per handler is
// scrambled (some see zero, some see several). Run under -race to additionally
// catch the data race directly.
func TestPlannerHandler_ConcurrentAnnotatedQueriesIsolated(t *testing.T) {
	// Planning runs regardless of matcher contents; an empty matcher is enough.
	// Shared across goroutines, so the planners still contend on whatever the
	// matcher and its options hold.
	matcher := NewMemoryPatternMatcher(nil)

	q, err := parser.ParseQuery(`[:find ?e ?v :where [?e :x/v ?v]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	const goroutines = 32
	counts := make([]int32, goroutines)
	errs := make([]error, goroutines)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Each goroutine gets its OWN handler, registered on the options its
			// own executor is built with. EnableAlgebraOptimizer so the handler
			// is exercised; no Cache so every query re-plans (and re-emits)
			// rather than hitting a cached plan.
			exec := NewExecutorWithOptions(matcher, nil, planner.PlannerOptions{
				EnableAlgebraOptimizer: true,
				Handler: func(e annotations.Event) {
					if e.Name == "algebra/bridge-begin" {
						atomic.AddInt32(&counts[idx], 1)
					}
				},
			})
			rel, err := exec.Execute(q)
			if err != nil {
				errs[idx] = err
				return
			}
			_ = collectTuples(rel)
		}(i)
	}
	wg.Wait()

	for i := 0; i < goroutines; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: query failed: %v", i, errs[i])
		}
		if got := atomic.LoadInt32(&counts[i]); got != 1 {
			t.Fatalf("goroutine %d: expected exactly 1 algebra/bridge-begin in its own "+
				"collector, got %d (per-query handler leaked across concurrent queries "+
				"via shared planner state)", i, got)
		}
	}
}
