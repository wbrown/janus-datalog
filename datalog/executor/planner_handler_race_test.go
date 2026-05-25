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
// A per-query annotation handler must not be installed as mutable state on the
// planner that every query shares through one Executor. When it is, concurrent
// annotated queries overwrite or clear each other's handler, so a query's
// algebra-bridge events are dropped or delivered to another query's collector —
// and the handler field write/read is itself a data race.
//
// optimizeViaAlgebra emits exactly one "algebra/bridge-begin" per planning call,
// and this executor uses no plan cache, so every query plans independently. With
// correct per-query handler threading, each collector sees exactly one
// bridge-begin: its own. Under shared-handler state, the count per collector is
// scrambled (some see zero, some see several). Run under -race to additionally
// catch the data race directly.
func TestPlannerHandler_ConcurrentAnnotatedQueriesIsolated(t *testing.T) {
	// Planning runs regardless of matcher contents; an empty matcher is enough.
	matcher := NewMemoryPatternMatcher(nil)

	// EnableAlgebraOptimizer so the handler is exercised; no Cache so every query
	// re-plans (and re-emits) rather than hitting a cached plan.
	exec := NewExecutorWithOptions(matcher, nil, planner.PlannerOptions{
		EnableAlgebraOptimizer: true,
	})

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
			// Each goroutine gets its OWN handler/collector.
			handler := func(e annotations.Event) {
				if e.Name == "algebra/bridge-begin" {
					atomic.AddInt32(&counts[idx], 1)
				}
			}
			ctx := NewContext(handler)
			rel, err := exec.ExecuteWithContext(ctx, q)
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
