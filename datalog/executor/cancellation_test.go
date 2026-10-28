package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// A query under a done context is refused at entry, before any work, with
// the context's error, and reported as every refusal is: invoked once and
// completed once with that error. A fixpoint under a context that is
// cancelled while it runs stops at its next iteration with the context's
// error.

func TestExecuteWithRelations_RefusedWhenContextDone(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(3))
	q := mustParse(t, `[:find ?p :where [?p :page/kind "leaf"]]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var events []annotations.Event
			opts := mode.plannerOptions()
			opts.Handler = func(e annotations.Event) { events = append(events, e) }
			exec := NewExecutorWithOptions(matcher, nil, opts)
			_, err := exec.ExecuteWithRelations(NewContext().WithCancellation(ctx), q, nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("query under a done context returned %v, want the context's error", err)
			}
			names := make([]string, len(events))
			for i, e := range events {
				names[i] = e.Name
			}
			if len(events) != 2 || events[0].Name != annotations.QueryInvoked || events[1].Name != annotations.QueryComplete {
				t.Fatalf("observed %v, want the invocation and the completion and no other event: the query is refused before any work and reported", names)
			}
			if events[1].Data[annotations.KeySuccess] != false || events[1].Data[annotations.KeyError] != err.Error() {
				t.Errorf("the completion reports %v, want success false and the context's error %q", events[1].Data, err.Error())
			}
		})
	}
}

// TestEvaluateRules_CancelledAtIteration cancels the context from the first
// iteration event of a closure; the fixpoint stops at the next iteration with
// the context's error.
func TestEvaluateRules_CancelledAtIteration(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(5))
	rs := mustRules(t, followsRulesText)
	q := mustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			iterations := 0
			opts := mode.plannerOptions()
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
