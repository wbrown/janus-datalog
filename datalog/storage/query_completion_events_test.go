package storage

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A handler sees one completion per query the database's Query path is
// asked, where the query ends. A query that runs completes when its result is
// exhausted or closed, whichever comes first, with success and the tuples
// drained by then; a result exhausted and then closed completes once, and so
// does a renamed result. A query that fails in execution completes with the
// failure before Query returns, and a query whose drain ends in a read fault
// completes with the fault and the tuples drained before it. Analyze collects
// the completion with the rest of the query's events.
func TestQuery_ACompletionReachesTheHandlerWhereTheQueryEnds(t *testing.T) {
	facts := []fact{
		{named("e1"), ":t/n", int64(1)}, {named("e2"), ":t/n", int64(2)}, {named("e3"), ":t/n", int64(3)},
		{named("e1"), ":edge", named("e2")}, {named("e2"), ":edge", named("e3")},
	}
	text := `[:find ?e :where [?e :t/n _]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var mu sync.Mutex
			var events []annotations.Event
			var onIteration func()
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				mu.Lock()
				events = append(events, e)
				cancel := onIteration
				mu.Unlock()
				if e.Name == annotations.RulesIteration && cancel != nil {
					cancel()
				}
			}})
			commitFacts(t, d, facts)
			reset := func() {
				mu.Lock()
				events = nil
				onIteration = nil
				mu.Unlock()
			}
			eventsNamed := func(name string) []annotations.Event {
				mu.Lock()
				defer mu.Unlock()
				var found []annotations.Event
				for _, e := range events {
					if e.Name == name {
						found = append(found, e)
					}
				}
				return found
			}
			requireCompleted := func(t *testing.T, tuples int) {
				t.Helper()
				require.Len(t, eventsNamed(annotations.QueryInvoked), 1, "the query is reported invoked once")
				completed := eventsNamed(annotations.QueryComplete)
				require.Len(t, completed, 1, "the query is reported completed once")
				require.Equal(t, true, completed[0].Data[annotations.KeySuccess], "the completion reports success")
				require.Equal(t, tuples, completed[0].Data[annotations.KeyTuplesCount], "the completion carries the tuples drained")
				require.Equal(t, 1, completed[0].Data[annotations.KeyRelationsCount], "the completion carries the one result relation")
				require.False(t, completed[0].End.IsZero(), "the completion carries the query's end")
				require.False(t, completed[0].End.Before(completed[0].Start), "the completion ends after the query began")
			}

			t.Run("a result exhausted and then closed", func(t *testing.T) {
				reset()
				rel, err := d.Query(text)
				require.NoError(t, err)
				require.Empty(t, eventsNamed(annotations.QueryComplete), "nothing completes before the result is drained")
				it := rel.Iterator()
				drained := 0
				for it.Next() {
					drained++
				}
				require.NoError(t, it.Error())
				require.Equal(t, 3, drained)
				requireCompleted(t, 3)
				require.NoError(t, it.Close())
				requireCompleted(t, 3)
			})

			t.Run("a result closed before it is exhausted", func(t *testing.T) {
				reset()
				rel, err := d.Query(text)
				require.NoError(t, err)
				it := rel.Iterator()
				require.True(t, it.Next())
				require.NoError(t, it.Close())
				requireCompleted(t, 1)
			})

			t.Run("a renamed result", func(t *testing.T) {
				reset()
				rel, err := d.Query(text)
				require.NoError(t, err)
				subst, err := query.SubstitutionOf([]query.Symbol{datalog.NewSymbol("?e")}, []query.Symbol{datalog.NewSymbol("?x")})
				require.NoError(t, err)
				renamed, err := rel.Rename(subst)
				require.NoError(t, err)
				tuples, err := executor.CollectTuples(renamed, nil)
				require.NoError(t, err)
				require.Len(t, tuples, 3)
				requireCompleted(t, 3)
			})

			t.Run("a query that fails in execution", func(t *testing.T) {
				reset()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				mu.Lock()
				onIteration = cancel
				mu.Unlock()
				rs := mustRuleSet(t, `[[(reach ?a ?b) [?a :edge ?b]] [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]]`)
				_, err := d.Query(`[:find ?x ?y :in $ % :where (reach ?x ?y)]`, rs, WithContext(ctx))
				require.ErrorIs(t, err, context.Canceled, "the closure under a context cancelled at its first iteration fails with the context's error")
				require.Len(t, eventsNamed(annotations.QueryInvoked), 1, "the query is reported invoked once")
				completed := eventsNamed(annotations.QueryComplete)
				require.Len(t, completed, 1, "the query is reported completed once")
				require.Equal(t, false, completed[0].Data[annotations.KeySuccess], "the completion reports the failure")
				require.Contains(t, err.Error(), completed[0].Data[annotations.KeyError], "the completion carries the failure")
			})

			t.Run("Analyze collects the completion", func(t *testing.T) {
				reset()
				analyzed, err := d.Analyze(text)
				require.NoError(t, err)
				var completed []annotations.Event
				for _, e := range analyzed.Events {
					if e.Name == annotations.QueryComplete {
						completed = append(completed, e)
					}
				}
				require.Len(t, completed, 1, "Analyze collects the query's one completion")
				require.Equal(t, true, completed[0].Data[annotations.KeySuccess])
				require.Equal(t, 3, completed[0].Data[annotations.KeyTuplesCount], "the completion carries the tuples Analyze drained")
			})

			t.Run("a drain that ends in a read fault", func(t *testing.T) {
				var faultMu sync.Mutex
				var faultEvents []annotations.Event
				faulting, _, _ := openInjectedFaultDatabase(t, mode, 1, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
					faultMu.Lock()
					faultEvents = append(faultEvents, e)
					faultMu.Unlock()
				}})
				rel, err := faulting.Query(`[:find ?v :where [?e :doc/blob ?v]]`)
				require.NoError(t, err)
				it := rel.Iterator()
				require.True(t, it.Next(), "the scan yields one datom before it fails")
				require.False(t, it.Next())
				require.ErrorIs(t, it.Error(), errInjectedScanFault)
				faultMu.Lock()
				defer faultMu.Unlock()
				var completed []annotations.Event
				for _, e := range faultEvents {
					if e.Name == annotations.QueryComplete {
						completed = append(completed, e)
					}
				}
				require.Len(t, completed, 1, "the query is reported completed once")
				require.Equal(t, false, completed[0].Data[annotations.KeySuccess], "the completion reports the fault")
				require.Contains(t, completed[0].Data[annotations.KeyError], errInjectedScanFault.Error(), "the completion carries the fault")
				require.Equal(t, 1, completed[0].Data[annotations.KeyTuplesCount], "the completion carries the tuples drained before the fault")
			})
		})
	}
}
