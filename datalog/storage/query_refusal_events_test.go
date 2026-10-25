package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// A handler sees every query the database's Query path is asked, once each:
// the invocation with the query as received and, when the query is refused,
// a completion carrying the refusal, whether the refusal is raised before the
// executor's entry or at it. A refusal before the executor's entry reports no
// preparation and no plan, since neither ran.
func TestQuery_ARefusalBeforeTheExecutorsEntryReachesTheHandler(t *testing.T) {
	facts := []fact{{named("e1"), ":t/n", int64(1)}}
	refusals := []struct {
		name, text, refusal string
		inputs              []interface{}
	}{
		{"fewer inputs than the :in consumes", `[:find ?e :in $ ?v :where [?e :t/n ?v]]`, "not enough inputs", nil},
		{"more inputs than the :in consumes", `[:find ?e :where [?e :t/n 1]]`, "too many inputs", []interface{}{int64(1)}},
		{"a value of the wrong shape for a collection input", `[:find ?e :in $ [?v ...] :where [?e :t/n ?v]]`, "expected slice or array for collection input", []interface{}{int64(1)}},
		{"a declared source the call does not pass", `[:find ?e :in $ $other :where [$other ?e :t/n _]]`, "declared in :in clause but not provided", nil},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var mu sync.Mutex
			var events []annotations.Event
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				mu.Lock()
				events = append(events, e)
				mu.Unlock()
			}})
			commitFacts(t, d, facts)
			reset := func() {
				mu.Lock()
				events = nil
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
			requireReported := func(t *testing.T, text string, err error) {
				rendered, perr := parser.ParseQuery(text)
				require.NoError(t, perr)
				invoked := eventsNamed(annotations.QueryInvoked)
				require.Len(t, invoked, 1, "the query is reported invoked once")
				require.Equal(t, rendered.String(), invoked[0].Data[annotations.KeyQuery], "the invocation carries the query as received")
				completed := eventsNamed(annotations.QueryComplete)
				require.Len(t, completed, 1, "the query is reported completed once")
				require.Equal(t, false, completed[0].Data[annotations.KeySuccess], "the completion reports the refusal")
				require.Equal(t, err.Error(), completed[0].Data[annotations.KeyError], "the completion carries the refusal")
				require.Empty(t, eventsNamed(annotations.QueryPrepared), "no preparation is reported, since none ran")
				require.Empty(t, eventsNamed(annotations.QueryPlanCreated), "no plan is reported, since none was made")
			}
			for _, c := range refusals {
				t.Run(c.name, func(t *testing.T) {
					reset()
					_, err := d.Query(c.text, c.inputs...)
					require.ErrorContains(t, err, c.refusal)
					requireReported(t, c.text, err)
				})
			}
			t.Run("the first refusal through QueryInto", func(t *testing.T) {
				reset()
				var dest []int64
				err := d.QueryInto(&dest, refusals[0].text)
				require.ErrorContains(t, err, refusals[0].refusal)
				requireReported(t, refusals[0].text, err)
			})
			t.Run("a query that runs is reported invoked once", func(t *testing.T) {
				reset()
				text := `[:find ?e :where [?e :t/n 1]]`
				tuples, err := executor.CollectTuples(d.Query(text))
				require.NoError(t, err)
				require.Len(t, tuples, 1)
				require.Len(t, eventsNamed(annotations.QueryInvoked), 1, "the query is reported invoked once")
			})
		})
	}
}
