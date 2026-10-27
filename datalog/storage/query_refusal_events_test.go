package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// A handler sees every query the database is asked invoked once and
// completed once, whatever refuses it. A query that does not resolve, text
// that does not parse or a value that is neither text nor a query, is
// reported invoked with the value as received and completed with the error,
// through Query, QueryInto, QueryOneInto and an AsOf handle's Query. A query
// the realized plan's validation refuses is reported invoked and completed
// with the error the same way, through Query, QueryInto and a History
// handle's Query.
func TestQuery_ARefusalReachesTheHandlerAsACompletedQuery(t *testing.T) {
	unparsable := `[:find ?e :where [?e :t/n`
	unrealizable := `[:find ?x :where [?e :t/n 1]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var mu sync.Mutex
			var events []annotations.Event
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				mu.Lock()
				events = append(events, e)
				mu.Unlock()
			}})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("e1"), datalog.NewKeyword(":t/n"), int64(1)))
			asOf, err := tx.Commit()
			require.NoError(t, err)
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

			var many []datalog.Identity
			var one datalog.Identity
			routes := []struct {
				name    string
				invoke  func() error
				carries interface{}
				names   string
			}{
				{"Query given text that does not parse",
					func() error { _, err := d.Query(unparsable); return err }, unparsable, "failed to resolve query"},
				{"Query given a value that is neither text nor a query",
					func() error { _, err := d.Query(42); return err }, 42, "failed to resolve query"},
				{"QueryInto given text that does not parse",
					func() error { return d.QueryInto(&many, unparsable) }, unparsable, "failed to resolve query"},
				{"QueryOneInto given text that does not parse",
					func() error { _, err := d.QueryOneInto(&one, unparsable); return err }, unparsable, "failed to resolve query"},
				{"an AsOf handle's Query given text that does not parse",
					func() error { _, err := d.AsOf(asOf).Query(unparsable); return err }, unparsable, "failed to resolve query"},
				{"Query given a query the realized plan refuses",
					func() error { _, err := d.Query(unrealizable); return err }, nil, "?x"},
				{"QueryInto given a query the realized plan refuses",
					func() error { return d.QueryInto(&many, unrealizable) }, nil, "?x"},
				{"a History handle's Query given a query the realized plan refuses",
					func() error { _, err := d.History().Query(unrealizable); return err }, nil, "?x"},
			}
			for _, r := range routes {
				t.Run(r.name, func(t *testing.T) {
					mu.Lock()
					events = nil
					mu.Unlock()
					err := r.invoke()
					require.ErrorContains(t, err, r.names)
					invoked := eventsNamed(annotations.QueryInvoked)
					require.Len(t, invoked, 1, "the query is reported invoked once")
					if r.carries != nil {
						require.Equal(t, r.carries, invoked[0].Data[annotations.KeyQuery], "the invocation carries the query as received")
					}
					completed := eventsNamed(annotations.QueryComplete)
					require.Len(t, completed, 1, "the query is reported completed once")
					require.Equal(t, false, completed[0].Data[annotations.KeySuccess], "the completion reports the refusal")
					require.Contains(t, err.Error(), completed[0].Data[annotations.KeyError], "the completion carries the refusal")
				})
			}
		})
	}
}
