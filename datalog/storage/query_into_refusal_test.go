package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// A query QueryInto ran and whose destination it then refuses is reported
// completed once, and the completion reports the result closed before any
// tuple was drained: success, zero tuples. The refusal is QueryInto's error,
// not the query's.
func TestQueryInto_ARefusedDestinationEndsTheResultBeforeAnyTuple(t *testing.T) {
	type taggedWithAnAbsentSymbol struct {
		E datalog.Identity `datalog:"?e"`
		M int64            `datalog:"?m"`
	}
	text := `[:find ?e ?n :where [?e :t/n ?n]]`
	var structs []taggedWithAnAbsentSymbol
	var scalars []int64
	cases := []struct {
		name   string
		invoke func(d *Database) error
		names  string
	}{
		{"a struct tagged with a symbol the find lacks", func(d *Database) error { return d.QueryInto(&structs, text) }, "?m"},
		{"a scalar destination for two find elements", func(d *Database) error { return d.QueryInto(&scalars, text) }, "exactly 1 find element"},
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
			commitQueryEntryFixture(t, d)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					mu.Lock()
					events = nil
					mu.Unlock()
					err := c.invoke(d)
					require.ErrorContains(t, err, c.names, "the refusal is QueryInto's error")
					mu.Lock()
					defer mu.Unlock()
					var completed []annotations.Event
					for _, e := range events {
						if e.Name == annotations.QueryComplete {
							completed = append(completed, e)
						}
					}
					require.Len(t, completed, 1, "the query is reported completed once")
					require.Equal(t, true, completed[0].Data[annotations.KeySuccess], "the result was closed, not failed")
					require.Equal(t, 0, completed[0].Data[annotations.KeyTuplesCount], "the result was closed before any tuple was drained")
				})
			}
		})
	}
}
