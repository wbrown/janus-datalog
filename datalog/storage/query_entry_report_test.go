package storage

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

// queryEntryRoute is one caller route into the database's query entry, run
// on the database and the as-of target the fixture committed.
type queryEntryRoute struct {
	name   string
	invoke func(d *Database, asOf datalog.ElementID) error
}

// commitQueryEntryFixture commits the fixture's facts in order and returns
// the transaction's element, the target of an AsOf handle.
func commitQueryEntryFixture(t *testing.T, d *Database) datalog.ElementID {
	e1 := datalog.NewIdentity("e1")
	e2 := datalog.NewIdentity("e2")
	facts := []struct {
		e datalog.Identity
		a string
		v interface{}
	}{
		{e1, ":name", "e1"},
		{e1, ":t/n", int64(1)},
		{e2, ":name", "e2"},
		{e2, ":t/n", int64(2)},
	}
	tx := d.NewTransaction()
	for _, f := range facts {
		require.NoError(t, tx.Add(f.e, datalog.NewKeyword(f.a), f.v))
	}
	asOf, err := tx.Commit()
	require.NoError(t, err)
	return asOf
}

// A value that is neither text nor a query is refused with the resolve
// error, on every route into the query entry, under the formatter the
// annotations package ships as the renderer of every event.
func TestQuery_AValueThatIsNotAQueryIsRefusedUnderTheShippedFormatter(t *testing.T) {
	var many []datalog.Identity
	var one datalog.Identity
	routes := []queryEntryRoute{
		{"Query given an integer", func(d *Database, _ datalog.ElementID) error { _, err := d.Query(42); return err }},
		{"Query given nil", func(d *Database, _ datalog.ElementID) error { _, err := d.Query(nil); return err }},
		{"Query given a query builder it was not built from", func(d *Database, _ datalog.ElementID) error {
			e := qb.NewVar("e")
			_, err := d.Query(qb.Query().Find(e).Where(qb.Pat(e, qb.Kw(":t/n"), qb.Blank())))
			return err
		}},
		{"QueryInto given an integer", func(d *Database, _ datalog.ElementID) error { return d.QueryInto(&many, 42) }},
		{"QueryOneInto given an integer", func(d *Database, _ datalog.ElementID) error { _, err := d.QueryOneInto(&one, 42); return err }},
		{"an AsOf handle's Query given an integer", func(d *Database, asOf datalog.ElementID) error { _, err := d.AsOf(asOf).Query(42); return err }},
		{"a History handle's Query given an integer", func(d *Database, _ datalog.ElementID) error { _, err := d.History().Query(42); return err }},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var rendered bytes.Buffer
			formatter := annotations.NewOutputFormatter(&rendered)
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: formatter.Handle})
			asOf := commitQueryEntryFixture(t, d)
			for _, r := range routes {
				t.Run(r.name, func(t *testing.T) {
					var err error
					require.NotPanics(t, func() { err = r.invoke(d, asOf) }, "the refusal reaches the caller as an error")
					require.ErrorContains(t, err, "failed to resolve query")
				})
			}
		})
	}
}

// A query whose context is done is refused at entry, and the refusal is
// reported as every refusal is: invoked once and completed once with the
// context's error, on every route into the query entry.
func TestQuery_AQueryWhoseContextIsDoneReachesTheHandlerAsACompletedQuery(t *testing.T) {
	text := `[:find ?e :where [?e :t/n _]]`
	done, cancel := context.WithCancel(context.Background())
	cancel()
	var many []datalog.Identity
	var one datalog.Identity
	routes := []queryEntryRoute{
		{"Query", func(d *Database, _ datalog.ElementID) error { _, err := d.Query(text, WithContext(done)); return err }},
		{"QueryInto", func(d *Database, _ datalog.ElementID) error { return d.QueryInto(&many, text, WithContext(done)) }},
		{"QueryOneInto", func(d *Database, _ datalog.ElementID) error {
			_, err := d.QueryOneInto(&one, text, WithContext(done))
			return err
		}},
		{"an AsOf handle's Query", func(d *Database, asOf datalog.ElementID) error {
			_, err := d.AsOf(asOf).Query(text, WithContext(done))
			return err
		}},
		{"a History handle's Query", func(d *Database, _ datalog.ElementID) error {
			_, err := d.History().Query(text, WithContext(done))
			return err
		}},
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
			asOf := commitQueryEntryFixture(t, d)
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
			for _, r := range routes {
				t.Run(r.name, func(t *testing.T) {
					mu.Lock()
					events = nil
					mu.Unlock()
					err := r.invoke(d, asOf)
					require.ErrorIs(t, err, context.Canceled)
					require.Len(t, eventsNamed(annotations.QueryInvoked), 1, "the query is reported invoked once")
					completed := eventsNamed(annotations.QueryComplete)
					require.Len(t, completed, 1, "the query is reported completed once")
					require.Equal(t, false, completed[0].Data[annotations.KeySuccess], "the completion reports the refusal")
					require.Contains(t, err.Error(), completed[0].Data[annotations.KeyError], "the completion carries the refusal")
				})
			}
		})
	}
}

// A query QueryInto ran and whose destination it then refuses is reported
// completed once, as every query is.
func TestQueryInto_ADestinationItRefusesCompletesTheQuery(t *testing.T) {
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
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					mu.Lock()
					events = nil
					mu.Unlock()
					err := c.invoke(d)
					require.ErrorContains(t, err, c.names)
					require.Len(t, eventsNamed(annotations.QueryInvoked), 1, "the query is reported invoked once")
					require.Len(t, eventsNamed(annotations.QueryComplete), 1, "the query is reported completed once")
				})
			}
		})
	}
}
