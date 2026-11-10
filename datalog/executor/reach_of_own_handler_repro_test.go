package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// A test that installs its own handler on a mode's planner options, as
// TestSimpleSubquery, TestSubqueryFindClauseBugWithAnnotations and
// TestDecorrelationAnnotations do, has its reach recorded as every other
// test's is: under -reach the record under the test's name holds every option
// whose event its query fires on the legs it runs, and the legs legsFor hands
// a test of that recorded reach take every fired option at both settings.
// The query and the facts are TestSimpleSubquery's.
func TestATestWithItsOwnHandlerHasItsReachRecorded(t *testing.T) {
	aapl := datalog.NewIdentity("symbol:aapl")
	goog := datalog.NewIdentity("symbol:goog")
	price1 := datalog.NewIdentity("price:1")
	price2 := datalog.NewIdentity("price:2")
	price3 := datalog.NewIdentity("price:3")
	ticker := datalog.NewKeyword(":symbol/ticker")
	symbol := datalog.NewKeyword(":price/symbol")
	value := datalog.NewKeyword(":price/value")
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			`[:symbol/ticker "AAPL"]`: {
				{E: aapl, A: ticker, V: "AAPL", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			},
			`[:symbol/ticker "GOOG"]`: {
				{E: goog, A: ticker, V: "GOOG", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			},
			`[:price/symbol _]`: {
				{E: price1, A: symbol, V: aapl, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: price2, A: symbol, V: aapl, Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
				{E: price3, A: symbol, V: goog, Tx: datalog.ElementID{Lamport: 4, ReplicaID: 1}},
			},
			`[:price/value _]`: {
				{E: price1, A: value, V: 150.0, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: price2, A: value, V: 155.0, Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
				{E: price3, A: value, V: 2800.0, Tx: datalog.ElementID{Lamport: 4, ReplicaID: 1}},
			},
		},
	}
	q, err := parser.ParseQuery(`[:find ?symbol ?max-price
	             :where
	             [?s :symbol/ticker ?symbol]
	             [(q [:find (max ?price)
	                  :in $ ?sym
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/value ?price]]
	                 $ ?s) [[?max-price]]]]`)
	require.NoError(t, err)

	// The test's name in the record, as legsFor names it under -reach.
	const recordName = "TestATestWithItsOwnHandlerHasItsReachRecorded/measured"
	fired := map[option]bool{}
	for _, mode := range optimizerModes {
		mode.recordsReach = recordName
		opts := mode.plannerOptions()
		// The test's own handler, installed as the handler-installing tests do.
		opts.Handler = mode.recording(func(event annotations.Event) {
			if o, reached := optionReached(event); reached {
				fired[o] = true
			}
		})
		result, err := NewExecutorWithOptions(matcher, nil, opts).Execute(q)
		require.NoError(t, err)
		_, err = CollectTuples(result, nil)
		require.NoError(t, err)
	}
	require.NotEmpty(t, fired, "the query fires some option's event on the legs")

	reachRecorded.Lock()
	recorded := reachRecorded.byTest[recordName]
	reachRecorded.Unlock()
	var firedOptions, recordedOptions []option
	for _, o := range allOptions {
		if fired[o] {
			firedOptions = append(firedOptions, o)
		}
		if recorded[o] {
			recordedOptions = append(recordedOptions, o)
		}
	}
	legs := legsFor(t, recordedOptions...)
	var legNames []string
	for _, leg := range legs {
		legNames = append(legNames, leg.name)
	}
	require.Equal(t, firedOptions, recordedOptions, "the record holds every option the query fired")
	require.True(t, reachCovered(firedOptions, legs), "the legs of the recorded reach %v take every fired option %v at both settings", legNames, firedOptions)
}
