package db_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A test that installs its own handler at open, as
// TestPrefetchFillsTheCacheOnTheQueryRoute does, has its reach recorded as
// every other test's is: under -reach the record under the test's name holds
// every option whose event its query fires on the legs it runs, and the legs
// legsFor hands a test of that recorded reach take every fired option at both
// settings. The query and the facts are TestPrefetchFillsTheCacheOnTheQueryRoute's.
func TestADbTestWithItsOwnHandlerHasItsReachRecorded(t *testing.T) {
	const subjectSize = 120
	n, a, x := datalog.NewKeyword(":t/n"), datalog.NewKeyword(":t/a"), datalog.NewKeyword(":t/x")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: n, ValueType: schema.TypeLong, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: a, ValueType: schema.TypeLong, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: x, ValueType: schema.TypeLong, Cardinality: schema.CardinalityOne})
	const text = `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])]`

	// The test's name in the record, as legsFor names it under -reach.
	const recordName = "TestADbTestWithItsOwnHandlerHasItsReachRecorded/measured"
	var mu sync.Mutex
	fired := map[option]bool{}
	for _, mode := range optimizerModes {
		mode.recordsReach = recordName
		opts := mode.plannerOptions()
		opts.ScopeWindowCap = 128
		// The test's own handler, installed at open as the handler-installing
		// tests do.
		handler := func(event annotations.Event) {
			if o, reached := optionReached(event); reached {
				mu.Lock()
				fired[o] = true
				mu.Unlock()
			}
		}
		d, err := db.OpenMemory(db.WithSchema(s), db.WithPlannerOptions(opts), db.WithAnnotationHandler(mode.recording(handler)))
		require.NoError(t, err)
		tx := d.NewTransaction()
		for i := 1; i <= subjectSize; i++ {
			e := datalog.NewIdentity(fmt.Sprintf("e%d", i))
			require.NoError(t, tx.Add(e, n, int64(i)))
			require.NoError(t, tx.Add(e, a, int64(i)))
			require.NoError(t, tx.Add(e, x, int64(i)))
		}
		_, err = tx.Commit()
		require.NoError(t, err)
		_, err = executor.CollectTuples(d.Query(text))
		require.NoError(t, err)
		require.NoError(t, d.Close())
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
	legs := legsFor(t, optimizerModes, recordedOptions...)
	var legNames []string
	var legSettings []optionLeg
	for _, leg := range legs {
		legNames = append(legNames, leg.name)
		legSettings = append(legSettings, leg.optionLeg)
	}
	require.Equal(t, firedOptions, recordedOptions, "the record holds every option the query fired")
	require.True(t, reachCovered(firedOptions, legSettings), "the legs of the recorded reach %v take every fired option %v at both settings", legNames, firedOptions)
}
