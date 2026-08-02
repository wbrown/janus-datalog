package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestAbortedScanReportsThatItDidNotFinish pins what a *-complete event asserts.
//
// It fires either way. An arm that returns its completion only on success makes
// a failed scan indistinguishable from one that never ran, and leaves the reads
// it performed unaccounted — the reads a trace most needs, since they bought
// nothing. So the event carries whether the scan finished, and its funnel is
// read as a truncation when it did not.
//
// The counts are the load-bearing half. An aborted scan reporting no counts
// would be the same silence in a different shape.
func TestAbortedScanReportsThatItDidNotFinish(t *testing.T) {
	tag := datalog.NewKeyword(":person/tag")

	s, err := schema.NewBuilder().
		Attribute(":person/tag").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)

	injected := fmt.Errorf("simulated AEVT scan failure")
	for _, mode := range optimizerModes {
		for name, failing := range scanFailureShapes(injected) {
			t.Run(mode.name+"/"+name, func(t *testing.T) {
				// The matcher below is the thing under test and is built with the
				// handler directly; the database only supplies the store and fixture.
				var events []annotations.Event
				handler := func(ev annotations.Event) { events = append(events, ev) }

				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})

				e := datalog.NewIdentity("abort:alice")
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(e, tag, "dev"))
				_, err = tx.Commit()
				require.NoError(t, err)

				// The unbound cardinality-many arm scans AEVT to find entities, so
				// overriding that index aborts it partway through Next rather than
				// before it opens.
				matcher := NewPatternMatcherWithOptions(&indexScanOverrideStore{
					Store: db.Store(),
					index: AEVT,
					iter:  failing,
				}, executor.ExecutorOptions{Handler: handler})
				matcher.SetSchema(s)

				pattern := &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Constant{Value: tag},
						query.Variable{Name: datalog.NewSymbol("?v")},
					},
				}
				rel, err := matcher.MatchWithConstraints(query.PatternQuery(pattern), nil, nil)
				require.NoError(t, err, "the failure is deferred to iteration, not to Match")

				_, collectErr := executor.CollectTuples(rel, nil)
				require.Error(t, collectErr, "the scan failed and the failure must reach the caller")

				complete := lastEventNamed(events, annotations.StorageScanComplete)
				require.NotNil(t, complete,
					"a scan that aborted still ran, and an absent event is what makes "+
						"that indistinguishable from one that never opened")
				require.Equal(t, false, complete.Data[annotations.KeySuccess],
					"the scan did not finish, and the funnel below is what it got to")
				require.Contains(t, complete.Data, annotations.KeyDatomsScanned,
					"an aborted scan reports what it read; reporting nothing is the "+
						"same silence in a different shape")
			})
		}
	}
}

// TestCompletedScanSaysSo is the other half: without it, an arm could hardcode
// the aborted answer and the test above would still pass.
func TestCompletedScanSaysSo(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			testCompletedScanSaysSo(t, mode)
		})
	}
}

func testCompletedScanSaysSo(t *testing.T, mode optimizerMode) {
	// Registered at open. The assertion takes the last scan-complete event, so
	// the fixture's own events ahead of the query are harmless.
	var events []annotations.Event
	db := createOptimizerModeDB(t, mode, DatabaseOptions{
		Schema:            funnelSchema(t),
		AnnotationHandler: func(ev annotations.Event) { events = append(events, ev) },
	})

	funnelFixture(t, db)

	result, err := db.Query(`[:find ?e ?v :where [?e :person/tag ?v]]`)
	require.NoError(t, err)
	_, err = executor.CollectTuples(result, nil)
	require.NoError(t, err)

	complete := lastEventNamed(events, annotations.StorageScanComplete)
	require.NotNil(t, complete)
	require.Equal(t, true, complete.Data[annotations.KeySuccess],
		"the scan ran to the end, so its funnel is a total")
}
