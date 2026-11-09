package storage

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// The reach report. A test's reach is the options whose events its queries
// fire over the legs it runs, and it decides the legs legsFor gives the test.
// Under -reach=<path>, every database the harness opens records, under the
// top-level test's name, the option each event it emits reaches, and
// TestMain writes the record to the path when the run ends: one line per
// test, the test's name, a tab, and the options reached in a leg's name
// order, or "none". A leg is recorded as it runs, and an option a leg turns
// off fires nothing there, so the record over the legs a test runs is its
// reach. An option's event can depend on another option being off, as the
// symmetric join's does on scan sharing, which no one leg with every option
// on would show.
var reachReport = flag.String("reach", "", "write each test's reach, the options whose events its queries fire, to this file")

// reachRecorded is each top-level test's reach as the run records it.
var reachRecorded = struct {
	sync.Mutex
	byTest map[string]map[option]bool
}{byTest: map[string]map[option]bool{}}

// optionReached is the option an event's firing reaches, where it reaches
// one: a fused fetch or constraint reaches fusion, a shared scan's hit or
// miss reaches sharing, a prefetch trigger reaches prefetch, an applied
// decorrelation or get-else scan rewrite reaches the algebra optimizer, a
// join whose selected mode is the symmetric join reaches the symmetric hash
// join, and an aggregation whose selected strategy is streaming reaches
// streaming aggregation.
func optionReached(event annotations.Event) (option, bool) {
	switch event.Name {
	case annotations.PatternFusedFetch, annotations.PatternFusedConstraint:
		return fusionOption, true
	case annotations.ScanSharingCacheHit, annotations.ScanSharingCacheMiss:
		return sharingOption, true
	case annotations.PrefetchTrigger:
		return prefetchOption, true
	case annotations.AlgebraDecorrelateApply, annotations.AlgebraGetElseScanApply:
		return algebraOption, true
	case annotations.JoinStrategy:
		if event.Data["mode"] == "symmetric" {
			return symmetricOption, true
		}
	case annotations.AggregationStrategy:
		if event.Data["strategy"] == "streaming" {
			return aggregationOption, true
		}
	}
	return 0, false
}

// recordingReach is handler with the option every event it receives reaches
// recorded under t's top-level test, which enters the record whether or not
// an event fires. With no report to write it is handler itself.
func recordingReach(t testing.TB, handler annotations.Handler) annotations.Handler {
	if *reachReport == "" {
		return handler
	}
	test, _, _ := strings.Cut(t.Name(), "/")
	reachRecorded.Lock()
	if reachRecorded.byTest[test] == nil {
		reachRecorded.byTest[test] = map[option]bool{}
	}
	reachRecorded.Unlock()
	return func(event annotations.Event) {
		if o, reached := optionReached(event); reached {
			reachRecorded.Lock()
			reachRecorded.byTest[test][o] = true
			reachRecorded.Unlock()
		}
		if handler != nil {
			handler(event)
		}
	}
}

// renderReachReport is the report's text: one line per test in name order,
// the test's name, a tab, and the options reached in a leg's name order, or
// "none".
func renderReachReport(byTest map[string]map[option]bool) string {
	tests := make([]string, 0, len(byTest))
	for test := range byTest {
		tests = append(tests, test)
	}
	sort.Strings(tests)
	var report strings.Builder
	for _, test := range tests {
		var reached []string
		for _, o := range allOptions {
			if byTest[test][o] {
				reached = append(reached, o.String())
			}
		}
		line := strings.Join(reached, " ")
		if line == "" {
			line = "none"
		}
		fmt.Fprintf(&report, "%s\t%s\n", test, line)
	}
	return report.String()
}

// writeReachReport writes the record to path.
func writeReachReport(path string) error {
	reachRecorded.Lock()
	defer reachRecorded.Unlock()
	return os.WriteFile(path, []byte(renderReachReport(reachRecorded.byTest)), 0o644)
}

// seedReachFixture writes the facts the reach shapes read: two hundred
// entities of one kind, each carrying a value, ten of them a second value
// one above another's, and each with two children naming it as parent.
func seedReachFixture(t *testing.T, d *Database) {
	t.Helper()
	kind := datalog.NewKeyword(":reach/kind")
	v := datalog.NewKeyword(":reach/v")
	w := datalog.NewKeyword(":reach/w")
	parent := datalog.NewKeyword(":reach/parent")
	tx := d.NewTransaction()
	for i := 0; i < 200; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("reach:e%d", i))
		require.NoError(t, tx.Add(entity, kind, "thing"))
		require.NoError(t, tx.Add(entity, v, int64(i)))
		if i < 10 {
			require.NoError(t, tx.Add(entity, w, int64(i+1)))
		}
		for c := 0; c < 2; c++ {
			child := datalog.NewIdentity(fmt.Sprintf("reach:e%d:c%d", i, c))
			require.NoError(t, tx.Add(child, parent, entity))
		}
	}
	_, err := tx.Commit()
	require.NoError(t, err)
}

// Each option has a query shape whose event reaches it: on a leg turning the
// option on the shape's query fires the option's event, and on a leg turning
// it off the event is not fired. So the options a test's shapes reach are the
// options the record holds for it.
func TestEachOptionsShapeReachesIt(t *testing.T) {
	shapes := []struct {
		option option
		query  string
	}{
		{algebraOption, `[:find ?e ?c :where [?e :reach/kind "thing"] [(q [:find (count ?x) :in $ ?e :where [?x :reach/parent ?e]] $ ?e) [[?c]]]]`},
		{sharingOption, `[:find ?e :where [?e :reach/kind _] [?z :reach/kind _]]`},
		{prefetchOption, `[:find ?e ?v :where [?e :reach/kind "thing"] (or-default-join [[?e] ?v] [?e :reach/v ?v] [(ground 0) ?v])]`},
		{symmetricOption, `[:find ?q ?p :where [?q :reach/v ?a] [(+ ?a 1) ?b] [?p :reach/w ?c] [(identity ?c) ?b]]`},
		{fusionOption, `[:find ?e ?v :where [?e :reach/kind "thing"] [?e :reach/v ?v]]`},
		{aggregationOption, `[:find (count ?e) :where [?e :reach/kind "thing"]]`},
	}
	for _, shape := range shapes {
		t.Run(shape.option.String(), func(t *testing.T) {
			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					reached := map[option]bool{}
					d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(event annotations.Event) {
						if o, ok := optionReached(event); ok {
							reached[o] = true
						}
					}})
					seedReachFixture(t, d)
					_, err := executor.CollectTuples(d.Query(shape.query))
					require.NoError(t, err)
					require.Equal(t, shape.option.on(mode.optionLeg), reached[shape.option], "the shape fires %s's event where the leg turns it on", shape.option)
				})
			}
		})
	}
}

// The report lists each recorded test in name order with the options it
// reached in a leg's name order, and a test that reached none as "none".
func TestTheReachReportListsEachTestsOptions(t *testing.T) {
	record := map[string]map[option]bool{
		"TestWide":   {fusionOption: true, algebraOption: true, prefetchOption: true},
		"TestNarrow": {},
		"TestOne":    {sharingOption: true},
	}
	require.Equal(t, "TestNarrow\tnone\nTestOne\tsharing\nTestWide\talgebra prefetch fusion\n", renderReachReport(record))
}
