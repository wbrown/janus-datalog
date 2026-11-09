package tests

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// A test's reach is the options whose events its queries fire over the legs
// it runs. legsFor gives a test the legs its reach needs.
var reachReport = flag.String("reach", "", "the file to write each test's reach to when the run ends, every leg run and the options whose events its queries fired recorded under the test")

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

// TestMain runs the package's tests and, under -reach, writes the record of
// each test's reach when they end.
func TestMain(m *testing.M) {
	code := m.Run()
	if *reachReport != "" {
		if err := writeReachReport(*reachReport); err != nil {
			fmt.Fprintf(os.Stderr, "write the reach report: %v\n", err)
			os.Exit(1)
		}
	}
	os.Exit(code)
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
