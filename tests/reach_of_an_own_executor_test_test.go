package tests

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestTupleGroundOrFallback builds its executor and matcher from
// mode.plannerOptions() under a handler of its own. Its query applies the
// algebra optimizer's decorrelation (algebra/decorrelate-apply) on every leg
// with the algebra optimizer on, and selects streaming aggregation on every
// leg with it on. CLAUDE.md item 6: "A test that installs a handler of its own
// on the options installs it through the mode's recording ... so its reach is
// recorded as every other test's." Under -reach the record under the test's
// name holds the options its query fires, so legsFor runs it with the
// algebra optimizer off as well as on.
func TestAnOwnHandlerTestInTestsHasItsReachRecorded(t *testing.T) {
	saved := *reachReport
	*reachReport = filepath.Join(t.TempDir(), "reach.txt")
	t.Cleanup(func() { *reachReport = saved })

	backend := engineBackend(t)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			testTupleGroundOrFallback(t, createBackendModeDB(t, backend, mode, storage.DatabaseOptions{}), mode)
		})
	}
	reachRecorded.Lock()
	recorded := reachRecorded.byTest[t.Name()]
	reachRecorded.Unlock()
	require.True(t, recorded[algebraOption], "the record holds the algebra optimizer, whose decorrelation the query applies: %v", recorded)
	require.True(t, recorded[aggregationOption], "the record holds streaming aggregation, which the query selects: %v", recorded)
}
