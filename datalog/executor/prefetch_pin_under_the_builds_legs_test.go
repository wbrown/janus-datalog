package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// On every build the loop of
// TestAnOrDefaultBranchBuiltFromTheCacheReadsItsAttributeThroughALookup holds
// a leg with prefetch on (CLAUDE.md, "Testing Prefetch", and item 6: under
// js/wasm a mechanism test still runs on a leg that holds its pin).
func TestTheExecutorPrefetchLoopHoldsAPrefetchLegOnThisBuild(t *testing.T) {
	var names []string
	held := false
	for _, leg := range executor.OptimizerModes {
		names = append(names, leg.Name())
		held = held || leg.Prefetch()
	}
	require.True(t, held, "the legs %v hold no leg with prefetch on, so the mechanism's assertions run on none", names)
}
