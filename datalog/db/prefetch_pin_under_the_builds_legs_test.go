package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPrefetchFillsTheCacheOnTheQueryRoute asserts prefetch's mechanism with
// the option on and its absence with it off, each pin kept through legsWhere
// over legsFor of prefetch and fusion. On every build that loop holds a leg
// with prefetch on (CLAUDE.md item 6: under js/wasm a mechanism test still
// runs on a leg that holds its pin).
func TestThePrefetchRouteLoopHoldsAPrefetchLegOnThisBuild(t *testing.T) {
	var names []string
	held := false
	legs := legsFor(t, optimizerModes, prefetchOption, fusionOption)
	for _, leg := range append(legsWhere(legs, func(m optimizerMode) bool { return m.prefetch }), legsWhere(legs, func(m optimizerMode) bool { return !m.prefetch })...) {
		names = append(names, leg.name)
		held = held || leg.prefetch
	}
	require.True(t, held, "the legs %v hold no leg with prefetch on, so the mechanism's assertions run on none", names)
}
