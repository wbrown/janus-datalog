package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A test of prefetch's own mechanism asserts it on the prefetch-on legs it
// runs (CLAUDE.md, "Testing Prefetch"), and under js/wasm a mechanism test
// still runs on a leg that holds its pin (CLAUDE.md item 6). Each loop below
// is a prefetch mechanism test's loop as written in this package, its two
// pins each kept through legsWhere: TestFallbackPrefetchAnswersAsTheScanPath,
// TestFallbackPrefetch_AnswersAsTheScanPathAndFires and
// TestPrefetchFillsTheCacheOnTheQueryRouteAsOnTheExecutorRoute over legsFor
// of prefetch and fusion, TestFallbackPrefetchAnswersAsTheScanPathForARequiredValue
// over legsFor of prefetch alone, and TestFallbackServesThePrefetchedWindowFromTheCache
// and TestFallbackServesAnAttributeTheSchemaDoesNotDefineFromTheCache with
// fusion off on both pins. On every build, each loop holds a leg with prefetch
// on.
func TestEachPrefetchMechanismLoopHoldsAPrefetchLegOnThisBuild(t *testing.T) {
	both := legsFor(optimizerModes, prefetchOption, fusionOption)
	one := legsFor(optimizerModes, prefetchOption)
	loops := []struct {
		name string
		legs []optimizerMode
	}{
		{"prefetch on then off over legsFor prefetch fusion", append(legsWhere(both, func(m optimizerMode) bool { return m.prefetch }), legsWhere(both, func(m optimizerMode) bool { return !m.prefetch })...)},
		{"prefetch on then off over legsFor prefetch", append(legsWhere(one, func(m optimizerMode) bool { return m.prefetch }), legsWhere(one, func(m optimizerMode) bool { return !m.prefetch })...)},
		{"prefetch on then off, fusion off, over legsFor prefetch fusion", append(legsWhere(both, func(m optimizerMode) bool { return m.prefetch && !m.fusion }), legsWhere(both, func(m optimizerMode) bool { return !m.prefetch && !m.fusion })...)},
	}
	for _, loop := range loops {
		var names []string
		held := false
		for _, leg := range loop.legs {
			names = append(names, leg.name)
			held = held || leg.prefetch
		}
		require.True(t, held, "%s: the legs %v hold no leg with prefetch on, so the mechanism's assertions run on none", loop.name, names)
	}
}
