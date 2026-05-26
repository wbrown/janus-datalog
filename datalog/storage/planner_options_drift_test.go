package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDefaultPlannerOptions_MatchesDocumentedDefaults pins the default-active vs
// opt-in contract documented in docs/reference/PLANNER_OPTIONS.md. If
// DefaultPlannerOptions() ever changes, this fails — forcing the doc to be
// updated in lockstep. Regression guard for BUG_DOCUMENTATION_OPTION_DRIFT, whose
// whole point was that the docs and the executable defaults had drifted apart.
func TestDefaultPlannerOptions_MatchesDocumentedDefaults(t *testing.T) {
	opts := DefaultPlannerOptions()

	// Default-active: documented as on by default.
	require.True(t, opts.EnableAlgebraOptimizer, "EnableAlgebraOptimizer is documented default-active")
	require.True(t, opts.EnableIteratorComposition, "EnableIteratorComposition is documented default-active")
	require.True(t, opts.EnableTrueStreaming, "EnableTrueStreaming is documented default-active")
	require.True(t, opts.EnableParallelSubqueries, "EnableParallelSubqueries is documented default-active")
	require.True(t, opts.EnableStreamingAggregation, "EnableStreamingAggregation is documented default-active")

	// Opt-in: documented as off by default (must be set explicitly).
	require.False(t, opts.EnableScanSharing, "EnableScanSharing is documented opt-in")
	require.False(t, opts.EnableEntityPrefetch, "EnableEntityPrefetch is documented opt-in")
	require.False(t, opts.UseStreamingSubqueryUnion, "UseStreamingSubqueryUnion is documented opt-in")
	require.False(t, opts.UseComponentizedSubquery, "UseComponentizedSubquery is documented opt-in")
	require.False(t, opts.EnableSymmetricHashJoin, "EnableSymmetricHashJoin is documented opt-in")
	require.False(t, opts.EnableStreamingJoins, "EnableStreamingJoins is documented opt-in")
	require.False(t, opts.EnableStreamingAggregationDebug, "EnableStreamingAggregationDebug is documented opt-in")
	require.False(t, opts.EnableDebugLogging, "EnableDebugLogging is documented opt-in")

	// Numeric defaults.
	require.Equal(t, 0, opts.MaxSubqueryWorkers, "MaxSubqueryWorkers default is 0 (= runtime.NumCPU())")
	require.Equal(t, 0, opts.IndexNestedLoopThreshold, "IndexNestedLoopThreshold default is 0 (always HashJoinScan)")
}
