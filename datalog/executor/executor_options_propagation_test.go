// Reproduction for docs/bugs/BUG_PLANNER_OPTIONS_NOT_PROPAGATED_TO_MATCHER.md
// (executor half).
//
// The PlannerOptions -> ExecutorOptions conversion dropped a field, so a custom
// value never reached the executor's effective options. This is the field-set
// drift the bug report flags: the storage-side and executor-side conversions
// copied different subsets.
//
// The field originally dropped was IndexNestedLoopThreshold, removed with the
// iterator-reuse strategy. MaxSubqueryWorkers stands in for it: the same
// hazard, a non-boolean the hand-written converter must copy and can silently
// omit.

package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestNewExecutorWithOptions_HonorsNumericOption pins that a custom non-boolean
// option survives the PlannerOptions -> ExecutorOptions conversion into the
// executor's effective options.
func TestNewExecutorWithOptions_HonorsNumericOption(t *testing.T) {
	opts := planner.PlannerOptions{
		MaxSubqueryWorkers: 12345,
		EnableScanSharing:  true,
	}
	exec := NewExecutorWithOptions(nil, nil, opts)

	require.Equal(t, 12345, exec.options.MaxSubqueryWorkers,
		"custom MaxSubqueryWorkers must reach the executor's effective options")
	require.True(t, exec.options.EnableScanSharing,
		"sanity: a field the converter already copied must still be honored")
}
