// Reproduction for docs/bugs/BUG_PLANNER_OPTIONS_NOT_PROPAGATED_TO_MATCHER.md
// (executor half).
//
// The PlannerOptions -> ExecutorOptions conversion dropped
// IndexNestedLoopThreshold, so even a custom threshold never reached the
// executor's effective options. This is the field-set drift the bug report
// flags: the storage-side and executor-side conversions copied different
// subsets, and IndexNestedLoopThreshold was absent here.

package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestNewExecutorWithOptions_HonorsIndexNestedLoopThreshold pins that a custom
// IndexNestedLoopThreshold survives the PlannerOptions -> ExecutorOptions
// conversion into the executor's effective options.
func TestNewExecutorWithOptions_HonorsIndexNestedLoopThreshold(t *testing.T) {
	opts := planner.PlannerOptions{
		IndexNestedLoopThreshold: 12345,
		EnableScanSharing:        true,
	}
	exec := NewExecutorWithOptions(nil, nil, opts)

	require.Equal(t, 12345, exec.options.IndexNestedLoopThreshold,
		"custom IndexNestedLoopThreshold must reach the executor's effective options")
	require.True(t, exec.options.EnableScanSharing,
		"sanity: a field the converter already copied must still be honored")
}
