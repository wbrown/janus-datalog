// Reproduction for docs/bugs/BUG_PLANNER_OPTIONS_NOT_PROPAGATED_TO_MATCHER.md
// (executor half).
//
// PlannerOptions -> ExecutorOptions is a hand-written field-by-field copy, and
// so is the storage-side conversion. A field either converter omits compiles,
// runs, and silently leaves the caller's value at the zero default; nothing but
// a test that sets one and reads it back downstream can tell.
//
// MaxSubqueryWorkers is the probe because it is numeric: an omitted bool is
// caught only when the custom value differs from the default, while a numeric
// read back as zero is caught outright.

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
