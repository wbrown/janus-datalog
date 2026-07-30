// Reproduction for BUG_PLANNER_OPTIONS_NOT_PROPAGATED_TO_MATCHER.md
//
// Database.Query builds the executor from the database's effective
// PlannerOptions, but Database.Matcher() built its PatternMatcher from
// DefaultPlannerOptions(). Because a matcher stamps its ExecutorOptions onto
// every StreamingRelation/MaterializedRelation it returns, a custom-options
// query ran with the executor seeing the custom options while every
// storage-produced relation carried defaults — configuration drift inside a
// single query.

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// getDefaultExecutorOptions returns the default executor options for testing
func getDefaultExecutorOptions() executor.ExecutorOptions {
	opts := DefaultPlannerOptions()
	return executor.ExecutorOptions{
		EnableTrueStreaming:     opts.EnableTrueStreaming,
		EnableStreamingJoins:    opts.EnableStreamingJoins,
		EnableSymmetricHashJoin: opts.EnableSymmetricHashJoin,
		DefaultHashTableSize:    256,
	}
}

// TestDatabaseMatcher_HonorsCustomPlannerOptions asserts that relations
// produced by the default-source matcher carry the database's effective
// options, not DefaultPlannerOptions(). It exercises both drift directions:
// a field Matcher() copied but from the wrong source (EnableTrueStreaming) and
// fields Matcher() dropped entirely (EnableScanSharing, EnableEntityPrefetch).
func TestDatabaseMatcher_HonorsCustomPlannerOptions(t *testing.T) {
	custom := DefaultPlannerOptions()
	custom.EnableTrueStreaming = false // default true
	custom.EnableScanSharing = true    // default false; dropped by Matcher() pre-fix
	custom.EnableEntityPrefetch = true // default false; dropped by Matcher() pre-fix
	// A numeric field as well as booleans: a dropped bool is caught only when
	// the custom value differs from the default, while a numeric read back as
	// zero is caught outright.
	custom.MaxSubqueryWorkers = 7 // default 0, meaning 4

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           t.TempDir(),
		ReplicaID:      1,
		PlannerOptions: &custom,
	})
	require.NoError(t, err)
	defer db.Close()

	m := db.Matcher().(*PatternMatcher)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":test/attr")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
	rel, err := m.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	opts := rel.Options()
	require.False(t, opts.EnableTrueStreaming,
		"custom EnableTrueStreaming=false must reach matcher-produced relations")
	require.True(t, opts.EnableScanSharing,
		"custom EnableScanSharing=true must reach matcher-produced relations")
	require.True(t, opts.EnableEntityPrefetch,
		"custom EnableEntityPrefetch=true must reach matcher-produced relations")
	require.Equal(t, 7, opts.MaxSubqueryWorkers,
		"a custom numeric option must reach matcher-produced relations with its value intact")
}
