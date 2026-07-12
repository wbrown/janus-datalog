// Reproduction for docs/bugs/BUG_PLANNER_OPTIONS_NOT_PROPAGATED_TO_MATCHER.md
//
// Database.Query builds the executor from the database's effective
// PlannerOptions, but Database.Matcher() built its BadgerMatcher from
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
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestDatabaseMatcher_HonorsCustomPlannerOptions asserts that relations
// produced by the default-source matcher carry the database's effective
// options, not DefaultPlannerOptions(). It exercises both drift directions:
// fields Matcher() copied but from the wrong source (EnableTrueStreaming,
// IndexNestedLoopThreshold) and fields Matcher() dropped entirely
// (EnableScanSharing, EnableEntityPrefetch).
func TestDatabaseMatcher_HonorsCustomPlannerOptions(t *testing.T) {
	custom := DefaultPlannerOptions()
	custom.EnableTrueStreaming = false       // default true
	custom.EnableScanSharing = true          // default false; dropped by Matcher() pre-fix
	custom.EnableEntityPrefetch = true       // default false; dropped by Matcher() pre-fix
	custom.IndexNestedLoopThreshold = 999999 // default 0; built from defaults pre-fix

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           t.TempDir(),
		ReplicaID:      1,
		PlannerOptions: &custom,
	})
	require.NoError(t, err)
	defer db.Close()

	m := db.Matcher().(*BadgerMatcher)
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
	require.Equal(t, 999999, opts.IndexNestedLoopThreshold,
		"custom IndexNestedLoopThreshold must reach matcher-produced relations")
}
