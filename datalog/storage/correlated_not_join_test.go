package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestCorrelatedNotJoinPredicateInputMatchesUnoptimizedExecution(t *testing.T) {
	db, retained := openCorrelatedNotJoinDatabase(t)
	source := `[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		[?setEvent :event/goal ?goal]
		[?setEvent :event/type ?goalSet]
		(not-join [?goal ?goalSet]
			[?termEvent :event/goal ?goal]
			[?termEvent :event/type ?termType]
			[(!= ?termType ?goalSet)])]`

	baselineOptions := DefaultPlannerOptions()
	baselineOptions.EnableAlgebraOptimizer = false
	baseline := executePlannerOptions(t, db, source, nil, baselineOptions)
	require.Equal(t, [][]interface{}{{retained}}, baseline)

	optimizedOptions := DefaultPlannerOptions()
	optimizedOptions.EnableAlgebraOptimizer = true
	optimized := executePlannerOptions(t, db, source, nil, optimizedOptions)
	require.Equal(t, baseline, optimized)
}

func TestCorrelatedNotPredicateInputExecutesWithAlgebra(t *testing.T) {
	db, retained := openCorrelatedNotJoinDatabase(t)
	source := `[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		[?setEvent :event/goal ?goal]
		[?setEvent :event/type ?goalSet]
		(not
			[?termEvent :event/goal ?goal]
			[?termEvent :event/type ?termType]
			[(!= ?termType ?goalSet)])]`

	optimizedOptions := DefaultPlannerOptions()
	optimizedOptions.EnableAlgebraOptimizer = true
	optimized := executePlannerOptions(t, db, source, nil, optimizedOptions)
	require.Equal(t, [][]interface{}{{retained}}, optimized)
}

func TestCorrelatedNotJoinRequiresOuterInputsInHeader(t *testing.T) {
	db, _ := openCorrelatedNotJoinDatabase(t)
	source := `[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		[?setEvent :event/goal ?goal]
		[?setEvent :event/type ?goalSet]
		(not-join [?goal]
			[?termEvent :event/goal ?goal]
			[?termEvent :event/type ?termType]
			[(!= ?termType ?goalSet)])]`

	_, err := db.Query(source)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not-join header")
	require.Contains(t, err.Error(), "?goalSet")
}

func openCorrelatedNotJoinDatabase(t *testing.T) (*Database, datalog.Identity) {
	t.Helper()
	db, err := NewDatabase(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	excluded := datalog.NewIdentity("goal:excluded")
	retained := datalog.NewIdentity("goal:retained")
	excludedSet := datalog.NewIdentity("event:excluded:set")
	excludedTerm := datalog.NewIdentity("event:excluded:term")
	retainedSet := datalog.NewIdentity("event:retained:set")
	retainedTerm := datalog.NewIdentity("event:retained:term")
	entityType := datalog.NewKeyword(":entity/type")
	eventGoal := datalog.NewKeyword(":event/goal")
	eventType := datalog.NewKeyword(":event/type")
	goalType := datalog.NewKeyword(":type/goal")
	goalSetType := datalog.NewKeyword(":event.type/goal-set")
	goalTermType := datalog.NewKeyword(":event.type/goal-terminate")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(excluded, entityType, goalType))
	require.NoError(t, tx.Add(retained, entityType, goalType))
	require.NoError(t, tx.Add(excludedSet, eventGoal, excluded))
	require.NoError(t, tx.Add(excludedSet, eventType, goalSetType))
	require.NoError(t, tx.Add(excludedTerm, eventGoal, excluded))
	require.NoError(t, tx.Add(excludedTerm, eventType, goalTermType))
	require.NoError(t, tx.Add(retainedSet, eventGoal, retained))
	require.NoError(t, tx.Add(retainedSet, eventType, goalSetType))
	require.NoError(t, tx.Add(retainedTerm, eventGoal, retained))
	require.NoError(t, tx.Add(retainedTerm, eventType, goalSetType))
	_, err = tx.Commit()
	require.NoError(t, err)
	return db, retained
}
