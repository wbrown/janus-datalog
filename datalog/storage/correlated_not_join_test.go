package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestCorrelatedNotJoinPredicateInputMatchesUnoptimizedExecution(t *testing.T) {
	source := `[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		[?setEvent :event/goal ?goal]
		[?setEvent :event/type ?goalSet]
		(not-join [?goal ?goalSet]
			[?termEvent :event/goal ?goal]
			[?termEvent :event/type ?termType]
			[(!= ?termType ?goalSet)])]`

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, retained := openCorrelatedNotJoinDatabase(t, mode)

			baselineOptions := DefaultPlannerOptions()
			baselineOptions.EnableAlgebraOptimizer = false
			baseline := executePlannerOptions(t, db, source, nil, baselineOptions)
			require.Equal(t, [][]interface{}{{retained}}, baseline)

			optimizedOptions := DefaultPlannerOptions()
			optimizedOptions.EnableAlgebraOptimizer = true
			optimized := executePlannerOptions(t, db, source, nil, optimizedOptions)
			require.Equal(t, baseline, optimized)
		})
	}
}

func TestCorrelatedNotPredicateInputExecutesWithAlgebra(t *testing.T) {
	source := `[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		[?setEvent :event/goal ?goal]
		[?setEvent :event/type ?goalSet]
		(not
			[?termEvent :event/goal ?goal]
			[?termEvent :event/type ?termType]
			[(!= ?termType ?goalSet)])]`

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, retained := openCorrelatedNotJoinDatabase(t, mode)

			optimizedOptions := DefaultPlannerOptions()
			optimizedOptions.EnableAlgebraOptimizer = true
			optimized := executePlannerOptions(t, db, source, nil, optimizedOptions)
			require.Equal(t, [][]interface{}{{retained}}, optimized)
		})
	}
}

// TestCorrelatedOrJoinAllBranchesBindHeaderMatchesUnoptimizedExecution pins
// the correlated or-join used as a per-entity filter: every header variable
// is bound by the enclosing query, and the union of (tagged without flag)
// and (flagged) covers every entity. or-join is union semantics; the NOT
// branch sends the clause down the algebra compiler's correlated route,
// which must preserve that union — the optimized path must agree with the
// baseline.
func TestCorrelatedOrJoinAllBranchesBindHeaderMatchesUnoptimizedExecution(t *testing.T) {
	source := `[:find ?e
		:where
		[?e :x/tag _]
		(or-join [?e]
			(and [?e :x/tag _]
				(not [?e :x/flag true]))
			[?e :x/flag true])]`

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			tag := datalog.NewKeyword(":x/tag")
			flag := datalog.NewKeyword(":x/flag")
			e1 := datalog.NewIdentity("x:1")
			e2 := datalog.NewIdentity("x:2")
			e3 := datalog.NewIdentity("x:3")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e1, tag, "a"))
			require.NoError(t, tx.Add(e2, tag, "b"))
			require.NoError(t, tx.Add(e3, tag, "c"))
			require.NoError(t, tx.Add(e3, flag, true))
			_, err := tx.Commit()
			require.NoError(t, err)

			baselineOptions := DefaultPlannerOptions()
			baselineOptions.EnableAlgebraOptimizer = false
			baseline := executePlannerOptions(t, db, source, nil, baselineOptions)
			require.Len(t, baseline, 3, "every entity matches one branch")

			optimizedOptions := DefaultPlannerOptions()
			optimizedOptions.EnableAlgebraOptimizer = true
			optimized := executePlannerOptions(t, db, source, nil, optimizedOptions)
			require.ElementsMatch(t, baseline, optimized)
		})
	}
}

// openCorrelatedOrOutputsDatabase seeds the outputs-shape divergence data:
// e1 matches both branches with different ?v values, e2 matches only the
// second branch, e3 is excluded from the first branch by the NOT and lacks
// the second branch's attribute. Union semantics must produce
// (e1 1), (e1 2), (e2 3).
func openCorrelatedOrOutputsDatabase(t *testing.T, mode optimizerMode) *Database {
	t.Helper()
	db := createOptimizerModeDB(t, mode, DatabaseOptions{})

	tag := datalog.NewKeyword(":x/tag")
	flag := datalog.NewKeyword(":x/flag")
	attrA := datalog.NewKeyword(":x/a")
	attrB := datalog.NewKeyword(":x/b")
	e1 := datalog.NewIdentity("x:1")
	e2 := datalog.NewIdentity("x:2")
	e3 := datalog.NewIdentity("x:3")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e1, tag, "a"))
	require.NoError(t, tx.Add(e1, attrA, int64(1)))
	require.NoError(t, tx.Add(e1, attrB, int64(2)))
	require.NoError(t, tx.Add(e2, tag, "b"))
	require.NoError(t, tx.Add(e2, attrB, int64(3)))
	require.NoError(t, tx.Add(e3, tag, "c"))
	require.NoError(t, tx.Add(e3, attrA, int64(4)))
	require.NoError(t, tx.Add(e3, flag, true))
	_, err := tx.Commit()
	require.NoError(t, err)
	return db
}

// TestCorrelatedOrJoinWithOutputsMatchesUnoptimizedExecution pins the
// correlated or-join shape that produces values: branches bind ?v from
// different attributes, and an entity matching both branches contributes a
// tuple from each — union semantics, not first-match-wins. The NOT branch
// sends the clause down the algebra compiler's correlated route, which must
// preserve the union.
func TestCorrelatedOrJoinWithOutputsMatchesUnoptimizedExecution(t *testing.T) {
	source := `[:find ?e ?v
		:where
		[?e :x/tag _]
		(or-join [?e ?v]
			(and [?e :x/a ?v]
				(not [?e :x/flag true]))
			[?e :x/b ?v])]`

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openCorrelatedOrOutputsDatabase(t, mode)

			baselineOptions := DefaultPlannerOptions()
			baselineOptions.EnableAlgebraOptimizer = false
			baseline := executePlannerOptions(t, db, source, nil, baselineOptions)
			require.Len(t, baseline, 3, "e1 contributes a tuple per branch, e2 one, e3 none")

			optimizedOptions := DefaultPlannerOptions()
			optimizedOptions.EnableAlgebraOptimizer = true
			optimized := executePlannerOptions(t, db, source, nil, optimizedOptions)
			require.ElementsMatch(t, baseline, optimized)
		})
	}
}

// TestCorrelatedOrWithOutputsMatchesUnoptimizedExecution pins the same
// union-preservation invariant for plain (or ...) — the algebra compiler's
// correlated route for or is a separate call site from or-join and must
// gate on its own.
func TestCorrelatedOrWithOutputsMatchesUnoptimizedExecution(t *testing.T) {
	source := `[:find ?e ?v
		:where
		[?e :x/tag _]
		(or (and [?e :x/a ?v]
				(not [?e :x/flag true]))
			[?e :x/b ?v])]`

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openCorrelatedOrOutputsDatabase(t, mode)

			baselineOptions := DefaultPlannerOptions()
			baselineOptions.EnableAlgebraOptimizer = false
			baseline := executePlannerOptions(t, db, source, nil, baselineOptions)
			require.Len(t, baseline, 3, "e1 contributes a tuple per branch, e2 one, e3 none")

			optimizedOptions := DefaultPlannerOptions()
			optimizedOptions.EnableAlgebraOptimizer = true
			optimized := executePlannerOptions(t, db, source, nil, optimizedOptions)
			require.ElementsMatch(t, baseline, optimized)
		})
	}
}

// TestFullyDisjointNotRejectedAtQueryOutset pins the disjoint-NOT ruling
// end-to-end: a NOT body sharing no variable the query can bind is rejected
// at planning — before any execution, on both optimizer paths — with a
// message naming the clause and the unification rule. Previously this shape
// planned and then failed deep in the executor with "NOT clause variables
// not found in input relation".
func TestFullyDisjointNotRejectedAtQueryOutset(t *testing.T) {
	source := `[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		(not [?x :sys/killswitch true])]`

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, _ := openCorrelatedNotJoinDatabase(t, mode)

			for _, algebra := range []bool{false, true} {
				options := DefaultPlannerOptions()
				options.EnableAlgebraOptimizer = algebra
				_, err := runPlannerOptions(db, source, nil, options)
				require.Error(t, err, "algebra=%v", algebra)
				require.Contains(t, err.Error(), "(not ", "algebra=%v: must name the clause", algebra)
				require.Contains(t, err.Error(), "unify", "algebra=%v: must state the unification rule", algebra)
			}
		})
	}
}

func TestCorrelatedNotJoinRequiresOuterInputsInHeader(t *testing.T) {
	source := `[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		[?setEvent :event/goal ?goal]
		[?setEvent :event/type ?goalSet]
		(not-join [?goal]
			[?termEvent :event/goal ?goal]
			[?termEvent :event/type ?termType]
			[(!= ?termType ?goalSet)])]`

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, _ := openCorrelatedNotJoinDatabase(t, mode)
			_, err := db.Query(source)
			require.Error(t, err)
			require.Contains(t, err.Error(), "?goalSet")
			// Header completeness is a static property of the clause text,
			// enforced by NotJoinClause.Validate at the user boundaries —
			// both planner modes reject with the same message, before
			// planning. (Previously algebra-only; divergence resolved:
			// BUG_NOTJOIN_HEADER_VALIDATION_ONLY_ON_ALGEBRA_PATH.md.)
			require.Contains(t, err.Error(), "not-join header")
		})
	}
}

// openCorrelatedNotJoinDatabase seeds the goal/event fixture on the mode's
// backend. The differential tests below route planner options per execution
// and so ignore the database's own defaults; the mode still decides which
// store the fixture is written to and read back from.
func openCorrelatedNotJoinDatabase(t *testing.T, mode optimizerMode) (*Database, datalog.Identity) {
	t.Helper()
	db := createOptimizerModeDB(t, mode, DatabaseOptions{})

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
	_, err := tx.Commit()
	require.NoError(t, err)
	return db, retained
}
