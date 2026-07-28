package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestProjectionPlan(t *testing.T) {
	symE := datalog.NewSymbol("?e")
	symV := datalog.NewSymbol("?v")
	symName := datalog.NewSymbol("?name")
	symOther := datalog.NewSymbol("?other")

	t.Run("identity_when_symbols_match", func(t *testing.T) {
		plan := newProjectionPlan(
			[]query.Symbol{symE, symV},
			[]query.Symbol{symE, symV},
			[]query.Symbol{symE, symName},
		)
		assert.True(t, plan.identity)
	})

	t.Run("order_difference_is_not_identity", func(t *testing.T) {
		plan := newProjectionPlan(
			[]query.Symbol{symV, symE},
			[]query.Symbol{symE, symV},
			nil,
		)
		require.False(t, plan.identity)
		out := plan.project(Tuple{int64(10), "e"}, nil)
		assert.Equal(t, Tuple{"e", int64(10)}, out)
	})

	t.Run("outer_fills_symbols_the_branch_lacks", func(t *testing.T) {
		// Branch produces [?e ?v]; output wants [?e ?v ?name]; ?name comes
		// from the outer tuple [?e ?name].
		plan := newProjectionPlan(
			[]query.Symbol{symE, symV},
			[]query.Symbol{symE, symV, symName},
			[]query.Symbol{symE, symName},
		)
		require.False(t, plan.identity)
		out := plan.project(Tuple{"e1", int64(10)}, Tuple{"e1", "Alice"})
		assert.Equal(t, Tuple{"e1", int64(10), "Alice"}, out)
	})

	t.Run("branch_wins_over_outer_for_shared_symbols", func(t *testing.T) {
		plan := newProjectionPlan(
			[]query.Symbol{symE},
			[]query.Symbol{symE, symName},
			[]query.Symbol{symE, symName},
		)
		out := plan.project(Tuple{"branch-e"}, Tuple{"outer-e", "Alice"})
		assert.Equal(t, Tuple{"branch-e", "Alice"}, out)
	})

	t.Run("symbol_in_neither_source_stays_nil", func(t *testing.T) {
		plan := newProjectionPlan(
			[]query.Symbol{symE},
			[]query.Symbol{symE, symOther},
			[]query.Symbol{symName},
		)
		out := plan.project(Tuple{"e1"}, Tuple{"Alice"})
		assert.Equal(t, Tuple{"e1", nil}, out)
	})

	t.Run("short_outer_tuple_is_guarded", func(t *testing.T) {
		// The outer position exists in the symbol list but the tuple is
		// shorter — the position must be bounds-checked, not indexed blindly.
		plan := newProjectionPlan(
			[]query.Symbol{symE},
			[]query.Symbol{symE, symName},
			[]query.Symbol{symE, symName},
		)
		out := plan.project(Tuple{"e1"}, Tuple{"outer-e"})
		assert.Equal(t, Tuple{"e1", nil}, out)
	})
}

func TestProjectionForMemoizesPerBranch(t *testing.T) {
	symE := datalog.NewSymbol("?e")
	symV := datalog.NewSymbol("?v")
	symName := datalog.NewSymbol("?name")

	it := &OrFallbackIterator{
		outputSyms: []query.Symbol{symE, symV, symName},
		outerSyms:  []query.Symbol{symE, symName},
	}
	branchSyms := []query.Symbol{symE, symV}

	first := it.projectionFor(0, branchSyms)
	second := it.projectionFor(0, branchSyms)
	require.False(t, first.identity)
	assert.Equal(t, &first.branchPos[0], &second.branchPos[0],
		"repeat lookups must return the memoized plan, not a rebuild")

	// A different branch index gets its own plan.
	other := it.projectionFor(1, []query.Symbol{symE, symV, symName})
	assert.True(t, other.identity)
}

// countingLookupMatcher counts LookupAttribute calls through the bundle
// lookup double.
type countingLookupMatcher struct {
	*bundleLookupMatcher
	calls int
}

func (m *countingLookupMatcher) LookupAttribute(
	entity datalog.Identity,
	attr datalog.Keyword,
) (interface{}, bool, error) {
	m.calls++
	return m.bundleLookupMatcher.LookupAttribute(entity, attr)
}

func TestBuildBranchFromEACacheDedupsOuterEntities(t *testing.T) {
	symE := datalog.NewSymbol("?e")
	symName := datalog.NewSymbol("?name")
	symV := datalog.NewSymbol("?v")
	attr := datalog.NewKeyword(":item/priority")
	e1 := datalog.NewIdentity("eacache:e1")
	e2 := datalog.NewIdentity("eacache:e2")

	matcher := &countingLookupMatcher{bundleLookupMatcher: &bundleLookupMatcher{
		values: map[bundleLookupKey]interface{}{
			{entity: e1, attr: attr}: int64(1),
			{entity: e2, attr: attr}: int64(2),
		},
	}}
	exec := newQueryExecutor(matcher, nil, ExecutorOptions{})

	// The outer relation repeats e1 across distinct tuples. The collected
	// branch tuples must stay a set: one lookup and one tuple per entity.
	outerSyms := []query.Symbol{symE, symName}
	outerRel := NewMaterializedRelation(outerSyms, []Tuple{
		{e1, "first"},
		{e1, "second"},
		{e2, "third"},
	})

	it := &OrFallbackIterator{
		executor:          exec,
		outerRel:          outerRel,
		outerSyms:         outerSyms,
		branchVisibleSyms: []query.Symbol{symE},
	}
	branch := []query.Clause{
		&query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: symE},
				query.Constant{Value: attr},
				query.Variable{Name: symV},
			},
		},
	}

	cb := it.buildBranchFromEACache(branch)
	require.NoError(t, it.err)
	require.NotNil(t, cb)

	assert.Equal(t, 2, matcher.calls, "one lookup per distinct entity")

	matches := cb.probe(Tuple{e1, "first"})
	require.Len(t, matches, 1, "repeated outer entity must not duplicate tuples")
	assert.Equal(t, int64(1), matches[0][1])

	matches = cb.probe(Tuple{e2, "third"})
	require.Len(t, matches, 1)
	assert.Equal(t, int64(2), matches[0][1])
}

// TestBuildBranchFromEACacheEnvironmentNarrowsV pins the EA-cache arm's
// environment narrowing: when the branch pattern's V variable is bound by
// the environment relation, only tuples carrying the environment's value
// survive — the SemiJoin analog of the scan path's environment-narrowed
// input. Direct construction: no user-facing syntax reaches this arm with an
// environment-bound V today, so the arm is pinned at the unit level.
func TestBuildBranchFromEACacheEnvironmentNarrowsV(t *testing.T) {
	symE := datalog.NewSymbol("?e")
	symV := datalog.NewSymbol("?v")
	attr := datalog.NewKeyword(":item/priority")
	e1 := datalog.NewIdentity("eaenv:e1")
	e2 := datalog.NewIdentity("eaenv:e2")

	matcher := &countingLookupMatcher{bundleLookupMatcher: &bundleLookupMatcher{
		values: map[bundleLookupKey]interface{}{
			{entity: e1, attr: attr}: int64(1),
			{entity: e2, attr: attr}: int64(2),
		},
	}}
	exec := newQueryExecutor(matcher, nil, ExecutorOptions{})

	outerSyms := []query.Symbol{symE}
	outerRel := NewMaterializedRelation(outerSyms, []Tuple{{e1}, {e2}})

	it := &OrFallbackIterator{
		executor:          exec,
		outerRel:          outerRel,
		outerSyms:         outerSyms,
		branchVisibleSyms: []query.Symbol{symE},
		envRel: NewMaterializedRelation(
			[]query.Symbol{symV},
			[]Tuple{{int64(2)}},
		),
	}
	branch := []query.Clause{
		&query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: symE},
				query.Constant{Value: attr},
				query.Variable{Name: symV},
			},
		},
	}

	cb := it.buildBranchFromEACache(branch)
	require.NoError(t, it.err)
	require.NotNil(t, cb)

	matches := cb.probe(Tuple{e1})
	assert.Empty(t, matches, "e1's value 1 does not match the environment's 2")

	matches = cb.probe(Tuple{e2})
	require.Len(t, matches, 1)
	assert.Equal(t, int64(2), matches[0][1])
}
