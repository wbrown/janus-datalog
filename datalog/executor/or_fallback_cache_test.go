package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// cachedBranchOver is a branch's cache over the tuples of its result, keyed
// on keySyms at the subject's positions, filled as buildBranch fills it.
func cachedBranchOver(t *testing.T, branchResult Relation, outerSyms, keySyms []query.Symbol) *cachedBranch {
	cb := newCachedBranch(branchResult.Symbols(), outerSyms, keySyms, keySyms)
	tuples, err := CollectTuples(branchResult, nil)
	require.NoError(t, err)
	for _, tuple := range tuples {
		cb.Add(tuple)
	}
	return cb
}

func TestBuildCachedBranch(t *testing.T) {
	e1 := datalog.NewIdentity("entity:1")
	e2 := datalog.NewIdentity("entity:2")
	e3 := datalog.NewIdentity("entity:3")

	symE := datalog.NewSymbol("?e")
	symCount := datalog.NewSymbol("?count")

	// Branch result: 3 groups from a decorrelated subquery
	branchResult := NewMaterializedRelation(
		[]query.Symbol{symE, symCount},
		[]Tuple{
			{e1, int64(10)},
			{e2, int64(20)},
			{e3, int64(30)},
		},
	)

	outerSyms := []query.Symbol{symE}
	keySyms := []query.Symbol{symE}

	t.Run("build_and_probe", func(t *testing.T) {
		cb := cachedBranchOver(t, branchResult, outerSyms, keySyms)

		// Probe for entity:1
		matches := cb.Probe(Tuple{e1})
		require.Len(t, matches, 1)
		assert.Equal(t, e1, matches[0][0])
		assert.Equal(t, int64(10), matches[0][1])

		// Probe for entity:2
		matches = cb.Probe(Tuple{e2})
		require.Len(t, matches, 1)
		assert.Equal(t, int64(20), matches[0][1])

		// Probe for entity:3
		matches = cb.Probe(Tuple{e3})
		require.Len(t, matches, 1)
		assert.Equal(t, int64(30), matches[0][1])

		// Probe for non-existent entity
		matches = cb.Probe(Tuple{datalog.NewIdentity("entity:999")})
		assert.Nil(t, matches)
	})

	t.Run("no_key_symbol", func(t *testing.T) {
		// A result carrying no key symbol is every subject tuple's: the
		// branch reading nothing, probed with no key.
		unrelatedSyms := []query.Symbol{datalog.NewSymbol("?other")}
		cb := cachedBranchOver(t, branchResult, unrelatedSyms, nil)
		matches := cb.Probe(Tuple{"anything"})
		assert.Len(t, matches, 3, "every tuple answers a probe with no key")
	})

	t.Run("multiple_matches_per_key", func(t *testing.T) {
		// Two items for entity:1
		multiResult := NewMaterializedRelation(
			[]query.Symbol{symE, symCount},
			[]Tuple{
				{e1, int64(10)},
				{e1, int64(11)},
				{e2, int64(20)},
			},
		)

		cb := cachedBranchOver(t, multiResult, outerSyms, keySyms)

		matches := cb.Probe(Tuple{e1})
		assert.Len(t, matches, 2, "should return both matches for entity:1")

		matches = cb.Probe(Tuple{e2})
		assert.Len(t, matches, 1)
	})

	t.Run("probe_with_different_identity_instance", func(t *testing.T) {
		// The outer tuple's Identity and the branch result's Identity
		// may be different Go objects representing the same entity.
		// The cache must match on value equality, not pointer equality.
		e1copy := datalog.NewIdentity("entity:1") // different Go pointer, same hash
		cb := cachedBranchOver(t, branchResult, outerSyms, keySyms)

		matches := cb.Probe(Tuple{e1copy})
		require.Len(t, matches, 1, "should match on Identity value, not pointer")
		assert.Equal(t, int64(10), matches[0][1])
	})

	t.Run("outer_tuple_with_extra_symbols", func(t *testing.T) {
		// Outer tuple has [?e, ?name] but cache only keys on ?e
		symName := datalog.NewSymbol("?name")
		outerWithExtra := []query.Symbol{symE, symName}

		cb := cachedBranchOver(t, branchResult, outerWithExtra, keySyms)

		// Probe with full outer tuple
		matches := cb.Probe(Tuple{e1, "Alice"})
		require.Len(t, matches, 1)
		assert.Equal(t, int64(10), matches[0][1])
	})

	t.Run("decorrelated_subquery_result_shape", func(t *testing.T) {
		// Simulate the exact shape produced by a decorrelated or-join:
		// The SubqueryPattern binding maps inner ?s → outer ?project and
		// inner aggregates → outer binding vars.
		// outerSyms comes from the full outer relation (many symbols).
		symProject := datalog.NewSymbol("?project")
		symLabel := datalog.NewSymbol("?label")
		symCreatedAt := datalog.NewSymbol("?createdAt")
		symItemCount := datalog.NewSymbol("?itemCount")

		p1 := datalog.NewIdentity("project:1")
		p2 := datalog.NewIdentity("project:2")

		// Branch result from decorrelated SubqueryPattern: [?project, ?itemCount]
		decorrelatedResult := NewMaterializedRelation(
			[]query.Symbol{symProject, symItemCount},
			[]Tuple{
				{p1, int64(5)},
				{p2, int64(3)},
			},
		)

		// Outer relation has many symbols, ?project is first
		outerSyms := []query.Symbol{symProject, symLabel, symCreatedAt}

		cb := cachedBranchOver(t, decorrelatedResult, outerSyms, []query.Symbol{symProject})

		// Probe with outer tuple for project:1
		matches := cb.Probe(Tuple{p1, "Project 1", "2026-01-01"})
		require.Len(t, matches, 1, "should find project:1 in cache")
		assert.Equal(t, int64(5), matches[0][1])

		// Probe with outer tuple for project:2
		matches = cb.Probe(Tuple{p2, "Project 2", "2026-01-02"})
		require.Len(t, matches, 1, "should find project:2 in cache")
		assert.Equal(t, int64(3), matches[0][1])

		// Probe with non-existent project
		p3 := datalog.NewIdentity("project:3")
		matches = cb.Probe(Tuple{p3, "Project 3", "2026-01-03"})
		assert.Nil(t, matches, "should return nil for missing project")
	})
}

// A key's tuples come back from a probe in the order the result yielded
// them, however the keys interleave, and every tuple is held under its key.
func TestCachedBranchGroupsAKeysTuplesInCollectionOrder(t *testing.T) {
	e1 := datalog.NewIdentity("entity:1")
	e2 := datalog.NewIdentity("entity:2")
	e3 := datalog.NewIdentity("entity:3")
	symE := datalog.NewSymbol("?e")
	symCount := datalog.NewSymbol("?count")

	// Interleaved keys.
	branchResult := NewMaterializedRelation(
		[]query.Symbol{symE, symCount},
		[]Tuple{
			{e1, int64(10)},
			{e2, int64(20)},
			{e1, int64(11)},
			{e3, int64(30)},
			{e2, int64(21)},
			{e1, int64(12)},
		},
	)

	cb := cachedBranchOver(t, branchResult, []query.Symbol{symE}, []query.Symbol{symE})
	assert.Equal(t, 6, cb.Len(), "every tuple is held")
	assert.Equal(t, []Tuple{{e1, int64(10)}, {e1, int64(11)}, {e1, int64(12)}}, cb.Probe(Tuple{e1}))
	assert.Equal(t, []Tuple{{e2, int64(20)}, {e2, int64(21)}}, cb.Probe(Tuple{e2}))
	assert.Equal(t, []Tuple{{e3, int64(30)}}, cb.Probe(Tuple{e3}))
}

// Two distinct keys sharing one hash are told apart: each probe answers its
// own key's tuples, a third key under the hash misses, and a probe under a
// shared hash allocates nothing. int64(0) and false share a hash under
// hashValue, as does the zero ElementID.
func TestCachedBranchTellsTwoKeysUnderOneHashApart(t *testing.T) {
	zero, no, absent := int64(0), false, datalog.ElementID{}
	require.Equal(t, hashValue(zero), hashValue(no), "the fixture's two keys share one hash")
	require.Equal(t, hashValue(zero), hashValue(absent), "the fixture's absent key shares it")
	symE := datalog.NewSymbol("?e")
	symCount := datalog.NewSymbol("?count")
	branchResult := NewMaterializedRelation(
		[]query.Symbol{symE, symCount},
		[]Tuple{{zero, "zero"}, {no, "no"}, {zero, "nil"}},
	)
	cb := cachedBranchOver(t, branchResult, []query.Symbol{symE}, []query.Symbol{symE})

	assert.Equal(t, []Tuple{{zero, "zero"}, {zero, "nil"}}, cb.Probe(Tuple{zero}), "a probe returns only the probed key's tuples")
	assert.Equal(t, []Tuple{{no, "no"}}, cb.Probe(Tuple{no}))
	assert.Nil(t, cb.Probe(Tuple{absent}), "a key under the hash no tuple carries misses")

	probeTuple := Tuple{zero}
	var got []Tuple
	assert.Zero(t, testing.AllocsPerRun(1000, func() { got = cb.Probe(probeTuple) }), "a probe under a shared hash must not allocate")
	_ = got
}

func TestCachedBranchProbeAllocations(t *testing.T) {
	e1 := datalog.NewIdentity("entity:1")
	e2 := datalog.NewIdentity("entity:2")
	symE := datalog.NewSymbol("?e")
	symCount := datalog.NewSymbol("?count")

	branchResult := NewMaterializedRelation(
		[]query.Symbol{symE, symCount},
		[]Tuple{
			{e1, int64(10)},
			{e1, int64(11)},
			{e2, int64(20)},
		},
	)
	cb := cachedBranchOver(t, branchResult, []query.Symbol{symE}, []query.Symbol{symE})

	hit := Tuple{e1}
	miss := Tuple{datalog.NewIdentity("entity:999")}
	var got []Tuple
	assert.Zero(t, testing.AllocsPerRun(1000, func() { got = cb.Probe(hit) }),
		"hit probe must not allocate")
	assert.Zero(t, testing.AllocsPerRun(1000, func() { got = cb.Probe(miss) }),
		"miss probe must not allocate")
	_ = got
}
