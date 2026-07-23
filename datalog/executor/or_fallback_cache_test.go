package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestIsCacheableBranch(t *testing.T) {
	t.Run("uncorrelated_subquery", func(t *testing.T) {
		branch := []query.Clause{
			&query.SubqueryPattern{
				Inputs: []query.PatternElement{
					query.Constant{Value: datalog.SymDollar},
				},
			},
		}
		assert.True(t, isCacheableBranch(branch, false))
		assert.True(t, isCacheableBranch(branch, true))
	})

	t.Run("correlated_subquery", func(t *testing.T) {
		branch := []query.Clause{
			&query.SubqueryPattern{
				Inputs: []query.PatternElement{
					query.Constant{Value: datalog.SymDollar},
					query.Variable{Name: datalog.NewSymbol("?e")},
				},
			},
		}
		assert.False(t, isCacheableBranch(branch, false))
		assert.False(t, isCacheableBranch(branch, true))
	})

	t.Run("ground_only_branch", func(t *testing.T) {
		branch := []query.Clause{
			&query.Expression{
				Function: &query.GroundFunction{Value: 0},
				Binding:  datalog.NewSymbol("?count"),
			},
		}
		assert.False(t, isCacheableBranch(branch, false),
			"ground-only branches have no SubqueryPattern")
		assert.False(t, isCacheableBranch(branch, true),
			"expression branches are not cacheable")
	})

	t.Run("no_clauses", func(t *testing.T) {
		assert.False(t, isCacheableBranch(nil, false))
		assert.False(t, isCacheableBranch(nil, true))
	})

	t.Run("data_pattern_in_or_join", func(t *testing.T) {
		branch := []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":project/title")},
					query.Variable{Name: datalog.NewSymbol("?title")},
				},
			},
		}
		assert.False(t, isCacheableBranch(branch, false),
			"DataPattern not cacheable outside or-join")
		assert.True(t, isCacheableBranch(branch, true),
			"DataPattern cacheable in or-join context")
	})
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

	t.Run("build_and_probe", func(t *testing.T) {
		cb, err := buildCachedBranch(branchResult, outerSyms, nil)
		require.NoError(t, err)
		require.NotNil(t, cb, "should build cache when shared symbols exist")

		// Probe for entity:1
		matches := cb.probe(Tuple{e1})
		require.Len(t, matches, 1)
		assert.Equal(t, e1, matches[0][0])
		assert.Equal(t, int64(10), matches[0][1])

		// Probe for entity:2
		matches = cb.probe(Tuple{e2})
		require.Len(t, matches, 1)
		assert.Equal(t, int64(20), matches[0][1])

		// Probe for entity:3
		matches = cb.probe(Tuple{e3})
		require.Len(t, matches, 1)
		assert.Equal(t, int64(30), matches[0][1])

		// Probe for non-existent entity
		matches = cb.probe(Tuple{datalog.NewIdentity("entity:999")})
		assert.Nil(t, matches)
	})

	t.Run("no_shared_symbols", func(t *testing.T) {
		unrelatedSyms := []query.Symbol{datalog.NewSymbol("?other")}
		cb, err := buildCachedBranch(branchResult, unrelatedSyms, nil)
		require.NoError(t, err)
		assert.Nil(t, cb, "no cache when no shared symbols")
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

		cb, err := buildCachedBranch(multiResult, outerSyms, nil)
		require.NoError(t, err)
		require.NotNil(t, cb)

		matches := cb.probe(Tuple{e1})
		assert.Len(t, matches, 2, "should return both matches for entity:1")

		matches = cb.probe(Tuple{e2})
		assert.Len(t, matches, 1)
	})

	t.Run("probe_with_different_identity_instance", func(t *testing.T) {
		// The outer tuple's Identity and the branch result's Identity
		// may be different Go objects representing the same entity.
		// The cache must match on value equality, not pointer equality.
		e1copy := datalog.NewIdentity("entity:1") // different Go pointer, same hash
		cb, err := buildCachedBranch(branchResult, outerSyms, nil)
		require.NoError(t, err)
		require.NotNil(t, cb)

		matches := cb.probe(Tuple{e1copy})
		require.Len(t, matches, 1, "should match on Identity value, not pointer")
		assert.Equal(t, int64(10), matches[0][1])
	})

	t.Run("outer_tuple_with_extra_symbols", func(t *testing.T) {
		// Outer tuple has [?e, ?name] but cache only keys on ?e
		symName := datalog.NewSymbol("?name")
		outerWithExtra := []query.Symbol{symE, symName}

		cb, err := buildCachedBranch(branchResult, outerWithExtra, nil)
		require.NoError(t, err)
		require.NotNil(t, cb)

		// Probe with full outer tuple
		matches := cb.probe(Tuple{e1, "Alice"})
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

		cb, err := buildCachedBranch(decorrelatedResult, outerSyms, nil)
		require.NoError(t, err)
		require.NotNil(t, cb)

		// Probe with outer tuple for project:1
		matches := cb.probe(Tuple{p1, "Project 1", "2026-01-01"})
		require.Len(t, matches, 1, "should find project:1 in cache")
		assert.Equal(t, int64(5), matches[0][1])

		// Probe with outer tuple for project:2
		matches = cb.probe(Tuple{p2, "Project 2", "2026-01-02"})
		require.Len(t, matches, 1, "should find project:2 in cache")
		assert.Equal(t, int64(3), matches[0][1])

		// Probe with non-existent project
		p3 := datalog.NewIdentity("project:3")
		matches = cb.probe(Tuple{p3, "Project 3", "2026-01-03"})
		assert.Nil(t, matches, "should return nil for missing project")
	})
}

func TestCachedBranchSpanGrouping(t *testing.T) {
	e1 := datalog.NewIdentity("entity:1")
	e2 := datalog.NewIdentity("entity:2")
	e3 := datalog.NewIdentity("entity:3")
	symE := datalog.NewSymbol("?e")
	symCount := datalog.NewSymbol("?count")

	// Interleaved keys: grouping must make each key's rows contiguous.
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

	cb, err := buildCachedBranch(branchResult, []query.Symbol{symE}, nil)
	require.NoError(t, err)
	require.NotNil(t, cb)

	// Spans tile the shared backing: every row belongs to exactly one span,
	// and every row within an unmixed span carries the span's key.
	require.Len(t, cb.spans, 3, "one span per distinct key")
	total := int32(0)
	for _, span := range cb.spans {
		require.False(t, span.mixed, "distinct real values must not collide")
		segment := cb.rows[span.start:span.end]
		require.NotEmpty(t, segment)
		for _, row := range segment[1:] {
			assert.True(t, rowKeysEqual(segment[0], row, cb.rowPos),
				"rows within an unmixed span must share one key")
		}
		total += span.end - span.start
	}
	assert.Equal(t, int32(len(cb.rows)), total, "spans cover every row exactly once")

	// Fanout probes return all rows for the key, in collection order.
	matches := cb.probe(Tuple{e1})
	require.Len(t, matches, 3)
	assert.Equal(t, int64(10), matches[0][1])
	assert.Equal(t, int64(11), matches[1][1])
	assert.Equal(t, int64(12), matches[2][1])

	// The probe result is a window into the shared backing, not a copy.
	span := cb.spans[hashTuplePositions(Tuple{e1}, cb.probePos)]
	assert.True(t, &matches[0] == &cb.rows[span.start],
		"probe must return a subslice of the shared backing")
}

func TestCachedBranchMixedSpanCollision(t *testing.T) {
	// Two distinct keys sharing one hash cannot be constructed from real
	// values, so drive regroupCollidingSpans on a hand-placed state: rows of
	// two keys laid into one span, with each probe hash mapping to it — the
	// state counting-sort placement produces when key hashes collide.
	e1 := datalog.NewIdentity("entity:1")
	e2 := datalog.NewIdentity("entity:2")
	e3 := datalog.NewIdentity("entity:3")
	symE := datalog.NewSymbol("?e")
	symCount := datalog.NewSymbol("?count")

	rows := []Tuple{
		{e1, int64(10)},
		{e2, int64(20)},
		{e1, int64(11)},
	}
	h1 := hashTuplePositions(Tuple{e1}, []int{0})
	h2 := hashTuplePositions(Tuple{e2}, []int{0})
	h3 := hashTuplePositions(Tuple{e3}, []int{0})
	cb := &cachedBranch{
		groupedRowIndex: &groupedRowIndex{
			rows: rows,
			spans: map[uint64]rowSpan{
				h1: {start: 0, end: 3},
				h2: {start: 0, end: 3},
				h3: {start: 0, end: 3}, // hash resolves to the span, key absent
			},
			probePos: []int{0},
			rowPos:   []int{0},
		},
		branchSyms: []query.Symbol{symE, symCount},
	}
	cb.regroupCollidingSpans()

	require.True(t, cb.spans[h1].mixed)
	require.True(t, cb.spans[h2].mixed)
	require.Len(t, cb.collisions[h1], 2, "two distinct keys → two groups")

	matches := cb.probe(Tuple{e1})
	require.Len(t, matches, 2, "mixed-span probe returns only the probed key's rows")
	assert.Equal(t, int64(10), matches[0][1])
	assert.Equal(t, int64(11), matches[1][1])

	matches = cb.probe(Tuple{e2})
	require.Len(t, matches, 1)
	assert.Equal(t, int64(20), matches[0][1])

	// A key whose hash resolves to a mixed span but matches no group misses.
	assert.Nil(t, cb.probe(Tuple{e3}))

	// The mixed probe path allocates nothing either.
	probeTuple := Tuple{e1}
	var got []Tuple
	allocs := testing.AllocsPerRun(1000, func() { got = cb.probe(probeTuple) })
	assert.Zero(t, allocs, "mixed-span probe must not allocate")
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
	cb, err := buildCachedBranch(branchResult, []query.Symbol{symE}, nil)
	require.NoError(t, err)
	require.NotNil(t, cb)

	hit := Tuple{e1}
	miss := Tuple{datalog.NewIdentity("entity:999")}
	var got []Tuple
	assert.Zero(t, testing.AllocsPerRun(1000, func() { got = cb.probe(hit) }),
		"hit probe must not allocate")
	assert.Zero(t, testing.AllocsPerRun(1000, func() { got = cb.probe(miss) }),
		"miss probe must not allocate")
	_ = got
}
