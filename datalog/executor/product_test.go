package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestProductPreservesDisjointSymbols verifies that Product() of disjoint
// relations produces a combined relation with all symbols, and that
// projecting for subquery input dedup can find symbols from any constituent
// relation.
func TestProductPreservesDisjointSymbols(t *testing.T) {
	rel1 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?a"), datalog.NewSymbol("?v")},
		[]Tuple{{"alice", ":name", "Alice"}, {"bob", ":name", "Bob"}},
	)
	rel2 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?config")},
		[]Tuple{{"config-1"}},
	)

	product := Relations{rel1, rel2}.Product()
	require.NotNil(t, product)

	syms := product.Symbols()
	symNames := make([]string, len(syms))
	for i, s := range syms {
		symNames[i] = s.String()
	}

	assert.Contains(t, symNames, "?person")
	assert.Contains(t, symNames, "?a")
	assert.Contains(t, symNames, "?v")
	assert.Contains(t, symNames, "?config")

	mat := product.Materialize()
	assert.Equal(t, 2, mat.Size())

	dataSymbols := filterSourceSymbols([]query.Symbol{datalog.NewSymbol("?config")})
	combos, err := mat.Project(dataSymbols)
	require.NoError(t, err)
	assert.Equal(t, 1, combos.Size(), "should find 1 unique ?config value")
}

// TestProductMaterializeSurfacesSourceError verifies that materializing a
// Product() of disjoint relations surfaces a constituent relation's deferred
// iterator error rather than laundering it into an empty result. This is the
// "product path" of docs/bugs/BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES.md;
// it is reached in production where a subquery's input groups are disjoint and
// the combined relation is materialized (query_executor.go input combination).
func TestProductMaterializeSurfacesSourceError(t *testing.T) {
	// failAfter 0 → yields no tuples and reports its failure via Error(), exactly
	// like a storage scan whose value fails to decode (e.g. a missing Tier-3 blob).
	failing := newFailingRelation(0, Tuple{int64(1)})
	other := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?config")},
		[]Tuple{{"config-1"}},
	)

	product := Relations{failing, other}.Product()
	require.IsType(t, &ProductRelation{}, product, "two disjoint relations must form a ProductRelation")

	mat := product.Materialize()
	require.ErrorIs(t, driveErr(mat), errInjectedIterator,
		"materializing a product must surface a constituent's deferred error, not drop it")
}

// TestProductProjectionRestoresSetOnReduction pins the streaming projection of
// a product: reducing the symbol set can collapse distinct product tuples
// (every ?x pairs with every ?y), so the projection must restore set
// semantics — the dedup the subquery input-combination extraction relies on.
func TestProductProjectionRestoresSetOnReduction(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	left := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(1)}, {int64(2)}})
	right := NewMaterializedRelation([]query.Symbol{y}, []Tuple{{int64(10)}, {int64(20)}})

	product := Relations{left, right}.Product()
	combos, err := product.Project([]query.Symbol{x})
	require.NoError(t, err)
	_, dedups := combos.(*StreamingRelation).iterator.(*DedupIterator)
	require.True(t, dedups, "a reducing projection of a product must deduplicate")
	rows, err := CollectTuples(combos, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{int64(1)}, {int64(2)}}, rows)
}

// TestProductProjectionPermutationSkipsDedup pins the permutation arm: a
// projection that reorders the full symbol set is injective on tuples, so the
// product streams through without a dedup pass or its seen-map state.
func TestProductProjectionPermutationSkipsDedup(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	left := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(1)}, {int64(2)}})
	right := NewMaterializedRelation([]query.Symbol{y}, []Tuple{{int64(10)}, {int64(20)}})

	product := Relations{left, right}.Product()
	reordered, err := product.Project([]query.Symbol{y, x})
	require.NoError(t, err)
	_, dedups := reordered.(*StreamingRelation).iterator.(*DedupIterator)
	require.False(t, dedups, "a permutation is injective on tuples — no dedup pass")
	rows, err := CollectTuples(reordered, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{
		{int64(10), int64(1)}, {int64(20), int64(1)},
		{int64(10), int64(2)}, {int64(20), int64(2)},
	}, rows)
}
