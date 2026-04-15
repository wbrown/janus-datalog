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
// getUniqueInputCombinations can find symbols from any constituent relation.
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

	combos, err := getUniqueInputCombinations(mat, []query.Symbol{datalog.NewSymbol("?config")})
	require.NoError(t, err)
	assert.Len(t, combos, 1, "should find 1 unique ?config value")
}
