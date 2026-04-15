package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestGetUniqueInputCombinations_MissingSymbolReturnsError verifies that
// getUniqueInputCombinations returns an error when an input symbol is
// not found in the relation, instead of silently returning nil.
func TestGetUniqueInputCombinations_MissingSymbolReturnsError(t *testing.T) {
	// Create a relation with symbols [?a, ?b]
	rel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
		[]Tuple{{"x", int64(1)}, {"y", int64(2)}},
	)

	t.Run("present_symbol_succeeds", func(t *testing.T) {
		combos, err := getUniqueInputCombinations(rel, []query.Symbol{datalog.NewSymbol("?a")})
		require.NoError(t, err)
		assert.Len(t, combos, 2)
	})

	t.Run("missing_symbol_returns_error", func(t *testing.T) {
		combos, err := getUniqueInputCombinations(rel, []query.Symbol{datalog.NewSymbol("?missing")})
		assert.Error(t, err, "should error when input symbol is not in relation")
		assert.Nil(t, combos)
		assert.Contains(t, err.Error(), "?missing")
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("one_present_one_missing_returns_error", func(t *testing.T) {
		combos, err := getUniqueInputCombinations(rel, []query.Symbol{
			datalog.NewSymbol("?a"),
			datalog.NewSymbol("?missing"),
		})
		assert.Error(t, err)
		assert.Nil(t, combos)
	})

	t.Run("source_symbol_skipped", func(t *testing.T) {
		// Source symbols (like $) use index -1 and should not trigger the error.
		combos, err := getUniqueInputCombinations(rel, []query.Symbol{
			datalog.NewSymbol("$"),
			datalog.NewSymbol("?a"),
		})
		require.NoError(t, err)
		assert.Len(t, combos, 2)
	})
}
