package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
)

// =============================================================================
// TxRangePredicate Tests
// =============================================================================

func TestTxRangePredicateString(t *testing.T) {
	pred := &TxRangePredicate{
		TxVar: datalog.NewSymbol("?tx"),
		Low:   1000,
		High:  2000,
	}
	assert.Equal(t, "(tx-between ?tx 1000 2000)", pred.String())
}

func TestTxRangePredicateRequiredSymbols(t *testing.T) {
	pred := &TxRangePredicate{
		TxVar: datalog.NewSymbol("?tx"),
		Low:   1000,
		High:  2000,
	}
	symbols := pred.RequiredSymbols()
	require.Len(t, symbols, 1)
	assert.Equal(t, datalog.NewSymbol("?tx"), symbols[0])
}

func TestTxRangePredicateEval(t *testing.T) {
	pred := &TxRangePredicate{
		TxVar: datalog.NewSymbol("?tx"),
		Low:   1000,
		High:  2000,
	}

	t.Run("value in range (uint64)", func(t *testing.T) {
		bindings := map[Symbol]interface{}{
			datalog.NewSymbol("?tx"): uint64(1500),
		}
		result, err := pred.Eval(bindings)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("value in range (int64)", func(t *testing.T) {
		bindings := map[Symbol]interface{}{
			datalog.NewSymbol("?tx"): int64(1500),
		}
		result, err := pred.Eval(bindings)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("value in range (int)", func(t *testing.T) {
		bindings := map[Symbol]interface{}{
			datalog.NewSymbol("?tx"): 1500,
		}
		result, err := pred.Eval(bindings)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("value at low bound (inclusive)", func(t *testing.T) {
		bindings := map[Symbol]interface{}{
			datalog.NewSymbol("?tx"): uint64(1000),
		}
		result, err := pred.Eval(bindings)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("value at high bound (inclusive)", func(t *testing.T) {
		bindings := map[Symbol]interface{}{
			datalog.NewSymbol("?tx"): uint64(2000),
		}
		result, err := pred.Eval(bindings)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("value below range", func(t *testing.T) {
		bindings := map[Symbol]interface{}{
			datalog.NewSymbol("?tx"): uint64(500),
		}
		result, err := pred.Eval(bindings)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("value above range", func(t *testing.T) {
		bindings := map[Symbol]interface{}{
			datalog.NewSymbol("?tx"): uint64(3000),
		}
		result, err := pred.Eval(bindings)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("unbound variable returns error", func(t *testing.T) {
		bindings := map[Symbol]interface{}{}
		_, err := pred.Eval(bindings)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not bound")
	})

	t.Run("unsupported type returns error", func(t *testing.T) {
		bindings := map[Symbol]interface{}{
			datalog.NewSymbol("?tx"): "not a number",
		}
		_, err := pred.Eval(bindings)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot extract Lamport")
	})
}

func TestTxRangePredicateSelectivity(t *testing.T) {
	pred := &TxRangePredicate{
		TxVar: datalog.NewSymbol("?tx"),
		Low:   1000,
		High:  2000,
	}
	// Default estimate is 0.1 (10%)
	assert.Equal(t, 0.1, pred.Selectivity())
}

func TestTxRangePredicateCanPushToStorage(t *testing.T) {
	pred := &TxRangePredicate{
		TxVar: datalog.NewSymbol("?tx"),
		Low:   1000,
		High:  2000,
	}
	assert.True(t, pred.CanPushToStorage())
}
