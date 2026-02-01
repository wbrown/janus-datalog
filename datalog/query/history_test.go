package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
)

// =============================================================================
// HistoryPredicate Tests
// =============================================================================

func TestHistoryPredicateString(t *testing.T) {
	t.Run("history all", func(t *testing.T) {
		hp := &HistoryPredicate{
			Type: HistoryAll,
		}
		assert.Equal(t, "(history)", hp.String())
	})

	t.Run("as-of", func(t *testing.T) {
		hp := &HistoryPredicate{
			Type:          HistoryAsOf,
			TxVar:         datalog.NewSymbol("?tx"),
			TargetLamport: 5000,
		}
		assert.Equal(t, "(as-of ?tx 5000)", hp.String())
	})

	t.Run("unknown type", func(t *testing.T) {
		hp := &HistoryPredicate{
			Type: HistoryPredicateType(99),
		}
		assert.Equal(t, "(history ?)", hp.String())
	})
}

func TestHistoryPredicateRequiredSymbols(t *testing.T) {
	t.Run("history all has no required symbols", func(t *testing.T) {
		hp := &HistoryPredicate{
			Type: HistoryAll,
		}
		symbols := hp.RequiredSymbols()
		assert.Empty(t, symbols)
	})

	t.Run("as-of requires tx variable", func(t *testing.T) {
		hp := &HistoryPredicate{
			Type:          HistoryAsOf,
			TxVar:         datalog.NewSymbol("?tx"),
			TargetLamport: 5000,
		}
		symbols := hp.RequiredSymbols()
		require.Len(t, symbols, 1)
		assert.Equal(t, datalog.NewSymbol("?tx"), symbols[0])
	})

	t.Run("as-of with nil TxVar has no required symbols", func(t *testing.T) {
		hp := &HistoryPredicate{
			Type:          HistoryAsOf,
			TxVar:         nil,
			TargetLamport: 5000,
		}
		symbols := hp.RequiredSymbols()
		assert.Empty(t, symbols)
	})
}

func TestHistoryPredicateEval(t *testing.T) {
	t.Run("eval always returns true", func(t *testing.T) {
		hp := &HistoryPredicate{
			Type: HistoryAll,
		}
		bindings := map[Symbol]interface{}{
			datalog.NewSymbol("?e"):  "entity1",
			datalog.NewSymbol("?tx"): int64(1000),
		}
		result, err := hp.Eval(bindings)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("as-of eval also returns true", func(t *testing.T) {
		hp := &HistoryPredicate{
			Type:          HistoryAsOf,
			TxVar:         datalog.NewSymbol("?tx"),
			TargetLamport: 5000,
		}
		bindings := map[Symbol]interface{}{
			datalog.NewSymbol("?tx"): int64(1000),
		}
		result, err := hp.Eval(bindings)
		require.NoError(t, err)
		assert.True(t, result)
	})
}

func TestHistoryPredicateSelectivity(t *testing.T) {
	hp := &HistoryPredicate{
		Type: HistoryAll,
	}
	// History predicates don't filter, so selectivity is 1.0
	assert.Equal(t, 1.0, hp.Selectivity())
}

func TestHistoryPredicateCanPushToStorage(t *testing.T) {
	hp := &HistoryPredicate{
		Type: HistoryAll,
	}
	// History predicates are handled at storage level
	assert.True(t, hp.CanPushToStorage())
}

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

// =============================================================================
// Query Helper Method Tests
// =============================================================================

func TestQueryIsHistoryQuery(t *testing.T) {
	t.Run("query without history predicate", func(t *testing.T) {
		q := &Query{
			Where: []Clause{
				&DataPattern{
					Elements: []PatternElement{
						Variable{Name: datalog.NewSymbol("?e")},
						Constant{Value: datalog.NewKeyword(":person/name")},
						Variable{Name: datalog.NewSymbol("?name")},
					},
				},
			},
		}
		assert.False(t, q.IsHistoryQuery())
	})

	t.Run("query with history predicate", func(t *testing.T) {
		q := &Query{
			Where: []Clause{
				&DataPattern{
					Elements: []PatternElement{
						Variable{Name: datalog.NewSymbol("?e")},
						Constant{Value: datalog.NewKeyword(":person/name")},
						Variable{Name: datalog.NewSymbol("?name")},
					},
				},
				&HistoryPredicate{
					Type: HistoryAll,
				},
			},
		}
		assert.True(t, q.IsHistoryQuery())
	})
}

func TestQueryGetHistoryPredicate(t *testing.T) {
	t.Run("query without history predicate returns nil", func(t *testing.T) {
		q := &Query{
			Where: []Clause{
				&DataPattern{
					Elements: []PatternElement{
						Variable{Name: datalog.NewSymbol("?e")},
						Constant{Value: datalog.NewKeyword(":person/name")},
						Variable{Name: datalog.NewSymbol("?name")},
					},
				},
			},
		}
		assert.Nil(t, q.GetHistoryPredicate())
	})

	t.Run("query with history predicate returns it", func(t *testing.T) {
		hp := &HistoryPredicate{
			Type:          HistoryAsOf,
			TxVar:         datalog.NewSymbol("?tx"),
			TargetLamport: 5000,
		}
		q := &Query{
			Where: []Clause{
				&DataPattern{
					Elements: []PatternElement{
						Variable{Name: datalog.NewSymbol("?e")},
						Constant{Value: datalog.NewKeyword(":person/name")},
						Variable{Name: datalog.NewSymbol("?name")},
					},
				},
				hp,
			},
		}
		result := q.GetHistoryPredicate()
		require.NotNil(t, result)
		assert.Equal(t, HistoryAsOf, result.Type)
		assert.Equal(t, uint64(5000), result.TargetLamport)
	})
}
