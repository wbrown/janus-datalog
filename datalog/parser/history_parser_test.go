package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// =============================================================================
// History Predicate Parsing Tests
// =============================================================================

func TestParseHistoryPredicate(t *testing.T) {
	t.Run("valid history predicate with no args", func(t *testing.T) {
		args := []query.PatternElement{}
		pred, err := parseHistoryPredicate(args)
		require.NoError(t, err)

		hp, ok := pred.(*query.HistoryPredicate)
		require.True(t, ok)
		assert.Equal(t, query.HistoryAll, hp.Type)
	})

	t.Run("history predicate with args returns error", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?x")},
		}
		_, err := parseHistoryPredicate(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "takes no arguments")
	})
}

func TestParseAsOfPredicate(t *testing.T) {
	t.Run("valid as-of predicate", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: int64(5000)},
		}
		pred, err := parseAsOfPredicate(args)
		require.NoError(t, err)

		hp, ok := pred.(*query.HistoryPredicate)
		require.True(t, ok)
		assert.Equal(t, query.HistoryAsOf, hp.Type)
		assert.Equal(t, datalog.NewSymbol("?tx"), hp.TxVar)
		assert.Equal(t, uint64(5000), hp.TargetLamport)
	})

	t.Run("as-of with int (not int64)", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: 5000},
		}
		pred, err := parseAsOfPredicate(args)
		require.NoError(t, err)

		hp := pred.(*query.HistoryPredicate)
		assert.Equal(t, uint64(5000), hp.TargetLamport)
	})

	t.Run("as-of with uint64", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: uint64(5000)},
		}
		pred, err := parseAsOfPredicate(args)
		require.NoError(t, err)

		hp := pred.(*query.HistoryPredicate)
		assert.Equal(t, uint64(5000), hp.TargetLamport)
	})

	t.Run("as-of with wrong number of args", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
		}
		_, err := parseAsOfPredicate(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly 2 arguments")
	})

	t.Run("as-of first arg not variable", func(t *testing.T) {
		args := []query.PatternElement{
			query.Constant{Value: int64(1000)},
			query.Constant{Value: int64(5000)},
		}
		_, err := parseAsOfPredicate(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be a variable")
	})

	t.Run("as-of second arg not integer", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: "not-a-number"},
		}
		_, err := parseAsOfPredicate(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be an integer")
	})

	t.Run("as-of second arg is variable (not allowed)", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Variable{Name: datalog.NewSymbol("?lamport")},
		}
		_, err := parseAsOfPredicate(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "constant integer")
	})
}

func TestParseTxBetweenPredicate(t *testing.T) {
	t.Run("valid tx-between predicate", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: int64(1000)},
			query.Constant{Value: int64(2000)},
		}
		pred, err := parseTxBetweenPredicate(args)
		require.NoError(t, err)

		tp, ok := pred.(*query.TxRangePredicate)
		require.True(t, ok)
		assert.Equal(t, datalog.NewSymbol("?tx"), tp.TxVar)
		assert.Equal(t, uint64(1000), tp.Low)
		assert.Equal(t, uint64(2000), tp.High)
	})

	t.Run("tx-between with int values", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: 1000},
			query.Constant{Value: 2000},
		}
		pred, err := parseTxBetweenPredicate(args)
		require.NoError(t, err)

		tp := pred.(*query.TxRangePredicate)
		assert.Equal(t, uint64(1000), tp.Low)
		assert.Equal(t, uint64(2000), tp.High)
	})

	t.Run("tx-between with uint64 values", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: uint64(1000)},
			query.Constant{Value: uint64(2000)},
		}
		pred, err := parseTxBetweenPredicate(args)
		require.NoError(t, err)

		tp := pred.(*query.TxRangePredicate)
		assert.Equal(t, uint64(1000), tp.Low)
		assert.Equal(t, uint64(2000), tp.High)
	})

	t.Run("tx-between with equal bounds", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: int64(1000)},
			query.Constant{Value: int64(1000)},
		}
		pred, err := parseTxBetweenPredicate(args)
		require.NoError(t, err)

		tp := pred.(*query.TxRangePredicate)
		assert.Equal(t, uint64(1000), tp.Low)
		assert.Equal(t, uint64(1000), tp.High)
	})

	t.Run("tx-between wrong number of args", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: int64(1000)},
		}
		_, err := parseTxBetweenPredicate(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly 3 arguments")
	})

	t.Run("tx-between first arg not variable", func(t *testing.T) {
		args := []query.PatternElement{
			query.Constant{Value: int64(500)},
			query.Constant{Value: int64(1000)},
			query.Constant{Value: int64(2000)},
		}
		_, err := parseTxBetweenPredicate(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be a variable")
	})

	t.Run("tx-between low > high returns error", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: int64(2000)},
			query.Constant{Value: int64(1000)},
		}
		_, err := parseTxBetweenPredicate(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be <= high")
	})

	t.Run("tx-between negative low value", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: int64(-1)},
			query.Constant{Value: int64(2000)},
		}
		_, err := parseTxBetweenPredicate(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-negative")
	})

	t.Run("tx-between non-integer low value", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: "not-a-number"},
			query.Constant{Value: int64(2000)},
		}
		_, err := parseTxBetweenPredicate(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be an integer")
	})

	t.Run("tx-between variable as bound (not allowed)", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Variable{Name: datalog.NewSymbol("?low")},
			query.Constant{Value: int64(2000)},
		}
		_, err := parseTxBetweenPredicate(args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "constant integer")
	})
}

func TestParsePredicateDispatch(t *testing.T) {
	t.Run("parsePredicate routes to history", func(t *testing.T) {
		args := []query.PatternElement{}
		pred, err := parsePredicate("history", args)
		require.NoError(t, err)

		_, ok := pred.(*query.HistoryPredicate)
		assert.True(t, ok)
	})

	t.Run("parsePredicate routes to as-of", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: int64(5000)},
		}
		pred, err := parsePredicate("as-of", args)
		require.NoError(t, err)

		hp, ok := pred.(*query.HistoryPredicate)
		assert.True(t, ok)
		assert.Equal(t, query.HistoryAsOf, hp.Type)
	})

	t.Run("parsePredicate routes to tx-between", func(t *testing.T) {
		args := []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?tx")},
			query.Constant{Value: int64(1000)},
			query.Constant{Value: int64(2000)},
		}
		pred, err := parsePredicate("tx-between", args)
		require.NoError(t, err)

		_, ok := pred.(*query.TxRangePredicate)
		assert.True(t, ok)
	})
}
