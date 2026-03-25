package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

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
