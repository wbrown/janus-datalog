package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A hash join on a symbol a side lacks is the query's error, never an empty
// answer: the error names the symbol and reaches the caller when the result
// is driven, through the package functions, the executor's methods and the
// relation's method alike. A join on a symbol both sides carry answers the
// joined tuples under the union of the symbols.
func TestHashJoin_AKeySymbolASideLacksIsAnError(t *testing.T) {
	a, b, c, z := datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c"), datalog.NewSymbol("?z")
	left := NewMaterializedRelation([]query.Symbol{a, b}, []Tuple{{int64(1), int64(2)}})
	right := NewMaterializedRelation([]query.Symbol{a, c}, []Tuple{{int64(1), int64(3)}})
	exec := NewExecutor(NewMemoryPatternMatcher(nil), nil)
	joins := []struct {
		name string
		join func(syms []query.Symbol) Relation
	}{
		{"HashJoin", func(syms []query.Symbol) Relation { return HashJoin(left, right, syms) }},
		{"SymmetricHashJoin", func(syms []query.Symbol) Relation { return SymmetricHashJoin(left, right, syms) }},
		{"Executor.HashJoin", func(syms []query.Symbol) Relation { return exec.HashJoin(left, right, syms) }},
		{"Executor.SymmetricHashJoin", func(syms []query.Symbol) Relation { return exec.SymmetricHashJoin(left, right, syms) }},
		{"Relation.HashJoin", func(syms []query.Symbol) Relation { return left.HashJoin(right, syms) }},
	}
	for _, j := range joins {
		t.Run(j.name, func(t *testing.T) {
			joined := j.join([]query.Symbol{a})
			require.Equal(t, []query.Symbol{a, b, c}, joined.Symbols())
			tuples, err := CollectTuples(joined, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), int64(2), int64(3)}}, tuples)
			for _, missing := range []query.Symbol{b, z} {
				_, err := CollectTuples(j.join([]query.Symbol{missing}), nil)
				require.ErrorContains(t, err, missing.String(), "a join on a symbol a side lacks is an error naming the symbol")
			}
		})
	}
}
