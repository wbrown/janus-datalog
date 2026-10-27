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

// A semi join or an anti join on a symbol a side lacks is the query's error
// in the same words: the error names the symbol and reaches the caller when
// the result is driven, through the package functions and the relation's
// methods alike, and never as a panic. On a symbol both sides carry the semi
// join answers the left tuples with a match and the anti join the left tuples
// without one, under the left side's symbols.
func TestSemiAndAntiJoin_AKeySymbolASideLacksIsAnError(t *testing.T) {
	a, b, c, z := datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c"), datalog.NewSymbol("?z")
	left := NewMaterializedRelation([]query.Symbol{a, b}, []Tuple{{int64(1), int64(2)}, {int64(4), int64(5)}})
	right := NewMaterializedRelation([]query.Symbol{a, c}, []Tuple{{int64(1), int64(3)}})
	matched := [][]interface{}{{int64(1), int64(2)}}
	unmatched := [][]interface{}{{int64(4), int64(5)}}
	joins := []struct {
		name string
		join func(syms []query.Symbol) Relation
		onA  [][]interface{}
	}{
		{"SemiJoin", func(syms []query.Symbol) Relation { return SemiJoin(left, right, syms) }, matched},
		{"AntiJoin", func(syms []query.Symbol) Relation { return AntiJoin(left, right, syms) }, unmatched},
		{"Relation.SemiJoin", func(syms []query.Symbol) Relation { return left.SemiJoin(right, syms) }, matched},
		{"Relation.AntiJoin", func(syms []query.Symbol) Relation { return left.AntiJoin(right, syms) }, unmatched},
	}
	for _, j := range joins {
		t.Run(j.name, func(t *testing.T) {
			joined := j.join([]query.Symbol{a})
			require.Equal(t, []query.Symbol{a, b}, joined.Symbols())
			tuples, err := CollectTuples(joined, nil)
			require.NoError(t, err)
			require.Equal(t, j.onA, tuples)
			for _, missing := range []query.Symbol{b, c, z} {
				var err error
				require.NotPanics(t, func() { _, err = CollectTuples(j.join([]query.Symbol{missing}), nil) },
					"a join on a symbol a side lacks is an error, never a panic")
				require.ErrorContains(t, err, missing.String(), "a join on a symbol a side lacks is an error naming the symbol")
			}
		})
	}
}
