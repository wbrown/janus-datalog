package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A limit over a symmetric hash join of an expression's result with a held
// relation answers the expression's failure past the tuple it keeps, with
// the expression's result on either side, where each side holds more tuples
// than the join reads in one batch. The held relation holds a tuple at every
// key, the failing tuple's included, so the failure is at an assignment
// where every other clause holds.
func TestRefute_ALimitOverASymmetricHashJoinOfAnEvaluatingSideLongerThanABatchAnswersItsFailure(t *testing.T) {
	a, b, c := datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")
	plus := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:   datalog.SymAdd,
			Args: []query.Term{query.VariableTerm{Symbol: a}, query.ConstantTerm{Value: int64(1)}},
		},
		Binding: b,
	}
	const n = 250
	evaluated := func() Relation {
		values := make([]Tuple, 0, n+1)
		for i := 0; i < n; i++ {
			values = append(values, Tuple{int64(i)})
		}
		values = append(values, Tuple{"x"})
		return evaluateExpressionWithLookup(NewMaterializedRelation([]query.Symbol{a}, values), plus, nil, nil)
	}
	holding := func() Relation {
		held := make([]Tuple, 0, n+1)
		for i := 0; i < n; i++ {
			held = append(held, Tuple{int64(i), int64(7)})
		}
		held = append(held, Tuple{"x", int64(7)})
		return NewMaterializedRelation([]query.Symbol{a, c}, held)
	}
	on := []query.Symbol{a}
	opts := ExecutorOptions{}
	for _, j := range []struct {
		name string
		join func() Relation
	}{
		{"the expression's result on the left", func() Relation { return SymmetricHashJoinWithOptions(evaluated(), holding(), on, opts) }},
		{"the expression's result on the right", func() Relation { return SymmetricHashJoinWithOptions(holding(), evaluated(), on, opts) }},
	} {
		t.Run(j.name, func(t *testing.T) {
			kept, err := CollectTuples(NewLimitRelation(j.join(), 1), nil)
			require.ErrorContains(t, err, "is not a number", "the limit kept %v with no error", kept)
		})
	}
}
