package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// An expression clause evaluates as its result is pulled: executing it over a
// tuple outside the function's domain answers the relation over the input's
// symbols and the binding's, and the failure surfaces from that relation when
// it is driven.
func TestExpressionEvaluatesAsItsResultIsPulled(t *testing.T) {
	exec := newQueryExecutor(&MockMatcher{}, nil, ExecutorOptions{})
	n, m := datalog.NewSymbol("?n"), datalog.NewSymbol("?m")
	names := NewMaterializedRelation([]query.Symbol{n}, []Tuple{{"Alice"}})
	expr := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:   datalog.SymAdd,
			Args: []query.Term{query.VariableTerm{Symbol: n}, query.ConstantTerm{Value: int64(1)}},
		},
		Binding: m,
	}

	groups, err := exec.executeExpression(NewContext(), expr, []Relation{names})
	require.NoError(t, err, "executing the clause evaluates nothing")
	require.Len(t, groups, 1)
	require.Equal(t, []query.Symbol{n, m}, groups[0].Symbols())

	_, err = CollectTuples(groups[0], nil)
	require.ErrorContains(t, err, "is not a number", "the failure surfaces when the result is driven")
}
