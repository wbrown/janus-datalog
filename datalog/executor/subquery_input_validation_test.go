package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Subquery input assembly is a closed taxonomy: an input element is a
// Variable resolved from the outer relation, or a Constant (including source
// markers). An unresolvable variable or an unknown element kind is a loud
// error — previously both silently appended nil as the binding value, feeding
// a nil into the nested query.

func TestSubqueryInputAssemblyRejectsUnresolvedVariable(t *testing.T) {
	subq := &query.SubqueryPattern{
		Query:  &query.Query{In: []query.InputSpec{query.ScalarInput{Symbol: datalog.NewSymbol("?x")}}},
		Inputs: []query.PatternElement{query.Variable{Name: datalog.NewSymbol("?missing")}},
	}
	_, err := subqueryInputRelations(subq, nil, nil, ExecutorOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "?missing")
}

func TestSubqueryInputAssemblyRejectsUnknownElementKind(t *testing.T) {
	subq := &query.SubqueryPattern{
		Query:  &query.Query{In: []query.InputSpec{query.ScalarInput{Symbol: datalog.NewSymbol("?x")}}},
		Inputs: []query.PatternElement{query.Blank{}},
	}
	_, err := subqueryInputRelations(subq, nil, nil, ExecutorOptions{})
	require.Error(t, err)
}

// Empty subquery results take their schema from BindingForm.BoundVariables;
// per-form coverage is pinned by TestBindingFormBoundVariables in the query
// package.
