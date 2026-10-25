package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A subquery's call passes a closed taxonomy of arguments: a Variable the
// subject or the environment binds, or a Constant. A variable nothing binds
// and an argument of any other kind are the clause's errors, never a nil
// bound into the inner query.

func TestSubqueryInputAssemblyRejectsUnresolvedVariable(t *testing.T) {
	x, v := datalog.NewSymbol("?x"), datalog.NewSymbol("?v")
	subq := &query.SubqueryPattern{
		Query: &query.Query{
			Find: []query.FindElement{query.FindVariable{Symbol: v}},
			In:   []query.InputSpec{query.ScalarInput{Symbol: x}},
		},
		Inputs:  []query.PatternElement{query.Variable{Name: datalog.NewSymbol("?missing")}},
		Binding: query.RelationBinding{Variables: []query.Symbol{v}},
	}
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})
	_, err := exec.executeSubquery(NewContext(), subq, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "?missing")
}

func TestSubqueryInputAssemblyRejectsUnknownElementKind(t *testing.T) {
	x, v := datalog.NewSymbol("?x"), datalog.NewSymbol("?v")
	subq := &query.SubqueryPattern{
		Query: &query.Query{
			Find: []query.FindElement{query.FindVariable{Symbol: v}},
			In:   []query.InputSpec{query.ScalarInput{Symbol: x}},
		},
		Inputs:  []query.PatternElement{query.Blank{}},
		Binding: query.RelationBinding{Variables: []query.Symbol{v}},
	}
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})
	_, err := exec.executeSubquery(NewContext(), subq, nil)
	require.Error(t, err)
}

// Empty subquery results take their schema from BindingForm.BoundVariables;
// per-form coverage is pinned by TestBindingFormBoundVariables in the query
// package.
