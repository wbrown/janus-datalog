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
	_, err := createInputRelationsFromPatternWithOptions(subq, map[query.Symbol]interface{}{}, ExecutorOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "?missing")
}

func TestSubqueryInputAssemblyRejectsUnknownElementKind(t *testing.T) {
	subq := &query.SubqueryPattern{
		Query:  &query.Query{In: []query.InputSpec{query.ScalarInput{Symbol: datalog.NewSymbol("?x")}}},
		Inputs: []query.PatternElement{query.Blank{}},
	}
	_, err := createInputRelationsFromPatternWithOptions(subq, map[query.Symbol]interface{}{}, ExecutorOptions{})
	require.Error(t, err)
}

// extractBindingSymbols shapes empty subquery results; it must cover every
// BindingForm. ScalarBinding was previously absorbed by a silent default,
// dropping the bound symbol from the empty result's schema.
func TestExtractBindingSymbolsCoversEveryBindingForm(t *testing.T) {
	x, y := datalog.NewSymbol("?x"), datalog.NewSymbol("?y")
	require.Equal(t, []query.Symbol{x}, extractBindingSymbols(query.ScalarBinding{Variable: x}))
	require.Equal(t, []query.Symbol{x}, extractBindingSymbols(query.CollectionBinding{Variable: x}))
	require.Equal(t, []query.Symbol{x, y}, extractBindingSymbols(query.TupleBinding{Variables: []query.Symbol{x, y}}))
	require.Equal(t, []query.Symbol{x, y}, extractBindingSymbols(query.RelationBinding{Variables: []query.Symbol{x, y}}))
}
