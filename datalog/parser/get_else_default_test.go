package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// get-else's default is a function argument, and Datomic's grammar gives a
// function argument as a variable, a constant or a source. A variable default
// parses as the variable, renders as it, and parses back to the same query.
func TestGetElse_TakesAVariableDefault(t *testing.T) {
	q, err := ParseQuery(`[:find ?v :in $ ?d :where [?e :t/n _] [(get-else $ ?e :t/a ?d) ?v]]`)
	require.NoError(t, err)
	expr, ok := q.Where[1].(*query.Expression)
	require.True(t, ok, "the get-else clause is an expression, got %T", q.Where[1])
	getElse, ok := expr.Function.(*query.GetElseFunction)
	require.True(t, ok, "the expression's function is get-else, got %T", expr.Function)
	require.Equal(t, query.VariableTerm{Symbol: datalog.NewSymbol("?d")}, getElse.Default)

	rendered := q.String()
	require.Contains(t, rendered, `(get-else $ ?e :t/a ?d)`, "the rendering names the default variable")
	back, err := ParseQuery(rendered)
	require.NoError(t, err)
	require.True(t, q.Equal(back), "the rendering parses back to the same query")
	require.Equal(t, q.Hash(), back.Hash())
}

// Two texts that differ only in whether get-else's default is a variable or
// a constant are two queries.
func TestGetElse_DefaultDistinguishesTwoQueries(t *testing.T) {
	onVariable, err := ParseQuery(`[:find ?v :in $ ?d :where [?e :t/n _] [(get-else $ ?e :t/a ?d) ?v]]`)
	require.NoError(t, err)
	onConstant, err := ParseQuery(`[:find ?v :in $ ?d :where [?e :t/n _] [(get-else $ ?e :t/a "?d") ?v]]`)
	require.NoError(t, err)
	require.False(t, onVariable.Equal(onConstant))
	require.NotEqual(t, onVariable.Hash(), onConstant.Hash())
}
