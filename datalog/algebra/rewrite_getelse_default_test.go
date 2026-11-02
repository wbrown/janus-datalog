package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// rewriteGetElse compiles text, runs the get-else rewrite over it collecting
// the rewrite's records, and decompiles the result.
func rewriteGetElse(t *testing.T, text string) ([]query.Clause, []RewriteRecord) {
	t.Helper()
	q, err := parser.ParseQuery(text)
	require.NoError(t, err)
	root, err := Compile(q, nil)
	require.NoError(t, err)
	sink := &RewriteSink{Collect: true}
	optimized, err := NewOptimizer(GetElseScanRewritePass(sink)).Optimize(root)
	require.NoError(t, err)
	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	return clauses, sink.Records()
}

// A variable default the child relation provides is carried into the default
// branch: the join decompiles to an or-default-join requiring the entity and
// the default's variable, whose default branch is the get-else reading that
// variable.
func TestGetElseScanRewrite_CarriesAVariableDefaultIntoTheDefaultBranch(t *testing.T) {
	clauses, records := rewriteGetElse(t, `[:find ?e ?v :where [?e :t/n _] [?e :t/fallback ?d] [(get-else $ ?e :t/a ?d) ?v]]`)
	require.NotEmpty(t, records, "the rewrite records its decision on the get-else")
	for _, rec := range records {
		require.Equal(t, RewriteApplied, rec.Action, "record %+v", rec)
	}

	e, d := datalog.NewSymbol("?e"), datalog.NewSymbol("?d")
	var forms []*query.OrDefaultJoinClause
	query.WalkClauses(clauses, func(clause query.Clause) bool {
		if form, ok := clause.(*query.OrDefaultJoinClause); ok {
			forms = append(forms, form)
		}
		return true
	})
	require.Len(t, forms, 1, "the rewrite decompiles to one or-default-join")
	require.ElementsMatch(t, []query.Symbol{e, d}, forms[0].RequiredVars, "the form requires the entity and the default's variable")
	require.Len(t, forms[0].Branches, 2)
	require.Len(t, forms[0].Branches[1], 1)
	expr, ok := forms[0].Branches[1][0].(*query.Expression)
	require.True(t, ok, "the default branch is an expression, got %T", forms[0].Branches[1][0])
	getElse, ok := expr.Function.(*query.GetElseFunction)
	require.True(t, ok, "the default branch is a get-else, got %T", expr.Function)
	require.Equal(t, query.VariableTerm{Symbol: d}, getElse.Default)
}

// A variable default the child relation does not provide, here an input
// parameter, leaves the get-else to the per-tuple evaluation, and the rewrite
// records why.
func TestGetElseScanRewrite_DeclinesADefaultTheChildRelationDoesNotProvide(t *testing.T) {
	clauses, records := rewriteGetElse(t, `[:find ?e ?v :in $ ?d :where [?e :t/n _] [(get-else $ ?e :t/a ?d) ?v]]`)
	require.NotEmpty(t, records, "the rewrite records its decision on the get-else")
	for _, rec := range records {
		require.Equal(t, RewriteDeclined, rec.Action, "record %+v", rec)
		require.Equal(t, "default variable is not provided by the child relation", rec.Reason)
	}

	var getElses []*query.GetElseFunction
	query.WalkClauses(clauses, func(clause query.Clause) bool {
		if expr, ok := clause.(*query.Expression); ok {
			if getElse, ok := expr.Function.(*query.GetElseFunction); ok {
				getElses = append(getElses, getElse)
			}
		}
		return true
	})
	require.Len(t, getElses, 1, "the get-else stays an expression")
	require.Equal(t, query.VariableTerm{Symbol: datalog.NewSymbol("?d")}, getElses[0].Default)
}
