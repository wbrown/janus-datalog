package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func findOp(n *Node, op string) *Node {
	if n == nil {
		return nil
	}
	if n.Op == op {
		return n
	}
	for _, child := range n.Children {
		if found := findOp(child, op); found != nil {
			return found
		}
	}
	return nil
}

func TestCompileRuleInvocationRoundTrips(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?x ?y ?k
	                              :in $ %
	                              :where [?x :page/kind ?k]
	                                     (follows ?x ?y)
	                                     (marked ?y "read" _)
	                                     [(> ?k 1)]]`)
	require.NoError(t, err)

	compiled, err := Compile(q)
	require.NoError(t, err)
	analysis, err := Analyze(compiled)
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?k"), datalog.NewSymbol("?y")},
		analysis[compiled].Output)
	require.Empty(t, analysis[compiled].Required, "an invocation requires nothing of the enclosing scope")

	derived := findOp(compiled, RuleDerived)
	require.NotNil(t, derived, "the invocation compiles to a Derived node:\n%s", compiled)
	data, ok := derived.Data.(*Derived)
	require.True(t, ok, "Derived node data is %T", derived.Data)
	require.Same(t, q.Where[1], data.Invocation)
	require.Equal(t, []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}, data.Output)
	require.Equal(t, data.Output, analysis[derived].Output)
	require.Empty(t, derived.Children, "a Derived node is a leaf")

	optimized, err := NewOptimizer(DefaultPasses(&RewriteSink{})...).Optimize(compiled)
	require.NoError(t, err)
	refreshed, err := RefreshSchemas(optimized)
	require.NoError(t, err)
	clauses, err := Decompile(refreshed)
	require.NoError(t, err)

	var invocations []*query.RuleInvocation
	for _, c := range clauses {
		if inv, ok := c.(*query.RuleInvocation); ok {
			invocations = append(invocations, inv)
		}
	}
	require.Len(t, invocations, 2, "both invocations decompile: %v", clauses)
	require.Same(t, q.Where[1], invocations[0], "decompilation yields the clause itself")
	require.Same(t, q.Where[2], invocations[1])
	require.Equal(t, []query.Symbol{datalog.NewSymbol("?y")}, invocations[1].Variables(),
		"a constant and a blank argument bind nothing")
}

func TestRefreshSchemasDerived(t *testing.T) {
	inv := &query.RuleInvocation{
		Name: datalog.NewSymbol("follows"),
		Args: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?x")},
			query.Variable{Name: datalog.NewSymbol("?y")},
		},
	}
	node := &Node{Op: RuleDerived, Data: &Derived{Invocation: inv, Output: inv.Variables()}}
	refreshed, err := RefreshSchemas(node)
	require.NoError(t, err)
	require.NotSame(t, node, refreshed)
	data := refreshed.Data.(*Derived)
	require.Same(t, inv, data.Invocation)
	require.Equal(t, inv.Variables(), data.Output)
	require.Equal(t, "(follows ?x ?y)", data.String())
}
