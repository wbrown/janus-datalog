package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A context derived from one inside a fixpoint's body carries the fixpoint's
// held invocations, as it carries the rule set and the derived relations.
func TestContext_AHeldValueSurvivesEveryDerivation(t *testing.T) {
	ctx := NewContext().WithHeld(query.HeldByFixpoint)
	require.Equal(t, query.HeldByFixpoint, ctx.Held())
	require.Equal(t, query.HeldByFixpoint, ctx.WithEnvironment(nil).Held(), "WithEnvironment")
	require.Equal(t, query.HeldByFixpoint, ctx.WithRules(nil).Held(), "WithRules")
	require.Equal(t, query.HeldByFixpoint, ctx.WithDerived(nil).Held(), "WithDerived")
	require.Equal(t, query.HeldByFixpoint, ctx.WithContinuations(newRuleContinuations()).Held(), "WithContinuations")
	require.Equal(t, query.HeldByFixpoint, forkContext(ctx).Held(), "forkContext")
	require.Equal(t, query.HeldNone, NewContext().Held(), "a query's own context holds nothing")
}

// A scope's context holds the scope's required run under query.ScopeRequired
// beside every relation the enclosing context holds, carries no environment,
// keeps the run through every derivation made inside the scope, and a nested
// scope's run supersedes it there alone. The enclosing context holds no run.
func TestContext_AScopesRequiredRunIsHeldBesideTheDerivedRelations(t *testing.T) {
	reach := datalog.NewSymbol("reach")
	derived := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?a")}, []Tuple{{int64(1)}})
	run := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?s")}, []Tuple{{int64(2)}})
	env := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?in")}, []Tuple{{int64(3)}})
	enclosing := NewContext().WithEnvironment(env).WithDerived(map[query.Symbol]Relation{reach: derived})

	scope := enclosing.WithRequiredRun(run)
	got, held := scope.Derived(query.ScopeRequired)
	require.True(t, held, "the scope holds its required run")
	require.Same(t, run, got)
	got, held = scope.Derived(reach)
	require.True(t, held, "the scope holds the enclosing context's relations")
	require.Same(t, derived, got)
	require.Nil(t, scope.Environment(), "a scope carries no environment")

	for name, inner := range map[string]*Context{
		"WithEnvironment":   scope.WithEnvironment(nil),
		"WithRules":         scope.WithRules(nil),
		"WithHeld":          scope.WithHeld(query.HeldByFixpoint),
		"WithContinuations": scope.WithContinuations(newRuleContinuations()),
		"forkContext":       forkContext(scope),
	} {
		got, held := inner.Derived(query.ScopeRequired)
		require.True(t, held, name)
		require.Same(t, run, got, name)
	}

	nestedRun := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?t")}, []Tuple{{int64(4)}})
	nested := scope.WithRequiredRun(nestedRun)
	got, _ = nested.Derived(query.ScopeRequired)
	require.Same(t, nestedRun, got, "a nested scope's run supersedes the enclosing scope's")
	got, _ = scope.Derived(query.ScopeRequired)
	require.Same(t, run, got, "the enclosing scope keeps its own run")

	_, held = enclosing.Derived(query.ScopeRequired)
	require.False(t, held, "the enclosing context holds no run")
}
