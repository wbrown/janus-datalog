package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
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
