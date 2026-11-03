package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ruleEvents holds the events a handler saw.
type ruleEvents struct{ events []annotations.Event }

func (r *ruleEvents) handle(e annotations.Event) { r.events = append(r.events, e) }

// count is how many events of one name were seen.
func (r *ruleEvents) count(name string) int {
	n := 0
	for _, e := range r.events {
		if e.Name == name {
			n++
		}
	}
	return n
}

// entered is the tuples that entered a delta: the delta sizes of every
// iteration event, summed.
func (r *ruleEvents) entered() int {
	n := 0
	for _, e := range r.events {
		if e.Name != annotations.RulesIteration {
			continue
		}
		for _, size := range e.Data["delta"].(map[query.Symbol]int) {
			n += size
		}
	}
	return n
}

// predicates is every predicate an iteration event reported a delta for.
func (r *ruleEvents) predicates() []query.Symbol {
	var out []query.Symbol
	seen := map[query.Symbol]bool{}
	for _, e := range r.events {
		if e.Name != annotations.RulesIteration {
			continue
		}
		for pred := range e.Data["delta"].(map[query.Symbol]int) {
			if !seen[pred] {
				seen[pred] = true
				out = append(out, pred)
			}
		}
	}
	return out
}

// The continuations a context carries reach every context derived from it.
func TestContext_ContinuationsAreCarriedByEveryDerivedContext(t *testing.T) {
	continued := newRuleContinuations()
	ctx := NewContext().WithContinuations(continued)
	require.Same(t, continued, ctx.continued)
	for name, derived := range map[string]*Context{
		"WithEnvironment":  ctx.WithEnvironment(nil),
		"WithDerived":      ctx.WithDerived(nil),
		"WithRules":        ctx.WithRules(nil),
		"WithCancellation": ctx.WithCancellation(context.Background()),
		"forkContext":      forkContext(ctx),
	} {
		require.Same(t, continued, derived.continued, name)
	}
	require.Nil(t, NewContext().continued, "a context carries no continuations until a form execution gives it some")
}

// An invocation's evaluation is the one planned for its clause, read from
// the plan's record alone: whole when nothing is bound and no required
// predicate is reached; over the rewrite for the bound positions, with the
// seed predicate the windows fill, when something is bound; and over the
// rewrite with its head all free, unseeded, for a free invocation of a
// predicate reaching one with required positions. Each rewrite is reported
// once through the magic-sets event.
func TestInvocationEvaluation_IsWholeWithNothingBoundAndRewrittenOtherwise(t *testing.T) {
	follows := datalog.NewSymbol("follows")
	x, y := datalog.NewSymbol("?x"), datalog.NewSymbol("?y")
	inv := &query.RuleInvocation{Name: follows, Args: []query.PatternElement{query.Variable{Name: x}, query.Variable{Name: y}}}
	events := &ruleEvents{}
	opts := defaultPlannerOptions()
	opts.Handler = events.handle
	exec := NewExecutorWithOptions(nil, nil, opts).queryExecutor(nil)
	rs := mustRules(t, followsRulesText)

	plannedWhole, err := planner.PlanEvaluation(exec.executor.planner, rs, follows, []bool{false, false}, nil)
	require.NoError(t, err)
	whole := exec.invocationEvaluation(inv, plannedWhole)
	require.Same(t, rs, whole.rs)
	require.Equal(t, follows, whole.predicate)
	require.Nil(t, whole.seed, "nothing bound: the predicate whole")
	require.Zero(t, events.count(annotations.RulesMagicSets))

	plannedBound, err := planner.PlanEvaluation(exec.executor.planner, rs, follows, []bool{true, false}, nil)
	require.NoError(t, err)
	bound := exec.invocationEvaluation(inv, plannedBound)
	require.NotSame(t, rs, bound.rs, "something bound: the rewritten set")
	require.NotEqual(t, follows, bound.predicate, "the adorned predicate")
	require.NotNil(t, bound.seed, "the seed predicate the windows fill")
	require.Equal(t, 1, events.count(annotations.RulesMagicSets), "the rewrite is reported once")

	reaching := mustRules(t, `[[(near [?p] ?q) [?p :page/next ?q]] [(hop ?a ?c) [?a :page/next ?b] (near ?b ?c)]]`)
	hop := datalog.NewSymbol("hop")
	hopInv := &query.RuleInvocation{Name: hop, Args: []query.PatternElement{query.Variable{Name: x}, query.Variable{Name: y}}}
	plannedAllFree, err := planner.PlanEvaluation(exec.executor.planner, reaching, hop, []bool{false, false}, nil)
	require.NoError(t, err)
	allFree := exec.invocationEvaluation(hopInv, plannedAllFree)
	require.NotSame(t, reaching, allFree.rs, "a required predicate reached: the rewritten set")
	require.NotEqual(t, hop, allFree.predicate, "the predicate adorned all free")
	require.Nil(t, allFree.seed, "nothing bound: no seed")
	require.Equal(t, 2, events.count(annotations.RulesMagicSets), "the all-free rewrite is reported once")
}
