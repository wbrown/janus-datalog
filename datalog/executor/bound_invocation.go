package executor

import (
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A bound invocation at the top level composed over the modules
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2, §4.4, §4.8): the
// invocation is one body under the header of its bound variables as the
// required run and its free variables as the outputs, built once per window
// over the window's distinct keys less those an earlier window ran, its
// fixpoint seeded by the window's keys and continued across the windows, and
// probed per window tuple. The result is the subject's tuples each joined
// with the tuples derived for its key, over the subject's symbols and the
// invocation's free variables; it stands in the consumed groups' place,
// reports no size, and reads the subject once.

// executeBoundInvocation runs the invocation as one body over the window of
// its subject, the groups holding a variable argument joined into one, under
// the header of the variable arguments the subject or the environment binds,
// with its other variable arguments as the outputs, framed through
// planner.WindowFraming as the planner's walk frames it. The environment's
// values for the required arguments the subject lacks ride the frames'
// environment relation, as a not-join's do. The result's keys survive when
// the invocation has no output, a verdict per key. The frame's invocation
// clause stands for inv in the form execution's continuations, so a
// derivation at either continues inv's one evaluation (ruleContinuations).
func (e *DefaultQueryExecutor) executeBoundInvocation(ctx *Context, inv *query.RuleInvocation, groups Relations) (Relations, error) {
	symbols := inv.Variables()
	subject, consumed := e.findOuterRelation(symbols, groups)
	outerSyms := subject.Symbols()
	envSymbols, _ := environmentTuple(ctx.Environment())
	framed := planner.WindowFraming(symbols, append(append([]query.Symbol(nil), outerSyms...), envSymbols...))
	required, find := framed.Required, framed.Find
	outputs := make([]query.Symbol, len(find))
	for i, elem := range find {
		outputs[i] = elem.(query.FindVariable).Symbol
	}
	environment, err := environmentRelation(ctx, required, outerSyms)
	if err != nil {
		return nil, err
	}
	resultSyms := append(append([]query.Symbol(nil), outerSyms...), outputs...)
	properties := orProperties(subject.Properties(), outputs, nil, resultSyms, len(outputs) == 0)

	f := framing{
		form:        "invocation",
		subject:     subject,
		required:    required,
		find:        find,
		given:       required,
		standsFor:   required,
		key:         keySymbols(required, outerSyms),
		environment: environment,
	}
	plans, err := frameBranches(e, ctx, [][]query.Clause{{inv}}, f)
	if err != nil {
		return nil, err
	}
	it := &subqueryIterator{
		windowedIterator: newWindowedIterator(e, ctx, f, plans, e.options),
		matchEmit:        matchEmit{outerSyms: outerSyms, outputSyms: resultSyms},
	}
	it.continued.standFor(framedInvocation(plans[0]), inv)
	it.policy = formPolicy{answer: it.answer}
	result := NewStreamingRelationWithProperties(resultSyms, it, e.options, properties)
	return replaceConsumedGroups(groups, consumed, result, e.options.Handler), nil
}

// framedInvocation is the invocation clause a bound invocation's frame plan
// holds: the frame's one clause other than scope/required. The plan is served
// from the cache under the frame's text, so the clause is the object of the
// plan as first made, whichever framing made it.
func framedInvocation(branch branchRun) *query.RuleInvocation {
	for _, phase := range branch.plan.Phases {
		for _, clause := range phase.Query.Where {
			if framed, ok := clause.(*query.RuleInvocation); ok && framed.Name != query.ScopeRequired {
				return framed
			}
		}
	}
	panic("bound invocation: the frame's plan holds no invocation clause other than scope/required")
}
