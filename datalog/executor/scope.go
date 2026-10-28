package executor

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// planScope plans the scope under the rule set the enclosing query runs
// with and the invocations the context holds, through the executor's
// planner. The plan is served from the plan cache under the scope's query
// text and those bindings, the ones the planner's walk at PlanQuery planned
// it under, so a scope is planned once however many runs read it.
func (e *DefaultQueryExecutor) planScope(ctx *Context, s planner.Scope) (*planner.RealizedPlan, error) {
	return e.executor.planner.PlanScope(s, query.Bindings{Rules: ctx.Rules(), Held: ctx.Held()}, e.options.Handler)
}

// runPlan runs a scope's plan once over run, the relation of its required
// run, and returns the scope's result over its header. The plan runs under
// a context that holds run as the scope's required run beside every relation
// the enclosing context holds, so an invocation inside the scope of a
// predicate the fixpoint is deriving reads the stratum's relations. The
// context carries no environment: a query input reaches a scope only through
// the header. The run reports no phase: the form's own events report a
// branch's execution, and a phase event per window would be a handler call
// per window beside the window's own event.
func (e *DefaultQueryExecutor) runPlan(ctx *Context, plan *planner.RealizedPlan, run Relation) (Relation, error) {
	return e.executor.runPhases(ctx.WithRequiredRun(run), plan, nil, nil)
}

// runScope plans the scope and runs it once over run.
func (e *DefaultQueryExecutor) runScope(ctx *Context, s planner.Scope, run Relation) (Relation, error) {
	plan, err := e.planScope(ctx, s)
	if err != nil {
		return nil, err
	}
	return e.runPlan(ctx, plan, run)
}

// requiredRun is the run a form's scopes run over on the unit route, where no
// group holds a header symbol: the environment's tuple over the required
// variables, which the environment alone binds, and the unit relation when
// the required run is empty. A required variable the environment does not
// bind is the projection's error.
func (e *DefaultQueryExecutor) requiredRun(ctx *Context, required []query.Symbol) (*MaterializedRelation, error) {
	if len(required) == 0 {
		return e.executor.unit, nil
	}
	var outer Relation = e.executor.unit
	if env := ctx.Environment(); env != nil {
		var fromEnvironment []query.Symbol
		for _, sym := range required {
			if query.ContainsSymbol(env.Symbols(), sym) {
				fromEnvironment = append(fromEnvironment, sym)
			}
		}
		if len(fromEnvironment) > 0 {
			values, err := env.Project(fromEnvironment)
			if err != nil {
				return nil, fmt.Errorf("the environment's required variables: %w", err)
			}
			outer = outer.Join(values)
		}
	}
	projected, err := outer.Project(required)
	if err != nil {
		return nil, fmt.Errorf("the required run: %w", err)
	}
	var tuples []Tuple
	if err := collectTuplesInto(&tuples, projected); err != nil {
		return nil, fmt.Errorf("the required run: %w", err)
	}
	return newMaterializedRelationFromSet(required, tuples, e.options, deduplicatedProperties(required)), nil
}

// scopeResult unions the results of a form's scopes over the header and
// joins the union with the environment's tuple on the outputs the
// environment binds: an output another clause binds is joined, never
// superseded, and the environment is such a binder.
func (e *DefaultQueryExecutor) scopeResult(ctx *Context, form planner.Scope, results []Relation) (Relation, error) {
	result := unionRelations(results, form.Header(), e.options)
	env := ctx.Environment()
	if env == nil {
		return result, nil
	}
	var shared []query.Symbol
	for _, sym := range form.Outputs() {
		if query.ContainsSymbol(env.Symbols(), sym) {
			shared = append(shared, sym)
		}
	}
	if len(shared) == 0 {
		return result, nil
	}
	values, err := env.Project(shared)
	if err != nil {
		return nil, fmt.Errorf("the environment's outputs: %w", err)
	}
	return result.Join(values), nil
}

// materialized collects a relation into one held in memory, which a
// fallback's union and anti-join read twice. The runner reads the relation
// whole here, so an error its source deferred is the runner's error, for a
// relation already held in memory as for one collected.
func (e *DefaultQueryExecutor) materialized(rel Relation) (*MaterializedRelation, error) {
	if mat, ok := rel.(*MaterializedRelation); ok {
		if mat.err != nil {
			return nil, mat.err
		}
		return mat, nil
	}
	var tuples []Tuple
	if err := collectTuplesInto(&tuples, rel); err != nil {
		return nil, err
	}
	return newMaterializedRelationFromSet(rel.Symbols(), tuples, e.options, rel.Properties()), nil
}

// hasTuple reports whether a relation yields a tuple: by its size when the
// relation knows it, otherwise by one pull of its iterator. An iterator
// filling the relation's replay cache is read to its end instead, since a
// cache closed before its end leaves the relation unreadable.
func hasTuple(rel Relation) (bool, error) {
	if size := rel.Size(); size >= 0 {
		return size > 0, nil
	}
	it := rel.Iterator()
	has := it.Next()
	completeCache(it)
	err := it.Error()
	if closeErr := it.Close(); err == nil {
		err = closeErr
	}
	return has, err
}

// executeOrJoinClause runs the union over its subject, the groups holding a
// header symbol, and reports it through the or-clause events. With no such
// group the subject is the unit relation: each branch runs once over the
// unit relation's run, the environment's tuple over the required run, each
// result is read whole, the route reports the union event and one branch
// completion per branch, and the union joins at the collapse as a new
// group. Otherwise an OrFallbackRelation streams the subject in windows,
// runs each branch once per window over the window's keys, probes the kept
// result per window tuple, and stands in the place of the groups it
// consumed. Its completion fires at
// construction with no size, and a branch failure surfaces when the result
// is driven;
// on the unit route a branch failure is the completion's error and the
// clause's.
func (e *DefaultQueryExecutor) executeOrJoinClause(ctx *Context, clause *query.OrJoinClause, groups Relations) (Relations, error) {
	handler := e.options.Handler
	var start time.Time
	if handler != nil {
		start = time.Now()
		handler(annotations.Event{
			Name:  annotations.OrClauseBegin,
			Start: start,
			Data: map[string]interface{}{
				"branch_count": len(clause.Branches),
			},
		})
	}

	find := make([]query.FindElement, len(clause.OutputVars))
	for i, sym := range clause.OutputVars {
		find[i] = query.FindVariable{Symbol: sym}
	}
	form := planner.Scope{Required: clause.RequiredVars, Find: find}
	subject, consumed := e.findOuterRelation(form.Header(), groups)
	var result Relation
	var err error
	semantics := "correlated-union"
	if len(consumed) == 0 {
		semantics = "union"
		if handler != nil {
			handler(annotations.Event{
				Name:  annotations.OrClauseUnion,
				Start: time.Now(),
				Data: map[string]interface{}{
					"branch_count": len(clause.Branches),
				},
			})
		}
		var run *MaterializedRelation
		run, err = e.requiredRun(ctx, form.Required)
		results := make([]Relation, 0, len(clause.Branches))
		for i := 0; err == nil && i < len(clause.Branches); i++ {
			var branchStart time.Time
			if handler != nil {
				branchStart = time.Now()
			}
			var branchResult Relation
			var answered *MaterializedRelation
			branchResult, err = e.runScope(ctx, planner.Scope{Required: form.Required, Find: form.Find, Clauses: clause.Branches[i]}, run)
			if err == nil {
				answered, err = e.materialized(branchResult)
			}
			if err != nil {
				err = fmt.Errorf("OR branch %d execution failed: %w", i+1, err)
				break
			}
			if handler != nil {
				branchEnd := time.Now()
				handler(annotations.Event{
					Name:    annotations.OrClauseBranchComplete,
					Start:   branchStart,
					End:     branchEnd,
					Latency: branchEnd.Sub(branchStart),
					Data: map[string]interface{}{
						"branch_index": i,
						"result_size":  answered.Size(),
						"mode":         "union",
					},
				})
			}
			results = append(results, answered)
		}
		if err == nil {
			var union Relation
			union, err = e.scopeResult(ctx, form, results)
			if err == nil {
				result, err = e.materialized(union)
			}
		}
		if err == nil {
			groups = append(groups, result)
		}
	} else {
		var rel *OrFallbackRelation
		rel, err = NewOrFallbackRelation(e, ctx, clause.RequiredVars, clause.OutputVars, clause.Branches, subject, e.options, false)
		if err == nil {
			result = rel
			groups = replaceConsumedGroups(groups, consumed, rel, e.options.Handler)
		}
	}

	if handler != nil {
		end := time.Now()
		data := map[string]interface{}{
			"semantics":            semantics,
			annotations.KeySuccess: err == nil,
		}
		if err != nil {
			data["error"] = err.Error()
		} else {
			data["result_size"] = result.Size()
		}
		handler(annotations.Event{
			Name:    annotations.OrClauseComplete,
			Start:   start,
			End:     end,
			Latency: end.Sub(start),
			Data:    data,
		})
	}
	if err != nil {
		return nil, err
	}
	return groups, nil
}

// executeOrDefaultJoinClause runs the fallback over its subject, the groups
// holding the required run, through an OrFallbackRelation with
// short-circuit: per subject tuple the first branch with a result answers
// it, a branch built once is probed, and a carried output is filtered after
// the decision. The result stands in the place of the groups it consumed. A
// required run no group holds has the unit relation as its subject, over
// which the relation decides once. The fallback reports through the
// relation's events and no or-clause event.
func (e *DefaultQueryExecutor) executeOrDefaultJoinClause(ctx *Context, clause *query.OrDefaultJoinClause, groups Relations) (Relations, error) {
	subject, consumed := e.findOuterRelation(clause.RequiredVars, groups)
	rel, err := NewOrFallbackRelation(e, ctx, clause.RequiredVars, clause.OutputVars, clause.Branches, subject, e.options, true)
	if err != nil {
		return nil, err
	}
	return replaceConsumedGroups(groups, consumed, rel, e.options.Handler), nil
}

// executeNotJoinClause runs the not-join composed over the modules: its body
// is framed under the all-required header as the fallback frames a branch,
// the subject is the groups holding a header symbol, and the result, a
// streaming relation over the subject's symbols with its properties, yields
// the subject's tuples with no match in the subject's order and stands in
// the place of the groups it consumed. A header the environment alone binds
// has the unit relation as its subject: the body is built once over the
// environment's tuple, and the result's one tuple over no symbol is kept
// when the body has none and dropped when it has a result, which the find
// boundary's verdict then reads. A refusal of the body's plan is the
// clause's error, and a failure of the body's scan is the result's,
// surfacing when it is driven.
func (e *DefaultQueryExecutor) executeNotJoinClause(ctx *Context, clause *query.NotJoinClause, groups Relations) (Relations, error) {
	subject, consumed := e.findOuterRelation(clause.RequiredVars, groups)
	environment, err := environmentRelation(ctx, clause.RequiredVars, subject.Symbols())
	if err != nil {
		return nil, err
	}
	f := framing{
		form:        "not-join",
		subject:     subject,
		required:    clause.RequiredVars,
		given:       clause.RequiredVars,
		standsFor:   clause.RequiredVars,
		key:         keySymbols(clause.RequiredVars, subject.Symbols()),
		environment: environment,
	}
	plans, err := frameBranches(e, ctx, [][]query.Clause{clause.Clauses}, f)
	if err != nil {
		return nil, err
	}
	it := &notJoinIterator{windowedIterator: newWindowedIterator(e, ctx, f, plans, e.options)}
	it.policy = formPolicy{answer: it.answer}
	result := NewStreamingRelationWithProperties(subject.Symbols(), it, e.options, subject.Properties())
	return replaceConsumedGroups(groups, consumed, result, e.options.Handler), nil
}
