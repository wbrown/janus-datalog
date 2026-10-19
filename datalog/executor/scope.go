package executor

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A scope is a nested form's body as the executor runs it: a header, the
// required run and the find beyond it, over a clause list. Each branch of an
// or-join or an or-default-join is one scope under the form's header, its
// find the outputs as variables; a not-join's body is one under its
// all-required header; and a subquery's inner query is one under its data
// inputs, its find the inner :find as written, aggregates included. A scope
// reads the header and nothing else of the enclosing scope.
type scope struct {
	required []query.Symbol
	find     []query.FindElement
	clauses  []query.Clause
}

// outputs is the symbols the scope's find binds beyond the required run, a
// variable's own and an aggregate's by its display form.
func (s scope) outputs() []query.Symbol {
	return extractFindSymbols(s.find)
}

// header is the required run followed by the outputs: the symbols the
// scope's query finds and its result carries.
func (s scope) header() []query.Symbol {
	outputs := s.outputs()
	header := make([]query.Symbol, 0, len(s.required)+len(outputs))
	header = append(header, s.required...)
	return append(header, outputs...)
}

// query frames the scope as the query the executor plans: it finds the
// required run as variables followed by the scope's find, declares $ and %,
// and opens with the invocation of scope/required over the required run,
// followed by the scope's clauses. The planner leads with the invocation,
// since scope/required is a seed-restricted predicate, and the text carries
// the binding, so the plan caches under it.
func (s scope) query() *query.Query {
	find := make([]query.FindElement, 0, len(s.required)+len(s.find))
	for _, sym := range s.required {
		find = append(find, query.FindVariable{Symbol: sym})
	}
	find = append(find, s.find...)
	args := make([]query.PatternElement, len(s.required))
	for i, sym := range s.required {
		args[i] = query.Variable{Name: sym}
	}
	where := make([]query.Clause, 0, len(s.clauses)+1)
	where = append(where, &query.RuleInvocation{Name: query.ScopeRequired, Args: args})
	where = append(where, s.clauses...)
	return &query.Query{
		Find:  find,
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}},
		Where: where,
	}
}

// planScope plans the scope's query under the rule set the enclosing query
// runs with. The plan is served from the plan cache under the query's text,
// so a scope is planned once however many runs read it.
func (e *DefaultQueryExecutor) planScope(ctx *Context, s scope) (*planner.RealizedPlan, error) {
	plan, err := e.executor.planner.PlanQuery(s.query(), query.Bindings{Rules: ctx.Rules()}, e.options.Handler)
	if err != nil {
		return nil, fmt.Errorf("planning the scope: %w", err)
	}
	return plan, nil
}

// runPlan runs a scope's plan once over run, the relation of its required
// run, and returns the scope's result over its header. The plan runs under
// a context that holds run under scope/required beside every relation the
// enclosing context holds, so an invocation inside the scope of a predicate
// the fixpoint is deriving reads the stratum's relations. The context
// carries no environment: a query input reaches a scope only through the
// header. The run reports no phase: the form's own events report a branch's
// execution, and a phase event per subject tuple would be a handler call per
// tuple on the per-tuple path.
func (e *DefaultQueryExecutor) runPlan(ctx *Context, plan *planner.RealizedPlan, run Relation) (Relation, error) {
	held := make(map[query.Symbol]Relation, len(ctx.derived)+1)
	for name, rel := range ctx.derived {
		held[name] = rel
	}
	held[query.ScopeRequired] = run
	return e.executor.runPhases(ctx.WithEnvironment(nil).WithDerived(held), plan, nil, nil)
}

// runScope plans the scope and runs it once over run.
func (e *DefaultQueryExecutor) runScope(ctx *Context, s scope, run Relation) (Relation, error) {
	plan, err := e.planScope(ctx, s)
	if err != nil {
		return nil, err
	}
	return e.runPlan(ctx, plan, run)
}

// requiredRun is the relation a form's scopes run over: the subject, the
// groups holding the required run joined into one relation, joined with
// the environment's tuple on the required variables the environment alone
// binds, and projected onto the required run. The groups the subject reads
// are materialized in place, so the join of the form's result with the
// groups reads them again. A required run no group and no environment
// binds a variable of is the unit relation's, over which a scope runs once.
func (e *DefaultQueryExecutor) requiredRun(ctx *Context, required []query.Symbol, groups Relations) (run *MaterializedRelation, subject Relation, consumed []int, err error) {
	for i, group := range groups {
		if containsAny(group.Symbols(), required) {
			groups[i] = group.Materialize()
		}
	}
	subject, consumed = e.findOuterRelation(required, groups)
	subject = subject.Materialize()
	outer := subject
	if env := ctx.Environment(); env != nil {
		var fromEnvironment []query.Symbol
		for _, sym := range required {
			if query.ContainsSymbol(env.Symbols(), sym) && !query.ContainsSymbol(outer.Symbols(), sym) {
				fromEnvironment = append(fromEnvironment, sym)
			}
		}
		if len(fromEnvironment) > 0 {
			values, err := env.Project(fromEnvironment)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("the environment's required variables: %w", err)
			}
			outer = outer.Join(values)
		}
	}
	if len(required) == 0 {
		return NewUnitRelation(e.options), subject, consumed, nil
	}
	projected, err := outer.Project(required)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("the required run: %w", err)
	}
	var tuples []Tuple
	if err := collectTuplesInto(&tuples, projected); err != nil {
		return nil, nil, nil, fmt.Errorf("the required run: %w", err)
	}
	return newMaterializedRelationFromSet(required, tuples, e.options, deduplicatedProperties(required)), subject, consumed, nil
}

// scopeResult unions the results of a form's scopes over the header and
// joins the union with the environment's tuple on the outputs the
// environment binds: an output another clause binds is joined, never
// superseded, and the environment is such a binder.
func (e *DefaultQueryExecutor) scopeResult(ctx *Context, form scope, results []Relation) (Relation, error) {
	result := unionRelations(results, form.header(), e.options)
	env := ctx.Environment()
	if env == nil {
		return result, nil
	}
	var shared []query.Symbol
	for _, sym := range form.outputs() {
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
// group. Otherwise an OrFallbackRelation streams the subject, runs each
// branch per subject tuple or probes it when it is built once, and stands
// in the place of the groups it consumed. Its completion fires at
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
	form := scope{required: clause.RequiredVars, find: find}
	subject, consumed := e.findOuterRelation(form.header(), groups)
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
		run, _, _, err = e.requiredRun(ctx, form.required, nil)
		results := make([]Relation, 0, len(clause.Branches))
		for i := 0; err == nil && i < len(clause.Branches); i++ {
			var branchStart time.Time
			if handler != nil {
				branchStart = time.Now()
			}
			var branchResult Relation
			var answered *MaterializedRelation
			branchResult, err = e.runScope(ctx, scope{required: form.required, find: form.find, clauses: clause.Branches[i]}, run)
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
		rel := NewOrFallbackRelation(e, ctx, clause.RequiredVars, clause.OutputVars, clause.Branches, subject, e.options, false)
		result = rel
		groups = replaceConsumedGroups(groups, consumed, rel, e.options.Handler)
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
	rel := NewOrFallbackRelation(e, ctx, clause.RequiredVars, clause.OutputVars, clause.Branches, subject, e.options, true)
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
	runSyms, joinSyms := runSymbols(clause.RequiredVars, subject.Symbols())
	environment, err := environmentRelation(ctx, runSyms, subject.Symbols())
	if err != nil {
		return nil, err
	}
	f := framing{
		form:        "not-join",
		subject:     subject,
		required:    clause.RequiredVars,
		given:       clause.RequiredVars,
		runSyms:     runSyms,
		key:         joinSyms,
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
