package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// optimizeAlgebra compiles a query's clauses to relational algebra and applies
// the configured equivalence passes without flattening the optimized tree. The
// full query goes to Compile — its :in symbols seed the bridge's clause
// ordering (an input-bound correlate is bindable), so stripping them here
// would reject valid queries. The bindings are what the caller holds when it
// asks for the plan; the rule set among them is what the compiler and the
// passes fold under. Both trees return so provenance consumers can hold the
// before and after. answers is the sources' answer the get-else scan rewrite
// reads.
func optimizeAlgebra(q *query.Query, bindings query.Bindings, handler annotations.Handler, sink *algebra.RewriteSink, answers algebra.MatchAnswersLookup) (compiled *algebra.Node, optimized *algebra.Node, err error) {
	// The guard is the caller's, never the emitter's: `tree` is a whole-tree
	// walk through algebra.formatNode, and Go evaluates it before the call, so a
	// check inside emit would render both trees and discard them on every plan.
	emit := func(name string, data map[string]interface{}) {
		handler(annotations.Event{Name: name, Data: data})
	}

	if handler != nil {
		emit(annotations.AlgebraBridgeBegin, map[string]interface{}{
			"clause_count": len(q.Where),
		})
	}

	compiled, err = algebra.Compile(q, bindings.Rules)
	if err != nil {
		if handler != nil {
			emit(annotations.AlgebraCompileError, map[string]interface{}{
				annotations.KeyError: err.Error(),
			})
		}
		return nil, nil, fmt.Errorf("algebra compile: %w", err)
	}

	if handler != nil {
		emit(annotations.AlgebraCompiled, map[string]interface{}{
			"tree": compiled,
		})
	}

	optimizer := algebra.NewOptimizer(algebra.DefaultPasses(sink, answers)...)
	optimized, err = optimizer.Optimize(compiled)
	if err != nil {
		if handler != nil {
			emit(annotations.AlgebraOptimizeError, map[string]interface{}{
				annotations.KeyError: err.Error(),
			})
		}
		return nil, nil, fmt.Errorf("algebra optimize: %w", err)
	}

	if handler != nil {
		emit(annotations.AlgebraOptimized, map[string]interface{}{
			"tree": optimized,
		})
	}
	return compiled, optimized, nil
}

// optimizeViaAlgebra preserves the logical optimizer contract: Datalog in,
// Datalog out. Algebra is an internal representation only; physical phase
// construction remains the planner's responsibility.
//
// When expl is non-nil the bridge fills its algebra fields — the compiled
// tree, every pass's rewrite records, the final optimized tree, and the
// decompiled query — as values. expl is nil on the normal query path, which
// collects nothing. The get-else scan rewrite reads the options' answer,
// false for every source where they carry none.
func optimizeViaAlgebra(
	q *query.Query,
	bindings query.Bindings,
	options PlannerOptions,
	handler annotations.Handler,
	expl *AlgebraExplanation,
) (*query.Query, error) {
	if q == nil {
		return nil, fmt.Errorf("algebra optimize: nil query")
	}
	sink := &algebra.RewriteSink{Handler: handler, Collect: expl != nil}
	compiled, optimized, err := optimizeAlgebra(q, bindings, handler, sink, options.matchAnswersLookup)
	if err != nil {
		return nil, err
	}
	clauses, err := algebra.Decompile(optimized)
	if err != nil {
		if handler != nil {
			handler(annotations.Event{
				Name: annotations.AlgebraDecompileError,
				Data: map[string]interface{}{annotations.KeyError: err.Error()},
			})
		}
		return nil, fmt.Errorf("algebra decompile: %w", err)
	}
	rewritten := &query.Query{
		Find:         q.Find,
		In:           q.In,
		Where:        clauses,
		OrderBy:      q.OrderBy,
		Limit:        q.Limit,
		ScalarReturn: q.ScalarReturn,
	}
	if handler != nil {
		handler(annotations.Event{
			Name: annotations.AlgebraBridgeComplete,
			Data: map[string]interface{}{
				"input_clause_count":  len(q.Where),
				"output_clause_count": len(clauses),
			},
		})
	}
	if expl != nil {
		expl.Compiled = compiled
		expl.Rewrites = sink.Records()
		expl.Optimized = optimized
		expl.Rewritten = rewritten
	}
	return rewritten, nil
}
