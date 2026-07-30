package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// joinProjectPassName names the bridge-level projection-insertion transform
// in rewrite records and event data. It is a transform outside the pass
// framework, but its decisions report exactly like a pass's.
const joinProjectPassName = "join-project-insertion"

// optimizeAlgebra compiles a query's clauses to relational algebra and applies
// the configured equivalence passes without flattening the optimized tree. The
// full query goes to Compile — its :in symbols seed the bridge's clause
// ordering (an input-bound correlate is bindable), so stripping them here
// would reject valid queries. Both trees return so provenance consumers can
// hold the before and after.
func optimizeAlgebra(q *query.Query, handler annotations.Handler, sink *algebra.RewriteSink) (compiled *algebra.Node, optimized *algebra.Node, err error) {
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

	compiled, err = algebra.Compile(q)
	if err != nil {
		if handler != nil {
			emit(annotations.AlgebraCompileError, map[string]interface{}{
				"error": err.Error(),
			})
		}
		return nil, nil, fmt.Errorf("algebra compile: %w", err)
	}

	if handler != nil {
		emit(annotations.AlgebraCompiled, map[string]interface{}{
			"tree": compiled,
		})
	}

	optimizer := algebra.NewOptimizer(algebra.DefaultPasses(sink)...)
	optimized, err = optimizer.Optimize(compiled)
	if err != nil {
		if handler != nil {
			emit(annotations.AlgebraOptimizeError, map[string]interface{}{
				"error": err.Error(),
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
// collects nothing.
func optimizeViaAlgebra(
	q *query.Query,
	options PlannerOptions,
	handler annotations.Handler,
	expl *AlgebraExplanation,
) (*query.Query, error) {
	if q == nil {
		return nil, fmt.Errorf("algebra optimize: nil query")
	}
	sink := &algebra.RewriteSink{Handler: handler, Collect: expl != nil}
	compiled, optimized, err := optimizeAlgebra(q, handler, sink)
	if err != nil {
		return nil, err
	}
	if options.EnableJoinProjectInsertion {
		hasValueInput := false
		for _, input := range q.In {
			if _, isDatabase := input.(query.DatabaseInput); !isDatabase {
				hasValueInput = true
				break
			}
		}
		// sink is the composite literal above, so no nil check: a guard must
		// encode a claim that can be false.
		observing := sink.Collect || sink.Handler != nil
		if hasValueInput {
			// terminalSymbols walks the query, and on this branch nothing else
			// needs the answer.
			if observing {
				sink.Record(algebra.RewriteRecord{
					Pass:    joinProjectPassName,
					Action:  algebra.RewriteDeclined,
					Reason:  "query has value inputs",
					Subject: terminalSymbols(q),
				}, annotations.AlgebraJoinProjectSkip, map[string]interface{}{
					"reason": "query has value inputs",
				})
			}
		} else {
			// One walk for the transform and its record, so the record names
			// the symbols actually inserted rather than a second computation of
			// them.
			terminals := terminalSymbols(q)
			optimized, err = algebra.InsertJoinProjects(optimized, terminals)
			if err != nil {
				return nil, fmt.Errorf("algebra project insertion: %w", err)
			}
			if observing {
				sink.Record(algebra.RewriteRecord{
					Pass:    joinProjectPassName,
					Action:  algebra.RewriteApplied,
					Subject: terminals,
				}, annotations.AlgebraJoinProjectApply, map[string]interface{}{
					"terminal_symbols": terminals,
				})
			}
		}
	}
	clauses, err := algebra.Decompile(optimized)
	if err != nil {
		if handler != nil {
			handler(annotations.Event{
				Name: annotations.AlgebraDecompileError,
				Data: map[string]interface{}{"error": err.Error()},
			})
		}
		return nil, fmt.Errorf("algebra decompile: %w", err)
	}
	rewritten := *q
	rewritten.Where = clauses
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
		expl.Rewritten = &rewritten
	}
	return &rewritten, nil
}
