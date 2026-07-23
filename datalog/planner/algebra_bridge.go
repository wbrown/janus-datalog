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
	emit := func(name string, data map[string]interface{}) {
		if handler != nil {
			handler(annotations.Event{Name: name, Data: data})
		}
	}

	emit("algebra/bridge-begin", map[string]interface{}{
		"clause_count": len(q.Where),
	})

	compiled, err = algebra.Compile(q)
	if err != nil {
		emit("algebra/compile-error", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, nil, fmt.Errorf("algebra compile: %w", err)
	}

	emit("algebra/compiled", map[string]interface{}{
		"tree": compiled.String(),
	})

	optimizer := algebra.NewOptimizer(algebra.DefaultPasses(sink)...)
	optimized, err = optimizer.Optimize(compiled)
	if err != nil {
		emit("algebra/optimize-error", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, nil, fmt.Errorf("algebra optimize: %w", err)
	}

	emit("algebra/optimized", map[string]interface{}{
		"tree": optimized.String(),
	})
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
		if hasValueInput {
			sink.Record(algebra.RewriteRecord{
				Pass:    joinProjectPassName,
				Action:  algebra.RewriteDeclined,
				Reason:  "query has value inputs",
				Subject: fmt.Sprintf("%v", terminalSymbols(q)),
			}, "algebra/join-project-skip", map[string]interface{}{
				"reason": "query has value inputs",
			})
		} else {
			optimized, err = algebra.InsertJoinProjects(optimized, terminalSymbols(q))
			if err != nil {
				return nil, fmt.Errorf("algebra project insertion: %w", err)
			}
			sink.Record(algebra.RewriteRecord{
				Pass:    joinProjectPassName,
				Action:  algebra.RewriteApplied,
				Subject: fmt.Sprintf("%v", terminalSymbols(q)),
			}, "algebra/join-project-apply", map[string]interface{}{
				"terminal_symbols": fmt.Sprintf("%v", terminalSymbols(q)),
			})
		}
	}
	clauses, err := algebra.Decompile(optimized)
	if err != nil {
		if handler != nil {
			handler(annotations.Event{
				Name: "algebra/decompile-error",
				Data: map[string]interface{}{"error": err.Error()},
			})
		}
		return nil, fmt.Errorf("algebra decompile: %w", err)
	}
	rewritten := *q
	rewritten.Where = clauses
	if handler != nil {
		handler(annotations.Event{
			Name: "algebra/bridge-complete",
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
