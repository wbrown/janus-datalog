package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// optimizeAlgebra compiles clauses to relational algebra and applies the
// configured equivalence passes without flattening the optimized tree.
func optimizeAlgebra(clauses []query.Clause, handler annotations.Handler) (*algebra.Node, error) {
	emit := func(name string, data map[string]interface{}) {
		if handler != nil {
			handler(annotations.Event{Name: name, Data: data})
		}
	}

	emit("algebra/bridge-begin", map[string]interface{}{
		"clause_count": len(clauses),
	})

	q := &query.Query{Where: clauses}
	root, err := algebra.Compile(q)
	if err != nil {
		emit("algebra/compile-error", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("algebra compile: %w", err)
	}

	emit("algebra/compiled", map[string]interface{}{
		"tree": root.String(),
	})

	optimizer := algebra.NewOptimizer(algebra.DefaultPasses()...).WithHandler(handler)
	optimized, err := optimizer.Optimize(root)
	if err != nil {
		emit("algebra/optimize-error", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("algebra optimize: %w", err)
	}

	emit("algebra/optimized", map[string]interface{}{
		"tree": optimized.String(),
	})
	return optimized, nil
}

// optimizeViaAlgebra preserves the logical optimizer contract: Datalog in,
// Datalog out. Algebra is an internal representation only; physical phase
// construction remains the planner's responsibility.
func optimizeViaAlgebra(
	q *query.Query,
	options PlannerOptions,
	handler annotations.Handler,
) (*query.Query, error) {
	if q == nil {
		return nil, fmt.Errorf("algebra optimize: nil query")
	}
	optimized, err := optimizeAlgebra(q.Where, handler)
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
		if !hasValueInput {
			optimized, err = algebra.InsertJoinProjects(optimized, terminalSymbols(q))
			if err != nil {
				return nil, fmt.Errorf("algebra project insertion: %w", err)
			}
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
	return &rewritten, nil
}
