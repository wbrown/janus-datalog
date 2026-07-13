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
