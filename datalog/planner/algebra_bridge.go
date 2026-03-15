package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// optimizeViaAlgebra compiles clauses to a relational algebra IR, applies
// algebraic equivalence rules, then decompiles back to clauses.
// This is a pure clause transformation: []Clause → []Clause.
func optimizeViaAlgebra(clauses []query.Clause, opts PlannerOptions) ([]query.Clause, error) {
	q := &query.Query{Where: clauses}
	root, err := algebra.Compile(q)
	if err != nil {
		return nil, fmt.Errorf("algebra compile: %w", err)
	}

	optimizer := algebra.NewOptimizer(algebra.DefaultPasses()...)
	optimized, err := optimizer.Optimize(root)
	if err != nil {
		return nil, fmt.Errorf("algebra optimize: %w", err)
	}

	result, err := algebra.Decompile(optimized)
	if err != nil {
		return nil, fmt.Errorf("algebra decompile: %w", err)
	}

	return result, nil
}
