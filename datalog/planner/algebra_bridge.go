package planner

import (
	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// optimizeViaAlgebra compiles clauses to a relational algebra IR, applies
// algebraic equivalence rules, then decompiles back to clauses.
// This is a pure clause transformation: []Clause → []Clause.
func optimizeViaAlgebra(clauses []query.Clause, opts PlannerOptions) []query.Clause {
	// Compile to algebra IR
	q := &query.Query{Where: clauses}
	root, err := algebra.Compile(q)
	if err != nil {
		// Compilation failure is not fatal — fall through to unoptimized path
		return clauses
	}

	// Apply optimization passes
	optimizer := algebra.NewOptimizer(algebra.DefaultPasses()...)
	optimized, err := optimizer.Optimize(root)
	if err != nil {
		return clauses
	}

	// Decompile back to clauses
	result, err := algebra.Decompile(optimized)
	if err != nil {
		return clauses
	}

	return result
}
