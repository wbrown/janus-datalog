package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// optimizeViaAlgebra compiles clauses to a relational algebra IR, applies
// algebraic equivalence rules, then decompiles back to clauses.
// This is a pure clause transformation: []Clause → []Clause.
func optimizeViaAlgebra(clauses []query.Clause, handler annotations.Handler) ([]query.Clause, error) {
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

	result, err := algebra.Decompile(optimized)
	if err != nil {
		emit("algebra/decompile-error", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("algebra decompile: %w", err)
	}

	// Report what changed
	changed := len(result) != len(clauses)
	if !changed {
		for i := range result {
			if fmt.Sprintf("%T", result[i]) != fmt.Sprintf("%T", clauses[i]) {
				changed = true
				break
			}
		}
	}

	var resultTypes []string
	var resultDetails []string
	for _, c := range result {
		resultTypes = append(resultTypes, fmt.Sprintf("%T", c))
		detail := fmt.Sprintf("%T", c)
		switch cl := c.(type) {
		case *query.OrClause:
			detail += fmt.Sprintf(" branches=%d", len(cl.Branches))
		case *query.OrJoinClause:
			detail += fmt.Sprintf(" joinVars=%v branches=%d", cl.JoinVars, len(cl.Branches))
		case *query.OrDefaultClause:
			detail += fmt.Sprintf(" branches=%d", len(cl.Branches))
		case *query.OrDefaultJoinClause:
			detail += fmt.Sprintf(" joinVars=%v branches=%d", cl.JoinVars, len(cl.Branches))
		case *query.NotClause:
			detail += fmt.Sprintf(" innerClauses=%d", len(cl.Clauses))
		case *query.NotJoinClause:
			detail += fmt.Sprintf(" joinVars=%v innerClauses=%d", cl.JoinVars, len(cl.Clauses))
		}
		resultDetails = append(resultDetails, detail)
	}

	emit("algebra/bridge-complete", map[string]interface{}{
		"input_clauses":  len(clauses),
		"output_clauses": len(result),
		"changed":        changed,
		"output_types":   resultTypes,
		"output_details": resultDetails,
	})

	return result, nil
}
