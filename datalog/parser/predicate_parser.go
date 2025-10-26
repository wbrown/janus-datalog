package parser

import (
	"fmt"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// parsePredicate creates a concrete Predicate from a function pattern
func parsePredicate(fn string, args []query.PatternElement) (query.Predicate, error) {
	switch fn {
	case "=":
		return parseEquality(args)
	case "!=", "not=":
		return parseNotEqual(args)
	case "<", "<=", ">", ">=":
		return parseComparison(fn, args)
	case "ground":
		return parseGround(args)
	case "missing":
		return parseMissing(args)
	case "day", "month", "year", "hour", "minute", "second":
		// Time extraction predicates - these are FunctionPredicates
		return &query.FunctionPredicate{
			Fn:   fn,
			Args: args,
		}, nil
	default:
		// All other predicates become FunctionPredicates
		// This handles things like str/starts-with?, custom predicates, etc.
		return &query.FunctionPredicate{
			Fn:   fn,
			Args: args,
		}, nil
	}
}

// parseEquality handles = predicates
func parseEquality(args []query.PatternElement) (query.Predicate, error) {
	if len(args) == 2 {
		// Binary equality: [(= ?x ?y)]
		left := elementToTerm(args[0])
		right := elementToTerm(args[1])

		return &query.Comparison{
			Op:    query.OpEQ,
			Left:  left,
			Right: right,
		}, nil
	} else if len(args) > 2 {
		// Chained equality: [(= ?x ?y ?z)]
		terms := make([]query.Term, len(args))
		for i, arg := range args {
			terms[i] = elementToTerm(arg)
		}

		return &query.ChainedComparison{
			Op:    query.OpEQ,
			Terms: terms,
		}, nil
	}

	return nil, fmt.Errorf("equality requires at least 2 arguments, got %d", len(args))
}

// parseComparison handles <, <=, >, >= predicates
func parseComparison(fn string, args []query.PatternElement) (query.Predicate, error) {
	// Map function name to operator
	var op query.CompareOp
	switch fn {
	case "<":
		op = query.OpLT
	case "<=":
		op = query.OpLTE
	case ">":
		op = query.OpGT
	case ">=":
		op = query.OpGTE
	default:
		return nil, fmt.Errorf("unknown comparison operator: %s", fn)
	}

	// Handle both binary and chained comparisons
	if len(args) == 2 {
		// Binary comparison: [(< ?x 10)]
		left := elementToTerm(args[0])
		right := elementToTerm(args[1])

		return &query.Comparison{
			Op:    op,
			Left:  left,
			Right: right,
		}, nil
	} else if len(args) > 2 {
		// Chained comparison: [(< 0 ?x 100)]
		terms := make([]query.Term, len(args))
		for i, arg := range args {
			terms[i] = elementToTerm(arg)
		}

		return &query.ChainedComparison{
			Op:    op,
			Terms: terms,
		}, nil
	}

	return nil, fmt.Errorf("comparison requires at least 2 arguments, got %d", len(args))
}

// parseNotEqual handles != predicates
func parseNotEqual(args []query.PatternElement) (query.Predicate, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("!= requires exactly 2 arguments, got %d", len(args))
	}

	left := elementToTerm(args[0])
	right := elementToTerm(args[1])

	return &query.NotEqualPredicate{
		Comparison: query.Comparison{
			Op:    query.OpEQ,
			Left:  left,
			Right: right,
		},
	}, nil
}

// parseGround handles ground predicates
func parseGround(args []query.PatternElement) (query.Predicate, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("ground requires at least 1 argument")
	}

	var variables []query.Symbol
	for _, arg := range args {
		if v, ok := arg.(query.Variable); ok {
			variables = append(variables, v.Name)
		} else {
			return nil, fmt.Errorf("ground only accepts variables, got %T", arg)
		}
	}

	return &query.GroundPredicate{
		Variables: variables,
	}, nil
}

// parseMissing handles missing predicates
func parseMissing(args []query.PatternElement) (query.Predicate, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("missing requires at least 1 argument")
	}

	var variables []query.Symbol
	for _, arg := range args {
		if v, ok := arg.(query.Variable); ok {
			variables = append(variables, v.Name)
		} else {
			return nil, fmt.Errorf("missing only accepts variables, got %T", arg)
		}
	}

	return &query.MissingPredicate{
		Variables: variables,
	}, nil
}

// elementToTerm converts a query.PatternElement to a Term
func elementToTerm(elem query.PatternElement) query.Term {
	switch e := elem.(type) {
	case query.Variable:
		return query.VariableTerm{Symbol: e.Name}
	case query.Constant:
		return query.ConstantTerm{Value: e.Value}
	default:
		// For other types, treat as constant
		// This handles literals that might not be wrapped in Constant
		return query.ConstantTerm{Value: elem}
	}
}
