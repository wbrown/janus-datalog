package parser

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
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
	case "str/starts-with?":
		return parseStrStartsWith(args)
	case "ground":
		return parseGround(args)
	case "missing":
		return parseMissing(args)
	case "missing?":
		// Database function used as predicate filter: [(missing? $ ?e :attr)]
		return parseMissingAttrPredicate(args)
	case "day", "month", "year", "hour", "minute", "second":
		// Time extraction predicates - these are FunctionPredicates
		return &query.FunctionPredicate{
			Fn:   fn,
			Args: args,
		}, nil
	case "tx-between":
		return parseTxBetweenPredicate(args)
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
			Op:    datalog.SymEQ,
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
			Op:    datalog.SymEQ,
			Terms: terms,
		}, nil
	}

	return nil, fmt.Errorf("equality requires at least 2 arguments, got %d", len(args))
}

// parseComparison handles <, <=, >, >= predicates. The operator name resolves
// here, once, to its pre-interned symbol; downstream dispatch is pointer
// equality.
func parseComparison(fn string, args []query.PatternElement) (query.Predicate, error) {
	var op query.Symbol
	switch fn {
	case "<":
		op = datalog.SymLT
	case "<=":
		op = datalog.SymLTE
	case ">":
		op = datalog.SymGT
	case ">=":
		op = datalog.SymGTE
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
			Op:    datalog.SymEQ,
			Left:  left,
			Right: right,
		},
	}, nil
}

// parseStrStartsWith handles str/starts-with? predicates
func parseStrStartsWith(args []query.PatternElement) (query.Predicate, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("str/starts-with? requires exactly 2 arguments, got %d", len(args))
	}
	return &query.StrStartsWithPredicate{
		Value:  elementToTerm(args[0]),
		Prefix: elementToTerm(args[1]),
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

// parseMissingAttrPredicate parses (missing? $ ?entity :attribute) as a predicate
// This is used when missing? is used as a filter without a binding
func parseMissingAttrPredicate(args []query.PatternElement) (query.Predicate, error) {
	// Syntax: (missing? $ ?e :attr)
	if len(args) != 3 {
		return nil, fmt.Errorf("missing? requires exactly 3 arguments ($ entity attr), got %d", len(args))
	}

	// Validate database reference ($)
	if err := validateDatabaseRefPredicate(args[0]); err != nil {
		return nil, fmt.Errorf("missing?: %w", err)
	}

	// Parse entity (can be variable or constant)
	entity := elementToTerm(args[1])

	// Parse attribute (must be a keyword)
	attr, err := extractKeywordPredicate(args[2])
	if err != nil {
		return nil, fmt.Errorf("missing?: attribute must be a keyword: %w", err)
	}

	// Create a MissingFunction and wrap it in a DatabaseFunctionPredicate
	missingFn := &query.MissingFunction{
		Entity: entity,
		Attr:   attr,
	}

	return &query.DatabaseFunctionPredicate{
		Function: missingFn,
	}, nil
}

// validateDatabaseRefPredicate validates that an argument is the database reference ($)
// This is a copy for the predicate parser to avoid circular imports
func validateDatabaseRefPredicate(arg query.PatternElement) error {
	switch a := arg.(type) {
	case query.Variable:
		if a.Name == datalog.SymDollar {
			return nil
		}
		return fmt.Errorf("expected database reference ($), got variable %s", a.Name)
	case query.Constant:
		if sym, ok := a.Value.(query.Symbol); ok && sym == datalog.SymDollar {
			return nil
		}
		if str, ok := a.Value.(string); ok && str == "$" {
			return nil
		}
		return fmt.Errorf("expected database reference ($), got %v", a.Value)
	default:
		return fmt.Errorf("expected database reference ($), got %T", arg)
	}
}

// extractKeywordPredicate extracts a Keyword from a pattern element
// This is a copy for the predicate parser to avoid circular imports
func extractKeywordPredicate(arg query.PatternElement) (datalog.Keyword, error) {
	switch a := arg.(type) {
	case query.Constant:
		switch v := a.Value.(type) {
		case datalog.Keyword:
			return v, nil
		case string:
			if len(v) > 0 && v[0] == ':' {
				return datalog.NewKeyword(v), nil
			}
			return nil, fmt.Errorf("string %q is not a keyword", v)
		default:
			return nil, fmt.Errorf("expected keyword, got %T", v)
		}
	default:
		return nil, fmt.Errorf("expected keyword constant, got %T", arg)
	}
}

// parseTxBetweenPredicate parses [(tx-between ?tx 1000 2000)]
// Filters results to transactions within a Lamport range [low, high] inclusive.
func parseTxBetweenPredicate(args []query.PatternElement) (query.Predicate, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("tx-between requires exactly 3 arguments (variable, low, high), got %d", len(args))
	}

	// First argument must be a variable
	txVar, ok := args[0].(query.Variable)
	if !ok {
		return nil, fmt.Errorf("tx-between first argument must be a variable, got %T", args[0])
	}

	// Second argument (low) must be an integer
	low, err := extractUint64(args[1], "tx-between low")
	if err != nil {
		return nil, err
	}

	// Third argument (high) must be an integer
	high, err := extractUint64(args[2], "tx-between high")
	if err != nil {
		return nil, err
	}

	if low > high {
		return nil, fmt.Errorf("tx-between: low (%d) must be <= high (%d)", low, high)
	}

	return &query.TxRangePredicate{
		TxVar: txVar.Name,
		Low:   low,
		High:  high,
	}, nil
}

// extractUint64 extracts a uint64 from a pattern element
func extractUint64(arg query.PatternElement, context string) (uint64, error) {
	switch v := arg.(type) {
	case query.Constant:
		switch n := v.Value.(type) {
		case int64:
			if n < 0 {
				return 0, fmt.Errorf("%s must be non-negative, got %d", context, n)
			}
			return uint64(n), nil
		case int:
			if n < 0 {
				return 0, fmt.Errorf("%s must be non-negative, got %d", context, n)
			}
			return uint64(n), nil
		case uint64:
			return n, nil
		default:
			return 0, fmt.Errorf("%s must be an integer, got %T", context, v.Value)
		}
	default:
		return 0, fmt.Errorf("%s must be a constant integer, got %T", context, arg)
	}
}
