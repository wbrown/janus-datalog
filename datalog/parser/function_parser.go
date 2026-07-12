package parser

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// parseFunction creates a concrete Function from expression pattern arguments
// Note: Database functions (get-else, missing?, get-some) return query.DatabaseFunction
// which also satisfies query.Function through embedding.
func parseFunction(fn string, args []query.PatternElement) (query.Function, error) {
	switch fn {
	case "+", "-", "*", "/":
		return parseArithmetic(fn, args)
	case "=", "<", "<=", ">", ">=", "!=":
		return parseComparisonFunction(fn, args)
	case "str":
		return parseStringConcat(args)
	case "year", "month", "day", "hour", "minute", "second":
		return parseTimeExtraction(fn, args)
	case "ground":
		return parseGroundFunction(args)
	case "identity":
		return parseIdentity(args)
	// Database functions - require $ and database access
	case "get-else":
		return parseGetElse(args)
	case "missing?":
		return parseMissingAttr(args)
	case "get-some":
		return parseGetSome(args)
	// Vector functions for CRDT cardinality-vector attributes
	case "nth":
		return parseNth(args)
	case "first":
		return parseFirst(args)
	case "last":
		return parseLast(args)
	case "length":
		return parseLength(args)
	case "contains?":
		return parseContains(args)
	case "index-of":
		return parseIndexOf(args)
	case "subvec":
		return parseSubvec(args)
	case "enumerate":
		return parseEnumerate(args)
	default:
		return nil, fmt.Errorf("unsupported function: %s", fn)
	}
}

// parseArithmetic handles arithmetic functions
func parseArithmetic(fn string, args []query.PatternElement) (query.Function, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("%s requires at least 1 argument, got 0", fn)
	}

	var op query.ArithmeticOp
	switch fn {
	case "+":
		op = query.OpAdd
	case "-":
		op = query.OpSubtract
	case "*":
		op = query.OpMultiply
	case "/":
		op = query.OpDivide
	}

	terms := make([]query.Term, len(args))
	for i, argument := range args {
		terms[i] = elementToTerm(argument)
	}
	return &query.ArithmeticFunction{Op: op, Args: terms}, nil
}

// parseComparisonFunction handles comparison operators as functions with bindings
// Example: [(> ?count 0) ?flag] binds true/false to ?flag
func parseComparisonFunction(fn string, args []query.PatternElement) (query.Function, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("%s requires exactly 2 arguments, got %d", fn, len(args))
	}

	var op query.CompareOp
	switch fn {
	case "=":
		op = query.OpEQ
	case "<":
		op = query.OpLT
	case "<=":
		op = query.OpLTE
	case ">":
		op = query.OpGT
	case ">=":
		op = query.OpGTE
	case "!=":
		op = query.OpNE
	}

	comparison := &query.Comparison{
		Op:    op,
		Left:  elementToTerm(args[0]),
		Right: elementToTerm(args[1]),
	}

	return &query.ComparisonFunction{
		Comparison: comparison,
	}, nil
}

// parseStringConcat handles str function
func parseStringConcat(args []query.PatternElement) (query.Function, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("str requires at least 1 argument")
	}

	terms := make([]query.Term, len(args))
	for i, arg := range args {
		terms[i] = elementToTerm(arg)
	}

	return &query.StringConcatFunction{
		Terms: terms,
	}, nil
}

// parseTimeExtraction handles time extraction functions
func parseTimeExtraction(field string, args []query.PatternElement) (query.Function, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("%s requires exactly 1 argument, got %d", field, len(args))
	}

	return &query.TimeExtractionFunction{
		Field:    field,
		TimeTerm: elementToTerm(args[0]),
	}, nil
}

// parseGroundFunction handles ground function - binds constant value(s)
// Supports both scalar: [(ground 42) ?x] and tuple: [(ground [1 2 3]) [?a ?b ?c]]
func parseGroundFunction(args []query.PatternElement) (query.Function, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ground function requires exactly 1 argument, got %d", len(args))
	}

	// Vector constant (tuple ground)
	if vectorConst, ok := args[0].(query.VectorConstant); ok {
		return &query.GroundFunction{
			Value: vectorConst.Values,
		}, nil
	}

	// Scalar constant
	if constant, ok := args[0].(query.Constant); ok {
		return &query.GroundFunction{
			Value: constant.Value,
		}, nil
	}

	return nil, fmt.Errorf("ground function requires a constant or vector, got %T", args[0])
}

// parseIdentity handles identity function - passes through a value unchanged
func parseIdentity(args []query.PatternElement) (query.Function, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("identity requires exactly 1 argument, got %d", len(args))
	}

	return &query.IdentityFunction{
		Arg: elementToTerm(args[0]),
	}, nil
}

// parseAggregate creates an AggregateFunction from a function name and variable
func parseAggregate(fn string, varName query.Symbol) (query.AggregateFunction, error) {
	switch fn {
	case "count":
		return &query.CountAggregate{Var: varName}, nil
	case "sum":
		return &query.SumAggregate{Var: varName}, nil
	case "avg":
		return &query.AvgAggregate{Var: varName}, nil
	case "min":
		return &query.MinAggregate{Var: varName}, nil
	case "max":
		return &query.MaxAggregate{Var: varName}, nil
	default:
		return nil, fmt.Errorf("unsupported aggregate function: %s", fn)
	}
}

// =============================================================================
// Database Function Parsers
// =============================================================================

// parseGetElse parses (get-else $ ?entity :attribute default-value)
// Returns the attribute value if it exists, or the default if missing.
func parseGetElse(args []query.PatternElement) (query.Function, error) {
	// Syntax: (get-else $ ?e :attr default)
	// args[0] = $ (database reference)
	// args[1] = ?e (entity variable or constant)
	// args[2] = :attr (attribute keyword)
	// args[3] = default (any value)
	if len(args) != 4 {
		return nil, fmt.Errorf("get-else requires exactly 4 arguments ($ entity attr default), got %d", len(args))
	}

	// Validate database reference ($)
	if err := validateDatabaseRef(args[0]); err != nil {
		return nil, fmt.Errorf("get-else: %w", err)
	}

	// Parse entity (can be variable or constant)
	entity := elementToTerm(args[1])

	// Parse attribute (must be a keyword)
	attr, err := extractKeyword(args[2])
	if err != nil {
		return nil, fmt.Errorf("get-else: attribute must be a keyword: %w", err)
	}

	// Parse default value
	defaultVal, err := extractConstantValue(args[3])
	if err != nil {
		return nil, fmt.Errorf("get-else: default must be a constant value: %w", err)
	}

	return &query.GetElseFunction{
		Entity:  entity,
		Attr:    attr,
		Default: defaultVal,
	}, nil
}

// parseMissingAttr parses (missing? $ ?entity :attribute)
// Returns true if the entity does NOT have the specified attribute.
// Note: Named parseMissingAttr to avoid conflict with parseMissing in predicate_parser.go
func parseMissingAttr(args []query.PatternElement) (query.Function, error) {
	// Syntax: (missing? $ ?e :attr)
	// args[0] = $ (database reference)
	// args[1] = ?e (entity variable or constant)
	// args[2] = :attr (attribute keyword)
	if len(args) != 3 {
		return nil, fmt.Errorf("missing? requires exactly 3 arguments ($ entity attr), got %d", len(args))
	}

	// Validate database reference ($)
	if err := validateDatabaseRef(args[0]); err != nil {
		return nil, fmt.Errorf("missing?: %w", err)
	}

	// Parse entity (can be variable or constant)
	entity := elementToTerm(args[1])

	// Parse attribute (must be a keyword)
	attr, err := extractKeyword(args[2])
	if err != nil {
		return nil, fmt.Errorf("missing?: attribute must be a keyword: %w", err)
	}

	return &query.MissingFunction{
		Entity: entity,
		Attr:   attr,
	}, nil
}

// parseGetSome parses (get-some $ ?entity :attr1 :attr2 ...)
// Returns the first attribute that exists along with its value.
func parseGetSome(args []query.PatternElement) (query.Function, error) {
	// Syntax: (get-some $ ?e :attr1 :attr2 :attr3 ...)
	// args[0] = $ (database reference)
	// args[1] = ?e (entity variable or constant)
	// args[2..] = :attr1, :attr2, ... (attribute keywords)
	if len(args) < 3 {
		return nil, fmt.Errorf("get-some requires at least 3 arguments ($ entity attr...), got %d", len(args))
	}

	// Validate database reference ($)
	if err := validateDatabaseRef(args[0]); err != nil {
		return nil, fmt.Errorf("get-some: %w", err)
	}

	// Parse entity (can be variable or constant)
	entity := elementToTerm(args[1])

	// Parse attributes (must all be keywords)
	attrs := make([]datalog.Keyword, 0, len(args)-2)
	for i := 2; i < len(args); i++ {
		attr, err := extractKeyword(args[i])
		if err != nil {
			return nil, fmt.Errorf("get-some: argument %d must be a keyword: %w", i, err)
		}
		attrs = append(attrs, attr)
	}

	return &query.GetSomeFunction{
		Entity: entity,
		Attrs:  attrs,
	}, nil
}

// validateDatabaseRef validates that an argument is the database reference ($)
func validateDatabaseRef(arg query.PatternElement) error {
	switch a := arg.(type) {
	case query.Variable:
		if a.Name == datalog.SymDollar {
			return nil
		}
		return fmt.Errorf("expected database reference ($), got variable %s", a.Name)
	case query.Constant:
		// $ is parsed as Constant{Value: Symbol("$")}
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

// extractKeyword extracts a Keyword from a pattern element
func extractKeyword(arg query.PatternElement) (datalog.Keyword, error) {
	switch a := arg.(type) {
	case query.Constant:
		switch v := a.Value.(type) {
		case datalog.Keyword:
			return v, nil
		case string:
			// Allow string that looks like a keyword
			if len(v) > 0 && v[0] == ':' {
				return datalog.NewKeyword(v), nil
			}
			return nil, fmt.Errorf("string %q is not a keyword (must start with :)", v)
		default:
			return nil, fmt.Errorf("expected keyword, got %T", v)
		}
	default:
		return nil, fmt.Errorf("expected keyword constant, got %T", arg)
	}
}

// extractConstantValue extracts the value from a constant pattern element
func extractConstantValue(arg query.PatternElement) (interface{}, error) {
	switch a := arg.(type) {
	case query.Constant:
		return a.Value, nil
	case query.VectorConstant:
		return a.Values, nil
	case query.Variable:
		// Variables are not allowed as default values
		return nil, fmt.Errorf("expected constant, got variable %s", a.Name)
	default:
		return nil, fmt.Errorf("expected constant, got %T", arg)
	}
}

// =============================================================================
// Vector Function Parsers
// =============================================================================

// parseNth parses (nth ?vec ?idx) - get element at index
func parseNth(args []query.PatternElement) (query.Function, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("nth requires exactly 2 arguments (vector, index), got %d", len(args))
	}
	return &query.NthFunction{
		VecTerm:   elementToTerm(args[0]),
		IndexTerm: elementToTerm(args[1]),
	}, nil
}

// parseFirst parses (first ?vec) - get first element
func parseFirst(args []query.PatternElement) (query.Function, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("first requires exactly 1 argument (vector), got %d", len(args))
	}
	return &query.FirstFunction{
		VecTerm: elementToTerm(args[0]),
	}, nil
}

// parseLast parses (last ?vec) - get last element
func parseLast(args []query.PatternElement) (query.Function, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("last requires exactly 1 argument (vector), got %d", len(args))
	}
	return &query.LastFunction{
		VecTerm: elementToTerm(args[0]),
	}, nil
}

// parseLength parses (length ?vec) - get vector length
func parseLength(args []query.PatternElement) (query.Function, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("length requires exactly 1 argument (vector), got %d", len(args))
	}
	return &query.LengthFunction{
		VecTerm: elementToTerm(args[0]),
	}, nil
}

// parseContains parses (contains? ?vec ?val) - check membership
func parseContains(args []query.PatternElement) (query.Function, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("contains? requires exactly 2 arguments (vector, value), got %d", len(args))
	}
	return &query.ContainsFunction{
		VecTerm:   elementToTerm(args[0]),
		ValueTerm: elementToTerm(args[1]),
	}, nil
}

// parseIndexOf parses (index-of ?vec ?val) - find index of value
func parseIndexOf(args []query.PatternElement) (query.Function, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("index-of requires exactly 2 arguments (vector, value), got %d", len(args))
	}
	return &query.IndexOfFunction{
		VecTerm:   elementToTerm(args[0]),
		ValueTerm: elementToTerm(args[1]),
	}, nil
}

// parseSubvec parses (subvec ?vec ?start ?end) - get slice of vector
func parseSubvec(args []query.PatternElement) (query.Function, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("subvec requires exactly 3 arguments (vector, start, end), got %d", len(args))
	}
	return &query.SubvecFunction{
		VecTerm:   elementToTerm(args[0]),
		StartTerm: elementToTerm(args[1]),
		EndTerm:   elementToTerm(args[2]),
	}, nil
}

// parseEnumerate parses (enumerate ?vec) - decompose into (index, value) pairs
func parseEnumerate(args []query.PatternElement) (query.Function, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("enumerate requires exactly 1 argument (vector), got %d", len(args))
	}
	return &query.EnumerateFunction{
		VecTerm: elementToTerm(args[0]),
	}, nil
}
