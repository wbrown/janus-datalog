package parser

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/edn"
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
		// User-defined functions: a name with a registered implementation
		// (query.DefaultRegistry.RegisterImplementation) parses to a
		// CustomFunction; anything else is unknown, rejected here at the
		// boundary. Registration must precede parsing.
		if _, ok := query.DefaultRegistry.Implementation(fn); ok {
			terms := make([]query.Term, len(args))
			for i, arg := range args {
				terms[i] = elementToTerm(arg)
			}
			return query.CustomFunction{Fn: fn, Args: terms}, nil
		}
		return nil, fmt.Errorf("unsupported function: %s", fn)
	}
}

// parseArithmetic handles arithmetic functions
func parseArithmetic(fn string, args []query.PatternElement) (query.Function, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("%s requires at least 1 argument, got 0", fn)
	}

	// The operator name resolves here, once, to its pre-interned symbol;
	// downstream dispatch is pointer equality.
	var op query.Symbol
	switch fn {
	case "+":
		op = datalog.SymAdd
	case "-":
		op = datalog.SymSubtract
	case "*":
		op = datalog.SymMultiply
	case "/":
		op = datalog.SymDivide
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

	var op query.Symbol
	switch fn {
	case "=":
		op = datalog.SymEQ
	case "<":
		op = datalog.SymLT
	case "<=":
		op = datalog.SymLTE
	case ">":
		op = datalog.SymGT
	case ">=":
		op = datalog.SymGTE
	case "!=":
		op = datalog.SymNE
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

// parseTimeExtraction handles time extraction functions. The field name
// resolves here, once, to its pre-interned symbol.
func parseTimeExtraction(field string, args []query.PatternElement) (query.Function, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("%s requires exactly 1 argument, got %d", field, len(args))
	}

	var fieldSym query.Symbol
	switch field {
	case "year":
		fieldSym = datalog.SymYear
	case "month":
		fieldSym = datalog.SymMonth
	case "day":
		fieldSym = datalog.SymDay
	case "hour":
		fieldSym = datalog.SymHour
	case "minute":
		fieldSym = datalog.SymMinute
	case "second":
		fieldSym = datalog.SymSecond
	default:
		return nil, fmt.Errorf("unknown time extraction function: %s", field)
	}

	return &query.TimeExtractionFunction{
		Field:    fieldSym,
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

// =============================================================================
// Database Function Parsers
// =============================================================================

// parseGetElse parses (get-else $src ?entity :attribute default-value)
// Returns the attribute value the source holds, or the default if missing.
func parseGetElse(args []query.PatternElement) (query.Function, error) {
	// Syntax: (get-else $src ?e :attr default)
	// args[0] = $src (the source, $ or a named source)
	// args[1] = ?e (entity variable or constant)
	// args[2] = :attr (attribute keyword)
	// args[3] = default (a variable or a constant)
	if len(args) != 4 {
		return nil, fmt.Errorf("get-else requires exactly 4 arguments (source entity attr default), got %d", len(args))
	}

	source, err := parseFunctionSource(args[0])
	if err != nil {
		return nil, fmt.Errorf("get-else: %w", err)
	}

	// Parse entity (can be variable or constant)
	entity := elementToTerm(args[1])

	// Parse attribute (must be a keyword)
	attr, err := extractKeyword(args[2])
	if err != nil {
		return nil, fmt.Errorf("get-else: attribute must be a keyword: %w", err)
	}

	// Parse the default: a variable read per tuple, or a constant
	var defaultTerm query.Term
	if variable, ok := args[3].(query.Variable); ok {
		defaultTerm = query.VariableTerm{Symbol: variable.Name}
	} else {
		defaultVal, err := extractConstantValue(args[3])
		if err != nil {
			return nil, fmt.Errorf("get-else: default must be a variable or a constant: %w", err)
		}
		defaultTerm = query.ConstantTerm{Value: defaultVal}
	}

	return &query.GetElseFunction{
		Source:  source,
		Entity:  entity,
		Attr:    attr,
		Default: defaultTerm,
	}, nil
}

// parseMissingAttr parses (missing? $src ?entity :attribute), as an
// expression and, wrapped by parseMissingAttrPredicate, as a predicate.
// Returns true if the entity does NOT have the specified attribute in the source.
// Note: Named parseMissingAttr to avoid conflict with parseMissing in predicate_parser.go
func parseMissingAttr(args []query.PatternElement) (*query.MissingFunction, error) {
	// Syntax: (missing? $src ?e :attr)
	// args[0] = $src (the source, $ or a named source)
	// args[1] = ?e (entity variable or constant)
	// args[2] = :attr (attribute keyword)
	if len(args) != 3 {
		return nil, fmt.Errorf("missing? requires exactly 3 arguments (source entity attr), got %d", len(args))
	}

	source, err := parseFunctionSource(args[0])
	if err != nil {
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
		Source: source,
		Entity: entity,
		Attr:   attr,
	}, nil
}

// parseGetSome parses (get-some $src ?entity :attr1 :attr2 ...)
// Returns the first attribute that exists along with its value.
func parseGetSome(args []query.PatternElement) (query.Function, error) {
	// Syntax: (get-some $src ?e :attr1 :attr2 :attr3 ...)
	// args[0] = $src (the source, $ or a named source)
	// args[1] = ?e (entity variable or constant)
	// args[2..] = :attr1, :attr2, ... (attribute keywords)
	if len(args) < 3 {
		return nil, fmt.Errorf("get-some requires at least 3 arguments (source entity attr...), got %d", len(args))
	}

	source, err := parseFunctionSource(args[0])
	if err != nil {
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
		Source: source,
		Entity: entity,
		Attrs:  attrs,
	}, nil
}

// parseFunctionSource reads a database function's first argument, the source
// the function reads: $ or a named source, a symbol beginning with $, as a
// data pattern's source and an :in entry's source are read.
func parseFunctionSource(arg query.PatternElement) (query.Symbol, error) {
	if constant, ok := arg.(query.Constant); ok {
		if sym, ok := constant.Value.(query.Symbol); ok && sym.IsSource() {
			return sym, nil
		}
	}
	return nil, fmt.Errorf("expected a source ($ or $name) as the first argument, got %v", arg)
}

// extractKeyword extracts a Keyword from a pattern element
func extractKeyword(arg query.PatternElement) (datalog.Keyword, error) {
	switch a := arg.(type) {
	case query.Constant:
		switch v := a.Value.(type) {
		case datalog.Keyword:
			return v, nil
		case string:
			if err := edn.ValidateKeyword(v); err != nil {
				return nil, fmt.Errorf("string %q is not a keyword: %w", v, err)
			}
			return datalog.NewKeyword(v), nil
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
