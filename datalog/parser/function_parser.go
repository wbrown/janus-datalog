package parser

import (
	"fmt"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// parseFunction creates a concrete Function from expression pattern arguments
func parseFunction(fn string, args []query.PatternElement) (query.Function, error) {
	switch fn {
	case "+", "-", "*", "/":
		return parseArithmetic(fn, args)
	case "str":
		return parseStringConcat(args)
	case "year", "month", "day", "hour", "minute", "second":
		return parseTimeExtraction(fn, args)
	case "ground":
		return parseGroundFunction(args)
	case "identity":
		return parseIdentity(args)
	default:
		return nil, fmt.Errorf("unsupported function: %s", fn)
	}
}

// parseArithmetic handles arithmetic functions
func parseArithmetic(fn string, args []query.PatternElement) (query.Function, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("%s requires exactly 2 arguments, got %d", fn, len(args))
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

	return &query.ArithmeticFunction{
		Op:    op,
		Left:  elementToTerm(args[0]),
		Right: elementToTerm(args[1]),
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

// parseGroundFunction handles ground function - binds a constant value
func parseGroundFunction(args []query.PatternElement) (query.Function, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ground function requires exactly 1 argument, got %d", len(args))
	}

	// Ground takes a constant value
	if constant, ok := args[0].(query.Constant); ok {
		return &query.GroundFunction{
			Value: constant.Value,
		}, nil
	}

	return nil, fmt.Errorf("ground function requires a constant value, got %T", args[0])
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
