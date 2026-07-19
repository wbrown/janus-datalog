package query

import (
	"fmt"
	"strings"
	"time"
)

// Function represents an expression that evaluates to a value
// It implements Pattern so it can be used in Query.Where
type Function interface {
	Pattern // Embeds Pattern interface (just String() method)

	// RequiredSymbols returns all symbols needed to evaluate this function
	RequiredSymbols() []Symbol

	// Eval evaluates the function with given bindings
	// Returns (result, error)
	Eval(bindings map[Symbol]interface{}) (interface{}, error)

	// ReturnType hints at what type this function returns
	ReturnType() string // "number", "string", "boolean", "time", "any"
}

// ArithmeticOp represents arithmetic operators
type ArithmeticOp string

const (
	OpAdd      ArithmeticOp = "+"
	OpSubtract ArithmeticOp = "-"
	OpMultiply ArithmeticOp = "*"
	OpDivide   ArithmeticOp = "/"
)

// ArithmeticFunction implements arithmetic operations
type ArithmeticFunction struct {
	Op   ArithmeticOp
	Args []Term
}

func (a ArithmeticFunction) RequiredSymbols() []Symbol {
	var symbols []Symbol
	for _, argument := range a.Args {
		symbols = append(symbols, argument.RequiredSymbols()...)
	}
	return symbols
}

func (a ArithmeticFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	if len(a.Args) == 0 {
		return nil, fmt.Errorf("%s requires at least one argument", a.Op)
	}

	values := make([]interface{}, len(a.Args))
	useFloat := a.Op == OpDivide
	for i, argument := range a.Args {
		value, ok := argument.Resolve(bindings)
		if !ok {
			return nil, fmt.Errorf("cannot resolve arithmetic operand %s", argument)
		}
		number, err := toNumber(value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", a.Op, err)
		}
		values[i] = number
		if _, ok := number.(float64); ok {
			useFloat = true
		}
	}

	if useFloat {
		result := toFloat64(values[0])
		switch a.Op {
		case OpAdd:
			for _, value := range values[1:] {
				result += toFloat64(value)
			}
		case OpSubtract:
			if len(values) == 1 {
				return -result, nil
			}
			for _, value := range values[1:] {
				result -= toFloat64(value)
			}
		case OpMultiply:
			for _, value := range values[1:] {
				result *= toFloat64(value)
			}
		case OpDivide:
			if len(values) == 1 {
				if result == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return 1 / result, nil
			}
			for _, value := range values[1:] {
				divisor := toFloat64(value)
				if divisor == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				result /= divisor
			}
		default:
			return nil, fmt.Errorf("unknown arithmetic operator: %s", a.Op)
		}
		return result, nil
	}

	result := toInt64(values[0])
	switch a.Op {
	case OpAdd:
		for _, value := range values[1:] {
			result += toInt64(value)
		}
	case OpSubtract:
		if len(values) == 1 {
			return -result, nil
		}
		for _, value := range values[1:] {
			result -= toInt64(value)
		}
	case OpMultiply:
		for _, value := range values[1:] {
			result *= toInt64(value)
		}
	default:
		return nil, fmt.Errorf("unknown arithmetic operator: %s", a.Op)
	}
	return result, nil
}

func (a ArithmeticFunction) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "(%s", a.Op)
	for _, argument := range a.Args {
		fmt.Fprintf(&b, " %s", argument)
	}
	b.WriteByte(')')
	return b.String()
}

func (a ArithmeticFunction) ReturnType() string {
	return "number"
}

// StringConcatFunction implements string concatenation
type StringConcatFunction struct {
	Terms []Term
}

func (s StringConcatFunction) RequiredSymbols() []Symbol {
	var symbols []Symbol
	for _, term := range s.Terms {
		symbols = append(symbols, term.RequiredSymbols()...)
	}
	return symbols
}

func (s StringConcatFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	var result string
	for _, term := range s.Terms {
		val, ok := term.Resolve(bindings)
		if !ok {
			return nil, fmt.Errorf("cannot resolve term %s", term)
		}
		result += toString(val)
	}
	return result, nil
}

func (s StringConcatFunction) String() string {
	str := "(str"
	for _, term := range s.Terms {
		str += " " + term.String()
	}
	str += ")"
	return str
}

func (s StringConcatFunction) ReturnType() string {
	return "string"
}

// TimeExtractionFunction extracts components from time values
type TimeExtractionFunction struct {
	Field    string // "year", "month", "day", "hour", "minute", "second"
	TimeTerm Term
}

func (t TimeExtractionFunction) RequiredSymbols() []Symbol {
	return t.TimeTerm.RequiredSymbols()
}

func (t TimeExtractionFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	timeVal, ok := t.TimeTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("cannot resolve time term %s", t.TimeTerm)
	}

	tm, ok := timeVal.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected time.Time, got %T", timeVal)
	}

	switch t.Field {
	case "year":
		return int64(tm.Year()), nil
	case "month":
		return int64(tm.Month()), nil
	case "day":
		return int64(tm.Day()), nil
	case "hour":
		return int64(tm.Hour()), nil
	case "minute":
		return int64(tm.Minute()), nil
	case "second":
		return int64(tm.Second()), nil
	default:
		return nil, fmt.Errorf("unknown time field: %s", t.Field)
	}
}

func (t TimeExtractionFunction) String() string {
	return fmt.Sprintf("(%s %s)", t.Field, t.TimeTerm)
}

func (t TimeExtractionFunction) ReturnType() string {
	return "number"
}

// GroundFunction binds a constant value to a variable
// Example: [(ground 42) ?x] binds 42 to ?x
type GroundFunction struct {
	Value interface{}
}

func (g GroundFunction) RequiredSymbols() []Symbol {
	return nil // No symbols required, just returns the constant
}

func (g GroundFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	return g.Value, nil
}

func (g GroundFunction) String() string {
	// Handle vector values for tuple ground
	if values, ok := g.Value.([]interface{}); ok {
		parts := make([]string, len(values))
		for i, v := range values {
			parts[i] = FormatValueEDN(v)
		}
		result := "["
		for i, p := range parts {
			if i > 0 {
				result += " "
			}
			result += p
		}
		result += "]"
		return fmt.Sprintf("(ground %s)", result)
	}
	return fmt.Sprintf("(ground %s)", FormatValueEDN(g.Value))
}

func (g GroundFunction) ReturnType() string {
	return "any"
}

// IdentityFunction passes through a value unchanged
// Example: [(identity ?x) ?y] binds the value of ?x to ?y
type IdentityFunction struct {
	Arg Term
}

func (i IdentityFunction) RequiredSymbols() []Symbol {
	return i.Arg.RequiredSymbols()
}

func (i IdentityFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	val, ok := i.Arg.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("cannot resolve argument %s", i.Arg)
	}
	return val, nil
}

func (i IdentityFunction) String() string {
	return fmt.Sprintf("(identity %s)", i.Arg)
}

func (i IdentityFunction) ReturnType() string {
	return "any"
}

// toNumber normalizes a numeric operand to int64 or float64 (Go integer and
// float widths normalize; there are no wrapper types). Anything else —
// including numeric strings — is a loud error: strings become values of
// other types by boundary construction, never by evaluation-time parsing.
func toNumber(val interface{}) (interface{}, error) {
	switch v := val.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return nil, fmt.Errorf("operand %v (%T) is not a number", val, val)
	}
}

// toInt64 and toFloat64 convert between the two shapes toNumber produces.
// Any other type here is a bug: every operand passed through toNumber first.
func toInt64(val interface{}) int64 {
	switch v := val.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		panic(fmt.Sprintf("BUG: non-normalized arithmetic operand %T reached toInt64", val))
	}
}

func toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	default:
		panic(fmt.Sprintf("BUG: non-normalized arithmetic operand %T reached toFloat64", val))
	}
}

func toString(val interface{}) string {
	return fmt.Sprintf("%v", val)
}

// ComparisonFunction wraps a Comparison as a Function
// This allows comparisons to be used in expression bindings
type ComparisonFunction struct {
	Comparison *Comparison
}

func (c ComparisonFunction) RequiredSymbols() []Symbol {
	return c.Comparison.RequiredSymbols()
}

func (c ComparisonFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	// Evaluate the comparison and return the boolean result as interface{}
	result, err := c.Comparison.Eval(bindings)
	return result, err
}

func (c ComparisonFunction) ReturnType() string {
	return "boolean"
}

func (c ComparisonFunction) String() string {
	return c.Comparison.String()
}

// ChainedComparisonFunction wraps a ChainedComparison as a Function
// This allows chained comparisons to be used in expression bindings
// Example: [(< 0 ?x 100) ?in-range] binds the boolean result to ?in-range
type ChainedComparisonFunction struct {
	ChainedComparison *ChainedComparison
}

func (c ChainedComparisonFunction) RequiredSymbols() []Symbol {
	return c.ChainedComparison.RequiredSymbols()
}

func (c ChainedComparisonFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	// Evaluate the chained comparison and return the boolean result as interface{}
	result, err := c.ChainedComparison.Eval(bindings)
	return result, err
}

func (c ChainedComparisonFunction) ReturnType() string {
	return "boolean"
}

func (c ChainedComparisonFunction) String() string {
	return c.ChainedComparison.String()
}

// AndFunction combines multiple boolean terms with logical AND
// Used for synthesizing filter predicates in query rewriting
type AndFunction struct {
	Terms []Symbol // Variables that must all be true
}

func (a AndFunction) RequiredSymbols() []Symbol {
	return a.Terms
}

func (a AndFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	// All terms must be true
	for _, term := range a.Terms {
		val, ok := bindings[term]
		if !ok {
			return false, fmt.Errorf("variable %s not bound", term)
		}

		// Check if it's a boolean and true
		boolVal, isBool := val.(bool)
		if !isBool {
			// Non-boolean values are treated as false
			return false, nil
		}
		if !boolVal {
			// Short-circuit on first false
			return false, nil
		}
	}

	// All terms are true
	return true, nil
}

func (a AndFunction) ReturnType() string {
	return "boolean"
}

func (a AndFunction) String() string {
	if len(a.Terms) == 0 {
		return "(and)"
	}
	if len(a.Terms) == 1 {
		return a.Terms[0].String()
	}

	result := "(and"
	for _, term := range a.Terms {
		result += " " + term.String()
	}
	result += ")"
	return result
}
