package query

import (
	"fmt"
	"strconv"
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
	Op    ArithmeticOp
	Left  Term
	Right Term
}

func (a ArithmeticFunction) RequiredSymbols() []Symbol {
	symbols := a.Left.RequiredSymbols()
	symbols = append(symbols, a.Right.RequiredSymbols()...)
	return symbols
}

func (a ArithmeticFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	leftVal, leftOk := a.Left.Resolve(bindings)
	if !leftOk {
		return nil, fmt.Errorf("cannot resolve left operand %s", a.Left)
	}

	rightVal, rightOk := a.Right.Resolve(bindings)
	if !rightOk {
		return nil, fmt.Errorf("cannot resolve right operand %s", a.Right)
	}

	// Convert to numbers
	left := toNumber(leftVal)
	right := toNumber(rightVal)

	// Determine if we need float arithmetic
	_, leftIsFloat := left.(float64)
	_, rightIsFloat := right.(float64)
	useFloat := leftIsFloat || rightIsFloat

	if useFloat {
		leftFloat := toFloat64(left)
		rightFloat := toFloat64(right)

		switch a.Op {
		case OpAdd:
			return leftFloat + rightFloat, nil
		case OpSubtract:
			return leftFloat - rightFloat, nil
		case OpMultiply:
			return leftFloat * rightFloat, nil
		case OpDivide:
			if rightFloat == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return leftFloat / rightFloat, nil
		}
	} else {
		leftInt := toInt64(left)
		rightInt := toInt64(right)

		switch a.Op {
		case OpAdd:
			return leftInt + rightInt, nil
		case OpSubtract:
			return leftInt - rightInt, nil
		case OpMultiply:
			return leftInt * rightInt, nil
		case OpDivide:
			if rightInt == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			// Integer division returns float for compatibility
			return float64(leftInt) / float64(rightInt), nil
		}
	}

	return nil, fmt.Errorf("unknown arithmetic operator: %s", a.Op)
}

func (a ArithmeticFunction) String() string {
	return fmt.Sprintf("(%s %s %s)", a.Op, a.Left, a.Right)
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
	return fmt.Sprintf("(ground %v)", g.Value)
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

// Helper functions for type conversion
func toNumber(val interface{}) interface{} {
	switch v := val.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case float32:
		return float64(v)
	case float64:
		return v
	case string:
		// Try parsing as int first
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		// Try parsing as float
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return int64(0)
}

func toInt64(val interface{}) int64 {
	switch v := val.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	default:
		return 0.0
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
		return string(a.Terms[0])
	}

	result := "(and"
	for _, term := range a.Terms {
		result += " " + string(term)
	}
	result += ")"
	return result
}
