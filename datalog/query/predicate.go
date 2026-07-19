package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// Predicate represents a boolean condition that can be evaluated against tuples
// It implements Clause so it can be used in Query.Where
type Predicate interface {
	Clause // Embeds Clause interface (Pattern + clause())

	// RequiredSymbols returns all symbols needed to evaluate this predicate
	RequiredSymbols() []Symbol

	// Eval evaluates the predicate against a binding map
	// Returns (passes, error)
	Eval(bindings map[Symbol]interface{}) (bool, error)

	// Optimizer hints
	Selectivity() float64   // 0.0 to 1.0, estimated fraction that pass
	CanPushToStorage() bool // Can this be evaluated at storage level?
}

// Term represents either a variable or a constant value in a predicate
type Term interface {
	// Resolve returns the value of this term given bindings
	// For constants, returns the constant value
	// For variables, looks up in bindings
	Resolve(bindings map[Symbol]interface{}) (interface{}, bool)

	// RequiredSymbols returns any symbols this term needs
	RequiredSymbols() []Symbol

	String() string
}

// VariableTerm represents a variable like ?x
type VariableTerm struct {
	Symbol Symbol
}

func (v VariableTerm) Resolve(bindings map[Symbol]interface{}) (interface{}, bool) {
	val, ok := bindings[v.Symbol]
	return val, ok
}

func (v VariableTerm) RequiredSymbols() []Symbol {
	return []Symbol{v.Symbol}
}

func (v VariableTerm) String() string {
	return v.Symbol.String()
}

// ConstantTerm represents a literal value like 5 or "hello"
type ConstantTerm struct {
	Value interface{}
}

func (c ConstantTerm) Resolve(bindings map[Symbol]interface{}) (interface{}, bool) {
	return c.Value, true
}

func (c ConstantTerm) RequiredSymbols() []Symbol {
	return nil
}

func (c ConstantTerm) String() string {
	return FormatValueEDN(c.Value)
}

// Comparison implements comparison predicates: [(< ?x 10)], [(>= ?y ?z)], etc.
// Op is one of the pre-interned operator symbols (datalog.SymEQ, SymNE,
// SymLT, SymLTE, SymGT, SymGTE); dispatch is pointer equality.
type Comparison struct {
	Op    Symbol
	Left  Term
	Right Term
}

func (c Comparison) RequiredSymbols() []Symbol {
	symbols := c.Left.RequiredSymbols()
	symbols = append(symbols, c.Right.RequiredSymbols()...)
	return symbols
}

func (c Comparison) Eval(bindings map[Symbol]interface{}) (bool, error) {
	leftVal, leftOk := c.Left.Resolve(bindings)
	if !leftOk {
		return false, fmt.Errorf("cannot resolve left term %s", c.Left)
	}

	rightVal, rightOk := c.Right.Resolve(bindings)
	if !rightOk {
		return false, fmt.Errorf("cannot resolve right term %s", c.Right)
	}

	// = and != are value equality (ValuesEqual): type-strict, the same
	// relation join keys use, so the planner's equi-join rewrite of
	// [(= ?x ?y)] into a join condition preserves semantics. The ordering
	// operators use the total order (CompareValues), where int and float
	// compare by magnitude — (>= 3 3.0) is true while (= 3 3.0) is false,
	// as in Clojure.
	switch c.Op {
	case datalog.SymEQ:
		return datalog.ValuesEqual(leftVal, rightVal), nil
	case datalog.SymNE:
		return !datalog.ValuesEqual(leftVal, rightVal), nil
	case datalog.SymLT:
		return datalog.CompareValues(leftVal, rightVal) < 0, nil
	case datalog.SymLTE:
		return datalog.CompareValues(leftVal, rightVal) <= 0, nil
	case datalog.SymGT:
		return datalog.CompareValues(leftVal, rightVal) > 0, nil
	case datalog.SymGTE:
		return datalog.CompareValues(leftVal, rightVal) >= 0, nil
	default:
		return false, fmt.Errorf("unknown comparison operator: %v", c.Op)
	}
}

func (c Comparison) String() string {
	return fmt.Sprintf("[(%s %s %s)]", c.Op, c.Left, c.Right)
}

func (c Comparison) Selectivity() float64 {
	// Basic heuristics
	switch c.Op {
	case datalog.SymEQ:
		return 0.1 // Equality is typically selective
	case datalog.SymLT, datalog.SymGT:
		return 0.3 // Less/greater typically filter ~70%
	case datalog.SymLTE, datalog.SymGTE:
		return 0.33 // Slightly less selective
	default:
		return 0.5
	}
}

func (c Comparison) CanPushToStorage() bool {
	// Can push if comparing a variable to a constant
	_, leftIsVar := c.Left.(VariableTerm)
	_, rightIsVar := c.Right.(VariableTerm)
	return leftIsVar != rightIsVar // XOR - one is var, one is const
}

// ChainedComparison implements Clojure-style chained comparisons: [(< 0 ?x 100)]
// Op is one of the pre-interned operator symbols, as in Comparison.
type ChainedComparison struct {
	Op    Symbol
	Terms []Term
}

func (c ChainedComparison) RequiredSymbols() []Symbol {
	var symbols []Symbol
	for _, term := range c.Terms {
		symbols = append(symbols, term.RequiredSymbols()...)
	}
	return symbols
}

func (c ChainedComparison) Eval(bindings map[Symbol]interface{}) (bool, error) {
	if len(c.Terms) < 2 {
		return false, fmt.Errorf("chained comparison requires at least 2 terms")
	}

	// Evaluate each adjacent pair
	for i := 0; i < len(c.Terms)-1; i++ {
		leftVal, leftOk := c.Terms[i].Resolve(bindings)
		if !leftOk {
			return false, fmt.Errorf("cannot resolve term %s", c.Terms[i])
		}

		rightVal, rightOk := c.Terms[i+1].Resolve(bindings)
		if !rightOk {
			return false, fmt.Errorf("cannot resolve term %s", c.Terms[i+1])
		}

		// Check if this pair satisfies the operator. Equality is
		// ValuesEqual (type-strict); ordering is CompareValues, as in
		// Comparison.Eval.
		ok := false
		switch c.Op {
		case datalog.SymLT:
			ok = datalog.CompareValues(leftVal, rightVal) < 0
		case datalog.SymLTE:
			ok = datalog.CompareValues(leftVal, rightVal) <= 0
		case datalog.SymGT:
			ok = datalog.CompareValues(leftVal, rightVal) > 0
		case datalog.SymGTE:
			ok = datalog.CompareValues(leftVal, rightVal) >= 0
		case datalog.SymEQ:
			ok = datalog.ValuesEqual(leftVal, rightVal)
		default:
			return false, fmt.Errorf("unknown comparison operator: %v", c.Op)
		}

		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func (c ChainedComparison) String() string {
	s := fmt.Sprintf("[(%s", c.Op)
	for _, term := range c.Terms {
		s += " " + term.String()
	}
	s += ")]"
	return s
}

func (c ChainedComparison) Selectivity() float64 {
	// More terms = more selective
	return 1.0 / float64(len(c.Terms))
}

func (c ChainedComparison) CanPushToStorage() bool {
	// Can only push simple cases
	if len(c.Terms) != 3 {
		return false
	}
	// Middle term should be variable, outer terms constants
	if _, ok := c.Terms[1].(VariableTerm); !ok {
		return false
	}
	if _, ok := c.Terms[0].(ConstantTerm); !ok {
		return false
	}
	if _, ok := c.Terms[2].(ConstantTerm); !ok {
		return false
	}
	return true
}

// GroundPredicate checks if all specified variables are bound (have values)
type GroundPredicate struct {
	Variables []Symbol
}

func (g GroundPredicate) RequiredSymbols() []Symbol {
	// Ground doesn't require symbols to be bound - it checks if they are
	return nil
}

func (g GroundPredicate) Eval(bindings map[Symbol]interface{}) (bool, error) {
	for _, sym := range g.Variables {
		if _, ok := bindings[sym]; !ok {
			return false, nil
		}
	}
	return true, nil
}

func (g GroundPredicate) String() string {
	s := "[(ground"
	for _, v := range g.Variables {
		s += " " + v.String()
	}
	s += ")]"
	return s
}

func (g GroundPredicate) Selectivity() float64 {
	// Ground is usually quite selective
	return 0.1
}

func (g GroundPredicate) CanPushToStorage() bool {
	return false // Can't push ground checks to storage
}

// MissingPredicate checks if specified variables are NOT bound
type MissingPredicate struct {
	Variables []Symbol
}

func (m MissingPredicate) RequiredSymbols() []Symbol {
	return nil // Missing checks for absence
}

func (m MissingPredicate) Eval(bindings map[Symbol]interface{}) (bool, error) {
	for _, sym := range m.Variables {
		if _, ok := bindings[sym]; ok {
			return false, nil // Variable is bound, so not missing
		}
	}
	return true, nil
}

func (m MissingPredicate) String() string {
	s := "[(missing"
	for _, v := range m.Variables {
		s += " " + v.String()
	}
	s += ")]"
	return s
}

func (m MissingPredicate) Selectivity() float64 {
	return 0.9 // Missing is usually not very selective
}

func (m MissingPredicate) CanPushToStorage() bool {
	return false
}

// NotEqualPredicate handles != comparisons
type NotEqualPredicate struct {
	Comparison // Embed comparison but invert the result
}

func (n NotEqualPredicate) Eval(bindings map[Symbol]interface{}) (bool, error) {
	result, err := n.Comparison.Eval(bindings)
	if err != nil {
		return false, err
	}
	return !result, nil // Invert the equality result
}

func (n NotEqualPredicate) String() string {
	return fmt.Sprintf("[(!= %s %s)]", n.Left, n.Right)
}

// FunctionPredicate handles arbitrary function predicates like str/starts-with?
type FunctionPredicate struct {
	Fn   string
	Args []PatternElement
}

func (f FunctionPredicate) RequiredSymbols() []Symbol {
	var syms []Symbol
	for _, arg := range f.Args {
		if v, ok := arg.(Variable); ok {
			syms = append(syms, Symbol(v.Name))
		}
	}
	return syms
}

func (f FunctionPredicate) Eval(bindings map[Symbol]interface{}) (bool, error) {
	// For now, we'll handle a few common functions
	switch f.Fn {
	case "str/starts-with?":
		if len(f.Args) != 2 {
			return false, fmt.Errorf("str/starts-with? requires 2 arguments, got %d", len(f.Args))
		}
		// Get the string value
		var str string
		if v, ok := f.Args[0].(Variable); ok {
			val, exists := bindings[Symbol(v.Name)]
			if !exists {
				return false, fmt.Errorf("variable %s not bound", v.Name)
			}
			str, ok = val.(string)
			if !ok {
				return false, nil // Not a string, can't start with prefix
			}
		} else if c, ok := f.Args[0].(Constant); ok {
			str, ok = c.Value.(string)
			if !ok {
				return false, nil
			}
		}

		// Get the prefix
		var prefix string
		if v, ok := f.Args[1].(Variable); ok {
			val, exists := bindings[Symbol(v.Name)]
			if !exists {
				return false, fmt.Errorf("variable %s not bound", v.Name)
			}
			prefix, ok = val.(string)
			if !ok {
				return false, nil
			}
		} else if c, ok := f.Args[1].(Constant); ok {
			prefix, ok = c.Value.(string)
			if !ok {
				return false, nil
			}
		}

		return len(str) >= len(prefix) && str[:len(prefix)] == prefix, nil

	default:
		// Unknown function - for now just return false
		// In a real implementation, we'd have a registry of functions
		return false, fmt.Errorf("unknown predicate function: %s", f.Fn)
	}
}

func (f FunctionPredicate) String() string {
	s := fmt.Sprintf("[(%s", f.Fn)
	for _, arg := range f.Args {
		s += " " + arg.String()
	}
	s += ")]"
	return s
}

func (f FunctionPredicate) Selectivity() float64 {
	// Default selectivity for unknown functions
	return 0.5
}

func (f FunctionPredicate) CanPushToStorage() bool {
	// Most custom functions can't be pushed to storage
	return false
}

func (f FunctionPredicate) clause() {}
