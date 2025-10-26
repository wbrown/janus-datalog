package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Filter represents any filtering operation on tuples
type Filter interface {
	// RequiredSymbols returns the symbols this filter needs
	RequiredSymbols() []query.Symbol

	// Evaluate returns true if the tuple passes the filter
	Evaluate(tuple Tuple, columns []query.Symbol) bool

	// String returns a string representation of the filter
	String() string
}

// ComparisonFilter implements comparisons like (< ?x 10)
type ComparisonFilter struct {
	Function string
	Symbol   query.Symbol
	Value    interface{}
}

func (f ComparisonFilter) RequiredSymbols() []query.Symbol {
	return []query.Symbol{f.Symbol}
}

func (f ComparisonFilter) Evaluate(tuple Tuple, columns []query.Symbol) bool {
	// Find the column index
	idx := -1
	for i, col := range columns {
		if col == f.Symbol {
			idx = i
			break
		}
	}

	if idx < 0 || idx >= len(tuple) {
		return false
	}

	return evaluateComparison(f.Function, tuple[idx], f.Value)
}

func (f ComparisonFilter) String() string {
	return fmt.Sprintf("(%s %s %v)", f.Function, f.Symbol, f.Value)
}

// BinaryFilter compares two variables like (< ?x ?y)
type BinaryFilter struct {
	Function string
	Left     query.Symbol
	Right    query.Symbol
}

func (f BinaryFilter) RequiredSymbols() []query.Symbol {
	return []query.Symbol{f.Left, f.Right}
}

func (f BinaryFilter) Evaluate(tuple Tuple, columns []query.Symbol) bool {
	// Find column indices
	leftIdx := -1
	rightIdx := -1

	for i, col := range columns {
		if col == f.Left {
			leftIdx = i
		}
		if col == f.Right {
			rightIdx = i
		}
	}

	if leftIdx < 0 || rightIdx < 0 || leftIdx >= len(tuple) || rightIdx >= len(tuple) {
		return false
	}

	return evaluateComparison(f.Function, tuple[leftIdx], tuple[rightIdx])
}

func (f BinaryFilter) String() string {
	return fmt.Sprintf("(%s %s %s)", f.Function, f.Left, f.Right)
}

// VariadicFilter implements variadic comparisons like (< ?a ?b ?c 100)
type VariadicFilter struct {
	Function string
	Args     []query.PatternElement
}

func (f VariadicFilter) RequiredSymbols() []query.Symbol {
	var symbols []query.Symbol
	for _, arg := range f.Args {
		if v, ok := arg.(query.Variable); ok {
			symbols = append(symbols, v.Name)
		}
	}
	return symbols
}

func (f VariadicFilter) Evaluate(tuple Tuple, columns []query.Symbol) bool {
	// Resolve all arguments to values
	values := make([]interface{}, len(f.Args))
	for i, arg := range f.Args {
		switch a := arg.(type) {
		case query.Variable:
			// Find the column index
			idx := -1
			for j, col := range columns {
				if col == a.Name {
					idx = j
					break
				}
			}
			if idx < 0 || idx >= len(tuple) {
				return false
			}
			values[i] = tuple[idx]
		case query.Constant:
			values[i] = a.Value
		default:
			return false
		}
	}

	// Check that each adjacent pair satisfies the comparison
	for i := 0; i < len(values)-1; i++ {
		if !evaluateComparison(f.Function, values[i], values[i+1]) {
			return false
		}
	}

	return true
}

func (f VariadicFilter) String() string {
	args := make([]string, len(f.Args))
	for i, arg := range f.Args {
		args[i] = fmt.Sprint(arg)
	}
	return fmt.Sprintf("(%s %v)", f.Function, args)
}

// FilterRelation applies a filter to a relation
func FilterRelation(rel Relation, filter Filter) Relation {
	// Check if all required symbols are present
	cols := rel.Columns()
	for _, sym := range filter.RequiredSymbols() {
		found := false
		for _, col := range cols {
			if col == sym {
				found = true
				break
			}
		}
		if !found {
			// Missing required symbol - return empty relation
			return NewMaterializedRelation(cols, nil)
		}
	}

	// Apply filter
	predFunc := func(tuple Tuple) bool {
		return filter.Evaluate(tuple, cols)
	}

	return Select(rel, predFunc)
}

// evaluateComparison evaluates a comparison function
func evaluateComparison(function string, left, right interface{}) bool {
	// Check for custom functions first
	if result, found, err := CallCustomFunction(function, []interface{}{left, right}); found {
		if err != nil {
			return false
		}
		if b, ok := result.(bool); ok {
			return b
		}
		return false
	}

	switch function {
	case "<":
		return datalog.CompareValues(left, right) < 0
	case ">":
		return datalog.CompareValues(left, right) > 0
	case "<=":
		return datalog.CompareValues(left, right) <= 0
	case ">=":
		return datalog.CompareValues(left, right) >= 0
	case "=":
		return datalog.CompareValues(left, right) == 0
	case "!=":
		return datalog.CompareValues(left, right) != 0
	default:
		// Unknown comparison
		return false
	}
}
