package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// Vector functions for cardinality-vector attributes in CRDT storage.
// These functions operate on vectors ([]interface{}) returned from vector queries.

// NthFunction returns the element at a given index in a vector.
// Usage: [(nth ?vec ?n) ?val]
// Returns nil for out-of-bounds indices (not an error).
type NthFunction struct {
	VecTerm   Term
	IndexTerm Term
}

func (f NthFunction) RequiredSymbols() []Symbol {
	symbols := f.VecTerm.RequiredSymbols()
	return append(symbols, f.IndexTerm.RequiredSymbols()...)
}

func (f NthFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	vecVal, ok := f.VecTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("nth: cannot resolve vector argument %s", f.VecTerm)
	}

	idxVal, ok := f.IndexTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("nth: cannot resolve index argument %s", f.IndexTerm)
	}

	vec, ok := VectorElements(vecVal)
	if !ok {
		return nil, fmt.Errorf("nth: first argument must be a vector, got %T", vecVal)
	}

	idx, err := toVectorIndex(idxVal)
	if err != nil {
		return nil, fmt.Errorf("nth: %w", err)
	}
	if idx < 0 || idx >= int64(len(vec)) {
		return nil, nil // Out of bounds returns nil (not an error)
	}

	return vec[idx], nil
}

func (f NthFunction) String() string {
	return fmt.Sprintf("(nth %s %s)", f.VecTerm, f.IndexTerm)
}

// FirstFunction returns the first element of a vector.
// Usage: [(first ?vec) ?val]
// Returns nil for empty vectors.
type FirstFunction struct {
	VecTerm Term
}

func (f FirstFunction) RequiredSymbols() []Symbol {
	return f.VecTerm.RequiredSymbols()
}

func (f FirstFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	vecVal, ok := f.VecTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("first: cannot resolve vector argument %s", f.VecTerm)
	}

	vec, ok := VectorElements(vecVal)
	if !ok {
		return nil, fmt.Errorf("first: argument must be a vector, got %T", vecVal)
	}

	if len(vec) == 0 {
		return nil, nil // Empty vector returns nil
	}

	return vec[0], nil
}

func (f FirstFunction) String() string {
	return fmt.Sprintf("(first %s)", f.VecTerm)
}

// LastFunction returns the last element of a vector.
// Usage: [(last ?vec) ?val]
// Returns nil for empty vectors.
type LastFunction struct {
	VecTerm Term
}

func (f LastFunction) RequiredSymbols() []Symbol {
	return f.VecTerm.RequiredSymbols()
}

func (f LastFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	vecVal, ok := f.VecTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("last: cannot resolve vector argument %s", f.VecTerm)
	}

	vec, ok := VectorElements(vecVal)
	if !ok {
		return nil, fmt.Errorf("last: argument must be a vector, got %T", vecVal)
	}

	if len(vec) == 0 {
		return nil, nil // Empty vector returns nil
	}

	return vec[len(vec)-1], nil
}

func (f LastFunction) String() string {
	return fmt.Sprintf("(last %s)", f.VecTerm)
}

// LengthFunction returns the length of a vector.
// Usage: [(length ?vec) ?n]
type LengthFunction struct {
	VecTerm Term
}

func (f LengthFunction) RequiredSymbols() []Symbol {
	return f.VecTerm.RequiredSymbols()
}

func (f LengthFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	vecVal, ok := f.VecTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("length: cannot resolve vector argument %s", f.VecTerm)
	}

	vec, ok := VectorElements(vecVal)
	if !ok {
		return nil, fmt.Errorf("length: argument must be a vector, got %T", vecVal)
	}

	return int64(len(vec)), nil
}

func (f LengthFunction) String() string {
	return fmt.Sprintf("(length %s)", f.VecTerm)
}

// ContainsFunction returns true if a value is in a vector.
// Usage: [(contains? ?vec ?val)]
// Note: This is typically used as a predicate (filter), not a binding function.
type ContainsFunction struct {
	VecTerm   Term
	ValueTerm Term
}

func (f ContainsFunction) RequiredSymbols() []Symbol {
	symbols := f.VecTerm.RequiredSymbols()
	return append(symbols, f.ValueTerm.RequiredSymbols()...)
}

func (f ContainsFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	vecVal, ok := f.VecTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("contains?: cannot resolve vector argument %s", f.VecTerm)
	}

	searchVal, ok := f.ValueTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("contains?: cannot resolve value argument %s", f.ValueTerm)
	}

	vec, ok := VectorElements(vecVal)
	if !ok {
		return nil, fmt.Errorf("contains?: first argument must be a vector, got %T", vecVal)
	}

	for _, v := range vec {
		if datalog.ValuesEqual(v, searchVal) {
			return true, nil
		}
	}

	return false, nil
}

func (f ContainsFunction) String() string {
	return fmt.Sprintf("(contains? %s %s)", f.VecTerm, f.ValueTerm)
}

// IndexOfFunction returns the index of a value in a vector, or -1 if not found.
// Usage: [(index-of ?vec ?val) ?idx]
type IndexOfFunction struct {
	VecTerm   Term
	ValueTerm Term
}

func (f IndexOfFunction) RequiredSymbols() []Symbol {
	symbols := f.VecTerm.RequiredSymbols()
	return append(symbols, f.ValueTerm.RequiredSymbols()...)
}

func (f IndexOfFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	vecVal, ok := f.VecTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("index-of: cannot resolve vector argument %s", f.VecTerm)
	}

	searchVal, ok := f.ValueTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("index-of: cannot resolve value argument %s", f.ValueTerm)
	}

	vec, ok := VectorElements(vecVal)
	if !ok {
		return nil, fmt.Errorf("index-of: first argument must be a vector, got %T", vecVal)
	}

	for i, v := range vec {
		if datalog.ValuesEqual(v, searchVal) {
			return int64(i), nil
		}
	}

	return int64(-1), nil // Not found
}

func (f IndexOfFunction) String() string {
	return fmt.Sprintf("(index-of %s %s)", f.VecTerm, f.ValueTerm)
}

// SubvecFunction returns a slice of a vector from start (inclusive) to end (exclusive).
// Usage: [(subvec ?vec ?start ?end) ?sub]
// Bounds are clamped to valid range.
type SubvecFunction struct {
	VecTerm   Term
	StartTerm Term
	EndTerm   Term
}

func (f SubvecFunction) RequiredSymbols() []Symbol {
	symbols := f.VecTerm.RequiredSymbols()
	symbols = append(symbols, f.StartTerm.RequiredSymbols()...)
	return append(symbols, f.EndTerm.RequiredSymbols()...)
}

func (f SubvecFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	vecVal, ok := f.VecTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("subvec: cannot resolve vector argument %s", f.VecTerm)
	}

	startVal, ok := f.StartTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("subvec: cannot resolve start argument %s", f.StartTerm)
	}

	endVal, ok := f.EndTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("subvec: cannot resolve end argument %s", f.EndTerm)
	}

	vec, ok := VectorElements(vecVal)
	if !ok {
		return nil, fmt.Errorf("subvec: first argument must be a vector, got %T", vecVal)
	}

	start, err := toVectorIndex(startVal)
	if err != nil {
		return nil, fmt.Errorf("subvec: %w", err)
	}
	end, err := toVectorIndex(endVal)
	if err != nil {
		return nil, fmt.Errorf("subvec: %w", err)
	}

	// Clamp bounds
	if start < 0 {
		start = 0
	}
	if end > int64(len(vec)) {
		end = int64(len(vec))
	}
	if start >= end {
		return []interface{}{}, nil // Empty slice
	}

	// Return copy to avoid aliasing
	result := make([]interface{}, end-start)
	copy(result, vec[start:end])
	return result, nil
}

func (f SubvecFunction) String() string {
	return fmt.Sprintf("(subvec %s %s %s)", f.VecTerm, f.StartTerm, f.EndTerm)
}

// EnumerateFunction yields a vector's elements as (index, value) tuples, a
// relation bound under the relation form: [(enumerate ?vec) [[?idx ?val]]].
type EnumerateFunction struct {
	VecTerm Term
}

func (f EnumerateFunction) RequiredSymbols() []Symbol {
	return f.VecTerm.RequiredSymbols()
}

// Eval is the collection of (index, value) tuples as a vector of vectors,
// the value a relation ground holds.
func (f EnumerateFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	vecVal, ok := f.VecTerm.Resolve(bindings)
	if !ok {
		return nil, fmt.Errorf("enumerate: cannot resolve vector argument %s", f.VecTerm)
	}

	vec, ok := VectorElements(vecVal)
	if !ok {
		return nil, fmt.Errorf("enumerate: argument must be a vector, got %T", vecVal)
	}

	result := make([]interface{}, len(vec))
	for i, v := range vec {
		result[i] = []interface{}{int64(i), v}
	}
	return result, nil
}

func (f EnumerateFunction) String() string {
	return fmt.Sprintf("(enumerate %s)", f.VecTerm)
}

// Vector operation functions

// VectorElements is the elements of a vector value, and whether the value is
// one: a []interface{} as it is, a typed slice of strings, integers, floats
// or booleans as values, and any other value no vector.
func VectorElements(val interface{}) ([]interface{}, bool) {
	switch v := val.(type) {
	case []interface{}:
		return v, true
	case []string:
		result := make([]interface{}, len(v))
		for i, s := range v {
			result[i] = s
		}
		return result, true
	case []int64:
		result := make([]interface{}, len(v))
		for i, n := range v {
			result[i] = n
		}
		return result, true
	case []float64:
		result := make([]interface{}, len(v))
		for i, f := range v {
			result[i] = f
		}
		return result, true
	case []int:
		result := make([]interface{}, len(v))
		for i, n := range v {
			result[i] = int64(n)
		}
		return result, true
	case []bool:
		result := make([]interface{}, len(v))
		for i, b := range v {
			result[i] = b
		}
		return result, true
	default:
		return nil, false
	}
}

// toVectorIndex converts an index operand to int64. Indices are integers
// (Go integer widths normalize); floats and strings do not coerce. Callers
// wrap the error with their own context.
func toVectorIndex(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("index must be an integer, got %v (%T)", val, val)
	}
}
