package executor

import (
	"fmt"
	"reflect"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// getUniqueInputCombinations extracts unique combinations of input values.
// This is a pure function that performs data transformation.
func getUniqueInputCombinations(
	rel Relation,
	inputSymbols []query.Symbol,
) (combinations []map[query.Symbol]interface{}, resultErr error) {
	// Find symbol indices for input symbols
	indices := make([]int, len(inputSymbols))
	for i, sym := range inputSymbols {
		if sym.IsSource() {
			// Source marker - not a symbol, use special index
			indices[i] = -1
		} else {
			indices[i] = SymbolIndex(rel, sym)
			if indices[i] < 0 {
				return nil, fmt.Errorf("subquery input symbol %s not found in outer relation (available: %v)", sym, rel.Symbols())
			}
		}
	}

	// Collect unique combinations. Dedup by typed value identity via TupleKeyMap
	// (which compares values with datalog.ValuesEqual on hash collision), never by
	// string rendering — fmt.Sprintf("%v")+"|" is not injective and collapses
	// distinct combinations (e.g. int64(5) vs "5", or "a|b"+"c" vs "a"+"b|c").
	// Source markers are constant execution context, identical on every tuple, so
	// they are excluded from the dedup key.
	seen := NewTupleKeyMap()
	it := rel.Iterator()
	defer func() {
		if closeErr := it.Close(); resultErr == nil {
			resultErr = closeErr
		}
	}()

	for it.Next() {
		tuple := it.Tuple()

		// Extract input values; build the dedup key from data values only.
		values := make(map[query.Symbol]interface{})
		var keyValues Tuple

		for i, sym := range inputSymbols {
			if sym.IsSource() {
				// Source marker - pass it through as-is; not part of the key.
				values[sym] = sym
			} else {
				idx := indices[i]
				if idx < len(tuple) {
					values[sym] = tuple[idx]
					keyValues = append(keyValues, tuple[idx])
				}
			}
		}

		key := NewTupleKeyFull(keyValues)
		if !seen.PutIfAbsent(key, struct{}{}) {
			combinations = append(combinations, values)
		}
	}

	resultErr = it.Error()
	return combinations, resultErr
}

func createInputRelationsFromPatternWithOptions(subq *query.SubqueryPattern, outerValues map[query.Symbol]interface{}, opts ExecutorOptions) ([]Relation, error) {
	// Process the subquery's actual inputs in order
	var orderedValues []interface{}
	for _, input := range subq.Inputs {
		switch inp := input.(type) {
		case query.Variable:
			// Get value from outer query
			if val, ok := outerValues[inp.Name]; ok {
				orderedValues = append(orderedValues, val)
			} else {
				// Variable not found - shouldn't happen
				orderedValues = append(orderedValues, nil)
			}
		case query.Constant:
			// Check if it's a source marker
			if sym, ok := inp.Value.(query.Symbol); ok && sym.IsSource() {
				// Source marker - pass through
				orderedValues = append(orderedValues, sym)
			} else {
				// Regular constant - pass the value directly
				orderedValues = append(orderedValues, inp.Value)
			}
		default:
			// Unknown input type
			orderedValues = append(orderedValues, nil)
		}
	}

	// Now create relations based on the :in clause
	return createInputRelationsFromValuesWithOptions(subq.Query, orderedValues, opts)
}

// createInputRelationsFromValuesWithOptions creates relations from ordered input values with options.
func createInputRelationsFromValuesWithOptions(q *query.Query, orderedValues []interface{}, opts ExecutorOptions) ([]Relation, error) {
	var relations []Relation

	// Datomic semantics: an omitted :in defaults to [$] (the default database
	// source), not zero inputs. Apply that default before validating arity so a
	// subquery whose nested query has no :in still accepts the supplied source
	// marker instead of being rejected as an over-supply.
	inputs := q.In
	if len(inputs) == 0 {
		inputs = []query.InputSpec{query.DatabaseInput{Name: datalog.NewSymbol("$")}}
	}

	// Check if we have the correct number of inputs
	expectedInputs := 0
	for _, input := range inputs {
		switch inp := input.(type) {
		case query.DatabaseInput:
			expectedInputs++ // Database REQUIRES explicit $
		case query.ScalarInput:
			expectedInputs++
		case query.RelationInput:
			expectedInputs += len(inp.Symbols)
		case query.TupleInput:
			expectedInputs += len(inp.Symbols)
		case query.CollectionInput:
			expectedInputs++
		}
	}

	if len(orderedValues) != expectedInputs {
		return nil, fmt.Errorf("subquery input arity mismatch: nested query :in declares %d required value(s) (%v) "+
			"but the call supplied %d (%v)", expectedInputs, inputs, len(orderedValues), orderedValues)
	}

	// Process :in clause to create appropriate relations
	valueIndex := 0
	for _, input := range inputs {
		switch inp := input.(type) {
		case query.DatabaseInput:
			// Expect an explicit $ symbol at this position
			if valueIndex < len(orderedValues) {
				// Check if it's a source marker
				if sym, ok := orderedValues[valueIndex].(query.Symbol); ok && sym.IsSource() {
					// Source marker present - skip it
					valueIndex++
				} else {
					return nil, fmt.Errorf("subquery input: expected a database source ($) at position %d "+
						"of nested :in (%v), got %v", valueIndex, inputs, orderedValues[valueIndex])
				}
			}

		case query.ScalarInput:
			// Create a single-value relation
			if valueIndex < len(orderedValues) {
				rel := NewMaterializedRelationWithOptions(
					[]query.Symbol{inp.Symbol},
					[]Tuple{{orderedValues[valueIndex]}},
					opts,
				)
				relations = append(relations, rel)
				valueIndex++
			}

		case query.RelationInput:
			// For now, treat as scalar inputs (will be enhanced later)
			// This handles the common case where subqueries use scalar inputs
			if valueIndex+len(inp.Symbols) <= len(orderedValues) {
				tuple := make(Tuple, len(inp.Symbols))
				for i := range inp.Symbols {
					tuple[i] = orderedValues[valueIndex+i]
				}
				rel := NewMaterializedRelationWithOptions(inp.Symbols, []Tuple{tuple}, opts)
				relations = append(relations, rel)
				valueIndex += len(inp.Symbols)
			}

		case query.TupleInput:
			// Create a single-tuple relation
			if valueIndex+len(inp.Symbols) <= len(orderedValues) {
				tuple := make(Tuple, len(inp.Symbols))
				for i := range inp.Symbols {
					tuple[i] = orderedValues[valueIndex+i]
				}
				rel := NewMaterializedRelationWithOptions(inp.Symbols, []Tuple{tuple}, opts)
				relations = append(relations, rel)
				valueIndex += len(inp.Symbols)
			}

		case query.CollectionInput:
			// Create a single-symbol relation with one tuple per collection element
			if valueIndex < len(orderedValues) {
				var tuples []Tuple

				// Use reflection to detect and unpack slices
				val := reflect.ValueOf(orderedValues[valueIndex])
				if val.Kind() == reflect.Slice {
					// Unpack slice into individual tuples (pre-allocate to avoid reallocation)
					tuples = make([]Tuple, val.Len())
					for i := 0; i < val.Len(); i++ {
						tuples[i] = Tuple{val.Index(i).Interface()}
					}
				} else {
					// Single value - wrap in tuple
					tuples = []Tuple{{orderedValues[valueIndex]}}
				}

				rel := NewMaterializedRelationWithOptions(
					[]query.Symbol{inp.Symbol},
					tuples,
					opts,
				)
				relations = append(relations, rel)
				valueIndex++
			}
		}
	}

	return relations, nil
}

// applyBindingForm applies the binding form to transform subquery results.
// The input relation may be streaming (Size() == -1), so this function
// iterates rather than indexing — Size()/Get() are only safe on already
// materialized relations.
//
// Output shape by binding form:
//   - TupleBinding, ScalarBinding: 1-tuple MaterializedRelation on match,
//     empty MaterializedRelation when subquery returns no rows (datalog
//     "pattern fails to match" semantics). Cardinality is validated by
//     reading at most one extra tuple after the first.
//   - RelationBinding: StreamingRelation that wraps the input iterator
//     and emits inputValues++tuple per Next(). Preserves end-to-end
//     streaming through the subquery → union boundary.
func applyBindingForm(result Relation, binding query.BindingForm, inputValues map[query.Symbol]interface{}, inputSymbols []query.Symbol) (Relation, error) {
	switch b := binding.(type) {
	case query.TupleBinding:
		// TupleBinding [[?a ?b]]: subquery must return exactly one
		// tuple; its N positions bind to the N variables. Arity of the
		// subquery's schema must match len(Variables).
		return applyExactlyOneBinding(result, inputValues, inputSymbols, b.Variables, "tuple", len(b.Variables))

	case query.ScalarBinding:
		// ScalarBinding ?x: subquery must return exactly one tuple
		// with exactly one component; ScalarBinding is the arity-1 case
		// of TupleBinding.
		return applyExactlyOneBinding(result, inputValues, inputSymbols, []query.Symbol{b.Variable}, "scalar", 1)

	case query.CollectionBinding:
		// [?coll ...] - collect all values from a single symbol into a collection.
		return nil, fmt.Errorf("collection binding not yet implemented")

	case query.RelationBinding:
		resultSymbols := result.Symbols()
		if len(b.Variables) != len(resultSymbols) {
			return nil, fmt.Errorf("relation binding expects %d symbols, got %d", len(b.Variables), len(resultSymbols))
		}

		realInputSymbols := filterSourceSymbols(inputSymbols)
		outSymbols := make([]query.Symbol, len(realInputSymbols)+len(b.Variables))
		copy(outSymbols, realInputSymbols)
		copy(outSymbols[len(realInputSymbols):], b.Variables)

		// Pre-compute the input-value prefix — it's constant for this
		// applyBindingForm call, applied to every row of the subquery.
		prefix := make([]interface{}, len(realInputSymbols))
		for i, sym := range realInputSymbols {
			prefix[i] = inputValues[sym]
		}

		// Stream: wrap the subquery's iterator and emit prefix++row per
		// Next(). No buffering.
		wrapped := &prefixingIterator{
			inner:   result.Iterator(),
			prefix:  prefix,
			bodyLen: len(b.Variables),
		}
		properties := result.Properties().renameSymbols(resultSymbols, b.Variables)
		return NewStreamingRelationWithProperties(
			outSymbols,
			wrapped,
			result.Options(),
			properties,
		), nil

	default:
		return nil, fmt.Errorf("unsupported binding form: %T", binding)
	}
}

// filterSourceSymbols returns inputSymbols with source markers ($, $foo)
// removed — those are execution context, not data variables.
func filterSourceSymbols(inputSymbols []query.Symbol) []query.Symbol {
	out := make([]query.Symbol, 0, len(inputSymbols))
	for _, s := range inputSymbols {
		if !s.IsSource() {
			out = append(out, s)
		}
	}
	return out
}

// applyExactlyOneBinding performs the binding transform for TupleBinding
// and ScalarBinding — both require the subquery to return exactly one
// tuple, and differ only in error-message phrasing and expected arity.
//
// Empty subquery result → returns an empty relation (datalog "pattern
// fails to match" semantics, not an error).
// More than one tuple → returns an error naming the binding form.
// Arity mismatch against expectedArity → returns an error upfront
// without iterating; the schema check is a pure property of the
// subquery's find spec.
func applyExactlyOneBinding(
	result Relation,
	inputValues map[query.Symbol]interface{},
	inputSymbols []query.Symbol,
	bindingVars []query.Symbol,
	label string,
	expectedArity int,
) (Relation, error) {
	realInputSymbols := filterSourceSymbols(inputSymbols)
	outSymbols := make([]query.Symbol, len(realInputSymbols)+len(bindingVars))
	copy(outSymbols, realInputSymbols)
	copy(outSymbols[len(realInputSymbols):], bindingVars)

	// Schema check upfront — pure property of the subquery's find
	// spec, no iteration required.
	if got := len(result.Symbols()); got != expectedArity {
		return nil, fmt.Errorf("%s binding expects %d symbol(s), got %d", label, expectedArity, got)
	}

	first, moreThanOne, err := readAtMostTwo(result)
	if err != nil {
		return nil, err
	}
	if first == nil {
		return NewMaterializedRelation(outSymbols, []Tuple{}), nil
	}
	if moreThanOne {
		return nil, fmt.Errorf("%s binding expects exactly 1 result, got more than 1", label)
	}

	// INVARIANT: subquery tuples contain no nil values.
	for i, val := range first {
		if val == nil {
			return nil, fmt.Errorf("subquery result contains nil value at position %d - this violates datalog semantics", i)
		}
	}

	tuple := make(Tuple, len(outSymbols))
	for i, sym := range realInputSymbols {
		tuple[i] = inputValues[sym]
	}
	for i := range bindingVars {
		tuple[len(realInputSymbols)+i] = first[i]
	}
	return NewMaterializedRelation(outSymbols, []Tuple{tuple}), nil
}

// readAtMostTwo advances the relation's iterator by up to two tuples,
// returning a copy of the first (safe to retain after the iterator is
// closed) and a cardinality indicator:
//
//	moreThanOne == false, first == nil: empty relation.
//	moreThanOne == false, first != nil: exactly one tuple.
//	moreThanOne == true:                more than one tuple; iteration
//	                                    stops immediately after seeing
//	                                    the second.
//
// Used by TupleBinding / ScalarBinding to enforce cardinality without
// draining the subquery result.
func readAtMostTwo(rel Relation) (first Tuple, moreThanOne bool, err error) {
	it := rel.Iterator()
	defer it.Close()

	if !it.Next() {
		return nil, false, it.Error()
	}

	// Copy the first tuple — the underlying iterator may reuse its
	// workspace on the next Next() call.
	src := it.Tuple()
	first = make(Tuple, len(src))
	copy(first, src)

	if it.Next() {
		return first, true, it.Error()
	}
	return first, false, it.Error()
}

// prefixingIterator wraps an Iterator and emits [prefix... inner...] on
// each Next(). Used by RelationBinding to preserve streaming — the
// subquery's iterator flows through without buffering.
//
// Reuses its output buffer across Next() calls: callers that cache
// tuples must copy (same contract as other streaming iterators in the
// codebase; StreamingRelation.RequiresCopy() returns true).
type prefixingIterator struct {
	inner   Iterator
	prefix  []interface{}
	bodyLen int
	buf     Tuple
}

func (p *prefixingIterator) Next() bool {
	if !p.inner.Next() {
		return false
	}
	innerTuple := p.inner.Tuple()
	if p.buf == nil {
		p.buf = make(Tuple, len(p.prefix)+p.bodyLen)
		copy(p.buf, p.prefix)
	}
	for i := 0; i < p.bodyLen; i++ {
		if i < len(innerTuple) {
			p.buf[len(p.prefix)+i] = innerTuple[i]
		}
	}
	return true
}

func (p *prefixingIterator) Tuple() Tuple { return p.buf }

func (p *prefixingIterator) Close() error { return p.inner.Close() }

func (p *prefixingIterator) Error() error { return p.inner.Error() }
