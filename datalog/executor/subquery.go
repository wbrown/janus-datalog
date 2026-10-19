package executor

import (
	"fmt"
	"reflect"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The subquery composed over the modules
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.4): the inner query is
// one body framed over its data inputs under the call's substitution, built
// once per window over the window's distinct combinations less those an
// earlier window ran, its answer presented under the enclosing names, and
// probed per window tuple. The result is the subject's tuples each joined
// with its matches, over the subject's symbols and the binding symbols the
// subject lacks; a binding symbol the subject carries is joined after the
// probe, never bound into the inner query. The result reports no size, the
// subject is read once, and nothing of it is held beyond the window.

// subqueryIterator is the composition under the subquery's policy: no
// narrowing, no record, the plan's build, and the shared emit over the one
// body.
type subqueryIterator struct {
	windowedIterator
	matchEmit
}

// answer drains the matches being emitted first, then answers the window's
// tuples in turn: the body's index is probed for the tuple's key, its matches
// agreeing with the tuple on the binding symbols the subject carries are
// emitted projected onto the result's symbols, and a tuple with no agreeing
// match is dropped. Returns false when the window is answered.
func (it *subqueryIterator) answer() bool {
	for {
		if it.matches != nil {
			tuple, ok := it.next()
			if !ok {
				continue
			}
			it.currentTuple = tuple
			return true
		}
		if it.windowPos >= len(it.reader.window) {
			return false
		}
		tuple := it.reader.window[it.windowPos]
		it.windowPos++
		body := it.branchCache[0]
		matches := it.agreeing(it.probe(body, tuple), body.branchSyms, tuple)
		if first, ok := it.begin(0, body.branchSyms, matches, tuple); ok {
			it.currentTuple = first
			return true
		}
	}
}

// probe is the body's matches for a window tuple: its index probed by the
// tuple for a frame reading no collection, and otherwise once per key the
// tuple's members stand for, the matches of every key together and distinct
// at the branch positions the result reads, since two members can answer one
// match.
func (it *subqueryIterator) probe(body *cachedBranch, tuple Tuple) []Tuple {
	frame := &it.frames[it.frameOf[0]]
	if len(frame.probeExpandPos) == 0 {
		return body.Probe(tuple)
	}
	var read []int
	for _, sym := range it.outputSyms {
		if pos := query.SymbolIndex(body.branchSyms, sym); pos >= 0 {
			read = append(read, pos)
		}
	}
	seen := NewTupleKeyMap()
	var matches []Tuple
	for _, key := range frame.probeKeys(tuple) {
		for _, match := range body.Probe(key) {
			if !seen.PutIfAbsentPositions(match, read, true) {
				matches = append(matches, match)
			}
		}
	}
	return matches
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

	// The call supplies one value per position of the :in, the $ an omitted
	// :in defaults to included; SubqueryPattern.Validate checks a subquery's
	// call against the same count as a static shape.
	expectedInputs := query.InputArity(q.In)
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

		case query.RulesInput:
			// The rule set rides the execution context; the marker at this
			// position binds no relation.
			if valueIndex < len(orderedValues) {
				if sym, ok := orderedValues[valueIndex].(query.Symbol); ok && sym == datalog.SymRules {
					valueIndex++
				} else {
					return nil, fmt.Errorf("subquery input: expected the rules marker (%%) at position %d "+
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
