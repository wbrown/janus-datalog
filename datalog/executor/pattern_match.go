package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// CollectorAware is an optional interface for pattern matchers that support annotation collectors
// DEPRECATED: Use HandlerProvider instead with decorator pattern
type CollectorAware interface {
	WithCollector(collector *annotations.Collector) CollectorAware
}

// HandlerProvider allows storage layer to access annotation handler for detailed events.
// The decorator pattern wraps at the PatternMatcher level, but storage needs to emit
// detailed events (hash join stats, scan metrics, etc) that happen deep inside Match().
// Storage checks if the matcher implements this interface to emit detailed events.
type HandlerProvider interface {
	// GetHandler returns the annotation handler if available, nil otherwise
	GetHandler() annotations.Handler
}

// NewMemoryPatternMatcher creates a pattern matcher for in-memory datoms.
// Returns an IndexedMemoryMatcher, which indexes datoms by (E, A, V) for
// 5-5000× speedups over linear scans depending on query shape.
func NewMemoryPatternMatcher(datoms []datalog.Datom) PatternMatcher {
	return NewIndexedMemoryMatcher(datoms)
}

// evaluateConstraints checks if a datom passes all constraints
func evaluateConstraints(datom *datalog.Datom, constraints []StorageConstraint) bool {
	for _, c := range constraints {
		if !c.Evaluate(datom) {
			return false
		}
	}
	return true
}

// bindPatternFromTuple creates a new pattern with variables replaced by tuple values
func bindPatternFromTuple(pattern *query.DataPattern, tuple Tuple, rel Relation) *query.DataPattern {
	// Get symbol positions in the relation
	symbols := rel.Symbols()
	symbolIndex := make(map[query.Symbol]int)
	for i, sym := range symbols {
		symbolIndex[sym] = i
	}

	// Create new pattern elements
	elements := make([]query.PatternElement, len(pattern.Elements))
	copy(elements, pattern.Elements)

	// Bind variables to tuple values
	for i, elem := range pattern.Elements {
		if v, ok := elem.(query.Variable); ok {
			if idx, found := symbolIndex[v.Name]; found && idx < len(tuple) {
				elements[i] = query.Constant{Value: tuple[idx]}
			}
		}
	}

	return &query.DataPattern{Elements: elements}
}

// matchesDatomWithPattern checks if a datom matches a pattern
func matchesDatomWithPattern(datom datalog.Datom, pattern *query.DataPattern) bool {
	// A DataPattern should have 3 or 4 elements: [e a v] or [e a v tx]
	if len(pattern.Elements) < 3 || len(pattern.Elements) > 4 {
		return false
	}

	// Check entity
	if !matchesElement(datom.E, pattern.Elements[0]) {
		return false
	}

	// Check attribute
	if !matchesElement(datom.A, pattern.Elements[1]) {
		return false
	}

	// Check value
	if !matchesElement(datom.V, pattern.Elements[2]) {
		return false
	}

	// Check transaction (if specified)
	if len(pattern.Elements) == 4 {
		if !matchesElement(datom.Tx, pattern.Elements[3]) {
			return false
		}
	}

	return true
}

// matchesElement checks if a datom component matches a pattern element
func matchesElement(value interface{}, element query.PatternElement) bool {
	switch elem := element.(type) {
	case query.Variable:
		// Variables match anything (pattern should be bound before calling)
		return true

	case query.Blank:
		// Blanks match anything
		return true

	case query.Constant:
		// Constants must match exactly
		return matchesConstant(value, elem.Value)

	case query.VectorConstant:
		// Vector literals match resolved vector values by value equality.
		return datalog.ValuesEqual(value, elem.Values)

	default:
		// PatternElement is a closed taxonomy (Variable, Blank, Constant,
		// VectorConstant); an unknown element is a bug, not a non-match.
		panic(fmt.Sprintf("BUG: unknown pattern element %T reached matchesElement", element))
	}
}

// matchesConstant checks if a value matches a constant
func matchesConstant(value, constant interface{}) bool {
	// Handle different type combinations
	switch v := value.(type) {
	case datalog.Identity:
		// Identities match only identities. Strings become entities by boundary
		// construction (NewIdentity, #identity literals), never by
		// comparison-time coercion; a string constant here is a typed non-match.
		if c, ok := constant.(datalog.Identity); ok {
			return v.Equal(c)
		}

	case datalog.Keyword:
		// Keywords match only keywords (interned pointer equality). Strings
		// become keywords by boundary construction (NewKeyword, :literal in
		// query text), never by comparison-time coercion.
		if c, ok := constant.(datalog.Keyword); ok {
			return v == c
		}

	case string:
		return v == constant

	case int64:
		switch c := constant.(type) {
		case int64:
			return v == c
		case int:
			return v == int64(c)
		}

	case float64:
		if c, ok := constant.(float64); ok {
			return v == c
		}

	case bool:
		if c, ok := constant.(bool); ok {
			return v == c
		}

	case uint64: // Transaction ID
		switch c := constant.(type) {
		case uint64:
			return v == c
		case int64:
			return v == uint64(c)
		case int:
			return v == uint64(c)
		}
	}

	// Fall back to interface equality
	return value == constant
}

// datomIterator lazily converts datoms to tuples during iteration
type datomIterator struct {
	datoms  []datalog.Datom
	pattern *query.DataPattern
	symbols []query.Symbol
	pos     int
	current Tuple
}

func (it *datomIterator) Next() bool {
	for it.pos+1 < len(it.datoms) {
		it.pos++
		if tuple := query.DatomToTuple(it.datoms[it.pos], it.pattern, it.symbols); tuple != nil {
			it.current = tuple
			return true
		}
	}
	return false
}

func (it *datomIterator) Tuple() Tuple {
	return it.current
}

func (it *datomIterator) Close() error {
	return nil
}

func (it *datomIterator) Error() error { return nil }

// datomsToRelation converts datoms to a streaming relation (zero-copy lazy evaluation)
func datomsToRelation(datoms []datalog.Datom, pattern *query.DataPattern, symbols []query.Symbol) Relation {
	return datomsToRelationWithOptions(datoms, pattern, symbols, ExecutorOptions{})
}

// datomsToRelationWithOptions converts datoms to a streaming relation with options
func datomsToRelationWithOptions(datoms []datalog.Datom, pattern *query.DataPattern, symbols []query.Symbol, opts ExecutorOptions) Relation {
	if len(symbols) == 0 || len(datoms) == 0 {
		return NewMaterializedRelationWithOptions(symbols, nil, opts)
	}

	iterator := &datomIterator{
		datoms:  datoms,
		pattern: pattern,
		symbols: symbols,
		pos:     -1,
	}

	return NewStreamingRelationWithOptions(symbols, iterator, opts)
}

// PatternToRelation converts pattern match results to a relation
func PatternToRelation(datoms []datalog.Datom, pattern *query.DataPattern) Relation {
	symbols := pattern.Symbols()
	return datomsToRelation(datoms, pattern, symbols)
}
