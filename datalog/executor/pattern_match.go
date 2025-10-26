package executor

import (
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

// TimeRangeAware allows matchers to receive time range constraints for optimization
type TimeRangeAware interface {
	WithTimeRanges(ranges []TimeRange) TimeRangeAware
}

// MemoryPatternMatcher matches patterns against in-memory datoms
// This is useful for testing and small datasets
type MemoryPatternMatcher struct {
	datoms []datalog.Datom
}

// NewMemoryPatternMatcher creates a pattern matcher for in-memory datoms
// Uses IndexedMemoryMatcher for performance (5-5000× faster depending on query)
func NewMemoryPatternMatcher(datoms []datalog.Datom) PatternMatcher {
	return NewIndexedMemoryMatcher(datoms)
}

// Match implements PatternMatcher.Match
func (m *MemoryPatternMatcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
	// No constraints available via this interface - delegate to MatchWithConstraints
	return m.MatchWithConstraints(pattern, bindings, nil)
}

// MatchWithConstraints implements PredicateAwareMatcher.MatchWithConstraints
func (m *MemoryPatternMatcher) MatchWithConstraints(
	pattern *query.DataPattern,
	bindings Relations,
	constraints []StorageConstraint,
) (Relation, error) {
	columns := pattern.ExtractColumns()

	// Extract options from bindings if available
	var opts ExecutorOptions
	if bindings != nil && len(bindings) > 0 {
		opts = bindings[0].Options()
	}

	if bindings == nil || len(bindings) == 0 {
		// No bindings - match all
		datoms, err := m.matchWithoutBindings(pattern, constraints)
		if err != nil {
			return nil, err
		}
		return datomsToRelationWithOptions(datoms, pattern, columns, opts), nil
	}

	// Find best binding relation
	bindingRel := bindings.FindBestForPattern(pattern)
	if bindingRel == nil || bindingRel.Size() == 0 {
		// No relevant bindings
		datoms, err := m.matchWithoutBindings(pattern, constraints)
		if err != nil {
			return nil, err
		}
		return datomsToRelationWithOptions(datoms, pattern, columns, opts), nil
	}

	// Match with bindings
	var allTuples []Tuple
	boundTuples := bindingRel.Sorted()
	//fmt.Printf("DEBUG: Matching with %d binding tuples\n", len(boundTuples))
	for _, tuple := range boundTuples {
		boundPattern := bindPatternFromTuple(pattern, tuple, bindingRel)
		datoms, err := m.matchWithBoundPattern(boundPattern, constraints)
		if err != nil {
			return nil, err
		}

		// Convert datoms to tuples
		for _, datom := range datoms {
			if tuple := query.DatomToTuple(datom, pattern, columns); tuple != nil {
				allTuples = append(allTuples, tuple)
			}
		}
	}

	return NewMaterializedRelationWithOptions(columns, allTuples, bindingRel.Options()), nil
}

// matchWithoutBindings returns datoms matching the pattern without bindings
func (m *MemoryPatternMatcher) matchWithoutBindings(pattern *query.DataPattern, constraints []StorageConstraint) ([]datalog.Datom, error) {
	var results []datalog.Datom

	for _, datom := range m.datoms {
		// Apply constraints first (early filtering)
		if !evaluateConstraints(&datom, constraints) {
			continue
		}

		if matchesDatomWithPattern(datom, pattern) {
			results = append(results, datom)
		}
	}

	return results, nil
}

// matchWithBoundPattern matches a pattern that has been bound with constants
func (m *MemoryPatternMatcher) matchWithBoundPattern(pattern *query.DataPattern, constraints []StorageConstraint) ([]datalog.Datom, error) {
	var results []datalog.Datom

	for _, datom := range m.datoms {
		// Apply constraints first (early filtering)
		if !evaluateConstraints(&datom, constraints) {
			continue
		}

		if matchesDatomWithPattern(datom, pattern) {
			results = append(results, datom)
		}
	}

	return results, nil
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
	symbols := rel.Columns()
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

	default:
		return false
	}
}

// matchesConstant checks if a value matches a constant
func matchesConstant(value, constant interface{}) bool {
	// Handle different type combinations
	switch v := value.(type) {
	case datalog.Identity:
		switch c := constant.(type) {
		case datalog.Identity:
			return v.Equal(c)
		case string:
			// Allow matching by string for convenience
			return v.String() == c
		}

	case datalog.Keyword:
		switch c := constant.(type) {
		case datalog.Keyword:
			return v.String() == c.String()
		case string:
			// Allow matching by string for convenience
			return v.String() == c
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
	columns []query.Symbol
	pos     int
	current Tuple
}

func (it *datomIterator) Next() bool {
	for it.pos+1 < len(it.datoms) {
		it.pos++
		if tuple := query.DatomToTuple(it.datoms[it.pos], it.pattern, it.columns); tuple != nil {
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

// datomsToRelation converts datoms to a streaming relation (zero-copy lazy evaluation)
func datomsToRelation(datoms []datalog.Datom, pattern *query.DataPattern, columns []query.Symbol) Relation {
	return datomsToRelationWithOptions(datoms, pattern, columns, ExecutorOptions{})
}

// datomsToRelationWithOptions converts datoms to a streaming relation with options
func datomsToRelationWithOptions(datoms []datalog.Datom, pattern *query.DataPattern, columns []query.Symbol, opts ExecutorOptions) Relation {
	if len(columns) == 0 || len(datoms) == 0 {
		return NewMaterializedRelationWithOptions(columns, nil, opts)
	}

	iterator := &datomIterator{
		datoms:  datoms,
		pattern: pattern,
		columns: columns,
		pos:     -1,
	}

	return NewStreamingRelationWithOptions(columns, iterator, opts)
}

// PatternToRelation converts pattern match results to a relation
func PatternToRelation(datoms []datalog.Datom, pattern *query.DataPattern) Relation {
	columns := pattern.ExtractColumns()
	return datomsToRelation(datoms, pattern, columns)
}
