package executor

import (
	"fmt"
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// IndexedMemoryMatcher is an optimized in-memory pattern matcher that uses hash indices
// for O(1) lookups instead of O(N) linear scans. This provides 5-10× speedup for typical queries.
type IndexedMemoryMatcher struct {
	datoms []datalog.Datom

	// Lazy-initialized indices (protected by buildMutex)
	buildMutex     sync.Once
	entityIndex    map[datalog.Identity][]int // E (interned pointer) → datom positions
	attributeIndex map[datalog.Keyword][]int  // A (interned pointer) → datom positions
	valueIndex     map[uint64][]int           // hash(V) → datom positions (NOTE: values are interface{}, indexed by hash; collisions filtered by exact match)
	eavIndex       map[eaIndexKey][]int       // (E, A) interned pointers → datom positions (all, for cardinality-many)

	// Optional collector for annotations (protected by collectorMutex for concurrent access)
	collectorMutex sync.RWMutex
	collector      *annotations.Collector

	// ExecutorOptions for configuring relation behavior (protected by optionsMutex)
	optionsMutex sync.RWMutex
	options      ExecutorOptions
}

// eaIndexKey keys the (entity, attribute) index by the interned E and A pointers,
// avoiding per-datom L85/string allocation. Interning makes pointer equality
// equivalent to value equality, so the pointers are stable, comparable map keys.
type eaIndexKey struct {
	e datalog.Identity
	a datalog.Keyword
}

// boundDatomIterator lazily matches bound patterns during iteration
// This enables streaming evaluation without materializing all results
type boundDatomIterator struct {
	matcher       *IndexedMemoryMatcher
	pattern       *query.DataPattern
	symbols       []query.Symbol
	constraints   []StorageConstraint
	boundTuples   []Tuple
	bindingRel    Relation
	boundIdx      int
	currentDatoms []datalog.Datom
	datomIdx      int
	current       Tuple
}

func (it *boundDatomIterator) Next() bool {
	for {
		// Try current batch of datoms
		for it.datomIdx < len(it.currentDatoms) {
			datom := it.currentDatoms[it.datomIdx]
			it.datomIdx++
			if tuple := query.DatomToTuple(datom, it.pattern, it.symbols); tuple != nil {
				it.current = tuple
				return true
			}
		}

		// Need next binding
		it.boundIdx++
		if it.boundIdx >= len(it.boundTuples) {
			return false // No more bindings
		}

		// Bind pattern and match
		boundPattern := bindPatternFromTuple(it.pattern, it.boundTuples[it.boundIdx], it.bindingRel)
		it.currentDatoms = it.matcher.matchWithIndex(boundPattern, it.constraints)
		it.datomIdx = 0
	}
}

func (it *boundDatomIterator) Tuple() Tuple {
	return it.current
}

func (it *boundDatomIterator) Close() error {
	return nil
}

func (it *boundDatomIterator) Error() error { return nil }

// NewIndexedMemoryMatcher creates a new indexed pattern matcher for in-memory datoms
func NewIndexedMemoryMatcher(datoms []datalog.Datom) *IndexedMemoryMatcher {
	return &IndexedMemoryMatcher{
		datoms: datoms,
	}
}

// buildIndices constructs hash indices for fast lookups
// Indices are built lazily on first use to avoid overhead if never queried
// Uses sync.Once to ensure thread-safe initialization for parallel queries
func (m *IndexedMemoryMatcher) buildIndices() {
	m.buildMutex.Do(func() {
		// Pre-size maps to avoid reallocation
		// Estimate: most queries touch 25-50% of unique entities/attributes
		estimatedSize := len(m.datoms) / 4
		if estimatedSize < 16 {
			estimatedSize = 16
		}

		m.entityIndex = make(map[datalog.Identity][]int, estimatedSize)
		m.attributeIndex = make(map[datalog.Keyword][]int, estimatedSize)
		m.valueIndex = make(map[uint64][]int, estimatedSize)
		m.eavIndex = make(map[eaIndexKey][]int, estimatedSize)

		for i, datom := range m.datoms {
			// Entity index: E → [positions]
			m.entityIndex[datom.E] = append(m.entityIndex[datom.E], i)

			// Attribute index: A → [positions] (interned pointer key)
			m.attributeIndex[datom.A] = append(m.attributeIndex[datom.A], i)

			// Value index: hash(V) → [positions]
			// Values are interface{} (string, int64, float64, bool, Identity, Keyword, time.Time, etc.)
			// so we index by hash and filter hash collisions during matching.
			// This is a two-phase approach: (1) hash lookup for candidates, (2) exact match for correctness.
			vHash := hashDatomValue(datom.V)
			m.valueIndex[vHash] = append(m.valueIndex[vHash], i)

			// EA index: (E, A) → [positions]
			// Store all datoms for each (E, A) pair to support cardinality-many attributes
			eaKey := eaIndexKey{e: datom.E, a: datom.A}
			m.eavIndex[eaKey] = append(m.eavIndex[eaKey], i)
		}
	})
}

// hashDatomValue computes a hash for a datom value
// Reuses the hashValue function from tuple_key.go
func hashDatomValue(v interface{}) uint64 {
	return hashValue(v)
}

// WithCollector sets the annotation collector and returns self for chaining
// Thread-safe for concurrent use by parallel subqueries
func (m *IndexedMemoryMatcher) WithCollector(collector *annotations.Collector) CollectorAware {
	m.collectorMutex.Lock()
	m.collector = collector
	m.collectorMutex.Unlock()
	return m
}

// WithOptions sets the executor options and returns self for chaining
func (m *IndexedMemoryMatcher) WithOptions(opts ExecutorOptions) *IndexedMemoryMatcher {
	m.optionsMutex.Lock()
	m.options = opts
	m.optionsMutex.Unlock()
	return m
}

// Match implements PatternMatcher.Match
func (m *IndexedMemoryMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	// Delegate to MatchWithConstraints with no constraints
	return m.MatchWithConstraints(q, bindings, nil)
}

// MatchWithConstraints implements PredicateAwareMatcher.MatchWithConstraints
func (m *IndexedMemoryMatcher) MatchWithConstraints(
	q *query.Query,
	bindings Relations,
	constraints []StorageConstraint,
) (Relation, error) {
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	if err := query.ValidateEntityBinding(extractPatternValue(pattern.GetE())); err != nil {
		return nil, err
	}
	if err := query.ValidateAttributeBinding(extractPatternValue(pattern.GetA())); err != nil {
		return nil, err
	}
	// Build indices on first use (lazy initialization)
	m.buildIndices()

	symbols := pattern.Symbols()

	// Extract options: prefer bindings, fall back to matcher's options
	m.optionsMutex.RLock()
	opts := m.options
	m.optionsMutex.RUnlock()
	if bindings != nil && len(bindings) > 0 {
		opts = bindings[0].Options()
	}

	if bindings == nil || len(bindings) == 0 {
		// No bindings - use index to match pattern
		datoms := m.matchWithIndex(pattern, constraints)
		return datomsToRelationWithOptions(datoms, pattern, symbols, opts), nil
	}

	// Find best binding relation
	bindingRel := bindings.FindBestForPattern(pattern)
	if bindingRel == nil {
		// No relevant bindings - use index
		datoms := m.matchWithIndex(pattern, constraints)
		return datomsToRelationWithOptions(datoms, pattern, symbols, opts), nil
	}
	if bindingRel.Size() == 0 {
		// An errored relation that materialized empty is not an empty
		// binding: its zero tuples mean the upstream scan failed. Falling
		// back to an unbound scan here laundered that failure into a
		// silent empty result.
		if err := EmptyRelationError(bindingRel); err != nil {
			return nil, err
		}
		datoms := m.matchWithIndex(pattern, constraints)
		return datomsToRelationWithOptions(datoms, pattern, symbols, opts), nil
	}

	// Project the binding relation onto the pattern's symbols: binding tuples
	// differing only in passenger symbols would rebind the identical pattern
	// and emit the same datoms again. Projection's set semantics makes the
	// binding tuples unique on the values that actually bind the pattern (the
	// storage matcher projects identically before its binding-driven paths).
	bindingRel = bindingRel.ProjectFromPattern(pattern)

	// Match with bindings - use streaming iterator for lazy evaluation
	// Prefer binding relation's options over matcher's options
	relOpts := bindingRel.Options()
	if relOpts == (ExecutorOptions{}) {
		relOpts = opts
	}

	sorted, err := bindingRel.Sorted()
	if err != nil {
		return nil, err
	}
	var iterator Iterator = &boundDatomIterator{
		matcher:     m,
		pattern:     pattern,
		symbols:     symbols,
		constraints: constraints,
		boundTuples: sorted,
		bindingRel:  bindingRel,
		boundIdx:    -1,
		datomIdx:    0,
	}
	if !patternCoversDatomIdentity(pattern) {
		// Raw datoms with part of their identity projected away: restore set
		// semantics at birth (DatomToTuple allocates fresh tuples — no copy).
		iterator = NewDedupIterator(iterator, 0, false)
	}

	return NewStreamingRelationWithOptions(symbols, iterator, relOpts), nil
}

// matchWithIndex performs indexed pattern matching
func (m *IndexedMemoryMatcher) matchWithIndex(pattern *query.DataPattern, constraints []StorageConstraint) []datalog.Datom {
	// Choose the best index based on which pattern elements are bound
	strategy := m.chooseStrategy(pattern)

	// Get candidate datom positions using the chosen index
	candidates := m.getCandidates(strategy)

	// Filter candidates by full pattern match and constraints
	var results []datalog.Datom
	for _, pos := range candidates {
		datom := m.datoms[pos]

		// Apply constraints first (early filtering)
		if !evaluateConstraints(&datom, constraints) {
			continue
		}

		// Check full pattern match
		if matchesDatomWithPattern(datom, pattern) {
			results = append(results, datom)
		}
	}

	return results
}

// matchStrategy represents different index lookup strategies
type matchStrategy interface {
	isMatchStrategy()
	String() string
}

// useEAIndex uses the EA index for O(1) lookup when both E and A are bound
type useEAIndex struct {
	e datalog.Identity
	a datalog.Keyword
}

func (useEAIndex) isMatchStrategy() {}
func (s useEAIndex) String() string { return "EA-index" }

// useEntityIndex uses the entity index when E is bound
type useEntityIndex struct {
	e datalog.Identity
}

func (useEntityIndex) isMatchStrategy() {}
func (s useEntityIndex) String() string { return "E-index" }

// useAttributeIndex uses the attribute index when A is bound
type useAttributeIndex struct {
	a datalog.Keyword
}

func (useAttributeIndex) isMatchStrategy() {}
func (s useAttributeIndex) String() string { return "A-index" }

// useValueIndex uses the value index when V is bound
type useValueIndex struct {
	v interface{}
}

func (useValueIndex) isMatchStrategy() {}
func (s useValueIndex) String() string { return "V-index" }

// useLinearScan falls back to linear scan when no index applies
type useLinearScan struct{}

func (useLinearScan) isMatchStrategy() {}
func (s useLinearScan) String() string { return "linear-scan" }

// chooseStrategy selects the best index based on which pattern elements are bound
// Priority order matches BadgerDB's index selection:
// 1. EA bound → O(1) lookup
// 2. E bound → O(K) where K = datoms with this entity
// 3. A bound → O(K) where K = datoms with this attribute
// 4. V bound → O(K) where K = datoms with this value
// 5. Nothing bound → O(N) linear scan
func (m *IndexedMemoryMatcher) chooseStrategy(pattern *query.DataPattern) matchStrategy {
	if pattern == nil || len(pattern.Elements) < 3 {
		return useLinearScan{}
	}

	e := extractPatternValue(pattern.GetE())
	a := extractPatternValue(pattern.GetA())
	v := extractPatternValue(pattern.GetV())

	// Priority 1: EA bound (most selective)
	if e != nil && a != nil {
		if eId, ok := e.(datalog.Identity); ok {
			if aKw, ok := a.(datalog.Keyword); ok {
				return useEAIndex{e: eId, a: aKw}
			}
		}
	}

	// Priority 2: E bound
	if e != nil {
		if eId, ok := e.(datalog.Identity); ok {
			return useEntityIndex{e: eId}
		}
	}

	// Priority 3: A bound
	if a != nil {
		if aKw, ok := a.(datalog.Keyword); ok {
			return useAttributeIndex{a: aKw}
		}
	}

	// Priority 4: V bound
	if v != nil {
		return useValueIndex{v: v}
	}

	// Priority 5: Nothing bound - linear scan
	return useLinearScan{}
}

// extractPatternValue extracts a concrete value from a pattern element
// Returns nil for variables and blanks
func extractPatternValue(elem query.PatternElement) interface{} {
	if elem == nil {
		return nil
	}
	switch e := elem.(type) {
	case query.Variable:
		return nil
	case query.Blank:
		return nil
	case query.Constant:
		return e.Value
	case query.VectorConstant:
		return e.Values
	default:
		// PatternElement is a closed taxonomy; an unknown element is a bug,
		// not an unbound position.
		panic(fmt.Sprintf("BUG: unknown pattern element %T reached extractPatternValue", elem))
	}
}

// getCandidates retrieves candidate datom positions using the chosen strategy
func (m *IndexedMemoryMatcher) getCandidates(strategy matchStrategy) []int {
	switch s := strategy.(type) {
	case useEAIndex:
		// O(1) lookup in EA index - returns all positions for cardinality-many
		return m.eavIndex[eaIndexKey{e: s.e, a: s.a}]

	case useEntityIndex:
		// O(1) lookup in entity index
		return m.entityIndex[s.e]

	case useAttributeIndex:
		// O(1) lookup in attribute index by interned pointer
		return m.attributeIndex[s.a]

	case useValueIndex:
		// O(1) lookup in value index by hash
		// Since values are interface{} (any type), we index by hash(V).
		// Multiple different values can hash to the same uint64 (collisions).
		// Candidates are filtered by exact value equality in matchWithIndex()
		// via matchesDatomWithPattern() → matchesConstant().
		hash := hashDatomValue(s.v)
		return m.valueIndex[hash]

	case useLinearScan:
		// Fallback: return all positions for linear scan
		positions := make([]int, len(m.datoms))
		for i := range m.datoms {
			positions[i] = i
		}
		return positions

	default:
		// Unknown strategy - fall back to linear scan
		positions := make([]int, len(m.datoms))
		for i := range m.datoms {
			positions[i] = i
		}
		return positions
	}
}
