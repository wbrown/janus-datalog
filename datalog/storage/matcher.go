package storage

import (
	"fmt"
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// PatternMatcher implements executor.PatternMatcher over a storage Store.
type PatternMatcher struct {
	store Store
	// reader carries every storage read the matcher performs. It defaults
	// to the store itself (each read opens its own storage transaction);
	// query-scoped matchers attach a ReadSession so all reads observe one
	// snapshot for the query's lifetime.
	reader StoreReader
	// sessionBounded marks that reader is a ReadSession, so cache reads must
	// bound themselves to its snapshot high-water mark (cacheBound). The
	// bound computes lazily once — the session's maximum is snapshot-constant.
	sessionBounded    bool
	boundOnce         sync.Once
	readBound         datalog.ElementID
	encoder           *BinaryKeyEncoder
	txID              *datalog.ElementID       // nil=latest CRDT-resolved, &ElementID{}=raw history, &ElementID{L,R}=as-of
	builderCache      *tupleBuilderCache       // Structurally-keyed tuple builders, shared with temporal-handle copies
	builderCacheOnce  sync.Once                // Ensures builderCache is initialized exactly once
	handler           annotations.Handler      // Set from HandlerProvider for detailed storage events
	options           executor.ExecutorOptions // Options for creating relations
	forceJoinStrategy *JoinStrategy            // Override join strategy selection for testing
	schema            schema.SchemaProvider    // Optional schema for cardinality-aware index selection
	cache             *Cache                   // CRDT resolution cache for O(1) access to resolved views
}

// encodeValueForSearch encodes a value for use in index lookups, applying
// compression if the encoder has it enabled. This ensures the search prefix
// matches the stored key's value encoding.
func encodeValueForSearch(v interface{}, encoder *BinaryKeyEncoder) []byte {
	if encoder.CompressionThreshold > 0 {
		vType, vData, _ := datalog.EncodeValue(v, encoder.CompressionThreshold)
		return append([]byte{byte(vType)}, vData...)
	}

	vType := byte(datalog.Type(v))
	vData := datalog.ValueBytes(v)
	return append([]byte{vType}, vData...)
}

// isHistoryMode returns true when raw (non-CRDT-resolved) datoms are requested.
func (m *PatternMatcher) isHistoryMode() bool {
	return m.txID != nil && *m.txID == (datalog.ElementID{})
}

// shouldFilterTx returns true if a datom should be skipped because it's
// from after the as-of target. Returns false in latest mode (nil) and
// history mode (&ElementID{}).
func (m *PatternMatcher) shouldFilterTx(datomTx datalog.ElementID) bool {
	return m.txID != nil && *m.txID != (datalog.ElementID{}) && m.txID.Less(datomTx)
}

// crdtTxID returns the ElementID to pass to CRDTResolvingIterator.
// In latest mode returns ElementID{} (no filter). In as-of mode returns the target.
// Must NOT be called in history mode (caller should check isHistoryMode first).
func (m *PatternMatcher) crdtTxID() datalog.ElementID {
	if m.txID != nil {
		return *m.txID
	}
	return datalog.ElementID{}
}

// cacheKey returns the cache key for (e, a) and whether this matcher mode may
// use the cache at all. History mode returns ok=false — raw datoms are never
// cached. Latest and concrete AsOf modes both return an (E, A) key; they live
// in separate Cache instances (the Database's global latest cache vs. the AsOf
// handle's private cache), so the key never crosses snapshots.
func (m *PatternMatcher) cacheKey(e Entity, a Attribute) (CacheKey, bool) {
	if m.isHistoryMode() {
		return CacheKey{}, false
	}
	return CacheKey{E: e, A: a}, true
}

// NewPatternMatcher creates a new pattern matcher for a storage backend.
// The tuple-builder cache initializes lazily on first use; Database-minted
// matchers arrive with the database's shared cache already set.
func NewPatternMatcher(store Store) *PatternMatcher {
	return &PatternMatcher{
		store:   store,
		reader:  store,
		encoder: store.Encoder(),
		options: executor.ExecutorOptions{}, // Default options
	}
}

// NewPatternMatcherWithOptions creates a new pattern matcher with specific options
func NewPatternMatcherWithOptions(store Store, opts executor.ExecutorOptions) *PatternMatcher {
	return &PatternMatcher{
		store:   store,
		reader:  store,
		encoder: store.Encoder(),
		options: opts,
	}
}

// AttachReadSession routes every subsequent storage read this matcher
// performs through the session, so the whole query observes one snapshot.
// Cache reads bound themselves to the session's high-water mark (see
// cacheBound), so the shared EA cache can never serve this matcher content
// newer than its snapshot. The caller owns the session's lifecycle; the
// matcher only reads through it.
func (m *PatternMatcher) AttachReadSession(session ReadSession) {
	m.reader = session
	m.sessionBounded = true
}

// cacheBound returns the snapshot high-water mark cache reads must respect:
// nil for latest-mode matchers (any fresh entry serves), the session's max
// ElementID for sessioned ones. Computed once per matcher; the session's
// maximum is snapshot-constant. A bound that fails to compute degrades to
// the zero ElementID — no cached entry serves, every lookup resolves
// through the session — which is safe, never wrong.
func (m *PatternMatcher) cacheBound() *datalog.ElementID {
	if !m.sessionBounded {
		return nil
	}
	m.boundOnce.Do(func() {
		if id, err := m.reader.MaxElementID(); err == nil {
			m.readBound = id
		}
	})
	return &m.readBound
}

// AsOf creates a matcher that sees the database as of a specific transaction.
// Passing a zero ElementID enters history mode (raw datoms, no CRDT resolution).
func (m *PatternMatcher) AsOf(txID datalog.ElementID) *PatternMatcher {
	// Ensure cache is initialized before sharing it
	m.builderCacheOnce.Do(func() {
		if m.builderCache == nil {
			m.builderCache = newTupleBuilderCache()
		}
	})

	return &PatternMatcher{
		store:          m.store,
		reader:         m.reader,
		sessionBounded: m.sessionBounded,
		encoder:        m.encoder,
		txID:           &txID,
		builderCache:   m.builderCache,
		handler:        m.handler,
		options:        m.options,
		schema:         m.schema,
		cache:          m.cache,
	}
}

// History creates a matcher that returns raw datoms without CRDT resolution.
// All historical versions are visible, including retracted values.
func (m *PatternMatcher) History() *PatternMatcher {
	return m.AsOf(datalog.ElementID{})
}

// SetHandler configures the handler for detailed storage events.
// This is called by WrapMatcher during construction.
// Also updates options.Collector so relations inherit the collector for join annotations.
func (m *PatternMatcher) SetHandler(handler annotations.Handler) {
	m.handler = handler
	if handler != nil {
		m.options.Collector = annotations.NewCollector(handler)
	}
}

// SetSchema sets the schema for cardinality-aware index selection.
// When schema is set, the matcher uses EATV for cardinality-one attributes
// and EAVT for cardinality-many attributes (for add-wins resolution).
func (m *PatternMatcher) SetSchema(s schema.SchemaProvider) {
	m.schema = s
}

// SetCache sets the CRDT resolution cache for O(1) access to resolved views.
// When cache is set, LookupAttribute() and related methods check the cache
// before scanning storage, providing O(1) access for cache hits.
func (m *PatternMatcher) SetCache(c *Cache) {
	m.cache = c
}

// tupleBuilderKey is the structural identity of an InternedTupleBuilder:
// where each datom position lands in the output tuple, and the output
// symbols themselves. Constants contribute nothing to a builder, so they are
// no part of the key — patterns differing only in constants share one
// builder, and building the key never renders the pattern.
type tupleBuilderKey struct {
	e, a, v, t int8
	out        [4]query.Symbol
	n          int8
}

func newTupleBuilderKey(pattern *query.DataPattern, symbols []query.Symbol) tupleBuilderKey {
	if len(symbols) > 4 {
		panic(fmt.Sprintf("a data pattern provides at most 4 symbols, got %d: %v", len(symbols), symbols))
	}
	key := tupleBuilderKey{
		e: tuplePosition(symbols, pattern.GetE()),
		a: tuplePosition(symbols, pattern.GetA()),
		v: tuplePosition(symbols, pattern.GetV()),
		t: tuplePosition(symbols, pattern.GetT()),
		n: int8(len(symbols)),
	}
	copy(key.out[:], symbols)
	return key
}

// tuplePosition returns the output-tuple position of a pattern element's
// variable, or -1 when the element is a constant, a wildcard, or a variable
// the output does not carry.
func tuplePosition(symbols []query.Symbol, element query.PatternElement) int8 {
	variable, ok := element.(query.Variable)
	if !ok {
		return -1
	}
	for i, sym := range symbols {
		if sym == variable.Name {
			return int8(i)
		}
	}
	return -1
}

// tupleBuilderCache shares structurally-keyed InternedTupleBuilders across
// every matcher a Database mints and their temporal-handle copies. A typed
// map under RWMutex keeps the warm lookup allocation-free (a sync.Map key
// would box per call).
type tupleBuilderCache struct {
	mu       sync.RWMutex
	builders map[tupleBuilderKey]*query.InternedTupleBuilder
}

func newTupleBuilderCache() *tupleBuilderCache {
	return &tupleBuilderCache{builders: make(map[tupleBuilderKey]*query.InternedTupleBuilder)}
}

func (c *tupleBuilderCache) get(key tupleBuilderKey) (*query.InternedTupleBuilder, bool) {
	c.mu.RLock()
	builder, ok := c.builders[key]
	c.mu.RUnlock()
	return builder, ok
}

// getOrStore keeps the first builder stored for a key, so concurrent misses
// converge on a single instance.
func (c *tupleBuilderCache) getOrStore(key tupleBuilderKey, builder *query.InternedTupleBuilder) *query.InternedTupleBuilder {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.builders[key]; ok {
		return existing
	}
	c.builders[key] = builder
	return builder
}

// getTupleBuilder returns the cached tuple builder for the pattern's
// structural identity, creating it on first use.
func (m *PatternMatcher) getTupleBuilder(pattern *query.DataPattern, symbols []query.Symbol) *query.InternedTupleBuilder {
	// Standalone matchers initialize their cache lazily here; Database-minted
	// matchers arrive with the database's shared cache already set.
	m.builderCacheOnce.Do(func() {
		if m.builderCache == nil {
			m.builderCache = newTupleBuilderCache()
		}
	})

	key := newTupleBuilderKey(pattern, symbols)
	if builder, ok := m.builderCache.get(key); ok {
		return builder
	}
	return m.builderCache.getOrStore(key, query.NewInternedTupleBuilder(pattern, symbols))
}

// ForceJoinStrategy overrides the join strategy selection for testing
// Pass nil to restore default behavior
func (m *PatternMatcher) ForceJoinStrategy(strategy *JoinStrategy) {
	m.forceJoinStrategy = strategy
}

// Deprecated functions removed - use Match() which returns executor.Relation

// bindPattern creates a new pattern with variables replaced by tuple values
func (m *PatternMatcher) bindPattern(pattern *query.DataPattern, tuple executor.Tuple, rel executor.Relation) *query.DataPattern {
	// Get symbol positions in the relation
	symbols := rel.Symbols()
	symbolIndex := make(map[query.Symbol]int)
	for i, sym := range symbols {
		symbolIndex[sym] = i
	}

	// Create new pattern elements
	elements := make([]query.PatternElement, len(pattern.Elements))
	copy(elements, pattern.Elements)

	// Bind entity (position 0)
	if len(elements) > 0 {
		if sym, ok := pattern.GetE().(query.Variable); ok {
			if idx, found := symbolIndex[sym.Name]; found && idx < len(tuple) {
				elements[0] = query.Constant{Value: tuple[idx]}
			}
		}
	}

	// Bind attribute (position 1)
	if len(elements) > 1 {
		if sym, ok := pattern.GetA().(query.Variable); ok {
			if idx, found := symbolIndex[sym.Name]; found && idx < len(tuple) {
				elements[1] = query.Constant{Value: tuple[idx]}
			}
		}
	}

	// Bind value (position 2)
	if len(elements) > 2 {
		if sym, ok := pattern.GetV().(query.Variable); ok {
			if idx, found := symbolIndex[sym.Name]; found && idx < len(tuple) {
				elements[2] = query.Constant{Value: tuple[idx]}
			}
		}
	}

	// Bind transaction (position 3) if present
	if len(elements) > 3 {
		if sym, ok := pattern.GetT().(query.Variable); ok {
			if idx, found := symbolIndex[sym.Name]; found && idx < len(tuple) {
				elements[3] = query.Constant{Value: tuple[idx]}
			}
		}
	}

	return &query.DataPattern{Elements: elements}
}

// extractValue extracts the value from a pattern element
func (m *PatternMatcher) extractValue(elem query.PatternElement) interface{} {
	switch e := elem.(type) {
	case query.Variable:
		// Variables match anything
		return nil
	case query.Blank:
		// Blanks match anything
		return nil
	case query.Constant:
		// Constants must match exactly
		return e.Value
	case query.VectorConstant:
		// Vector literals: return the Values slice for comparison
		// after RGA resolution. Empty slice means "match empty vector".
		return e.Values
	default:
		// PatternElement is a closed taxonomy; an unknown element is a bug,
		// not an unbound position.
		panic(fmt.Sprintf("BUG: unknown pattern element %T reached extractValue", elem))
	}
}

// chooseIndex selects the index whose component order lets the bound positions
// form a prefix, and returns that prefix as a ScanBound. It performs no
// encoding: the bound carries typed values and the store projects them.
func (m *PatternMatcher) chooseIndex(e, a, v, tx interface{}) ScanBound {
	// Priority order for index selection:
	// 1. EAVT - if E is bound
	// 2. AEVT - if A is bound but not E
	// 3. AVET - if A and V are bound but not E
	// 4. VAET - if V is bound but not E or A
	// 5. TAEV - if only Tx is bound
	// 6. EAVT - full scan if nothing is bound

	if e != nil {
		// E is bound
		if eID, ok := e.(datalog.Identity); ok {
			if a != nil {
				var aPtr datalog.Keyword
				switch kw := a.(type) {
				case datalog.Keyword:
					aPtr = kw
				}
				if aPtr != nil {
					if v != nil {
						// E, A and V bound. AEVT orders A → E → V → Tx, so the
						// three bound positions are its leading prefix and the
						// scan covers every Tx for them. Binding Tx as well
						// would name one datom, which no reader wants: Tx is
						// what resolution determines.
						return ScanBound{Index: AEVT, Prefix: []datalog.Value{aPtr, eID, v}}
					}

					// E and A bound, V unbound - use cardinality-aware index selection
					// For CRDT semantics:
					// - CardinalityMany: EAVT groups by V first, enabling add-wins resolution
					// - CardinalityOne/Vector: EATV orders by Tx first, first entry is current
					card := schema.CardinalityOne
					if m.schema != nil {
						if attrDef := m.schema.GetAttribute(aPtr); attrDef != nil {
							card = attrDef.Cardinality
						}
					}
					if card == schema.CardinalityMany {
						// EAVT: E → A → V → Tx - values grouped together for add-wins
						return ScanBound{Index: EAVT, Prefix: []datalog.Value{eID, aPtr}}
					}
					// EATV: E → A → Tx → V - first entry is current (highest Tx)
					return ScanBound{Index: EATV, Prefix: []datalog.Value{eID, aPtr}}
				}
			}

			// Only E bound - use EATV for CRDT resolution
			// EATV: E → A → Tx↓ → V - first entry for each (E, A) is LWW winner
			// This is required for CRDTResolvingIterator's "first entry wins" logic
			return ScanBound{Index: EATV, Prefix: []datalog.Value{eID}}
		}
	} else if a != nil {
		// A is bound but not E
		if aKw, ok := a.(datalog.Keyword); ok {
			// A and Tx bound (V unbound) — ATEV gives a direct [A][Tx↓] prefix scan,
			// landing on the exact (or nearest-descending) Tx for the attribute. The
			// equivalent on AETV would scan every entity, on AEVT every value.
			if tx != nil && v == nil {
				eid, ok := datalog.DerefElementID(tx)
				if !ok {
					panic(fmt.Sprintf("Tx must be ElementID, got %T", tx))
				}
				return ScanBound{Index: ATEV, Prefix: []datalog.Value{aKw, eid}}
			}

			if v != nil {
				// A and V bound - use AVET index
				return ScanBound{Index: AVET, Prefix: []datalog.Value{aKw, v}}
			}

			// Only A bound - use cardinality-aware index selection
			// For CRDT semantics:
			// - CardinalityMany: AEVT groups by V for add-wins resolution
			// - CardinalityOne/Vector: AETV orders by Tx first, first entry is current
			card := schema.CardinalityOne
			if m.schema != nil {
				if attrDef := m.schema.GetAttribute(aKw); attrDef != nil {
					card = attrDef.Cardinality
				}
			}
			if card == schema.CardinalityMany {
				// AEVT: A → E → V → Tx - values grouped together for add-wins
				return ScanBound{Index: AEVT, Prefix: []datalog.Value{aKw}}
			}
			// AETV: A → E → Tx → V - first entry is current (highest Tx)
			return ScanBound{Index: AETV, Prefix: []datalog.Value{aKw}}
		}
	} else if v != nil {
		// Only V bound - use VAET index with per-datom cardinality resolution
		// VAET: V → A → E → Tx↓ - groups by A, enabling efficient cardinality lookup
		return ScanBound{Index: VAET, Prefix: []datalog.Value{v}}
	} else if tx != nil {
		// Use TAEV index. Tx is always an ElementID by contract.
		eid, ok := datalog.DerefElementID(tx)
		if !ok {
			panic(fmt.Sprintf("Tx must be ElementID, got %T", tx))
		}
		return ScanBound{Index: TAEV, Prefix: []datalog.Value{eid}}
	}

	// Full scan on EATV for CRDT resolution
	// EATV: E → A → Tx↓ → V - first entry for each (E, A) is LWW winner
	// This is required for CRDTResolvingIterator's "first entry wins" logic
	return ScanBound{Index: EATV}
}

// typedPositionBindingCheck returns the per-tuple check behind
// filterTypedPositionBindings: whether the tuple's value for the pattern's
// entity-position variable is an Identity and its value for the
// attribute-position variable is a Keyword. Returns nil when neither typed
// position is bound by these symbols — no check is needed.
func typedPositionBindingCheck(pattern *query.DataPattern, symbols []query.Symbol) func(executor.Tuple) bool {
	eIdx, aIdx := -1, -1
	if v, ok := pattern.GetE().(query.Variable); ok {
		eIdx = query.SymbolIndex(symbols, v.Name)
	}
	if v, ok := pattern.GetA().(query.Variable); ok {
		aIdx = query.SymbolIndex(symbols, v.Name)
	}
	if eIdx < 0 && aIdx < 0 {
		return nil
	}
	return func(tuple executor.Tuple) bool {
		if eIdx >= 0 && eIdx < len(tuple) {
			if _, ok := tuple[eIdx].(datalog.Identity); !ok {
				return false
			}
		}
		if aIdx >= 0 && aIdx < len(tuple) {
			if _, ok := tuple[aIdx].(datalog.Keyword); !ok {
				return false
			}
		}
		return true
	}
}

// filterTypedPositionBindings drops binding tuples whose value for the
// pattern's entity-position variable is not an Identity, or whose value for
// the attribute-position variable is not a Keyword. Such values name no
// entity or attribute — the typed non-match of the equality join — so they
// contribute zero rows and no seek is constructed for them. Every join
// strategy applies this check at construction (the seek paths and the merge
// join filter their tuple slices; the hash join drops tuples while building
// its hash set), so matchesDatom only ever sees correctly typed position
// bindings. Tuples are returned unchanged (and unallocated) when neither
// typed position is bound by these symbols or every binding already has its
// position's type.
func filterTypedPositionBindings(pattern *query.DataPattern, symbols []query.Symbol, tuples []executor.Tuple) []executor.Tuple {
	typed := typedPositionBindingCheck(pattern, symbols)
	if typed == nil {
		return tuples
	}

	for i, tuple := range tuples {
		if typed(tuple) {
			continue
		}
		// First untyped binding found: copy-filter the remainder.
		filtered := make([]executor.Tuple, 0, len(tuples)-1)
		filtered = append(filtered, tuples[:i]...)
		for _, rest := range tuples[i+1:] {
			if typed(rest) {
				filtered = append(filtered, rest)
			}
		}
		return filtered
	}
	return tuples
}

// matchesDatom checks if a datom matches the pattern constraints
func (m *PatternMatcher) matchesDatom(datom *datalog.Datom, e, a, v, tx interface{}) bool {
	// Note: Identity is always a pointer type now, no dereferencing needed
	// Note: Do NOT dereference *Keyword - they must stay as interned pointers
	if ptr, ok := tx.(*uint64); ok {
		tx = *ptr
	}
	if ptr, ok := tx.(*datalog.ElementID); ok {
		tx = *ptr
	}

	// Check entity
	if e != nil {
		switch ev := e.(type) {
		case datalog.Identity:
			if !datom.E.Equal(ev) {
				return false
			}
		default:
			// Unreachable when entity bindings are validated at extraction
			// (validateEntityBinding). Fail loudly rather than fuzzy-match: the
			// entity position is inhabited only by Identity.
			panic(fmt.Sprintf("BUG: non-Identity entity binding reached matchesDatom: %T", e))
		}
	}

	// Check attribute
	if a != nil {
		switch av := a.(type) {
		case datalog.Keyword:
			// Pointer equality for interned keywords
			if datom.A != av {
				return false
			}
		default:
			// Unreachable when attribute bindings are validated at extraction
			// (validateAttributeBinding) and filtered at construction. Fail
			// loudly: the attribute position is inhabited only by Keyword.
			panic(fmt.Sprintf("BUG: non-Keyword attribute binding reached matchesDatom: %T", a))
		}
	}

	// Check value
	if v != nil {
		// Value comparison is more complex due to type variations
		if !m.valuesEqual(datom.V, v) {
			return false
		}
	}

	// Check transaction
	if tx != nil {
		switch txv := tx.(type) {
		case uint64:
			if datom.Tx.Lamport != txv {
				return false
			}
		case int64:
			if datom.Tx.Lamport != uint64(txv) {
				return false
			}
		case int:
			if datom.Tx.Lamport != uint64(txv) {
				return false
			}
		case datalog.ElementID:
			if datom.Tx != txv {
				return false
			}
		default:
			return false
		}
	}

	return true
}

// valuesEqual checks if two values are equal
func (m *PatternMatcher) valuesEqual(v1, v2 interface{}) bool {
	// Use the global ValuesEqual which handles pointers
	return datalog.ValuesEqual(v1, v2)
}

// indexName returns a string name for the index type (for debugging)
func indexName(idx IndexType) string {
	switch idx {
	case EAVT:
		return "EAVT"
	case EATV:
		return "EATV"
	case AEVT:
		return "AEVT"
	case AETV:
		return "AETV"
	case ATEV:
		return "ATEV"
	case AVET:
		return "AVET"
	case VAET:
		return "VAET"
	case TAEV:
		return "TAEV"
	default:
		return "UNKNOWN"
	}
}

// LookupAttribute retrieves the value of an attribute for an entity.
// This implements the query.EntityLookup interface for database functions
// like get-else, missing?, and get-some.
//
// Returns (value, true) if the attribute exists, (nil, false) otherwise.
// For cardinality-one, returns the current value (highest Tx via LWW).
// For cardinality-many, returns one of the current set members.
// CanFuseAttributeFetch reports whether a same-entity fetch of attr can be
// fused into a per-tuple LookupAttribute binding. True only when the
// attribute is declared CardinalityOne in the schema AND the matcher is not in
// history mode — the two conditions under which LookupAttribute returns the
// single value a matched pattern would. Returns false for schemaless,
// CardinalityMany/Vector, or history-mode matchers (raw multi-version reads).
func (m *PatternMatcher) CanFuseAttributeFetch(attr datalog.Keyword) bool {
	if m.schema == nil || m.txID != nil {
		return false
	}
	def := m.schema.GetAttribute(attr)
	return def != nil && def.Cardinality == schema.CardinalityOne
}

func (m *PatternMatcher) LookupAttribute(
	entity datalog.Identity,
	attr datalog.Keyword,
) (value interface{}, found bool, lookupErr error) {
	if entity == nil {
		return nil, false, nil
	}

	// Convert to storage format
	eBytes := entity.Bytes()
	aStorage := ToStorageDatom(datalog.Datom{A: attr}).A

	// Determine cardinality and value type for correct resolution
	card := schema.CardinalityOne // default
	var valueType schema.ValueType
	if m.schema != nil {
		if def := m.schema.GetAttribute(attr); def != nil {
			card = def.Cardinality
			valueType = def.ValueType
		}
	}

	// Try cache first for O(1) access. Concrete AsOf matchers use
	// transaction-scoped cache keys; History matchers bypass cache entirely.
	if m.cache != nil && !m.isHistoryMode() {
		eEntity := Entity(eBytes)
		var aAttr Attribute
		copy(aAttr[:], aStorage[:])
		key, _ := m.cacheKey(eEntity, aAttr)

		entry := m.cache.GetOrResolve(key, m, m.cacheBound())
		if entry != nil {
			switch card {
			case schema.CardinalityOne:
				if entry.OneValue() != nil {
					return entry.OneValue(), true, nil
				}
				return nil, false, nil
			case schema.CardinalityMany:
				set := entry.ManySet()
				if len(set) > 0 {
					// Return all values as slice for consistency with cardinality-vector
					result := make([]interface{}, 0, len(set))
					for _, v := range set {
						result = append(result, v)
					}
					return result, true, nil
				}
				return nil, false, nil
			case schema.CardinalityVector:
				list := entry.VectorList()
				if list == nil {
					return nil, false, nil // Never set
				}
				return typedVector(list, valueType), true, nil
			}
		}
	}

	// Fallback to storage scan (for as-of queries or when cache is not set)
	if card == schema.CardinalityOne {
		// For cardinality-one, use EATV index where Tx comes before V
		// Tx is encoded descending, so first entry = highest Tx = current value (LWW)
		iter, err := m.reader.ScanKeysOnly(ScanBound{
			Index:  EATV,
			Prefix: []datalog.Value{entity, attr},
		})
		if err != nil {
			return nil, false, err
		}
		defer func() {
			if closeErr := iter.Close(); lookupErr == nil {
				lookupErr = closeErr
			}
		}()

		for iter.Next() {
			datom, err := iter.Datom()
			if err != nil {
				return nil, false, err
			}

			// Check transaction filter for as-of queries
			if m.shouldFilterTx(datom.Tx) {
				continue
			}

			// First entry with valid Tx is the LWW winner. If it is a
			// Remove tombstone, the attribute does not currently exist.
			if datom.Op == datalog.OpCRDTRemove {
				return nil, false, nil
			}
			return datom.V, true, nil
		}
		if err := iter.Error(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}

	if card == schema.CardinalityVector {
		// For cardinality-vector, resolve the entire RGA and return as typed slice
		result, err := m.resolveVector(eBytes[:], aStorage[:])
		if err != nil {
			return nil, false, err
		}
		if len(result.Elements) == 0 && result.Stats.TotalElements == 0 {
			// Never-set: no datoms ever written for this (E, A)
			return nil, false, nil
		}
		// Either has live elements, or was explicitly cleared (tombstones exist)
		return typedVector(result.Elements, valueType), true, nil
	}

	// For cardinality-many, resolve the full set membership with add-wins
	// semantics.
	set, err := m.resolveAddWinsSet(eBytes[:], aStorage[:])
	if err != nil {
		return nil, false, err
	}
	if len(set.Members) == 0 {
		return nil, false, nil
	}
	members := make([]interface{}, 0, len(set.Members))
	for _, v := range set.Members {
		members = append(members, v)
	}
	return members, true, nil
}

// typedVector converts []any to a typed slice when the schema value type is known.
// For TypeString returns []string, for TypeLong returns []int64, etc.
// Falls back to returning the original []any if the type is unknown or mixed.
func typedVector(elements []any, vt schema.ValueType) any {
	switch vt {
	case schema.TypeString:
		result := make([]string, 0, len(elements))
		for _, e := range elements {
			if s, ok := e.(string); ok {
				result = append(result, s)
			} else {
				return elements // mixed types, return as-is
			}
		}
		return result
	case schema.TypeLong:
		result := make([]int64, 0, len(elements))
		for _, e := range elements {
			if n, ok := e.(int64); ok {
				result = append(result, n)
			} else {
				return elements
			}
		}
		return result
	case schema.TypeDouble:
		result := make([]float64, 0, len(elements))
		for _, e := range elements {
			if f, ok := e.(float64); ok {
				result = append(result, f)
			} else {
				return elements
			}
		}
		return result
	case schema.TypeBoolean:
		result := make([]bool, 0, len(elements))
		for _, e := range elements {
			if b, ok := e.(bool); ok {
				result = append(result, b)
			} else {
				return elements
			}
		}
		return result
	default:
		return elements
	}
}

// TypeDefault converts a default value to match the attribute's schema type.
// For vector attributes with TypeString, converts []interface{} to []string, etc.
// This implements query.TypedDefaulter.
func (m *PatternMatcher) TypeDefault(attr datalog.Keyword, defaultVal interface{}) interface{} {
	if m.schema == nil {
		return defaultVal
	}
	def := m.schema.GetAttribute(attr)
	if def == nil {
		return defaultVal
	}
	if def.Cardinality == schema.CardinalityVector || def.Cardinality == schema.CardinalityMany {
		if anySlice, ok := defaultVal.([]interface{}); ok {
			return typedVector(anySlice, def.ValueType)
		}
	}
	return defaultVal
}

// LookupAllAttributes retrieves all values of a cardinality-many attribute for an entity.
// Returns all matching values, or empty slice if none found.
func (m *PatternMatcher) LookupAllAttributes(entity datalog.Identity, attr datalog.Keyword) ([]interface{}, error) {
	if entity == nil {
		return nil, nil
	}

	// Convert to storage format
	eBytes := entity.Bytes()
	aStorage := ToStorageDatom(datalog.Datom{A: attr}).A

	// Try cache first for O(1) access. Concrete AsOf matchers use
	// transaction-scoped cache keys; History matchers bypass cache entirely.
	if m.cache != nil && !m.isHistoryMode() {
		eEntity := Entity(eBytes)
		var aAttr Attribute
		copy(aAttr[:], aStorage[:])
		key, _ := m.cacheKey(eEntity, aAttr)

		entry := m.cache.GetOrResolve(key, m, m.cacheBound())
		if entry != nil {
			// Determine cardinality
			card := schema.CardinalityOne
			if m.schema != nil {
				if def := m.schema.GetAttribute(attr); def != nil {
					card = def.Cardinality
				}
			}

			switch card {
			case schema.CardinalityMany:
				set := entry.ManySet()
				result := make([]interface{}, 0, len(set))
				for _, v := range set {
					result = append(result, v)
				}
				return result, nil
			case schema.CardinalityVector:
				return entry.VectorList(), nil
			case schema.CardinalityOne:
				// For cardinality-one, return single value as slice
				if entry.OneValue() != nil {
					return []interface{}{entry.OneValue()}, nil
				}
				return nil, nil
			}
		}
	}

	// Fallback to storage scan (for as-of queries or when cache is not set).
	// Infer cardinality from the CRDT ops present in the datoms and resolve
	// accordingly, rather than returning raw datoms including tombstones.
	return m.lookupAllAttributesFallback(entity, attr)
}

// lookupAllAttributesFallback resolves values for (E, A) without cache by
// inferring cardinality from the CRDT ops present in storage:
//   - OpNone → LWW (cardinality-one): return latest value by ElementID
//   - OpCRDTAdd/OpCRDTRemove → add-wins set (cardinality-many): resolve membership
//   - OpRGAInsert/OpRGATombstone → RGA vector (cardinality-vector): reconstruct ordered list
func (m *PatternMatcher) lookupAllAttributesFallback(entity datalog.Identity, attr datalog.Keyword) ([]interface{}, error) {
	// AEVT orders A → E → V → Tx, so (A, E) is its leading prefix.
	bound := ScanBound{Index: AEVT, Prefix: []datalog.Value{attr, entity}}

	// The set and vector resolvers below still take storage projections.
	eBytes := entity.Bytes()
	aStorage := ToStorageDatom(datalog.Datom{A: attr}).A

	// Peek at first datom to determine op type
	iter, err := m.reader.ScanKeysOnly(bound)
	if err != nil {
		return nil, fmt.Errorf("scanning AEVT for LookupAllAttributes: %w", err)
	}

	if !iter.Next() {
		iter.Close()
		return nil, nil
	}
	firstDatom, err := iter.Datom()
	if err != nil {
		iter.Close()
		return nil, fmt.Errorf("decoding first datom for LookupAllAttributes: %w", err)
	}
	firstOp := firstDatom.Op
	iter.Close()

	switch {
	case firstOp == datalog.OpCRDTAdd || firstOp == datalog.OpCRDTRemove:
		// Add-wins set resolution
		result, err := m.resolveAddWinsSet(eBytes, aStorage[:])
		if err != nil {
			return nil, fmt.Errorf("resolving add-wins set: %w", err)
		}
		values := make([]interface{}, 0, len(result.Members))
		for _, v := range result.Members {
			values = append(values, v)
		}
		return values, nil

	case firstOp == datalog.OpRGAInsert || firstOp == datalog.OpRGATombstone:
		// RGA vector resolution
		result, err := m.resolveVector(eBytes, aStorage[:])
		if err != nil {
			return nil, fmt.Errorf("resolving RGA vector: %w", err)
		}
		values := make([]interface{}, len(result.Elements))
		for i, v := range result.Elements {
			values[i] = v
		}
		return values, nil

	default:
		// LWW: return the value with the highest ElementID
		// Re-scan since we closed the iterator
		iter2, err := m.reader.ScanKeysOnly(bound)
		if err != nil {
			return nil, fmt.Errorf("re-scanning AEVT for LWW resolution: %w", err)
		}
		defer iter2.Close()

		var latestVal interface{}
		var latestTx datalog.ElementID
		found := false

		for iter2.Next() {
			datom, err := iter2.Datom()
			if err != nil {
				return nil, fmt.Errorf("re-scanning AEVT for LWW resolution: %w", err)
			}
			if m.shouldFilterTx(datom.Tx) {
				continue
			}
			if !found || datom.Tx.Compare(latestTx) > 0 {
				latestVal = datom.V
				latestTx = datom.Tx
				found = true
			}
		}
		// A failed scan is not "no value" — surface it.
		if err := iter2.Error(); err != nil {
			return nil, fmt.Errorf("re-scanning AEVT for LWW resolution: %w", err)
		}

		if !found {
			return nil, nil
		}
		return []interface{}{latestVal}, nil
	}
}
