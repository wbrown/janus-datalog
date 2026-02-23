package storage

import (
	"fmt"
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/codec"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// BadgerMatcher implements the executor.PatternMatcher interface using BadgerStore
type BadgerMatcher struct {
	store             *BadgerStore
	txID              *datalog.ElementID       // nil=latest CRDT-resolved, &ElementID{}=raw history, &ElementID{L,R}=as-of
	timeRanges        []executor.TimeRange     // For time range optimization
	builderCache      *sync.Map                // map[string]*query.InternedTupleBuilder - Thread-safe cache for tuple builders
	builderCacheOnce  sync.Once                // Ensures builderCache is initialized exactly once
	handler           annotations.Handler      // Set from HandlerProvider for detailed storage events
	options           executor.ExecutorOptions // Options for creating relations
	forceJoinStrategy *JoinStrategy            // Override join strategy selection for testing
	schema            schema.SchemaProvider    // Optional schema for cardinality-aware index selection
	cache             *Cache                   // CRDT resolution cache for O(1) access to resolved views
}

// isHistoryMode returns true when raw (non-CRDT-resolved) datoms are requested.
func (m *BadgerMatcher) isHistoryMode() bool {
	return m.txID != nil && *m.txID == (datalog.ElementID{})
}

// shouldFilterTx returns true if a datom should be skipped because it's
// from after the as-of target. Returns false in latest mode (nil) and
// history mode (&ElementID{}).
func (m *BadgerMatcher) shouldFilterTx(datomTx datalog.ElementID) bool {
	return m.txID != nil && *m.txID != (datalog.ElementID{}) && m.txID.Less(datomTx)
}

// crdtTxID returns the ElementID to pass to CRDTResolvingIterator.
// In latest mode returns ElementID{} (no filter). In as-of mode returns the target.
// Must NOT be called in history mode (caller should check isHistoryMode first).
func (m *BadgerMatcher) crdtTxID() datalog.ElementID {
	if m.txID != nil {
		return *m.txID
	}
	return datalog.ElementID{}
}

// NewBadgerMatcher creates a new pattern matcher for the BadgerStore
func NewBadgerMatcher(store *BadgerStore) *BadgerMatcher {
	return &BadgerMatcher{
		store:        store,
		builderCache: &sync.Map{},
		options:      executor.ExecutorOptions{}, // Default options
	}
}

// NewBadgerMatcherWithOptions creates a new pattern matcher with specific options
func NewBadgerMatcherWithOptions(store *BadgerStore, opts executor.ExecutorOptions) *BadgerMatcher {
	return &BadgerMatcher{
		store:        store,
		builderCache: &sync.Map{},
		options:      opts,
	}
}

// AsOf creates a matcher that sees the database as of a specific transaction.
// Passing a zero ElementID enters history mode (raw datoms, no CRDT resolution).
func (m *BadgerMatcher) AsOf(txID datalog.ElementID) *BadgerMatcher {
	// Ensure cache is initialized before sharing it
	m.builderCacheOnce.Do(func() {
		if m.builderCache == nil {
			m.builderCache = &sync.Map{}
		}
	})

	return &BadgerMatcher{
		store:        m.store,
		txID:         &txID,
		timeRanges:   m.timeRanges,
		builderCache: m.builderCache,
		handler:      m.handler,
		options:      m.options,
		schema:       m.schema,
		cache:        m.cache,
	}
}

// History creates a matcher that returns raw datoms without CRDT resolution.
// All historical versions are visible, including retracted values.
func (m *BadgerMatcher) History() *BadgerMatcher {
	return m.AsOf(datalog.ElementID{})
}

// SetHandler configures the handler for detailed storage events.
// This is called by WrapMatcher during construction.
// Also updates options.Collector so relations inherit the collector for join annotations.
func (m *BadgerMatcher) SetHandler(handler annotations.Handler) {
	m.handler = handler
	if handler != nil {
		m.options.Collector = annotations.NewCollector(handler)
	}
}

// SetSchema sets the schema for cardinality-aware index selection.
// When schema is set, the matcher uses EATV for cardinality-one attributes
// and EAVT for cardinality-many attributes (for add-wins resolution).
func (m *BadgerMatcher) SetSchema(s schema.SchemaProvider) {
	m.schema = s
}

// SetCache sets the CRDT resolution cache for O(1) access to resolved views.
// When cache is set, LookupAttribute() and related methods check the cache
// before scanning storage, providing O(1) access for cache hits.
func (m *BadgerMatcher) SetCache(c *Cache) {
	m.cache = c
}

// WithTimeRanges sets the time range constraints and returns self for chaining
func (m *BadgerMatcher) WithTimeRanges(ranges []executor.TimeRange) executor.TimeRangeAware {
	m.timeRanges = ranges
	return m
}

// getTupleBuilder returns a cached tuple builder or creates a new one
func (m *BadgerMatcher) getTupleBuilder(pattern *query.DataPattern, symbols []query.Symbol) *query.InternedTupleBuilder {
	// Initialize cache exactly once (for tests or code paths that don't use NewBadgerMatcher)
	m.builderCacheOnce.Do(func() {
		if m.builderCache == nil {
			m.builderCache = &sync.Map{}
		}
	})

	key := pattern.String()
	for _, sym := range symbols {
		key += "|" + sym.String()
	}

	if val, ok := m.builderCache.Load(key); ok {
		return val.(*query.InternedTupleBuilder)
	}

	builder := query.NewInternedTupleBuilder(pattern, symbols)
	actual, _ := m.builderCache.LoadOrStore(key, builder)
	return actual.(*query.InternedTupleBuilder)
}

// ForceJoinStrategy overrides the join strategy selection for testing
// Pass nil to restore default behavior
func (m *BadgerMatcher) ForceJoinStrategy(strategy *JoinStrategy) {
	m.forceJoinStrategy = strategy
}

// Deprecated functions removed - use Match() which returns executor.Relation

// bindPattern creates a new pattern with variables replaced by tuple values
func (m *BadgerMatcher) bindPattern(pattern *query.DataPattern, tuple executor.Tuple, rel executor.Relation) *query.DataPattern {
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

// matchWithoutRelation matches a pattern without any binding constraints
func (m *BadgerMatcher) matchWithoutRelation(pattern *query.DataPattern) ([]datalog.Datom, error) {
	return m.matchBoundPattern(pattern)
}

// matchBoundPattern matches a pattern that may have some constants bound
func (m *BadgerMatcher) matchBoundPattern(pattern *query.DataPattern) ([]datalog.Datom, error) {
	// Extract values from pattern
	var e, a, v, tx interface{}

	if elem := pattern.GetE(); elem != nil {
		e = m.extractValue(elem)
	}
	if elem := pattern.GetA(); elem != nil {
		a = m.extractValue(elem)
	}
	if elem := pattern.GetV(); elem != nil {
		v = m.extractValue(elem)
	}
	if elem := pattern.GetT(); elem != nil {
		tx = m.extractValue(elem)
	}

	// Determine cardinality for CRDT resolution
	// For cardinality-one with E+A bound, V unbound: return only current value (first result)
	returnOnlyFirst := false
	if e != nil && a != nil && v == nil {
		// E and A are bound, V is unbound - check cardinality
		card := schema.CardinalityOne // Default for schemaless
		if m.schema != nil {
			if aKw, ok := a.(datalog.Keyword); ok {
				if def := m.schema.GetAttribute(aKw); def != nil {
					card = def.Cardinality
				}
			}
		}
		// For cardinality-one (and vector), return only the first (= current) value
		// For cardinality-many, we need all values for add-wins resolution
		if card == schema.CardinalityOne || card == schema.CardinalityVector {
			returnOnlyFirst = true
		}
	}

	// Choose index and create scan range
	index, start, end := m.chooseIndex(e, a, v, tx)

	// Use key-only scanning since all datom information is encoded in the key
	// This avoids fetching redundant values from storage
	iter, err := m.store.ScanKeysOnly(index, start, end)
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}
	defer iter.Close()

	var results []datalog.Datom

	// Scan and collect matching datoms
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, err
		}

		// Check if datom is valid for our transaction view
		if m.shouldFilterTx(datom.Tx) {
			continue
		}

		// Check if datom matches all pattern constraints
		if m.matchesDatom(datom, e, a, v, tx) {
			results = append(results, *datom)

			// For cardinality-one/vector: first result is current value
			// EATV index with descending Tx means first = highest ElementID = current
			if returnOnlyFirst {
				break
			}
		}
	}

	return results, nil
}

// MatchWithHistory matches a pattern and returns ALL historical values, not just current.
// This is used for history queries where the user wants to see all versions of an attribute.
// Returns all datoms including their ElementIDs for temporal ordering.
//
// For cardinality-one: returns all historical versions ordered by descending Tx
// For cardinality-many: returns all add/remove operations for add-wins analysis
// For cardinality-vector: returns all RGA elements for reconstruction
func (m *BadgerMatcher) MatchWithHistory(pattern *query.DataPattern) ([]datalog.Datom, error) {
	// Extract values from pattern
	var e, a, v, tx interface{}

	if elem := pattern.GetE(); elem != nil {
		e = m.extractValue(elem)
	}
	if elem := pattern.GetA(); elem != nil {
		a = m.extractValue(elem)
	}
	if elem := pattern.GetV(); elem != nil {
		v = m.extractValue(elem)
	}
	if elem := pattern.GetT(); elem != nil {
		tx = m.extractValue(elem)
	}

	// Choose index and create scan range
	index, start, end := m.chooseIndex(e, a, v, tx)

	// Use key-only scanning since all datom information is encoded in the key
	iter, err := m.store.ScanKeysOnly(index, start, end)
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}
	defer iter.Close()

	var results []datalog.Datom

	// Scan and collect ALL matching datoms (no early termination for history)
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, err
		}

		// Check if datom is valid for our transaction view (as-of still applies)
		if m.shouldFilterTx(datom.Tx) {
			continue
		}

		// Check if datom matches all pattern constraints
		if m.matchesDatom(datom, e, a, v, tx) {
			results = append(results, *datom)
		}
	}

	return results, nil
}

// MatchAsOf matches a pattern and returns the value as of a specific Lamport time.
// This finds the first entry with Lamport <= targetLamport (the value that was current then).
// If no value existed at that time, returns nil.
func (m *BadgerMatcher) MatchAsOf(pattern *query.DataPattern, targetTx datalog.ElementID) ([]datalog.Datom, error) {
	// Extract values from pattern
	var e, a, v, tx interface{}

	if elem := pattern.GetE(); elem != nil {
		e = m.extractValue(elem)
	}
	if elem := pattern.GetA(); elem != nil {
		a = m.extractValue(elem)
	}
	if elem := pattern.GetV(); elem != nil {
		v = m.extractValue(elem)
	}
	if elem := pattern.GetT(); elem != nil {
		tx = m.extractValue(elem)
	}

	// Choose index and create scan range
	index, start, end := m.chooseIndex(e, a, v, tx)

	// Use key-only scanning
	iter, err := m.store.ScanKeysOnly(index, start, end)
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}
	defer iter.Close()

	var results []datalog.Datom

	// Find first entry with Lamport <= target (entries are in descending Tx order)
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, err
		}

		// Skip entries newer than target time
		if targetTx.Less(datom.Tx) {
			continue
		}

		// Also apply as-of filter if set
		if m.shouldFilterTx(datom.Tx) {
			continue
		}

		// Check if datom matches all pattern constraints
		if m.matchesDatom(datom, e, a, v, tx) {
			results = append(results, *datom)

			// For as-of queries, return only the first matching entry
			// (which is the value that was current at that Lamport time)
			if e != nil && a != nil && v == nil {
				// E and A bound, V unbound - return current value at that time
				break
			}
		}
	}

	return results, nil
}

// scanTimeRanges performs multi-range scanning on AVET index for time optimization
func (m *BadgerMatcher) scanTimeRanges(attr datalog.Keyword, tx interface{}) ([]datalog.Datom, error) {
	// Use map to deduplicate by entity ID (entities might appear in multiple ranges)
	seen := make(map[string]bool)
	var results []datalog.Datom

	encoder := m.store.encoder

	// Convert attribute to storage format (use interned pointer)
	attrPtr := datalog.NewKeyword(attr.String())
	aStorage := ToStorageDatom(datalog.Datom{A: attrPtr}).A

	// Scan each time range
	for _, timeRange := range m.timeRanges {
		// AVET index order: A + V + E + Tx
		// We want all datoms where A = :price/time AND Start <= V < End

		// For start key: encode [A, Start_time] as prefix
		// For end key: encode [A, End_time] as prefix
		// The encoder's EncodePrefix can create this for us

		// Encode the time values with their type prefix (same as in EncodeKey)
		startValue := datalog.ValueBytes(timeRange.Start)
		endValue := datalog.ValueBytes(timeRange.End)

		// Create prefix keys: AVET = A + V + ...
		start := encoder.EncodePrefix(AVET, aStorage[:], startValue)
		end := encoder.EncodePrefix(AVET, aStorage[:], endValue)

		// Scan this range
		iter, err := m.store.ScanKeysOnly(AVET, start, end)
		if err != nil {
			return nil, fmt.Errorf("time range scan failed: %w", err)
		}

		for iter.Next() {
			datom, err := iter.Datom()
			if err != nil {
				iter.Close()
				return nil, err
			}

			// Check transaction filter
			if m.shouldFilterTx(datom.Tx) {
				continue
			}

			// Check tx constraint
			if tx != nil {
				switch txv := tx.(type) {
				case uint64:
					if datom.Tx.Lamport != txv {
						continue
					}
				case int64:
					if datom.Tx.Lamport != uint64(txv) {
						continue
					}
				case int:
					if datom.Tx.Lamport != uint64(txv) {
						continue
					}
				case datalog.ElementID:
					if datom.Tx != txv {
						continue
					}
				default:
					continue
				}
			}

			// Deduplicate by entity ID
			eKey := datom.E.L85()
			if !seen[eKey] {
				seen[eKey] = true
				results = append(results, *datom)
			}
		}
		iter.Close()
	}

	return results, nil
}

// extractValue extracts the value from a pattern element
func (m *BadgerMatcher) extractValue(elem query.PatternElement) interface{} {
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
	default:
		return nil
	}
}

// chooseIndex selects the best index based on bound values
func (m *BadgerMatcher) chooseIndex(e, a, v, tx interface{}) (IndexType, []byte, []byte) {
	// Priority order for index selection:
	// 1. EAVT - if E is bound
	// 2. AEVT - if A is bound but not E
	// 3. AVET - if A and V are bound but not E
	// 4. VAET - if V is bound but not E or A
	// 5. TAEV - if only Tx is bound
	// 6. EAVT - full scan if nothing is bound

	encoder := m.store.encoder

	if e != nil {
		// E is bound
		if eId, ok := e.(datalog.Identity); ok {
			// Entity is already 20 bytes
			eBytes := eId.Bytes()

			if a != nil {
				// E and A are bound - use AEVT for direct lookup
				var aPtr datalog.Keyword
				switch kw := a.(type) {
				case datalog.Keyword:
					aPtr = kw
				}
				if aPtr != nil {
					// Convert to storage format
					aStorage := ToStorageDatom(datalog.Datom{A: aPtr}).A
					_ = aStorage // used below

					// Create a dummy datom to use encoder
					dummyDatom := &datalog.Datom{
						E:  eId,
						A:  aPtr,
						V:  nil,
						Tx: datalog.ElementID{},
					}

					if v != nil {
						// E, A, and V are bound - use AEVT prefix range
						// CRITICAL: Must use prefix range, not exact key!
						// EncodeKey includes Tx=0, but actual datoms have real Tx values.
						// The scan range [A+E+V+Tx(0), A+E+V+Tx(1)) would miss all real datoms.
						// Instead, use EncodePrefixRange to scan all Tx values for (A, E, V) prefix.
						sDatom := ToStorageDatom(*dummyDatom)
						sDatom.V = v
						vType := byte(datalog.Type(sDatom.V))
						var valueBytes []byte
						if _, isL85 := encoder.(*L85KeyEncoder); isL85 && vType == byte(datalog.TypeReference) {
							var vArr [20]byte
							copy(vArr[:], datalog.ValueBytes(sDatom.V))
							valueBytes = append([]byte{vType}, []byte(codec.EncodeFixed20(vArr))...)
						} else {
							vData := datalog.ValueBytes(sDatom.V)
							valueBytes = append([]byte{vType}, vData...)
						}
						// AEVT order: A + E + V + Tx, so prefix with (A, E, V) to scan all Tx
						start, end := encoder.EncodePrefixRange(AEVT, aStorage[:], eBytes[:], valueBytes)
						return AEVT, start, end
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
						start, end := encoder.EncodePrefixRange(EAVT, eBytes[:], aStorage[:])
						return EAVT, start, end
					}
					// EATV: E → A → Tx → V - first entry is current (highest Tx)
					start, end := encoder.EncodePrefixRange(EATV, eBytes[:], aStorage[:])
					return EATV, start, end
				}
			}

			// Only E bound - use EATV for CRDT resolution
			// EATV: E → A → Tx↓ → V - first entry for each (E, A) is LWW winner
			// This is required for CRDTResolvingIterator's "first entry wins" logic
			start, end := encoder.EncodePrefixRange(EATV, eBytes[:])
			return EATV, start, end
		}
	} else if a != nil {
		// A is bound but not E
		if aKw, ok := a.(datalog.Keyword); ok {
			// Convert to storage format (intern the keyword)
			aPtr := datalog.NewKeyword(aKw.String())
			aStorage := ToStorageDatom(datalog.Datom{A: aPtr}).A

			if v != nil {
				// A and V bound - use AVET index
				// Create dummy datom for encoding
				dummyDatom := &datalog.Datom{
					E:  datalog.NewIdentity(""),
					A:  aPtr,
					V:  v,
					Tx: datalog.ElementID{},
				}
				// Get value bytes with type prefix
				// Must match how EncodeKey encodes values!
				sDatom := ToStorageDatom(*dummyDatom)
				vType := byte(datalog.Type(sDatom.V))
				var valueBytes []byte

				// Check if we're using L85 encoding and have a reference value
				if _, isL85 := encoder.(*L85KeyEncoder); isL85 && vType == byte(datalog.TypeReference) {
					// L85 encoder stores references as type + L85-encoded bytes
					var vArr [20]byte
					copy(vArr[:], datalog.ValueBytes(sDatom.V))
					valueBytes = append([]byte{vType}, []byte(codec.EncodeFixed20(vArr))...)
				} else {
					// Binary encoder or non-reference values: type + raw bytes
					vData := datalog.ValueBytes(sDatom.V)
					valueBytes = append([]byte{vType}, vData...)
				}

				start, end := encoder.EncodePrefixRange(AVET, aStorage[:], valueBytes)
				return AVET, start, end
			}

			// Only A bound - use cardinality-aware index selection
			// For CRDT semantics:
			// - CardinalityMany: AEVT groups by V for add-wins resolution
			// - CardinalityOne/Vector: AETV orders by Tx first, first entry is current
			card := schema.CardinalityOne
			if m.schema != nil {
				if attrDef := m.schema.GetAttribute(aPtr); attrDef != nil {
					card = attrDef.Cardinality
				}
			}
			if card == schema.CardinalityMany {
				// AEVT: A → E → V → Tx - values grouped together for add-wins
				start, end := encoder.EncodePrefixRange(AEVT, aStorage[:])
				return AEVT, start, end
			}
			// AETV: A → E → Tx → V - first entry is current (highest Tx)
			start, end := encoder.EncodePrefixRange(AETV, aStorage[:])
			return AETV, start, end
		}
	} else if v != nil {
		// Only V bound - use VAET index with per-datom cardinality resolution
		// VAET: V → A → E → Tx↓ - groups by A, enabling efficient cardinality lookup
		// Create dummy datom for value encoding
		dummyDatom := &datalog.Datom{
			E:  datalog.NewIdentity(""),
			A:  datalog.NewKeyword(""),
			V:  v,
			Tx: datalog.ElementID{},
		}
		sDatom := ToStorageDatom(*dummyDatom)
		vType := byte(datalog.Type(sDatom.V))
		var valueBytes []byte

		// Check if we're using L85 encoding and have a reference value
		if _, isL85 := encoder.(*L85KeyEncoder); isL85 && vType == byte(datalog.TypeReference) {
			// L85 encoder stores references as type + L85-encoded bytes
			var vArr [20]byte
			copy(vArr[:], datalog.ValueBytes(sDatom.V))
			valueBytes = append([]byte{vType}, []byte(codec.EncodeFixed20(vArr))...)
		} else {
			// Binary encoder or non-reference values: type + raw bytes
			vData := datalog.ValueBytes(sDatom.V)
			valueBytes = append([]byte{vType}, vData...)
		}

		start, end := encoder.EncodePrefixRange(VAET, valueBytes)
		return VAET, start, end
	} else if tx != nil {
		// Use TAEV index
		var storageTx Tx
		if txID, ok := tx.(uint64); ok {
			storageTx = NewTxFromUint(txID)
		} else if eid, ok := datalog.DerefElementID(tx); ok {
			storageTx = NewTxFromElementID(eid)
		}
		if storageTx != (Tx{}) {
			encTx := encoder.EncodeTxForPrefix(storageTx)
			start, end := encoder.EncodePrefixRange(TAEV, encTx)
			return TAEV, start, end
		}
	}

	// Full scan on EATV for CRDT resolution
	// EATV: E → A → Tx↓ → V - first entry for each (E, A) is LWW winner
	// This is required for CRDTResolvingIterator's "first entry wins" logic
	start, end := encoder.EncodePrefixRange(EATV)
	return EATV, start, end
}

// matchesDatom checks if a datom matches the pattern constraints
func (m *BadgerMatcher) matchesDatom(datom *datalog.Datom, e, a, v, tx interface{}) bool {
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
			// For other types, try equality
			if datom.E.String() != fmt.Sprintf("%v", e) {
				return false
			}
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
		case string:
			if datom.A.String() != av {
				return false
			}
		default:
			return false
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
func (m *BadgerMatcher) valuesEqual(v1, v2 interface{}) bool {
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
func (m *BadgerMatcher) LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool) {
	if entity == nil {
		return nil, false
	}

	// Convert to storage format
	eBytes := entity.Bytes()
	aStorage := ToStorageDatom(datalog.Datom{A: attr}).A

	encoder := m.store.encoder

	// Determine cardinality for correct resolution
	card := schema.CardinalityOne // default
	if m.schema != nil {
		if def := m.schema.GetAttribute(attr); def != nil {
			card = def.Cardinality
		}
	}

	// Try cache first for O(1) access (only for latest state, not as-of queries)
	if m.cache != nil && m.txID == nil {
		eEntity := Entity(eBytes)
		var aAttr Attribute
		copy(aAttr[:], aStorage[:])
		key := CacheKey{E: eEntity, A: aAttr}

		entry := m.cache.GetOrResolve(key, m)
		if entry != nil {
			switch card {
			case schema.CardinalityOne:
				if entry.OneValue() != nil {
					return entry.OneValue(), true
				}
				return nil, false
			case schema.CardinalityMany:
				set := entry.ManySet()
				if len(set) > 0 {
					// Return all values as slice for consistency with cardinality-vector
					result := make([]interface{}, 0, len(set))
					for v := range set {
						result = append(result, v)
					}
					return result, true
				}
				return nil, false
			case schema.CardinalityVector:
				list := entry.VectorList()
				if len(list) > 0 {
					return list, true
				}
				return nil, false
			}
		}
	}

	// Fallback to storage scan (for as-of queries or when cache is not set)
	if card == schema.CardinalityOne {
		// For cardinality-one, use EATV index where Tx comes before V
		// Tx is encoded descending, so first entry = highest Tx = current value (LWW)
		start, end := encoder.EncodePrefixRange(EATV, eBytes[:], aStorage[:])

		iter, err := m.store.ScanKeysOnly(EATV, start, end)
		if err != nil {
			return nil, false
		}
		defer iter.Close()

		for iter.Next() {
			datom, err := iter.Datom()
			if err != nil {
				return nil, false
			}

			// Check transaction filter for as-of queries
			if m.shouldFilterTx(datom.Tx) {
				continue
			}

			// First entry with valid Tx is the current value (LWW)
			return datom.V, true
		}
		return nil, false
	}

	if card == schema.CardinalityVector {
		// For cardinality-vector, resolve the entire RGA and return as []any
		result, err := m.resolveVector(eBytes[:], aStorage[:])
		if err != nil {
			return nil, false
		}
		if len(result.Elements) == 0 {
			return nil, false
		}
		return result.Elements, true
	}

	// For cardinality-many, use AEVT and apply add-wins resolution
	// Must return ALL values that are currently in the set
	start, end := encoder.EncodePrefixRange(AEVT, aStorage[:], eBytes[:])

	iter, err := m.store.ScanKeysOnly(AEVT, start, end)
	if err != nil {
		return nil, false
	}
	defer iter.Close()

	// For cardinality-many, we need add-wins resolution
	// Track the highest add and remove lamport for each value
	valueAddLamport := make(map[interface{}]uint64)
	valueRemoveLamport := make(map[interface{}]uint64)

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, false
		}

		// Check transaction filter for as-of queries
		if m.shouldFilterTx(datom.Tx) {
			continue
		}

		if datom.Op == datalog.OpCRDTRemove {
			if datom.Tx.Lamport > valueRemoveLamport[datom.V] {
				valueRemoveLamport[datom.V] = datom.Tx.Lamport
			}
		} else {
			// OpCRDTAdd or no op (legacy)
			if datom.Tx.Lamport > valueAddLamport[datom.V] {
				valueAddLamport[datom.V] = datom.Tx.Lamport
			}
		}
	}

	// Build result: include values where add >= remove (add-wins on tie)
	var result []interface{}
	for v, addLamport := range valueAddLamport {
		removeLamport := valueRemoveLamport[v]
		if addLamport >= removeLamport { // add-wins on tie
			result = append(result, v)
		}
	}

	if len(result) > 0 {
		return result, true
	}
	return nil, false
}

// LookupAllAttributes retrieves all values of a cardinality-many attribute for an entity.
// Returns all matching values, or empty slice if none found.
func (m *BadgerMatcher) LookupAllAttributes(entity datalog.Identity, attr datalog.Keyword) []interface{} {
	if entity == nil {
		return nil
	}

	// Convert to storage format
	eBytes := entity.Bytes()
	aStorage := ToStorageDatom(datalog.Datom{A: attr}).A

	// Try cache first for O(1) access (only for latest state, not as-of queries)
	if m.cache != nil && m.txID == nil {
		eEntity := Entity(eBytes)
		var aAttr Attribute
		copy(aAttr[:], aStorage[:])
		key := CacheKey{E: eEntity, A: aAttr}

		entry := m.cache.GetOrResolve(key, m)
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
				for v := range set {
					result = append(result, v)
				}
				return result
			case schema.CardinalityVector:
				return entry.VectorList()
			case schema.CardinalityOne:
				// For cardinality-one, return single value as slice
				if entry.OneValue() != nil {
					return []interface{}{entry.OneValue()}
				}
				return nil
			}
		}
	}

	// Fallback to storage scan (for as-of queries or when cache is not set)
	encoder := m.store.encoder

	// Use AEVT index which orders by A, then E
	start, end := encoder.EncodePrefixRange(AEVT, aStorage[:], eBytes[:])

	iter, err := m.store.ScanKeysOnly(AEVT, start, end)
	if err != nil {
		return nil
	}
	defer iter.Close()

	var values []interface{}
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			continue
		}

		// Check transaction filter for as-of queries
		if m.shouldFilterTx(datom.Tx) {
			continue
		}

		values = append(values, datom.V)
	}

	return values
}
