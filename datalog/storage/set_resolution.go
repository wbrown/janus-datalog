package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// SetResolutionResult contains the resolved set membership and metadata
type SetResolutionResult struct {
	// Members maps hashable keys to original Go values in the set.
	// Keys are produced by setKey(). For most types the key equals the value.
	// For []byte, the key is string(bytes) but the value is the original []byte.
	// Point lookup: _, ok := Members[setKey(v)]
	// Iteration: for _, v := range Members
	Members map[interface{}]interface{}

	// MaxElementID is the highest ElementID seen during resolution
	// Used for cache freshness tracking
	MaxElementID datalog.ElementID
}

// resolveAddWinsSet scans entries for (E,A) and resolves current set membership
// using add-wins CRDT semantics.
//
// Key order is [E][A][type][value][Op][Tx↓], so for each value we see:
//  1. All entries for that value, ordered by Op then Tx (descending)
//  2. Within same value: OpCRDTAdd (1) sorts before OpCRDTRemove (2)
//  3. Within same Op: higher Tx sorts first
//
// Resolution for each value:
//   - Find highest Lamport with OpCRDTAdd (if any)
//   - Find highest Lamport with OpCRDTRemove (if any)
//   - Compare LAMPORT VALUES ONLY (not full ElementID):
//   - Higher Lamport wins
//   - Same Lamport (concurrent operations): Add wins
//
// NOTE: ReplicaID is NOT used for add-wins comparison. It's only used for
// LWW (cardinality-one). For add-wins, "concurrent" means same Lamport,
// and add always wins at concurrent operations regardless of ReplicaID.
func (m *PatternMatcher) resolveAddWinsSet(eBytes, aBytes []byte) (*SetResolutionResult, error) {
	// The caller holds storage projections; both constructors return the
	// canonical interned pointer for an already-interned value.
	var e Entity
	copy(e[:], eBytes)
	var a Attribute
	copy(a[:], aBytes)

	// Values live in the index keys; nothing here needs Badger values.
	iter, err := m.reader.ScanKeysOnly(ScanBound{
		Index: EAVT,
		Prefix: []datalog.Value{
			datalog.NewIdentityFromHash(e),
			datalog.InternKeywordFromBytes(a),
		},
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	acc := newAddWinsAccumulator(m)

	// The scan prefix fixes E and A of every entry; only the value span,
	// Tx, and Op vary. The in-tree iterators expose their raw keys, so
	// those spans are read directly and values decode only for members
	// that survive resolution — a full datom decode per historical entry
	// was 78% of the Set path's allocations. Concrete iterator types keep
	// the per-entry calls direct and inlinable; iterators from external
	// Store implementations resolve through Datom() with identical
	// semantics.
	if it, ok := iter.(*memoryIterator); ok {
		if err := scanAddWinsMemory(m.encoder, it, acc); err != nil {
			return nil, err
		}
		if err := it.Error(); err != nil {
			return nil, err
		}
		return acc.finish(it.blobs)
	}
	if blobs, ok, err := scanAddWinsBadger(m.encoder, iter, acc); ok {
		if err != nil {
			return nil, err
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
		return acc.finish(blobs)
	}

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, err
		}
		acc.observeDatom(datom)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return acc.finish(nil)
}

// addWinsState is the per-value resolution state: the highest add/remove
// Lamports plus the value in one of two forms — decoded (observeDatom), or
// its key-encoded span (observeSpan), decoded only if the member survives.
type addWinsState struct {
	highestAddTx    datalog.ElementID
	highestRemoveTx datalog.ElementID
	hasAdd          bool
	hasRemove       bool
	value           interface{}
	raw             []byte
}

func (s *addWinsState) record(tx datalog.ElementID, op datalog.CRDTOp) {
	if op == datalog.OpCRDTAdd {
		if !s.hasAdd || tx.Compare(s.highestAddTx) > 0 {
			s.highestAddTx = tx
			s.hasAdd = true
		}
	} else if op == datalog.OpCRDTRemove {
		if !s.hasRemove || tx.Compare(s.highestRemoveTx) > 0 {
			s.highestRemoveTx = tx
			s.hasRemove = true
		}
	}
}

// addWinsAccumulator is the single home of add-wins set resolution state:
// every scan variant feeds it, and finish applies the membership rule.
type addWinsAccumulator struct {
	matcher *PatternMatcher
	result  *SetResolutionResult
	states  map[string]*addWinsState
}

func newAddWinsAccumulator(m *PatternMatcher) *addWinsAccumulator {
	return &addWinsAccumulator{
		matcher: m,
		result:  &SetResolutionResult{Members: make(map[interface{}]interface{})},
		states:  make(map[string]*addWinsState),
	}
}

// observeSpan records one entry from its key-encoded value span. The span
// may alias iterator memory: the lookup does not allocate, and the span is
// copied once on first sight of a distinct value.
func (a *addWinsAccumulator) observeSpan(vSpan []byte, tx datalog.ElementID, op datalog.CRDTOp) {
	if a.matcher.shouldFilterTx(tx) {
		return
	}
	if tx.Compare(a.result.MaxElementID) > 0 {
		a.result.MaxElementID = tx
	}
	state, exists := a.states[string(vSpan)]
	if !exists {
		raw := append([]byte(nil), vSpan...)
		state = &addWinsState{raw: raw}
		a.states[string(raw)] = state
	}
	state.record(tx, op)
}

// observeDatom records one fully-decoded entry, for iterator types that do
// not expose raw keys.
func (a *addWinsAccumulator) observeDatom(datom *datalog.Datom) {
	if a.matcher.shouldFilterTx(datom.Tx) {
		return
	}
	if datom.Tx.Compare(a.result.MaxElementID) > 0 {
		a.result.MaxElementID = datom.Tx
	}
	vType := byte(datalog.Type(datom.V))
	vBytes := datalog.ValueBytes(datom.V)
	valueKey := string(append([]byte{vType}, vBytes...))
	state, exists := a.states[valueKey]
	if !exists {
		state = &addWinsState{value: datom.V}
		a.states[valueKey] = state
	}
	state.record(datom.Tx, datom.Op)
}

// finish resolves each value's membership using add-wins semantics.
// Members observed as spans decode here, through blobs — only survivors
// pay for a value decode.
func (a *addWinsAccumulator) finish(blobs BlobReader) (*SetResolutionResult, error) {
	for _, state := range a.states {
		inSet := false
		if state.hasAdd && !state.hasRemove {
			// Only adds, no removes - definitely in set
			inSet = true
		} else if state.hasAdd && state.hasRemove {
			// Both add and remove exist - compare Lamport timestamps only
			if state.highestAddTx.Lamport > state.highestRemoveTx.Lamport {
				// Add is more recent - in set
				inSet = true
			} else if state.highestAddTx.Lamport == state.highestRemoveTx.Lamport {
				// Same Lamport (concurrent operations) - add-wins
				inSet = true
			}
			// else: Remove has higher Lamport - not in set
		}
		// If only removes (no adds), not in set

		if !inSet {
			continue
		}
		value := state.value
		if value == nil {
			decoded, err := valueFromKeySpan(state.raw, blobs)
			if err != nil {
				return nil, err
			}
			value = decoded
		}
		key := value
		if b, ok := key.([]byte); ok {
			key = string(b) // []byte is not hashable as a Go map key
		}
		a.result.Members[key] = value
	}

	return a.result, nil
}

// scanAddWinsMemory drives the accumulator from a memory-store iterator's
// raw keys with direct, inlinable per-entry calls.
func scanAddWinsMemory(encoder *BinaryKeyEncoder, it *memoryIterator, acc *addWinsAccumulator) error {
	for it.Next() {
		_, _, vBytes, tx, op, _, err := encoder.DecodeKey(EAVT, it.Key())
		if err != nil {
			return err
		}
		acc.observeSpan(vBytes, Tx(tx).ToElementID(), datalog.CRDTOp(op))
	}
	return nil
}

// checkSetMembership checks if a specific value is currently in the set
// This is an optimized version that only scans entries for the specific value
//
// With Op in the key, the key format is:
// [prefix:1][E:20][A:32][type:1][value:var][Op:1][Tx:16]
//
// To find all entries for a specific value, we build prefix:
// [EAVT][E][A][type][value]
// This matches all ops (Add/Remove) for that value.
func (m *PatternMatcher) checkSetMembership(eBytes, aBytes []byte, v interface{}) (bool, error) {
	// The caller holds storage projections; both constructors return the
	// canonical interned pointer for an already-interned value.
	var e Entity
	copy(e[:], eBytes)
	var a Attribute
	copy(a[:], aBytes)

	// Bind [E][A][V] on EAVT, matching every op (Add/Remove) for that value.
	iter, err := m.reader.Scan(ScanBound{
		Index: EAVT,
		Prefix: []datalog.Value{
			datalog.NewIdentityFromHash(e),
			datalog.InternKeywordFromBytes(a),
			v,
		},
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()

	var highestAddTx datalog.ElementID
	var highestRemoveTx datalog.ElementID
	var hasAdd, hasRemove bool

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return false, err
		}

		elemID := datom.Tx

		// Track highest Tx for each operation type
		// Op is now directly on the datom
		if datom.Op == datalog.OpCRDTAdd {
			if !hasAdd || elemID.Compare(highestAddTx) > 0 {
				highestAddTx = elemID
				hasAdd = true
			}
		} else if datom.Op == datalog.OpCRDTRemove {
			if !hasRemove || elemID.Compare(highestRemoveTx) > 0 {
				highestRemoveTx = elemID
				hasRemove = true
			}
		}
	}
	if err := iter.Error(); err != nil {
		return false, err
	}

	// Determine membership using add-wins semantics
	// CRITICAL: Compare only Lamport values for add-wins.
	if !hasAdd {
		return false, nil // No adds ever - not in set
	}
	if !hasRemove {
		return true, nil // Only adds - in set
	}

	// Both exist - compare Lamport timestamps only
	// At same Lamport (concurrent operations), add wins
	return highestAddTx.Lamport >= highestRemoveTx.Lamport, nil
}
