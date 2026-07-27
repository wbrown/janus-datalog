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

	// Scanned is the index intake this resolution spent. Resolution reads the
	// index on the pattern's behalf and emits no event of its own, so the only
	// way the pattern can report what it cost is for the resolver to hand the
	// number back with the result.
	Scanned int

	// Bound is the run this resolution walked. The pattern arm announces it,
	// and it travels back rather than being rebuilt there so the announced run
	// and the walked run are the same value and cannot drift.
	Bound ScanBound
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
	bound := ScanBound{
		Index: EAVT,
		Prefix: []datalog.Value{
			datalog.NewIdentityFromHash(e),
			datalog.InternKeywordFromBytes(a),
		},
	}
	iter, err := m.reader.ScanKeysOnly(bound)
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
		return finishWithIntake(acc, it.blobs, iter, bound)
	}
	if blobs, ok, err := scanAddWinsBadger(m.encoder, iter, acc); ok {
		if err != nil {
			return nil, err
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
		return finishWithIntake(acc, blobs, iter, bound)
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
	return finishWithIntake(acc, nil, iter, bound)
}

// finishWithIntake completes an add-wins resolution and records what its scan
// read. Every arm of resolveAddWinsSet goes through here so the intake cannot
// be attached on one path and forgotten on another; the count has to be taken
// before the deferred Close.
func finishWithIntake(acc *addWinsAccumulator, blobs BlobReader, iter Iterator, bound ScanBound) (*SetResolutionResult, error) {
	result, err := acc.finish(blobs)
	if err != nil {
		return nil, err
	}
	result.Scanned = iter.Scanned()
	result.Bound = bound
	return result, nil
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

// checkSetMembership reports whether the value the bound names is currently in
// the set, scanning only the entries carrying that value.
//
// With Op in the key the format is
// [prefix:1][E:20][A:32][type:1][value:var][Op:1][Tx:16], so a bound binding
// [E][A][V] on EAVT matches every op (Add/Remove) for that value.
//
// The caller supplies the bound rather than the components, because the caller
// is the one that announces it: passing the same ScanBound value to the
// annotation and to the reader is what keeps the announced run and the walked
// run from drifting.
//
// Returns the scan's index intake alongside the answer. A variable-width V
// leaves this run inexact, so the store steps over the keys the byte range
// over-covers and the intake exceeds what the loop below sees — which is
// precisely the amplification the caller has to be able to report.
func (m *PatternMatcher) checkSetMembership(bound ScanBound) (bool, int, error) {
	iter, err := m.reader.Scan(bound)
	if err != nil {
		return false, 0, err
	}
	defer iter.Close()

	var highestAddTx datalog.ElementID
	var highestRemoveTx datalog.ElementID
	var hasAdd, hasRemove bool

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return false, iter.Scanned(), err
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
		return false, iter.Scanned(), err
	}
	scanned := iter.Scanned()

	// Determine membership using add-wins semantics
	// CRITICAL: Compare only Lamport values for add-wins.
	if !hasAdd {
		return false, scanned, nil // No adds ever - not in set
	}
	if !hasRemove {
		return true, scanned, nil // Only adds - in set
	}

	// Both exist - compare Lamport timestamps only
	// At same Lamport (concurrent operations), add wins
	return highestAddTx.Lamport >= highestRemoveTx.Lamport, scanned, nil
}
