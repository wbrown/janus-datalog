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
func (m *BadgerMatcher) resolveAddWinsSet(eBytes, aBytes []byte) (*SetResolutionResult, error) {
	// Build prefix for E+A
	prefix := make([]byte, 1+20+32) // prefix byte + E + A
	prefix[0] = byte(EAVT)
	copy(prefix[1:21], eBytes)
	copy(prefix[21:53], aBytes)

	// Scan EAVT for all entries with this E+A
	iter, err := m.store.Scan(EAVT, prefix, prefixEnd(prefix))
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	result := &SetResolutionResult{
		Members: make(map[interface{}]interface{}),
	}

	// Track state per value: map[valueKey] -> {highestAddTx, highestRemoveTx, hasAdd, hasRemove, value}
	type valueState struct {
		highestAddTx    datalog.ElementID
		highestRemoveTx datalog.ElementID
		hasAdd          bool
		hasRemove       bool
		value           interface{}
	}
	valueStates := make(map[string]*valueState)

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, err
		}
		if m.shouldFilterTx(datom.Tx) {
			continue
		}

		elemID := datom.Tx

		// Track global max ElementID
		if elemID.Compare(result.MaxElementID) > 0 {
			result.MaxElementID = elemID
		}

		// Build a key for this value (type + bytes)
		vType := byte(datalog.Type(datom.V))
		vBytes := datalog.ValueBytes(datom.V)
		valueKey := string(append([]byte{vType}, vBytes...))

		state, exists := valueStates[valueKey]
		if !exists {
			state = &valueState{value: datom.V}
			valueStates[valueKey] = state
		}

		// Track highest Tx for each operation type
		// Op is now directly on the datom (not decoded from value)
		if datom.Op == datalog.OpCRDTAdd {
			if !state.hasAdd || elemID.Compare(state.highestAddTx) > 0 {
				state.highestAddTx = elemID
				state.hasAdd = true
			}
		} else if datom.Op == datalog.OpCRDTRemove {
			if !state.hasRemove || elemID.Compare(state.highestRemoveTx) > 0 {
				state.highestRemoveTx = elemID
				state.hasRemove = true
			}
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	// Resolve each value's membership using add-wins semantics
	for _, state := range valueStates {
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

		if inSet {
			key := state.value
			if b, ok := key.([]byte); ok {
				key = string(b) // []byte is not hashable as a Go map key
			}
			result.Members[key] = state.value
		}
	}

	return result, nil
}

// prefixEnd returns the end key for a prefix scan (exclusive)
// This is the smallest key that is greater than all keys with the given prefix
func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)

	// Increment the last byte, handling overflow
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end
		}
		// Overflow - continue to next byte
	}

	// All bytes overflowed - return nil (scan to end)
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
func (m *BadgerMatcher) checkSetMembership(eBytes, aBytes []byte, v interface{}) (bool, error) {
	// Encode the user value to get its type and bytes
	vType := byte(datalog.Type(v))
	vData := datalog.ValueBytes(v)

	// Build prefix: [EAVT][E][A][type][value]
	prefix := make([]byte, 1+20+32+1+len(vData))
	prefix[0] = byte(EAVT)
	copy(prefix[1:21], eBytes)
	copy(prefix[21:53], aBytes)
	prefix[53] = vType
	copy(prefix[54:], vData)

	// Scan for entries with this specific value (both Add and Remove ops)
	iter, err := m.store.Scan(EAVT, prefix, prefixEnd(prefix))
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
