package storage

import (
	"bytes"

	"github.com/wbrown/janus-datalog/datalog"
)

// SetResolutionResult contains the resolved set membership and metadata
type SetResolutionResult struct {
	// Members contains values currently in the set (after add-wins resolution)
	// Uses map for O(1) membership lookup, as required by cache design.
	Members map[interface{}]bool

	// MaxElementID is the highest ElementID seen during resolution
	// Used for cache freshness tracking
	MaxElementID datalog.ElementID
}

// resolveAddWinsSet scans entries for (E,A) and resolves current set membership
// using add-wins CRDT semantics.
//
// Key order is [E][A][type][encoded_value][Op][Tx↓], so for each value we see:
//  1. All entries for that value, ordered by Op then Tx (descending)
//  2. Within same value: OpAdd (0) sorts before OpRemove (1)
//  3. Within same Op: higher Tx sorts first
//
// Resolution for each value:
//   - Find highest Lamport with OpAdd (if any)
//   - Find highest Lamport with OpRemove (if any)
//   - Compare LAMPORT VALUES ONLY (not full ElementID):
//     - Higher Lamport wins
//     - Same Lamport (concurrent operations): Add wins
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
		Members: make(map[interface{}]bool),
	}

	// Track state for current value being processed
	var currentValueBytes []byte
	var currentValue interface{}
	var highestAddTx datalog.ElementID
	var highestRemoveTx datalog.ElementID
	var hasAdd, hasRemove bool

	// Emit the current value if it's in the set
	emitValue := func() {
		if currentValue == nil {
			return
		}

		// Determine if value is in set using add-wins semantics
		// CRITICAL: Compare only Lamport values for add-wins.
		// At the same Lamport (concurrent operations), add wins regardless of ReplicaID.
		// ReplicaID is only used for LWW (last-writer-wins) on cardinality-one,
		// not for add-wins on cardinality-many.
		inSet := false
		if hasAdd && !hasRemove {
			// Only adds, no removes - definitely in set
			inSet = true
		} else if hasAdd && hasRemove {
			// Both add and remove exist - compare Lamport timestamps only
			if highestAddTx.Lamport > highestRemoveTx.Lamport {
				// Add is more recent - in set
				inSet = true
			} else if highestAddTx.Lamport == highestRemoveTx.Lamport {
				// Same Lamport (concurrent operations) - add-wins
				inSet = true
			}
			// else: Remove has higher Lamport - not in set
		}
		// If only removes (no adds), not in set

		if inSet {
			result.Members[currentValue] = true
		}
	}

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			continue
		}

		// Get ElementID from datom
		elemID := datom.Tx

		// Track global max ElementID
		if elemID.Compare(result.MaxElementID) > 0 {
			result.MaxElementID = elemID
		}

		// Decode the set entry from value bytes
		// Value is stored as []byte containing encoded SetEntry
		valueBytes, ok := datom.V.([]byte)
		if !ok {
			// Not a set entry - skip (shouldn't happen for cardinality-many)
			continue
		}

		entry, err := DecodeSetEntry(valueBytes)
		if err != nil {
			// Invalid set entry - skip
			continue
		}

		// Re-encode just the value part (without Op) for comparison
		// SetEntry format: [type:1][value:var][op:1]
		// We want [type:1][value:var] for grouping
		entryValueBytes := valueBytes[:len(valueBytes)-1]

		// Check if we've moved to a new value
		if !bytes.Equal(entryValueBytes, currentValueBytes) {
			// Emit previous value (if any)
			emitValue()

			// Reset tracking for new value
			currentValueBytes = entryValueBytes
			currentValue = entry.Value
			hasAdd = false
			hasRemove = false
			highestAddTx = datalog.ElementID{}
			highestRemoveTx = datalog.ElementID{}
		}

		// Track highest Tx for each operation type
		// Since we scan in descending Tx order, first occurrence is highest
		if entry.Op == OpAdd && !hasAdd {
			highestAddTx = elemID
			hasAdd = true
		} else if entry.Op == OpRemove && !hasRemove {
			highestRemoveTx = elemID
			hasRemove = true
		}
	}

	// Emit final value
	emitValue()

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
func (m *BadgerMatcher) checkSetMembership(eBytes, aBytes []byte, v interface{}) (bool, error) {
	// The stored value is a SetEntry encoded as []byte: [type:1][value:var][op:1]
	// The key format in EAVT is: [prefix:1][E:20][A:32][TypeBytes:1][SetEntry:var][Tx:16]
	//
	// To find all entries for a specific value (both Add and Remove ops),
	// we need to scan for prefix [E][A][TypeBytes][type:1][value:var]
	// This matches entries regardless of Op byte.

	// Encode the user value to get its type and bytes
	vType := byte(datalog.Type(v))
	vData := datalog.ValueBytes(v)

	// Build prefix: [EAVT prefix][E][A][TypeBytes][user value type][user value bytes]
	// TypeBytes = 0x07 (the type tag for []byte)
	prefix := make([]byte, 1+20+32+1+1+len(vData))
	prefix[0] = byte(EAVT)
	copy(prefix[1:21], eBytes)
	copy(prefix[21:53], aBytes)
	prefix[53] = byte(datalog.TypeBytes) // The SetEntry is stored as []byte
	prefix[54] = vType                   // First byte of SetEntry is the value's type
	copy(prefix[55:], vData)             // Then the value bytes

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
			continue
		}

		// Decode the set entry
		valueBytes, ok := datom.V.([]byte)
		if !ok {
			continue
		}

		entry, err := DecodeSetEntry(valueBytes)
		if err != nil {
			continue
		}

		elemID := datom.Tx

		// Track highest Tx for each operation type
		if entry.Op == OpAdd {
			if !hasAdd || elemID.Compare(highestAddTx) > 0 {
				highestAddTx = elemID
				hasAdd = true
			}
		} else if entry.Op == OpRemove {
			if !hasRemove || elemID.Compare(highestRemoveTx) > 0 {
				highestRemoveTx = elemID
				hasRemove = true
			}
		}
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
