package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// CRDTResolvingIterator wraps a storage iterator and applies CRDT resolution
// per (E, A) group. It buffers datoms until the (E, A) boundary changes,
// then resolves according to schema cardinality and yields surviving datoms.
//
// This ensures that regardless of how A becomes bound (constant, input, join),
// queries return CRDT-resolved current values, not all historical values.
//
// Resolution strategies by cardinality:
//   - CardinalityOne: LWW (Last-Write-Wins) - highest Lamport timestamp wins
//   - CardinalityMany: Add-wins set - track adds/removes per value
//   - CardinalityVector: RGA (Replicated Growable Array) - reconstruct ordered list
type CRDTResolvingIterator struct {
	source Iterator
	schema schema.SchemaProvider
	txID   uint64 // For as-of queries: only consider datoms with Tx.Lamport <= txID

	// Current (E, A) group being accumulated
	currentE    datalog.Identity
	currentA    datalog.Keyword
	hasGroup    bool
	groupBuffer []*datalog.Datom

	// Resolved datoms ready to emit
	resolved   []*datalog.Datom
	resolveIdx int

	// For detecting end of source
	sourceExhausted bool

	// Current datom being yielded
	currentDatom *datalog.Datom
}

// NewCRDTResolvingIterator creates a new CRDT-resolving iterator wrapper.
// If schema is nil, defaults to CardinalityOne (LWW) for all attributes.
// If txID > 0, only considers datoms with Tx.Lamport <= txID (as-of query).
func NewCRDTResolvingIterator(source Iterator, schema schema.SchemaProvider, txID uint64) *CRDTResolvingIterator {
	return &CRDTResolvingIterator{
		source:      source,
		schema:      schema,
		txID:        txID,
		groupBuffer: make([]*datalog.Datom, 0, 8), // Pre-allocate small buffer
	}
}

// Next advances to the next CRDT-resolved datom.
// Returns true if a datom is available, false when exhausted.
func (it *CRDTResolvingIterator) Next() bool {
	// First, emit any buffered resolved datoms
	if it.resolveIdx < len(it.resolved) {
		it.currentDatom = it.resolved[it.resolveIdx]
		it.resolveIdx++
		return true
	}

	// If source is exhausted, we're done
	if it.sourceExhausted {
		return false
	}

	// Read from source until (E, A) changes or source exhausted
	for {
		if !it.source.Next() {
			// Source exhausted - resolve final group
			it.sourceExhausted = true
			if len(it.groupBuffer) > 0 {
				it.resolved = it.resolveGroup()
				it.groupBuffer = it.groupBuffer[:0] // Reset buffer
				it.resolveIdx = 0
				if len(it.resolved) > 0 {
					it.currentDatom = it.resolved[0]
					it.resolveIdx = 1
					return true
				}
			}
			return false
		}

		datom, err := it.source.Datom()
		if err != nil {
			continue
		}

		// Apply as-of filtering if txID is set
		if it.txID > 0 && datom.Tx.Lamport > it.txID {
			continue
		}

		// Check if (E, A) changed
		eHash := datom.E.Hash()
		if it.hasGroup {
			currentEHash := it.currentE.Hash()
			if eHash != currentEHash || datom.A != it.currentA {
				// (E, A) changed - resolve current group
				it.resolved = it.resolveGroup()
				it.groupBuffer = it.groupBuffer[:0] // Reset buffer
				it.groupBuffer = append(it.groupBuffer, copyDatom(datom))
				it.currentE = datom.E
				it.currentA = datom.A
				it.resolveIdx = 0

				if len(it.resolved) > 0 {
					it.currentDatom = it.resolved[0]
					it.resolveIdx = 1
					return true
				}
				// Empty resolution - continue to next group
				continue
			}
		}

		// Same (E, A) or first datom - accumulate
		if !it.hasGroup {
			it.currentE = datom.E
			it.currentA = datom.A
			it.hasGroup = true
		}
		it.groupBuffer = append(it.groupBuffer, copyDatom(datom))
	}
}

// Datom returns the current CRDT-resolved datom.
func (it *CRDTResolvingIterator) Datom() (*datalog.Datom, error) {
	return it.currentDatom, nil
}

// Close releases resources held by the iterator.
func (it *CRDTResolvingIterator) Close() error {
	if it.source != nil {
		return it.source.Close()
	}
	return nil
}

// Seek positions the iterator at or after the given key.
// This resets the CRDT resolution state since we're jumping to a new position.
func (it *CRDTResolvingIterator) Seek(key []byte) {
	// Reset resolution state
	it.hasGroup = false
	it.groupBuffer = it.groupBuffer[:0]
	it.resolved = nil
	it.resolveIdx = 0
	it.sourceExhausted = false
	it.currentDatom = nil

	// Delegate to source
	it.source.Seek(key)
}

// ElementID returns the transaction ElementID of the current entry.
func (it *CRDTResolvingIterator) ElementID() datalog.ElementID {
	if it.currentDatom != nil {
		return it.currentDatom.Tx
	}
	return datalog.ElementID{}
}

// resolveGroup applies CRDT resolution to the buffered (E, A) group.
func (it *CRDTResolvingIterator) resolveGroup() []*datalog.Datom {
	if len(it.groupBuffer) == 0 {
		return nil
	}

	// Determine cardinality from schema
	card := schema.CardinalityOne // default for schemaless
	if it.schema != nil {
		if attr := it.schema.GetAttribute(it.currentA); attr != nil {
			card = attr.Cardinality
		}
	}

	switch card {
	case schema.CardinalityOne:
		return it.resolveLWW()
	case schema.CardinalityMany:
		return it.resolveAddWins()
	case schema.CardinalityVector:
		return it.resolveRGA()
	}
	return it.groupBuffer // unknown cardinality - return all
}

// resolveLWW applies Last-Write-Wins resolution for CardinalityOne.
// Returns the single datom with the highest Lamport timestamp.
func (it *CRDTResolvingIterator) resolveLWW() []*datalog.Datom {
	var winner *datalog.Datom
	for _, d := range it.groupBuffer {
		if winner == nil || d.Tx.Lamport > winner.Tx.Lamport ||
			(d.Tx.Lamport == winner.Tx.Lamport && d.Tx.ReplicaID > winner.Tx.ReplicaID) {
			winner = d
		}
	}
	if winner != nil {
		return []*datalog.Datom{winner}
	}
	return nil
}

// resolveAddWins applies add-wins set resolution for CardinalityMany.
// For each distinct value, compares highest add vs highest remove Lamport.
// Value is in set if add >= remove (add wins on tie).
func (it *CRDTResolvingIterator) resolveAddWins() []*datalog.Datom {
	// Track highest add and remove Lamport per value
	type valueState struct {
		highestAdd    uint64
		highestRemove uint64
		hasAdd        bool
		hasRemove     bool
		addDatom      *datalog.Datom // Keep the datom for the winning add
	}

	valueStates := make(map[string]*valueState)

	for _, d := range it.groupBuffer {
		// Use value's string representation as key
		vKey := valueKey(d.V)

		state, exists := valueStates[vKey]
		if !exists {
			state = &valueState{}
			valueStates[vKey] = state
		}

		if d.Op == datalog.OpCRDTAdd {
			if !state.hasAdd || d.Tx.Lamport > state.highestAdd ||
				(d.Tx.Lamport == state.highestAdd && d.Tx.ReplicaID > state.addDatom.Tx.ReplicaID) {
				state.highestAdd = d.Tx.Lamport
				state.hasAdd = true
				state.addDatom = d
			}
		} else if d.Op == datalog.OpCRDTRemove {
			if !state.hasRemove || d.Tx.Lamport > state.highestRemove {
				state.highestRemove = d.Tx.Lamport
				state.hasRemove = true
			}
		}
	}

	// Collect values where add wins
	var result []*datalog.Datom
	for _, state := range valueStates {
		if !state.hasAdd {
			continue // No add means not in set
		}
		if !state.hasRemove || state.highestAdd >= state.highestRemove {
			// Add wins (add >= remove)
			result = append(result, state.addDatom)
		}
	}

	return result
}

// resolveRGA applies RGA resolution for CardinalityVector.
// This is a simplified implementation that delegates to the existing resolver.
func (it *CRDTResolvingIterator) resolveRGA() []*datalog.Datom {
	// For RGA, we need the full vector reconstruction which is complex.
	// The existing code in matcher_relations.go has dedicated methods for vectors.
	// For now, return all datoms and let the vector-specific code handle it.
	//
	// TODO: Implement streaming RGA resolution here for consistency.
	// The challenge is that RGA needs position information and parent tracking
	// that isn't directly available from just the datom stream.
	return it.groupBuffer
}

// copyDatom creates a deep copy of a datom since iterators may reuse buffers.
func copyDatom(d *datalog.Datom) *datalog.Datom {
	if d == nil {
		return nil
	}
	return &datalog.Datom{
		E:  d.E,
		A:  d.A,
		V:  d.V,
		Tx: d.Tx,
		Op: d.Op,
	}
}

// valueKey converts a value to a string key for grouping in add-wins resolution.
func valueKey(v any) string {
	switch val := v.(type) {
	case string:
		return "s:" + val
	case int64:
		return fmt.Sprintf("i:%d", val)
	case float64:
		return fmt.Sprintf("f:%f", val)
	case bool:
		if val {
			return "b:t"
		}
		return "b:f"
	case datalog.Identity:
		if val == nil {
			return "id:nil"
		}
		hash := val.Hash()
		return "id:" + string(hash[:])
	case datalog.Keyword:
		return "kw:" + val.String()
	default:
		return fmt.Sprintf("v:%v", v)
	}
}
