package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// CRDTResolvingIterator wraps a storage iterator and applies CRDT resolution
// per (E, A) group using streaming.
//
// Key insight: EATV index stores Tx in descending order (highest Tx first).
// This means resolution is just filtering - no buffering needed.
//
// Resolution strategies by cardinality:
//   - CardinalityOne: Emit first entry, skip rest. Zero state.
//   - CardinalityMany: Emit qualifying ADDs immediately. Minimal state.
//   - CardinalityVector: TODO - needs discussion.
type CRDTResolvingIterator struct {
	source Iterator
	schema schema.SchemaProvider
	txID   uint64 // For as-of queries: only consider datoms with Tx.Lamport <= txID

	// Current (E, A) group tracking
	currentE datalog.Identity
	currentA datalog.Keyword
	hasGroup bool
	card     schema.Cardinality

	// CardinalityMany: streaming state (no buffering!)
	// Because we iterate Tx descending:
	// - First ADD for a value wins (unless tombstoned)
	// - REMOVE before ADD means remove has higher Tx
	emitted    map[any]bool   // values we've already emitted
	tombstones map[any]uint64 // value → remove Lamport (for add-wins-at-same-Tx)

	// CardinalityVector: TODO - placeholder for discussion
	rgaElements []rgaElement

	// Emit buffer for CardinalityVector only
	emitBuffer []*datalog.Datom
	emitIdx    int

	// For detecting end of source
	sourceExhausted bool

	// Current datom being yielded
	currentDatom *datalog.Datom
}

// rgaElement stores minimal state for RGA reconstruction (no datom pointers)
type rgaElement struct {
	id          datalog.ElementID
	afterRef    datalog.ElementID
	value       any
	tombstoneID *datalog.ElementID
}

// NewCRDTResolvingIterator creates a new CRDT-resolving iterator wrapper.
func NewCRDTResolvingIterator(source Iterator, schema schema.SchemaProvider, txID uint64) *CRDTResolvingIterator {
	return &CRDTResolvingIterator{
		source: source,
		schema: schema,
		txID:   txID,
	}
}

// Next advances to the next CRDT-resolved datom.
func (it *CRDTResolvingIterator) Next() bool {
	// First, emit any buffered results (CardinalityVector only)
	if it.emitIdx < len(it.emitBuffer) {
		it.currentDatom = it.emitBuffer[it.emitIdx]
		it.emitIdx++
		return true
	}

	// If source is exhausted, we're done
	if it.sourceExhausted {
		return false
	}

	for {
		if !it.source.Next() {
			// Source exhausted - emit final group if CardinalityVector
			it.sourceExhausted = true
			if it.hasGroup && it.card == schema.CardinalityVector {
				return it.emitRGAGroup()
			}
			return false
		}

		datom, err := it.source.Datom()
		if err != nil {
			continue
		}

		// Apply as-of filtering
		if it.txID > 0 && datom.Tx.Lamport > it.txID {
			continue
		}

		// Check if (E, A) changed
		isNewGroup := false
		if !it.hasGroup {
			isNewGroup = true
		} else {
			eHash := datom.E.Hash()
			currentEHash := it.currentE.Hash()
			if eHash != currentEHash || datom.A != it.currentA {
				isNewGroup = true
			}
		}

		if isNewGroup {
			// Emit pending CardinalityVector results before starting new group
			if it.hasGroup && it.card == schema.CardinalityVector {
				// For Vector, we need to buffer this datom and emit the old group
				// This is the ONE place we need a copy - at group boundary for Vector
				it.emitBuffer = it.resolveRGAGroup()
				it.emitIdx = 0
				it.startNewGroup(datom)
				if len(it.emitBuffer) > 0 {
					it.currentDatom = it.emitBuffer[it.emitIdx]
					it.emitIdx++
					return true
				}
				// Empty RGA group, continue with new group
			} else {
				it.startNewGroup(datom)
			}
		}

		// Process datom based on cardinality
		switch it.card {
		case schema.CardinalityOne:
			if isNewGroup {
				if datom.Op == datalog.OpCRDTRemove {
					// Value was retracted — attribute doesn't exist. Skip group.
					continue
				}
				// First entry for this (E, A) — emit (LWW winner)
				it.currentDatom = datom
				return true
			}
			// Same (E, A) — skip (already emitted or skipped the winner)
			continue

		case schema.CardinalityMany:
			if result := it.processAddWins(datom); result != nil {
				it.currentDatom = result
				return true
			}
			continue

		case schema.CardinalityVector:
			it.accumulateRGA(datom)
			continue

		case schema.CardinalityUnknown:
			// Default is CardinalityOne (LWW) — same resolution as CardinalityOne
			if isNewGroup {
				if datom.Op == datalog.OpCRDTRemove {
					continue
				}
				it.currentDatom = datom
				return true
			}
			continue
		}
	}
}

// startNewGroup initializes state for a new (E, A) group.
func (it *CRDTResolvingIterator) startNewGroup(datom *datalog.Datom) {
	it.currentE = datom.E
	it.currentA = datom.A
	it.hasGroup = true

	// Determine cardinality from schema
	// If no schema definition exists for this attribute, use CardinalityUnknown
	// which defaults to CardinalityOne (LWW) semantics
	it.card = schema.CardinalityUnknown
	if it.schema != nil {
		if attr := it.schema.GetAttribute(datom.A); attr != nil {
			it.card = attr.Cardinality
		}
	}

	// Reset state based on cardinality
	switch it.card {
	case schema.CardinalityOne:
		// No state needed
	case schema.CardinalityMany:
		it.emitted = make(map[any]bool)
		it.tombstones = make(map[any]uint64)
	case schema.CardinalityVector:
		it.rgaElements = it.rgaElements[:0]
	case schema.CardinalityUnknown:
		// Default is CardinalityOne (LWW) — no state needed
	}
}

// processAddWins handles a datom for CardinalityMany using streaming add-wins.
// Returns the datom to emit, or nil if this datom should be skipped.
//
// Because we iterate Tx DESCENDING:
// - First ADD for a value = highest Tx = emit (unless tombstoned)
// - REMOVE before ADD = remove has higher Tx = value is removed
// - REMOVE after ADD = we already emitted = ignore remove
func (it *CRDTResolvingIterator) processAddWins(datom *datalog.Datom) *datalog.Datom {
	v := datom.V

	if datom.Op == datalog.OpCRDTAdd {
		// Already emitted this value?
		if it.emitted[v] {
			return nil
		}

		// Check if tombstoned
		if tombstoneLamport, exists := it.tombstones[v]; exists {
			// Add wins at same Lamport, otherwise remove wins
			if datom.Tx.Lamport < tombstoneLamport {
				return nil // Remove wins
			}
			// Add wins (same or higher Lamport - but higher can't happen in descending order)
		}

		// Emit this add
		it.emitted[v] = true
		return datom

	} else if datom.Op == datalog.OpCRDTRemove {
		// Record tombstone (first one we see = highest Tx)
		if _, exists := it.tombstones[v]; !exists {
			it.tombstones[v] = datom.Tx.Lamport
		}
		return nil
	}

	return nil
}

// accumulateRGA collects RGA elements for tree reconstruction.
// Stores minimal state, not datom pointers.
func (it *CRDTResolvingIterator) accumulateRGA(datom *datalog.Datom) {
	if datom.Op == datalog.OpRGAInsert {
		it.rgaElements = append(it.rgaElements, rgaElement{
			id:       datom.Tx,
			afterRef: datom.AfterRef,
			value:    datom.V,
		})
	} else if datom.Op == datalog.OpRGATombstone {
		// Find and mark the target element, or create placeholder
		targetID := datom.AfterRef
		tombstoneID := datom.Tx
		found := false
		for i := range it.rgaElements {
			if it.rgaElements[i].id == targetID {
				if it.rgaElements[i].tombstoneID == nil || it.rgaElements[i].tombstoneID.Less(tombstoneID) {
					it.rgaElements[i].tombstoneID = &tombstoneID
				}
				found = true
				break
			}
		}
		if !found {
			// Tombstone arrived before insert
			it.rgaElements = append(it.rgaElements, rgaElement{
				id:          targetID,
				tombstoneID: &tombstoneID,
			})
		}
	}
}

// emitRGAGroup resolves the RGA group at source exhaustion.
func (it *CRDTResolvingIterator) emitRGAGroup() bool {
	it.emitBuffer = it.resolveRGAGroup()
	it.emitIdx = 0
	if len(it.emitBuffer) > 0 {
		it.currentDatom = it.emitBuffer[it.emitIdx]
		it.emitIdx++
		return true
	}
	return false
}

// resolveRGAGroup reconstructs the RGA tree and returns ordered datoms.
// Reconstructs datoms from stored state - no datom pointer storage needed.
func (it *CRDTResolvingIterator) resolveRGAGroup() []*datalog.Datom {
	if len(it.rgaElements) == 0 {
		return nil
	}

	// Deduplicate by ID (tombstone takes precedence)
	elemByID := make(map[datalog.ElementID]*rgaElement)
	for i := range it.rgaElements {
		e := &it.rgaElements[i]
		existing, exists := elemByID[e.id]
		if !exists {
			elemByID[e.id] = e
		} else {
			// Merge tombstone info
			if e.tombstoneID != nil && (existing.tombstoneID == nil || existing.tombstoneID.Less(*e.tombstoneID)) {
				existing.tombstoneID = e.tombstoneID
			}
			// Merge value/afterRef if we have it
			if existing.value == nil && e.value != nil {
				existing.value = e.value
				existing.afterRef = e.afterRef
			}
		}
	}

	// Build children map
	children := make(map[datalog.ElementID][]*rgaElement)
	for _, e := range elemByID {
		children[e.afterRef] = append(children[e.afterRef], e)
	}

	// Sort children by ID (ascending)
	for k := range children {
		sortRGAByID(children[k])
	}

	// DFS from HEAD, reconstruct datoms
	var result []*datalog.Datom
	visited := make(map[datalog.ElementID]bool)
	var walk func(id datalog.ElementID)
	walk = func(id datalog.ElementID) {
		if visited[id] {
			return
		}
		visited[id] = true

		for _, child := range children[id] {
			if child.tombstoneID == nil && child.value != nil {
				// Reconstruct datom from stored state
				result = append(result, &datalog.Datom{
					E:        it.currentE,
					A:        it.currentA,
					V:        child.value,
					Tx:       child.id,
					Op:       datalog.OpRGAInsert,
					AfterRef: child.afterRef,
				})
			}
			walk(child.id)
		}
	}
	walk(HEAD)

	return result
}

func sortRGAByID(elems []*rgaElement) {
	// Simple insertion sort (typically small lists)
	for i := 1; i < len(elems); i++ {
		for j := i; j > 0 && elems[j].id.Less(elems[j-1].id); j-- {
			elems[j], elems[j-1] = elems[j-1], elems[j]
		}
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
func (it *CRDTResolvingIterator) Seek(key []byte) {
	it.hasGroup = false
	it.emitted = nil
	it.tombstones = nil
	it.rgaElements = nil
	it.emitBuffer = nil
	it.emitIdx = 0
	it.sourceExhausted = false
	it.currentDatom = nil
	it.source.Seek(key)
}

// ElementID returns the transaction ElementID of the current entry.
func (it *CRDTResolvingIterator) ElementID() datalog.ElementID {
	if it.currentDatom != nil {
		return it.currentDatom.Tx
	}
	return datalog.ElementID{}
}
