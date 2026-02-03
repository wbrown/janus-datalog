package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// CRDT resolution functions that operate on pre-fetched datoms.
// These are pure functions that can be used by both:
// 1. Cache rebuild path (GetOrResolve → rebuild → storage scan → resolve)
// 2. Entity scan path (EAVT scan → group by attribute → resolve each)
//
// This factoring avoids redundant storage scans when processing multiple
// attributes for the same entity.

// ResolveLWWFromDatoms resolves cardinality-one using Last-Writer-Wins semantics.
// Input: datoms for a single (E, A) pair, any order.
// Output: current value (highest ElementID wins), max ElementID seen.
//
// Returns (nil, zero ElementID) if no datoms provided.
func ResolveLWWFromDatoms(datoms []datalog.Datom) (any, datalog.ElementID) {
	if len(datoms) == 0 {
		return nil, datalog.ElementID{}
	}

	var maxID datalog.ElementID
	var currentValue any

	for _, d := range datoms {
		if d.Tx.Compare(maxID) > 0 {
			maxID = d.Tx
			currentValue = d.V
		}
	}

	return currentValue, maxID
}

// ResolveAddWinsFromDatoms resolves cardinality-many using add-wins CRDT semantics.
// Input: datoms for a single (E, A) pair, any order. Datoms include Op field.
// Output: set of current members, max ElementID seen.
//
// Add-wins semantics:
// - For each unique value, track highest add tx and highest remove tx
// - If add has higher Lamport: in set
// - If remove has higher Lamport: not in set
// - Same Lamport (concurrent): add wins
func ResolveAddWinsFromDatoms(datoms []datalog.Datom) (map[any]bool, datalog.ElementID) {
	if len(datoms) == 0 {
		return make(map[any]bool), datalog.ElementID{}
	}

	var maxID datalog.ElementID

	// Track state per value
	type valueState struct {
		highestAddTx    datalog.ElementID
		highestRemoveTx datalog.ElementID
		hasAdd          bool
		hasRemove       bool
		value           any
	}
	valueStates := make(map[string]*valueState)

	for _, d := range datoms {
		// Track global max
		if d.Tx.Compare(maxID) > 0 {
			maxID = d.Tx
		}

		// Build key for this value (type + bytes)
		vType := byte(datalog.Type(d.V))
		vBytes := datalog.ValueBytes(d.V)
		valueKey := string(append([]byte{vType}, vBytes...))

		state, exists := valueStates[valueKey]
		if !exists {
			state = &valueState{value: d.V}
			valueStates[valueKey] = state
		}

		// Track highest tx per operation type
		if d.Op == datalog.OpCRDTAdd {
			if !state.hasAdd || d.Tx.Compare(state.highestAddTx) > 0 {
				state.highestAddTx = d.Tx
				state.hasAdd = true
			}
		} else if d.Op == datalog.OpCRDTRemove {
			if !state.hasRemove || d.Tx.Compare(state.highestRemoveTx) > 0 {
				state.highestRemoveTx = d.Tx
				state.hasRemove = true
			}
		}
	}

	// Resolve membership
	result := make(map[any]bool)
	for _, state := range valueStates {
		inSet := false
		if state.hasAdd && !state.hasRemove {
			inSet = true
		} else if state.hasAdd && state.hasRemove {
			if state.highestAddTx.Lamport > state.highestRemoveTx.Lamport {
				inSet = true
			} else if state.highestAddTx.Lamport == state.highestRemoveTx.Lamport {
				// Concurrent: add wins
				inSet = true
			}
		}
		if inSet {
			result[state.value] = true
		}
	}

	return result, maxID
}

// ResolveRGAFromDatoms resolves cardinality-vector using RGA reconstruction.
// Input: datoms for a single (E, A) pair, any order. Datoms must include Op and AfterRef.
// Output: ordered values, position index (element IDs), max ElementID seen.
//
// RGA semantics:
// - OpRGAInsert: element inserted after AfterRef position
// - OpRGATombstone: element deleted (AfterRef is the deleted element's ID)
// - Final order determined by tree reconstruction
func ResolveRGAFromDatoms(datoms []datalog.Datom) ([]any, []datalog.ElementID, datalog.ElementID) {
	if len(datoms) == 0 {
		return nil, nil, datalog.ElementID{}
	}

	// Build RGAElement map with deduplication
	elemByID := make(map[datalog.ElementID]RGAElement)

	for _, d := range datoms {
		if d.Op != datalog.OpRGAInsert && d.Op != datalog.OpRGATombstone {
			continue
		}

		if d.Op == datalog.OpRGAInsert {
			rgaElem := RGAElement{
				ID:       d.Tx,
				Value:    d.V,
				AfterRef: d.AfterRef,
			}

			existing, exists := elemByID[rgaElem.ID]
			if !exists {
				elemByID[rgaElem.ID] = rgaElem
			} else if existing.Tombstone == nil {
				elemByID[rgaElem.ID] = rgaElem
			}

		} else if d.Op == datalog.OpRGATombstone {
			targetID := d.AfterRef
			tombstoneID := d.Tx

			existing, exists := elemByID[targetID]
			if !exists {
				elemByID[targetID] = RGAElement{
					ID:        targetID,
					Value:     d.V,
					AfterRef:  datalog.ElementID{},
					Tombstone: &tombstoneID,
				}
			} else {
				if existing.Tombstone == nil || existing.Tombstone.Less(tombstoneID) {
					existing.Tombstone = &tombstoneID
					if existing.Value == nil {
						existing.Value = d.V
					}
					elemByID[targetID] = existing
				}
			}
		}
	}

	// Convert to slice
	elements := make([]RGAElement, 0, len(elemByID))
	for _, elem := range elemByID {
		elements = append(elements, elem)
	}

	if len(elements) == 0 {
		return nil, nil, datalog.ElementID{}
	}

	// Reconstruct with IDs
	withIDs := ReconstructRGAWithIDs(elements)

	// Extract values and positions
	values := make([]any, len(withIDs))
	positions := make([]datalog.ElementID, len(withIDs))
	for i, ewp := range withIDs {
		values[i] = ewp.Element.Value
		positions[i] = ewp.Element.ID
	}

	// Find max ElementID
	maxID := FindMaxElementID(elements)

	return values, positions, maxID
}
