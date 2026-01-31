package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// VectorResolutionResult contains the resolved vector and metadata
type VectorResolutionResult struct {
	// Elements contains the ordered values after RGA reconstruction
	Elements []any
	// MaxElementID is the highest ElementID seen (for cache versioning)
	MaxElementID datalog.ElementID
	// Stats provides debugging information
	Stats RGAStats
}

// resolveVector loads all RGA elements for (E, A) and reconstructs the ordered vector.
//
// This uses EATV index: E → A → Tx → V
// Each entry's V is an encoded RGAElement containing AfterRef, optional Tombstone, and Value.
// The Tx (ElementID) is the element's ID.
//
// After loading all elements, RGA reconstruction builds the final ordered list.
func (m *BadgerMatcher) resolveVector(eBytes, aBytes []byte) (*VectorResolutionResult, error) {
	// Use loadRGAElements which handles deduplication
	elements, err := m.loadRGAElements(eBytes, aBytes)
	if err != nil {
		return nil, err
	}

	// Reconstruct the ordered vector
	ordered := ReconstructRGA(elements)

	// Compute stats for debugging and cache versioning
	stats := ComputeRGAStats(elements)

	return &VectorResolutionResult{
		Elements:     ordered,
		MaxElementID: stats.MaxID,
		Stats:        stats,
	}, nil
}

// loadRGAElements loads all RGA elements for (E, A) without reconstruction.
// Used when you need access to the raw elements (e.g., for building position index).
//
// IMPORTANT: Handles deduplication by element ID. When the same element has multiple
// versions (e.g., original + tombstoned), the tombstoned version takes precedence.
// This supports Set() which writes a tombstone record with the same element ID.
func (m *BadgerMatcher) loadRGAElements(eBytes, aBytes []byte) ([]RGAElement, error) {
	// Build prefix for E+A on EATV index
	prefix := make([]byte, 1+20+32) // prefix byte + E + A
	prefix[0] = byte(EATV)
	copy(prefix[1:21], eBytes)
	copy(prefix[21:53], aBytes)

	// Scan EATV for all entries with this E+A
	iter, err := m.store.Scan(EATV, prefix, prefixEnd(prefix))
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// Use map to deduplicate by element ID
	// Tombstoned versions take precedence over non-tombstoned
	elemByID := make(map[datalog.ElementID]RGAElement)

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			continue
		}

		// The V field should be encoded RGA element data ([]byte)
		encodedRGA, ok := datom.V.([]byte)
		if !ok {
			continue
		}

		// Decode the RGA element
		rgaElem, err := DecodeRGAElement(datom.Tx, encodedRGA)
		if err != nil {
			continue
		}

		// Deduplication: if we already have this element, prefer tombstoned version
		existing, exists := elemByID[rgaElem.ID]
		if !exists {
			elemByID[rgaElem.ID] = rgaElem
		} else {
			// Prefer tombstoned version (element is deleted)
			if rgaElem.Tombstone != nil && existing.Tombstone == nil {
				elemByID[rgaElem.ID] = rgaElem
			}
			// If both have tombstones, keep the one with higher tombstone ID
			if rgaElem.Tombstone != nil && existing.Tombstone != nil {
				if existing.Tombstone.Less(*rgaElem.Tombstone) {
					elemByID[rgaElem.ID] = rgaElem
				}
			}
		}
	}

	// Convert map to slice
	elements := make([]RGAElement, 0, len(elemByID))
	for _, elem := range elemByID {
		elements = append(elements, elem)
	}

	return elements, nil
}
