package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// VectorResolutionResult contains the resolved vector and metadata
type VectorResolutionResult struct {
	// Elements contains the ordered values after RGA reconstruction
	Elements []any

	// Present reports whether the (E, A) carries any RGA datom at all, and it
	// is the resolved answer to whether the attribute exists.
	//
	// A vector that was never set and one whose every element was tombstoned
	// both reconstruct to zero live elements, so Elements cannot separate them;
	// only the presence of datoms can, and the two are different states — the
	// first has no value, the second has the empty vector. This is resolved
	// alongside the value rather than counted afterwards: a statistic describes
	// a resolution, it does not constitute one.
	Present bool

	// MaxElementID is the highest ElementID seen (for cache versioning)
	MaxElementID datalog.ElementID

	// Bound is the run this resolution walked. The pattern arm announces it,
	// and it travels back rather than being rebuilt there so the announced run
	// and the walked run are the same value and cannot drift.
	Bound ScanBound
}

// resolveVector loads all RGA elements for (E, A) and reconstructs the ordered vector.
//
// This uses EATV index: E → A → Tx → V → Op → AfterRef?
// V contains the raw value. Op is OpRGAInsert or OpRGATombstone.
// AfterRef is present only when Op.HasAfterRef() is true.
// The Tx (ElementID) is the element's ID.
//
// After loading all elements, RGA reconstruction builds the final ordered list.
func (m *PatternMatcher) resolveVector(eBytes, aBytes []byte, report *scanReport) (*VectorResolutionResult, error) {
	// Use loadRGAElements which handles deduplication
	elements, bound, err := m.loadRGAElements(eBytes, aBytes, report)
	if err != nil {
		return nil, err
	}

	// Reconstruct the ordered vector
	ordered := ReconstructRGA(elements)

	return &VectorResolutionResult{
		Elements: ordered,
		// Every insert and tombstone ever written for the (E, A) is in
		// elements, so their presence is the attribute's.
		Present:      len(elements) > 0,
		MaxElementID: FindMaxElementID(elements),
		Bound:        bound,
	}, nil
}

// loadRGAElements loads all RGA elements for (E, A) without reconstruction.
// Used when you need access to the raw elements (e.g., for building position index).
//
// - V is the raw value
// - Op is OpRGAInsert (3) or OpRGATombstone (4)
// - AfterRef is the position reference for inserts, or the target element ID for tombstones
//
// The elements are gathered by vectorElements.gather, so an element keeps the
// place its insert names whether its withdrawal or its insert is read first.
//
// Returns the run it walked alongside the elements, and accrues the scan's
// index intake into the report. An RGA group is every insert and tombstone ever
// written for the (E, A), so this is the resolution whose intake most exceeds
// what it produces, and the pattern above it has no other way to learn the
// difference. The bound travels back rather than being rebuilt by the caller
// that announces it, so the announced run and the walked run are the same value.
func (m *PatternMatcher) loadRGAElements(eBytes, aBytes []byte, report *scanReport) ([]RGAElement, ScanBound, error) {
	// The caller holds storage projections; both constructors return the
	// canonical interned pointer for an already-interned value.
	var e Entity
	copy(e[:], eBytes)
	var a Attribute
	copy(a[:], aBytes)

	// Scan EATV for all entries with this E+A
	bound := ScanBound{
		Index: EATV,
		Prefix: []datalog.Value{
			datalog.NewIdentityFromHash(e),
			datalog.InternKeywordFromBytes(a),
		},
	}
	// The scan accrues into the report as it closes, so a decode failure keeps the
	// intake it spent instead of losing it with the result it never returns.
	iter, err := OpenScan(m.reader, report, bound)
	if err != nil {
		return nil, bound, err
	}
	defer iter.Close()

	elemByID := make(vectorElements)
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, bound, fmt.Errorf("decode RGA element: %w", err)
		}

		// Apply temporal filtering for AsOf queries
		if m.shouldFilterTx(datom.Tx) {
			continue
		}

		elemByID.gather(datom)
	}
	if err := iter.Error(); err != nil {
		return nil, bound, fmt.Errorf("decode RGA element: %w", err)
	}

	// Convert map to slice
	elements := make([]RGAElement, 0, len(elemByID))
	for _, elem := range elemByID {
		elements = append(elements, elem)
	}

	return elements, bound, nil
}
