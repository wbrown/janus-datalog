package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// keyComponent identifies a datom position within an index's key order.
type keyComponent uint8

const (
	componentE keyComponent = iota
	componentA
	componentV
	componentTx
)

func (c keyComponent) String() string {
	switch c {
	case componentE:
		return "E"
	case componentA:
		return "A"
	case componentV:
		return "V"
	case componentTx:
		return "Tx"
	default:
		return fmt.Sprintf("keyComponent(%d)", uint8(c))
	}
}

// componentsPerIndex is the number of datom positions every index orders by.
// Op and AfterRef are key suffixes, not orderable components (see ScanBound).
const componentsPerIndex = 4

// componentOrder returns the order in which an index arranges the four datom
// positions. It is the same ordering encodeKeyWithParts lays down in bytes;
// the two are projections of one specification, held together by
// TestScanBoundEncodesAsPrefixRange.
func componentOrder(index IndexType) ([componentsPerIndex]keyComponent, error) {
	switch index {
	case EAVT:
		return [componentsPerIndex]keyComponent{componentE, componentA, componentV, componentTx}, nil
	case EATV:
		return [componentsPerIndex]keyComponent{componentE, componentA, componentTx, componentV}, nil
	case AEVT:
		return [componentsPerIndex]keyComponent{componentA, componentE, componentV, componentTx}, nil
	case AETV:
		return [componentsPerIndex]keyComponent{componentA, componentE, componentTx, componentV}, nil
	case ATEV:
		return [componentsPerIndex]keyComponent{componentA, componentTx, componentE, componentV}, nil
	case AVET:
		return [componentsPerIndex]keyComponent{componentA, componentV, componentE, componentTx}, nil
	case VAET:
		return [componentsPerIndex]keyComponent{componentV, componentA, componentE, componentTx}, nil
	case TAEV:
		return [componentsPerIndex]keyComponent{componentTx, componentA, componentE, componentV}, nil
	default:
		return [componentsPerIndex]keyComponent{}, fmt.Errorf("unknown index type: %v", index)
	}
}

// ScanBound names a contiguous run of one index: the leading components of
// that index's component order, each bound to a value. The k-th Prefix
// element binds the k-th component of Index's order, so elements carry no
// position tag of their own. An empty Prefix names the whole index.
//
// A bound is always a prefix, never an asymmetric start/end pair. Every index
// layout places Op last and AfterRef (when present) immediately before it, so
// neither is ever a bound component; the orderable positions are exactly E, A,
// V and Tx, and binding one requires binding every position ahead of it.
//
// Prefix elements are ordinary datalog values — Identity for E, Keyword for A,
// any domain value for V, ElementID for Tx — so a bound introduces no value
// kinds beyond the closed domain.
type ScanBound struct {
	Index  IndexType
	Prefix []datalog.Value
}
