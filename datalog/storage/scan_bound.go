package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
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

// ScanBound names the datoms of one index whose leading components — in that
// index's component order — equal the bound's values. The k-th Prefix element
// binds the k-th component of Index's order, so elements carry no position tag
// of their own. An empty Prefix names the whole index.
//
// This is a logical set, not a byte range, and the difference is load-bearing
// for any backend that projects the bound onto keys. A V payload carries no
// length, so the keys for "abcd" sort inside the range for "abc" interleaved
// with them, and no choice of endpoints separates the two. Narrowing to the
// datoms the bound actually names is the backend's obligation, not the
// caller's: a scan yields exactly these datoms, and how a backend achieves
// that is not seam vocabulary. See EncodedRun and runMembership for how the
// binary-key backends discharge it.
//
// Bound components are always leading components, never an interior selection.
// Every index layout places Op last and AfterRef (when present) immediately
// before it, so neither is ever a bound component; the orderable positions are
// exactly E, A, V and Tx, and binding one requires binding every position
// ahead of it.
//
// Prefix elements are ordinary datalog values — Identity for E, Keyword for A,
// any domain value for V, ElementID for Tx — so a bound introduces no value
// kinds beyond the closed domain.
type ScanBound struct {
	Index  IndexType
	Prefix []datalog.Value
}

// addBoundFields reports a scan bound into an annotation event's data: the
// index, the positions the run binds in that index's component order, and the
// values bound to them.
//
// This is the seam's own account of what it addressed. It deliberately does
// not carry the encoded key range: that is one backend's projection of the
// bound, uninterpretable by a consumer that does not hold the encoder, and
// absent entirely for a backend that compares typed components directly.
func addBoundFields(data map[string]interface{}, bound ScanBound) {
	positions, values := describeRun(bound.Index, bound.Prefix)
	data[annotations.KeyIndex] = bound.Index
	data[annotations.KeyBound] = positions
	data["bound.values"] = values
}

// describeRun names the positions one endpoint of a bound binds, in index's
// component order, alongside the rendered values bound to them. The slices are
// parallel — position k carries value k — which is the run's own structure: an
// ordered sequence of components, each fixed to a value. Both are empty for a
// whole-index run.
//
// Positions are named rather than implied so a consumer never needs the index
// layout to read the bound. Values are rendered to strings because an event's
// Data is a display surface, matching every neighbouring storage event; a
// consumer that needs the typed value has the query, not the annotation.
//
// An index outside the taxonomy renders its positions as "?": the scan itself
// fails loudly at encode time, so a renderer that refused here would replace a
// readable event with no event at all.
func describeRun(index IndexType, values []datalog.Value) (positions, rendered []string) {
	if len(values) == 0 {
		return nil, nil
	}
	order, err := componentOrder(index)
	positions = make([]string, 0, len(values))
	rendered = make([]string, 0, len(values))
	for i, v := range values {
		if err != nil || i >= len(order) {
			positions = append(positions, "?")
		} else {
			positions = append(positions, order[i].String())
		}
		rendered = append(rendered, fmt.Sprintf("%v", v))
	}
	return positions, rendered
}
