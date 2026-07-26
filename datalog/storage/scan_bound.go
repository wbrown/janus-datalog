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

// EncodedRun is a ScanBound projected onto binary keys: the range a scan walks,
// and the test deciding which keys inside it the bound actually names.
//
// The two are the same thing only when every bound component is fixed width.
// A V payload carries no length, so a range whose components include a
// variable-length V is a *prefix* range: the keys for "abcd" sort inside the
// range for "abc", interleaved with them (the byte after the shared prefix is
// 'd' on one side and the first byte of a hash on the other), so no choice of
// endpoints separates the two. What does separate them is length. Every
// component behind V is fixed width and Op announces AfterRef, so a key of the
// bound's own value has exactly one length for each Op class, and a key whose
// value merely starts with it is longer by the excess.
type EncodedRun struct {
	Start, End []byte

	// exact is true when the byte range already names the run and Holds needs
	// no test. memberSize is otherwise the length of a member key excluding
	// its tail, which keyTailSize reads from the key itself.
	exact      bool
	memberSize int
}

// Holds reports whether a key drawn from the range is one the bound names.
// It is meaningful only for keys already inside [Start, End).
func (r EncodedRun) Holds(key []byte) bool {
	if r.exact {
		return true
	}
	if len(key) == 0 {
		return false
	}
	return len(key) == r.memberSize+keyTailSize(key)
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
	data["index"] = bound.Index.String()
	data["bound"] = positions
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
