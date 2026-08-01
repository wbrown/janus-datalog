package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// compareDatoms orders two datoms by the component sequence an index names.
// The order comes from componentOrder, the same table the scan-bound encoder
// walks.
//
// The key is [prefix][components][AfterRef?][Op]; the prefix is constant within
// an index, so the comparison is the components in order then the tail. Tx and
// AfterRef sort descending, which is what the encoder's bitwise NOT achieves in
// byte space.
//
// This reproduces BinaryKeyEncoder's order except for values whose encoded
// payloads are prefixes of one another, where it cannot: the payload carries no
// length, so the key's order there is decided by whichever component follows V.
// See BUG_V_PAYLOAD_NOT_PREFIX_FREE. This comparator orders those by value.
func compareDatoms(index IndexType, a, b *datalog.Datom) int {
	order, err := componentOrder(index)
	if err != nil {
		panic(fmt.Sprintf("compareDatoms: %v", err))
	}
	return compareDatomsInOrder(order, a, b)
}

// compareDatomsInOrder is the comparison itself, over an already-resolved
// component order. A tree holds its index's order once and calls this directly,
// so descent does not repeat the lookup at every step.
func compareDatomsInOrder(order [componentsPerIndex]keyComponent, a, b *datalog.Datom) int {
	for _, c := range order {
		if cmp := compareComponent(c, a, b); cmp != 0 {
			return cmp
		}
	}
	return compareKeyTail(a, b)
}

// comparePrefixInOrder compares two datoms on the leading n components of an
// index's order and treats everything after them as equal. A ScanBound is
// equality on exactly those components, so this is what makes its run
// contiguous: seek to the first datom that is not less, then walk while this
// returns zero.
//
// It also means the probe need only carry the bound components. Fields the
// bound does not name are never read.
func comparePrefixInOrder(order [componentsPerIndex]keyComponent, n int, a, b *datalog.Datom) int {
	for i := 0; i < n; i++ {
		if cmp := compareComponent(order[i], a, b); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func compareComponent(c keyComponent, a, b *datalog.Datom) int {
	switch c {
	case componentE:
		return a.E.Compare(b.E)
	case componentA:
		return a.A.Compare(b.A)
	case componentV:
		return compareValueKeyForm(a.V, b.V)
	case componentTx:
		return -a.Tx.Compare(b.Tx)
	}
	panic(fmt.Sprintf("compareComponent: unknown component %v", c))
}

// compareValueKeyForm orders values as the key lays them out: the type tag
// byte, then the payload within that type.
func compareValueKeyForm(a, b datalog.Value) int {
	at, bt := datalog.Type(a), datalog.Type(b)
	switch {
	case at < bt:
		return -1
	case at > bt:
		return 1
	}
	return datalog.CompareValues(a, b)
}

// compareKeyTail orders the [AfterRef?][Op] suffix. Op decides whether AfterRef
// is present, so datoms differing in presence also differ in Op.
func compareKeyTail(a, b *datalog.Datom) int {
	if a.Op.HasAfterRef() && b.Op.HasAfterRef() {
		if cmp := -a.AfterRef.Compare(b.AfterRef); cmp != 0 {
			return cmp
		}
	}
	switch {
	case a.Op < b.Op:
		return -1
	case a.Op > b.Op:
		return 1
	}
	return 0
}
