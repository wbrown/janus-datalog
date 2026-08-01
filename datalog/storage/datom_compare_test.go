package storage

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestDatomComparatorMatchesKeyOrder pins the load-bearing property of the
// memory backend: for every index, the typed comparator induces the order
// BinaryKeyEncoder's bytes do. Index ordering is CRDT resolution, so an
// unintended disagreement means Memory and Badger resolve the same datom set
// differently — silently, and in a way no test above the storage seam sees.
//
// One disagreement is structural and expected: BUG_V_PAYLOAD_NOT_PREFIX_FREE.
// An encoded value carries no length or terminator, so when one payload is a
// prefix of another the key's order is decided by whichever component follows V
// rather than by the values. No typed comparator can reproduce that without
// rebuilding the encoder, and this one deliberately orders such pairs by value.
//
// The exemption is therefore stated as a precondition, not a skip: for every
// pair whose encoded payloads are *not* prefix-related, the orders must agree.
// A disagreement anywhere else fails.
//
// The check is pairwise rather than a sorted-sequence comparison so a failure
// names the two datoms that disagree instead of an index into a list.
func TestDatomComparatorMatchesKeyOrder(t *testing.T) {
	encoder := &BinaryKeyEncoder{}
	datoms := comparatorOrderFixture()

	for _, index := range Indices {
		t.Run(fmt.Sprintf("%v", index), func(t *testing.T) {
			keys := make([][]byte, len(datoms))
			for i := range datoms {
				keys[i] = encoder.EncodeKey(index, &datoms[i])
			}

			for i := range datoms {
				for j := range datoms {
					want := compareSign(bytes.Compare(keys[i], keys[j]))
					got := compareSign(compareDatoms(index, &datoms[i], &datoms[j]))
					if want == got {
						continue
					}
					if valueEncodingsArePrefixRelated(encoder, datoms[i].V, datoms[j].V) {
						continue
					}
					require.Failf(t, "comparator disagrees with key order",
						"index %v, and the values are not prefix-related so "+
							"BUG_V_PAYLOAD_NOT_PREFIX_FREE does not explain it\n"+
							"  a = %s\n  b = %s\n  key order %d, comparator %d",
						index, describeDatom(&datoms[i]), describeDatom(&datoms[j]), want, got)
				}
			}
		})
	}
}

// TestComparatorFixtureExercisesPrefixRelatedValues keeps the exemption above
// from going vacuous. If the fixture stopped containing prefix-related payloads,
// that test would pass while checking nothing about the case it carves out.
func TestComparatorFixtureExercisesPrefixRelatedValues(t *testing.T) {
	encoder := &BinaryKeyEncoder{}
	datoms := comparatorOrderFixture()

	for i := range datoms {
		for j := range datoms {
			if valueEncodingsArePrefixRelated(encoder, datoms[i].V, datoms[j].V) {
				return
			}
		}
	}
	require.Fail(t, "fixture contains no prefix-related value encodings",
		"the exemption in TestDatomComparatorMatchesKeyOrder covers nothing")
}

// valueEncodingsArePrefixRelated reports whether one value's encoded form is a
// strict prefix of the other's — the condition under which the key's byte order
// runs the shorter payload into the following component.
func valueEncodingsArePrefixRelated(encoder *BinaryKeyEncoder, a, b datalog.Value) bool {
	ab, _ := encoder.EncodeValueBytes(a)
	bb, _ := encoder.EncodeValueBytes(b)
	switch {
	case len(ab) < len(bb):
		return bytes.HasPrefix(bb, ab)
	case len(bb) < len(ab):
		return bytes.HasPrefix(ab, bb)
	}
	return false
}

// TestDatomComparatorIsAntisymmetric checks the comparator is a total order in
// its own right: reversing the arguments reverses the sign, and a datom equals
// itself. A comparator that fails this corrupts a tree regardless of whether it
// agrees with the encoder.
func TestDatomComparatorIsAntisymmetric(t *testing.T) {
	datoms := comparatorOrderFixture()
	for _, index := range Indices {
		t.Run(fmt.Sprintf("%v", index), func(t *testing.T) {
			for i := range datoms {
				require.Zero(t, compareDatoms(index, &datoms[i], &datoms[i]),
					"datom does not equal itself: %s", describeDatom(&datoms[i]))
				for j := range datoms {
					forward := compareSign(compareDatoms(index, &datoms[i], &datoms[j]))
					reverse := compareSign(compareDatoms(index, &datoms[j], &datoms[i]))
					require.Equal(t, forward, -reverse,
						"not antisymmetric\n  a = %s\n  b = %s",
						describeDatom(&datoms[i]), describeDatom(&datoms[j]))
				}
			}
		})
	}
}

// comparatorOrderFixture spans the cases where the typed order and the byte
// order can come apart: values of different types (the key writes a type tag
// ahead of the payload), string values where one is a prefix of the other (the
// payload carries no length, so the shorter one runs into whatever component
// follows), Tx differing in Lamport and in ReplicaID alone (both directions of
// the descending encoding), and every Op including the two that carry an
// AfterRef.
func comparatorOrderFixture() []datalog.Datom {
	entities := []datalog.Identity{
		datalog.NewIdentity("entity-one"),
		datalog.NewIdentity("entity-two"),
	}
	attrs := []datalog.Keyword{
		datalog.NewKeyword(":fixture/alpha"),
		datalog.NewKeyword(":fixture/beta"),
	}
	values := []datalog.Value{
		int64(1),
		int64(2),
		"abc",
		"abcd",
		"abd",
		true,
		false,
		3.5,
		[]byte("bin"),
		time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC),
		datalog.NewIdentity("ref-target"),
		datalog.NewKeyword(":fixture/as-value"),
	}
	txs := []datalog.ElementID{
		{Lamport: 1, ReplicaID: 1},
		{Lamport: 2, ReplicaID: 1},
		{Lamport: 2, ReplicaID: 2},
	}
	tails := []struct {
		op       datalog.CRDTOp
		afterRef datalog.ElementID
	}{
		{op: datalog.OpNone},
		{op: datalog.OpCRDTAdd},
		{op: datalog.OpCRDTRemove},
		{op: datalog.OpRGAInsert, afterRef: datalog.ElementID{Lamport: 5, ReplicaID: 1}},
		{op: datalog.OpRGAInsert, afterRef: datalog.ElementID{Lamport: 6, ReplicaID: 1}},
		{op: datalog.OpRGATombstone, afterRef: datalog.ElementID{Lamport: 5, ReplicaID: 1}},
	}

	out := make([]datalog.Datom, 0, len(entities)*len(attrs)*len(values)*len(txs)*len(tails))
	for _, e := range entities {
		for _, a := range attrs {
			for _, v := range values {
				for _, tx := range txs {
					for _, tail := range tails {
						out = append(out, datalog.Datom{
							E:        e,
							A:        a,
							V:        v,
							Tx:       tx,
							Op:       tail.op,
							AfterRef: tail.afterRef,
						})
					}
				}
			}
		}
	}
	return out
}

func compareSign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	}
	return 0
}

func describeDatom(d *datalog.Datom) string {
	return fmt.Sprintf("E=%s A=%s V=%#v Tx=%v Op=%d AfterRef=%v",
		d.E, d.A, d.V, d.Tx, d.Op, d.AfterRef)
}
