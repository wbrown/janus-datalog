package datalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestElementIDCompareAgreesWithLess pins Compare to the total order Less
// defines: Lamport first, ReplicaID as tiebreaker. Every ordered pair over a
// small cross product is checked, so the two can never drift apart.
func TestElementIDCompareAgreesWithLess(t *testing.T) {
	values := []uint64{0, 1, 2, ^uint64(0)}
	var ids []ElementID
	for _, lamport := range values {
		for _, replica := range values {
			ids = append(ids, ElementID{Lamport: lamport, ReplicaID: replica})
		}
	}

	for _, a := range ids {
		for _, b := range ids {
			want := 0
			switch {
			case a.Less(b):
				want = -1
			case b.Less(a):
				want = 1
			}
			require.Equal(t, want, a.Compare(b), "Compare(%v, %v)", a, b)
			require.Equal(t, -a.Compare(b), b.Compare(a), "antisymmetry (%v, %v)", a, b)
		}
	}
}
