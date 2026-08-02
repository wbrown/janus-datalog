package storage

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// latticeFixtureDatoms builds a duplicate-free set that collides on every
// component the derivations pivot on: many datoms per entity, few attributes,
// colliding values across types, colliding Lamports resolved by ReplicaID —
// and pairs identical through all four components that differ only in the
// [AfterRef?][Op] tail, so tail ordering is load-bearing in every index.
func latticeFixtureDatoms(n int) []*datalog.Datom {
	attrs := []datalog.Keyword{
		datalog.NewKeyword(":lattice/a"),
		datalog.NewKeyword(":lattice/b"),
		datalog.NewKeyword(":lattice/c"),
		datalog.NewKeyword(":lattice/d"),
		datalog.NewKeyword(":lattice/e"),
	}
	refs := []datalog.Identity{
		datalog.NewIdentity("lattice:ref:0"),
		datalog.NewIdentity("lattice:ref:1"),
	}
	instant := time.Date(2026, 5, 4, 3, 2, 1, 0, time.UTC)

	datoms := make([]*datalog.Datom, 0, n+n/16)
	for i := 0; i < n; i++ {
		var v datalog.Value
		switch i % 6 {
		case 0:
			v = int64(i % 13)
		case 1:
			v = fmt.Sprintf("v%02d", i%17)
		case 2:
			v = instant.Add(time.Duration(i%11) * time.Minute)
		case 3:
			v = i%2 == 0
		case 4:
			v = refs[i%len(refs)]
		case 5:
			v = []byte{byte(i % 7), byte(i % 3)}
		}
		datoms = append(datoms, &datalog.Datom{
			E: datalog.NewIdentity(fmt.Sprintf("lattice:entity:%d", i%89)),
			A: attrs[i%len(attrs)],
			V: v,
			// Lamport collides across i while (Lamport, ReplicaID) stays unique,
			// so Tx ordering exercises the tiebreak and no full duplicate exists.
			Tx: datalog.ElementID{Lamport: uint64(1 + i/3), ReplicaID: uint64(i%3 + 1)},
		})

		// Every 16th datom gets tail-only siblings: all four components equal,
		// distinguished by Op and by AfterRef. These stay adjacent in every
		// index order, so any derivation that mishandles the tail reorders them.
		if i%16 == 0 {
			base := *datoms[len(datoms)-1]
			insert := base
			insert.Op = datalog.OpRGAInsert
			insert.AfterRef = datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 9}
			later := base
			later.Op = datalog.OpRGAInsert
			later.AfterRef = datalog.ElementID{Lamport: uint64(i + 2), ReplicaID: 9}
			datoms = append(datoms, &insert, &later)
		}
	}
	return datoms
}

// requireLatticeMatchesDirectSorts holds versionFromDatoms to the reference it
// derives from: for each index, the built tree walks in exactly the order a
// direct sort of the same datoms produces.
func requireLatticeMatchesDirectSorts(t *testing.T, datoms []*datalog.Datom) {
	t.Helper()

	reference := make(map[IndexType][]*datalog.Datom, len(Indices))
	for _, index := range Indices {
		order, err := componentOrder(index)
		require.NoError(t, err)
		sorted := append([]*datalog.Datom(nil), datoms...)
		slices.SortFunc(sorted, func(a, b *datalog.Datom) int {
			return compareDatomsInOrder(order, a, b)
		})
		reference[index] = sorted
	}

	built := versionFromDatoms(append([]*datalog.Datom(nil), datoms...))
	for _, index := range Indices {
		require.Equal(t, len(datoms), built.tree(index).Len(), "index %v count", index)
		walked := walkTree(t, built.tree(index))
		require.Len(t, walked, len(datoms), "index %v walk length", index)
		for i := range walked {
			require.Same(t, reference[index][i], walked[i],
				"index %v diverges from its direct sort at %d", index, i)
		}
	}
}

func TestVersionFromDatomsMatchesDirectSorts(t *testing.T) {
	for _, n := range []int{0, 1, 2, 5000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			requireLatticeMatchesDirectSorts(t, latticeFixtureDatoms(n))
		})
	}
}

// TestVersionFromDatomsOnPresortedInput feeds the import's shape: the gathered
// slice already in EAVT order.
func TestVersionFromDatomsOnPresortedInput(t *testing.T) {
	datoms := latticeFixtureDatoms(5000)
	order, err := componentOrder(EAVT)
	require.NoError(t, err)
	slices.SortFunc(datoms, func(a, b *datalog.Datom) int {
		return compareDatomsInOrder(order, a, b)
	})
	requireLatticeMatchesDirectSorts(t, datoms)
}
