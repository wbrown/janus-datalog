package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// sortedTreeDatoms returns n distinct datoms in the given index's order. The
// values are spaced by two so a test can seek an absent target that falls
// between two present ones.
func sortedTreeDatoms(index IndexType, n int) []*datalog.Datom {
	attr := datalog.NewKeyword(":tree/value")
	out := make([]*datalog.Datom, n)
	for i := 0; i < n; i++ {
		out[i] = &datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("tree-entity-%d", i)),
			A:  attr,
			V:  int64(i * 2),
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
		}
	}
	order, err := componentOrder(index)
	if err != nil {
		panic(err)
	}
	sort.Slice(out, func(a, b int) bool {
		return compareDatomsInOrder(order, out[a], out[b]) < 0
	})
	return out
}

func walkTree(t *testing.T, tree *datomTree) []*datalog.Datom {
	t.Helper()
	var out []*datalog.Datom
	c := tree.cursor()
	for ok := c.seekFirst(); ok; ok = c.next() {
		out = append(out, c.datom())
	}
	return out
}

// TestDatomTreeBuildFromSortedRoundTrips covers each shape the bottom-up build
// produces: empty, a single partial leaf, exactly one full leaf, the first
// split into two leaves, and a size deep enough to require two branch levels.
func TestDatomTreeBuildFromSortedRoundTrips(t *testing.T) {
	sizes := []int{0, 1, branchingFactor - 1, branchingFactor, branchingFactor + 1, branchingFactor*branchingFactor + 1}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("n=%d", size), func(t *testing.T) {
			datoms := sortedTreeDatoms(EAVT, size)
			tree := newDatomTree(EAVT)
			tree.buildFromSorted(datoms)
			require.Equal(t, size, tree.Len())

			walked := walkTree(t, tree)
			require.Len(t, walked, size)
			for i := range datoms {
				require.Same(t, datoms[i], walked[i], "sequence diverges at %d", i)
			}
		})
	}
}

// TestDatomTreeBuildReachesExpectedDepth keeps the multi-level cases above
// honest: if the build ever packed differently, the largest size could collapse
// to a shallower tree and the branch-level code would stop being exercised
// while the round-trip test still passed.
func TestDatomTreeBuildReachesExpectedDepth(t *testing.T) {
	for _, tc := range []struct {
		size  int
		level int8
	}{
		{size: 1, level: 0},
		{size: branchingFactor, level: 0},
		{size: branchingFactor + 1, level: 1},
		{size: branchingFactor * branchingFactor, level: 1},
		{size: branchingFactor*branchingFactor + 1, level: 2},
	} {
		t.Run(fmt.Sprintf("n=%d", tc.size), func(t *testing.T) {
			tree := newDatomTree(EAVT)
			tree.buildFromSorted(sortedTreeDatoms(EAVT, tc.size))
			require.Equal(t, tc.level, tree.root.level)
		})
	}
}

func TestDatomTreeSeek(t *testing.T) {
	const size = branchingFactor*2 + 7
	datoms := sortedTreeDatoms(EAVT, size)
	tree := newDatomTree(EAVT)
	tree.buildFromSorted(datoms)

	t.Run("present target lands on it", func(t *testing.T) {
		c := tree.cursor()
		for i, want := range datoms {
			require.True(t, c.seek(want, fullDatomCompare), "seek failed at %d", i)
			require.Same(t, want, c.datom(), "seek landed wrong at %d", i)
		}
	})

	t.Run("absent target lands on the first greater", func(t *testing.T) {
		// Values are spaced by two, so a datom carrying an odd value sorts
		// between two present entries under any index whose order reaches V.
		c := tree.cursor()
		gaps := 0
		for i := 0; i+1 < len(datoms); i++ {
			between := *datoms[i]
			between.V = datoms[i].V.(int64) + 1
			if tree.compare(&between, datoms[i]) <= 0 || tree.compare(&between, datoms[i+1]) >= 0 {
				continue // this index orders E or Tx ahead of V; not a gap
			}
			gaps++
			require.True(t, c.seek(&between, fullDatomCompare))
			require.Same(t, datoms[i+1], c.datom(), "gap seek at %d", i)
		}
		require.NotZero(t, gaps, "no absent target fell between two present ones; this subtest checked nothing")
	})

	t.Run("target past the end exhausts", func(t *testing.T) {
		beyond := *datoms[len(datoms)-1]
		beyond.Tx = datalog.ElementID{Lamport: 0, ReplicaID: 0}
		beyond.E = datalog.NewIdentity("\xff\xff tree-entity-beyond")
		beyond.V = int64(size*2 + 1)

		c := tree.cursor()
		if c.seek(&beyond, fullDatomCompare) {
			require.NotSame(t, datoms[len(datoms)-1], c.datom())
		}
	})

	t.Run("walking from a seek yields the tail", func(t *testing.T) {
		start := len(datoms) / 3
		c := tree.cursor()
		require.True(t, c.seek(datoms[start], fullDatomCompare))
		for i := start; i < len(datoms); i++ {
			require.Same(t, datoms[i], c.datom(), "tail diverges at %d", i)
			require.Equal(t, i+1 < len(datoms), c.next(), "next disagreed at %d", i)
		}
	})
}

func TestDatomTreeEmpty(t *testing.T) {
	tree := newDatomTree(EAVT)
	tree.buildFromSorted(nil)
	require.Zero(t, tree.Len())

	c := tree.cursor()
	require.False(t, c.seekFirst())
	require.Nil(t, c.datom())
	require.False(t, c.next())

	probe := &datalog.Datom{
		E:  datalog.NewIdentity("absent"),
		A:  datalog.NewKeyword(":tree/value"),
		V:  int64(0),
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}
	require.False(t, c.seek(probe, fullDatomCompare))
	require.Nil(t, c.datom())
}

// TestDatomTreeBuildsEveryIndexOrder: the build and cursor are order-agnostic,
// but the comparator they call is not, and only EAVT is exercised above.
func TestDatomTreeBuildsEveryIndexOrder(t *testing.T) {
	for _, index := range Indices {
		t.Run(fmt.Sprintf("%v", index), func(t *testing.T) {
			datoms := sortedTreeDatoms(index, branchingFactor+13)
			tree := newDatomTree(index)
			tree.buildFromSorted(datoms)

			walked := walkTree(t, tree)
			require.Len(t, walked, len(datoms))
			for i := range datoms {
				require.Same(t, datoms[i], walked[i], "sequence diverges at %d", i)
			}
		})
	}
}
