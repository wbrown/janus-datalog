package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func removeAll(tree *datomTree, datoms []*datalog.Datom) *datomTree {
	b := tree.transient()
	for _, d := range datoms {
		b.remove(d)
	}
	return b.commit()
}

// TestTreeRemoveLeavesTheRestInOrder removes an interleaved half and checks the
// survivors still walk in order with the right count, at sizes spanning a
// single leaf through several branch levels.
func TestTreeRemoveLeavesTheRestInOrder(t *testing.T) {
	for _, size := range []int{1, 2, branchingFactor + 1, branchingFactor * 3} {
		t.Run(fmt.Sprintf("n=%d", size), func(t *testing.T) {
			sorted := sortedTreeDatoms(EAVT, size)
			tree := newDatomTree(EAVT)
			tree.buildFromSorted(sorted)

			var dropped, kept []*datalog.Datom
			for i, d := range sorted {
				if i%2 == 0 {
					dropped = append(dropped, d)
				} else {
					kept = append(kept, d)
				}
			}

			pruned := removeAll(tree, dropped)
			require.Equal(t, len(kept), pruned.Len())

			walked := walkTree(t, pruned)
			require.Len(t, walked, len(kept))
			for i := range kept {
				require.Same(t, kept[i], walked[i], "order diverges at %d", i)
			}
		})
	}
}

func TestTreeRemoveReportsWhetherItFound(t *testing.T) {
	sorted := sortedTreeDatoms(EAVT, 32)
	tree := newDatomTree(EAVT)
	tree.buildFromSorted(sorted)

	b := tree.transient()
	require.True(t, b.remove(sorted[7]), "removing a present datom reported it absent")
	require.False(t, b.remove(sorted[7]), "removing it twice reported it present")

	absent := *sorted[7]
	absent.E = datalog.NewIdentity("never-inserted")
	require.False(t, b.remove(&absent), "removing an absent datom reported it present")

	pruned := b.commit()
	require.Equal(t, len(sorted)-1, pruned.Len())
}

// TestTreeRemoveEverythingEmptiesTheTree: dropping empty nodes has to reach the
// root, or the tree keeps a spine of empty levels and Len disagrees with a walk.
func TestTreeRemoveEverythingEmptiesTheTree(t *testing.T) {
	sorted := sortedTreeDatoms(EAVT, branchingFactor*2+5)
	tree := newDatomTree(EAVT)
	tree.buildFromSorted(sorted)

	empty := removeAll(tree, sorted)
	require.Zero(t, empty.Len())
	require.Nil(t, empty.root, "an emptied tree kept a root")
	require.Empty(t, walkTree(t, empty))

	c := empty.cursor()
	require.False(t, c.seekFirst())
}

// TestTreeRemoveCollapsesASingleChildRoot: without this, deleting down to one
// subtree leaves the levels above it in place and every descent pays for them.
func TestTreeRemoveCollapsesASingleChildRoot(t *testing.T) {
	sorted := sortedTreeDatoms(EAVT, branchingFactor*3)
	tree := newDatomTree(EAVT)
	tree.buildFromSorted(sorted)
	require.False(t, tree.root.isLeaf(), "fixture must start branching")

	// Keep only the first leaf's worth, so exactly one subtree survives.
	pruned := removeAll(tree, sorted[branchingFactor:])
	require.Equal(t, branchingFactor, pruned.Len())
	require.True(t, pruned.root.isLeaf(), "root did not collapse to its only child")
	require.Len(t, walkTree(t, pruned), branchingFactor)
}

// TestTreeRemoveLeavesTheSourceTreeIntact: removal path-copies like insertion,
// so a version handed out earlier keeps everything it had.
func TestTreeRemoveLeavesTheSourceTreeIntact(t *testing.T) {
	sorted := sortedTreeDatoms(EAVT, branchingFactor*2)
	original := newDatomTree(EAVT)
	original.buildFromSorted(sorted)

	before := walkTree(t, original)
	pruned := removeAll(original, sorted[:branchingFactor])

	require.Equal(t, len(sorted), original.Len(), "source tree count changed")
	after := walkTree(t, original)
	require.Len(t, after, len(before))
	for i := range before {
		require.Same(t, before[i], after[i], "source tree contents changed at %d", i)
	}
	require.Equal(t, branchingFactor, pruned.Len())
}

// TestTreeRemoveThenSeekFindsTheSuccessor: a stale separator would send a seek
// into a subtree that no longer holds the target's range.
func TestTreeRemoveThenSeekFindsTheSuccessor(t *testing.T) {
	sorted := sortedTreeDatoms(EAVT, branchingFactor*2+11)
	tree := newDatomTree(EAVT)
	tree.buildFromSorted(sorted)

	// Drop a run from the middle, then seek each removed datom: every one must
	// land on the first survivor after it.
	start, end := branchingFactor-3, branchingFactor+7
	pruned := removeAll(tree, sorted[start:end])

	c := pruned.cursor()
	for i := start; i < end; i++ {
		require.True(t, c.seek(sorted[i], fullDatomCompare), "seek failed for removed datom %d", i)
		require.Same(t, sorted[end], c.datom(), "seek for removed %d landed wrong", i)
	}

	for i := 0; i < start; i++ {
		require.True(t, c.seek(sorted[i], fullDatomCompare), "survivor %d not found", i)
		require.Same(t, sorted[i], c.datom(), "survivor %d landed wrong", i)
	}
}

func TestTreeRemoveClearsVacatedSlots(t *testing.T) {
	sorted := sortedTreeDatoms(EAVT, branchingFactor*4)
	tree := newDatomTree(EAVT)
	tree.buildFromSorted(sorted)
	pruned := removeAll(tree, sorted[:branchingFactor*2])

	var checked int
	var walk func(n *node)
	walk = func(n *node) {
		full := n.keys[:cap(n.keys)]
		for i := len(n.keys); i < len(full); i++ {
			require.Nil(t, full[i], "datom slot %d past len retained a pointer", i)
			checked++
		}
		if n.children != nil {
			fullChildren := n.children[:cap(n.children)]
			for i := len(n.children); i < len(fullChildren); i++ {
				require.Nil(t, fullChildren[i], "child slot %d past len retained a pointer", i)
				checked++
			}
			for _, child := range n.children {
				walk(child)
			}
		}
	}
	walk(pruned.root)
	require.NotZero(t, checked, "no node had spare capacity; this checked nothing")
}

func TestTreeRemoveAcrossEveryIndexOrder(t *testing.T) {
	for _, index := range Indices {
		t.Run(fmt.Sprintf("%v", index), func(t *testing.T) {
			sorted := sortedTreeDatoms(index, branchingFactor+17)
			tree := newDatomTree(index)
			tree.buildFromSorted(sorted)

			pruned := removeAll(tree, sorted[3:9])
			kept := append(append([]*datalog.Datom{}, sorted[:3]...), sorted[9:]...)

			require.Equal(t, len(kept), pruned.Len())
			walked := walkTree(t, pruned)
			require.Len(t, walked, len(kept))
			for i := range kept {
				require.Same(t, kept[i], walked[i], "order diverges at %d", i)
			}
		})
	}
}
