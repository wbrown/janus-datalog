package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// insertAll applies datoms to a tree through one transient batch, in the order
// given, and returns the committed result.
func insertAll(tree *datomTree, datoms []*datalog.Datom) *datomTree {
	b := tree.transient()
	for _, d := range datoms {
		b.insert(d)
	}
	return b.commit()
}

// strided returns the datoms reordered so a batch never sees them in order:
// every second one ascending, then the rest descending.
func strided(datoms []*datalog.Datom) []*datalog.Datom {
	out := make([]*datalog.Datom, 0, len(datoms))
	for i := 0; i < len(datoms); i += 2 {
		out = append(out, datoms[i])
	}
	start := len(datoms) - 1
	if start%2 == 0 {
		start-- // step back to the largest odd index
	}
	for i := start; i >= 1; i -= 2 {
		out = append(out, datoms[i])
	}
	return out
}

// TestTreeInsertProducesSortedOrder: inserting out of order must still yield
// the index's order on a walk, at sizes spanning a single leaf, one split, and
// several branch levels.
func TestTreeInsertProducesSortedOrder(t *testing.T) {
	for _, size := range []int{1, 2, branchingFactor - 1, branchingFactor + 1, branchingFactor * 3} {
		t.Run(fmt.Sprintf("n=%d", size), func(t *testing.T) {
			sorted := sortedTreeDatoms(EAVT, size)
			shuffled := strided(sorted)
			require.Len(t, shuffled, size, "strided lost or duplicated entries")

			tree := insertAll(newDatomTree(EAVT), shuffled)
			require.Equal(t, size, tree.Len())

			walked := walkTree(t, tree)
			require.Len(t, walked, size)
			for i := range sorted {
				require.Same(t, sorted[i], walked[i], "order diverges at %d", i)
			}
		})
	}
}

// TestTreeInsertAscendingExercisesRightmostGrowth: inserting in ascending order
// takes the branch path where the target is past every separator, so the last
// child and each ancestor's separator for it have to grow.
func TestTreeInsertAscendingExercisesRightmostGrowth(t *testing.T) {
	sorted := sortedTreeDatoms(EAVT, branchingFactor*3)
	tree := insertAll(newDatomTree(EAVT), sorted)

	require.Equal(t, len(sorted), tree.Len())
	walked := walkTree(t, tree)
	require.Len(t, walked, len(sorted))
	for i := range sorted {
		require.Same(t, sorted[i], walked[i], "order diverges at %d", i)
	}
}

func TestTreeInsertIsASet(t *testing.T) {
	sorted := sortedTreeDatoms(EAVT, 64)
	tree := insertAll(newDatomTree(EAVT), sorted)

	b := tree.transient()
	for _, d := range sorted {
		require.False(t, b.insert(d), "re-inserting an existing datom reported it as new")
	}
	again := b.commit()
	require.Equal(t, len(sorted), again.Len())
	require.Len(t, walkTree(t, again), len(sorted))
}

// TestTreeInsertLeavesTheSourceTreeIntact is the property the whole persistent
// design rests on: a commit produces a new root, and anyone still holding the
// old one sees exactly what they saw before. Until this passes, a read session
// is not a retained root.
func TestTreeInsertLeavesTheSourceTreeIntact(t *testing.T) {
	base := sortedTreeDatoms(EAVT, branchingFactor*2)
	original := newDatomTree(EAVT)
	require.NoError(t, original.buildFromSorted(base))

	before := walkTree(t, original)
	beforeCount := original.Len()

	// A batch large enough to split leaves and grow the tree under the old root.
	added := sortedTreeDatoms(AEVT, branchingFactor)
	for i, d := range added {
		clone := *d
		clone.E = datalog.NewIdentity(fmt.Sprintf("inserted-entity-%d", i))
		added[i] = &clone
	}
	updated := insertAll(original, added)

	require.Equal(t, beforeCount, original.Len(), "source tree count changed")
	after := walkTree(t, original)
	require.Len(t, after, len(before))
	for i := range before {
		require.Same(t, before[i], after[i], "source tree contents changed at %d", i)
	}

	require.Equal(t, beforeCount+len(added), updated.Len())
	require.Len(t, walkTree(t, updated), beforeCount+len(added))
}

// TestTreeInsertSharesUntouchedSubtrees pins that a commit path-copies rather
// than deep-copies. Without this, persistence would still be correct and the
// memory model would be wrong: every commit would duplicate the store.
func TestTreeInsertSharesUntouchedSubtrees(t *testing.T) {
	base := sortedTreeDatoms(EAVT, branchingFactor*4)
	original := newDatomTree(EAVT)
	require.NoError(t, original.buildFromSorted(base))
	require.False(t, original.root.isLeaf(), "fixture must produce a branching tree")
	require.Greater(t, len(original.root.children), 2, "fixture must produce several subtrees")

	// One datom placed inside the first child's range.
	target := *base[0]
	target.E = datalog.NewIdentity("shared-subtree-probe")
	target.V = base[0].V
	b := original.transient()
	require.True(t, b.insert(&target))
	updated := b.commit()

	require.NotSame(t, original.root, updated.root, "root must be copied, not mutated")

	// Membership, not position: inserting into a full leaf splits it, which
	// shifts every later child one slot right. A surviving subtree keeps its
	// pointer, not its index.
	survivors := make(map[*node]bool, len(updated.root.children))
	for _, child := range updated.root.children {
		survivors[child] = true
	}
	shared := 0
	for _, child := range original.root.children {
		if survivors[child] {
			shared++
		}
	}
	require.NotZero(t, shared, "no subtree survived the commit by pointer; the batch deep-copied")
	require.Less(t, shared, len(original.root.children),
		"every subtree survived; the insert did not land anywhere")
}

// TestOwnerTokensAreDistinct guards the one thing that makes copy-on-write
// work: two builders must never share a token. Go hands back the same address
// for every zero-sized allocation, so dropping the byte from ownerToken makes
// every token equal, every node look self-owned, and every builder mutate
// published nodes in place beneath live readers. It fails as inconsistent reads
// far from the cause, so it is pinned here at the cause.
func TestOwnerTokensAreDistinct(t *testing.T) {
	tree := insertAll(newDatomTree(EAVT), sortedTreeDatoms(EAVT, 4))

	seen := make(map[*ownerToken]bool)
	for i := 0; i < 16; i++ {
		b := tree.transient()
		require.False(t, seen[b.token], "builder %d reused a previous builder's token", i)
		seen[b.token] = true
		b.commit()
	}
}

// TestTreeSplitClearsVacatedSlots pins a retention invariant nothing else can
// see: a split shortens the left node, but the collector scans a backing array
// to its capacity, so a stale pointer above len keeps a dead datom — or an
// entire dead subtree — reachable for the life of the store. On a heap that
// never returns pages that is a leak, and it is invisible through the API.
func TestTreeSplitClearsVacatedSlots(t *testing.T) {
	// Enough to split leaves and produce a branch level, so both the datom and
	// the child-pointer slices get truncated somewhere.
	tree := insertAll(newDatomTree(EAVT), sortedTreeDatoms(EAVT, branchingFactor*4))

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
	walk(tree.root)

	require.NotZero(t, checked, "no node had spare capacity; this checked nothing")
}

// TestTreeBuilderRejectsUseAfterCommit: a committed builder's nodes are
// published, so writing through it would mutate a tree readers can see.
func TestTreeBuilderRejectsUseAfterCommit(t *testing.T) {
	tree := insertAll(newDatomTree(EAVT), sortedTreeDatoms(EAVT, 8))
	b := tree.transient()
	b.commit()

	require.Panics(t, func() { b.commit() })
	require.Panics(t, func() {
		b.insert(&datalog.Datom{
			E:  datalog.NewIdentity("after-commit"),
			A:  datalog.NewKeyword(":tree/value"),
			V:  int64(1),
			Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
		})
	})
}

// TestTreeInsertMatchesBulkBuild: the two construction paths must agree. A
// split that placed entries differently would show up here and nowhere else.
func TestTreeInsertMatchesBulkBuild(t *testing.T) {
	for _, index := range Indices {
		t.Run(fmt.Sprintf("%v", index), func(t *testing.T) {
			sorted := sortedTreeDatoms(index, branchingFactor*2+31)

			built := newDatomTree(index)
			require.NoError(t, built.buildFromSorted(sorted))
			inserted := insertAll(newDatomTree(index), strided(sorted))

			require.Equal(t, built.Len(), inserted.Len())
			fromBuild := walkTree(t, built)
			fromInsert := walkTree(t, inserted)
			require.Len(t, fromInsert, len(fromBuild))
			for i := range fromBuild {
				require.Same(t, fromBuild[i], fromInsert[i], "paths disagree at %d", i)
			}
		})
	}
}
