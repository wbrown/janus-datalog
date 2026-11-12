package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// narrowBranching is a branching factor small enough against the default for
// one test's datoms to reach a third level.
const narrowBranching = 32

// treeStoreMadeWith opens a tree store whose nodes hold at most branching
// entries, zero asking for the default.
func treeStoreMadeWith(branching int) (*MemoryTreeStore, error) {
	return NewMemoryTreeStoreWithOptions(MemoryTreeStoreOptions{Encoder: &BinaryKeyEncoder{}, BranchingFactor: branching})
}

// widestNode is the most entries any node of any of the version's trees holds.
func widestNode(v *storeVersion) int {
	widest := 0
	var walk func(n *node)
	walk = func(n *node) {
		if n == nil {
			return
		}
		if len(n.keys) > widest {
			widest = len(n.keys)
		}
		for _, child := range n.children {
			walk(child)
		}
	}
	for _, index := range Indices {
		walk(v.tree(index).root)
	}
	return widest
}

// A tree store made with a branching factor holds no node of more entries than
// that, whichever way its datoms arrive: one batch over an empty store, which
// builds each tree from sorted datoms; one commit a datom, which inserts and
// splits; and the writes of a fork, which begins from its base's version. It
// holds the datoms a store of the default factor holds, in every index order.
func TestTreeStoreMadeWithABranchingFactorHoldsNoWiderNode(t *testing.T) {
	datoms := treeBatchDatoms(narrowBranching*narrowBranching + 9)
	half := len(datoms) / 2

	t.Run("one batch over an empty store", func(t *testing.T) {
		store, err := treeStoreMadeWith(narrowBranching)
		require.NoError(t, err)
		defer store.Close()
		require.NoError(t, store.Assert(datoms))
		require.LessOrEqual(t, widestNode(store.versions.read()), narrowBranching)
		requireSameIndexes(t, treeStoreHolding(t, datoms), store, "one batch")
	})

	t.Run("one commit a datom", func(t *testing.T) {
		store, err := treeStoreMadeWith(narrowBranching)
		require.NoError(t, err)
		defer store.Close()
		for i := range datoms {
			require.NoError(t, store.Assert(datoms[i:i+1]))
		}
		require.LessOrEqual(t, widestNode(store.versions.read()), narrowBranching)
		requireSameIndexes(t, treeStoreHolding(t, datoms), store, "one commit a datom")
	})

	t.Run("the writes of a fork", func(t *testing.T) {
		base, err := treeStoreMadeWith(narrowBranching)
		require.NoError(t, err)
		defer base.Close()
		require.NoError(t, base.Assert(datoms[:half]))
		ceiling, err := base.MaxElementID()
		require.NoError(t, err)
		forked, err := base.Fork(ceiling)
		require.NoError(t, err)
		defer forked.Close()
		for i := half; i < len(datoms); i++ {
			require.NoError(t, forked.Assert(datoms[i:i+1]))
		}
		fork, isTreeStore := forked.(*MemoryTreeStore)
		require.True(t, isTreeStore)
		require.LessOrEqual(t, widestNode(fork.versions.read()), narrowBranching)
		requireSameIndexes(t, treeStoreHolding(t, datoms), fork, "fork")
		requireSameIndexes(t, treeStoreHolding(t, datoms[:half]), base, "base")
	})
}

// A database opened on a tree store made with a branching factor commits into
// trees of that factor.
func TestDatabaseOnATreeStoreMadeWithABranchingFactorCommitsIntoItsTrees(t *testing.T) {
	store, err := treeStoreMadeWith(narrowBranching)
	require.NoError(t, err)
	defer store.Close()
	database, err := NewDatabaseWithOptions(DatabaseOptions{Store: store})
	require.NoError(t, err)
	defer database.Close()

	attr := datalog.NewKeyword(":branching/value")
	for i := 0; i < narrowBranching*4; i++ {
		tx := database.NewTransaction()
		require.NoError(t, tx.Set(datalog.NewIdentity(fmt.Sprintf("branching:entity:%d", i)), attr, int64(i)))
		_, err := tx.Commit()
		require.NoError(t, err)
	}
	version := store.versions.read()
	require.Greater(t, version.datomCount(), narrowBranching)
	require.LessOrEqual(t, widestNode(version), narrowBranching)
}

// Zero asks for the default factor, and a factor below the smallest a store
// accepts is refused.
func TestTreeStoreBranchingFactorDefaultAndRefusal(t *testing.T) {
	store, err := treeStoreMadeWith(0)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.Assert(treeBatchDatoms(branchingFactor*2+9)))
	require.Equal(t, branchingFactor, widestNode(store.versions.read()),
		"a batch over an empty store packs its leaves to the default factor")

	for _, refused := range []int{-1, 1, MinBranchingFactor - 1} {
		_, err := treeStoreMadeWith(refused)
		require.Error(t, err, "branching factor %d", refused)
	}
}

// deepestTree is the number of levels of the version's deepest tree.
func deepestTree(v *storeVersion) int {
	deepest := 0
	for _, index := range Indices {
		if root := v.tree(index).root; root != nil && int(root.level)+1 > deepest {
			deepest = int(root.level) + 1
		}
	}
	return deepest
}

// A store of the smallest factor holds its datoms in trees deeper than the
// cursor's inline path, and every index scans whole. One commit a datom in
// ascending Tx fills the Tx-major index at one end, so each split's other half
// stays at half the factor and the tree is as deep as that factor makes one.
func TestTreeStoreOfTheSmallestFactorScansPastTheCursorsInlinePath(t *testing.T) {
	store, err := treeStoreMadeWith(MinBranchingFactor)
	require.NoError(t, err)
	defer store.Close()

	datoms := treeBatchDatoms(2048)
	for i := range datoms {
		require.NoError(t, store.Assert(datoms[i:i+1]))
	}
	version := store.versions.read()
	require.LessOrEqual(t, widestNode(version), MinBranchingFactor)
	require.Greater(t, deepestTree(version), maxTreeDepth)
	requireSameIndexes(t, treeStoreHolding(t, datoms), store, "the smallest factor")
}
