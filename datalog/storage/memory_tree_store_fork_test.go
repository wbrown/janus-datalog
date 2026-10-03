package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// forkBaseSize spreads the base's datoms across several leaves of every tree,
// so a fork's writes land inside nodes the base still holds.
const forkBaseSize = branchingFactor*2 + 9

// treeStoreHolding returns a store holding exactly the datoms given: the
// expectation a fork or its base is compared against, index by index.
func treeStoreHolding(t *testing.T, runs ...[]datalog.Datom) *MemoryTreeStore {
	t.Helper()
	store := NewMemoryTreeStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, run := range runs {
		require.NoError(t, store.Assert(run))
	}
	return store
}

// requireSameIndexes checks that got holds what want holds, in every index
// order.
func requireSameIndexes(t *testing.T, want, got Store, which string) {
	t.Helper()
	for _, index := range Indices {
		require.Equal(t, scanIndexDatoms(t, want, index), scanIndexDatoms(t, got, index),
			"%s: index %v", which, index)
	}
}

// TestTreeStoreForkHoldsThePublishedVersion: a fork at its base's newest
// ElementID starts from what its base has published, in every index order.
func TestTreeStoreForkHoldsThePublishedVersion(t *testing.T) {
	datoms := treeBatchDatoms(forkBaseSize)

	base := NewMemoryTreeStore(&BinaryKeyEncoder{})
	defer base.Close()
	require.NoError(t, base.Assert(datoms))
	ceiling, err := base.MaxElementID()
	require.NoError(t, err)

	fork, err := base.Fork(ceiling)
	require.NoError(t, err)
	defer fork.Close()

	requireSameIndexes(t, treeStoreHolding(t, datoms), fork, "fork")
}

// TestTreeStoreForkHoldsItsBaseAsOfItsCeiling: a fork holds what its base held
// as of the ceiling it forked at, though the base holds more, and everything it
// writes itself, read through the store and through a read session. Its newest
// ElementID is the newest of the datoms it holds.
func TestTreeStoreForkHoldsItsBaseAsOfItsCeiling(t *testing.T) {
	datoms := treeBatchDatoms(forkBaseSize + 32)
	before := datoms[:forkBaseSize]
	past := datoms[forkBaseSize : forkBaseSize+16]
	written := datoms[forkBaseSize+16:]

	base := NewMemoryTreeStore(&BinaryKeyEncoder{})
	defer base.Close()
	require.NoError(t, base.Assert(before))
	ceiling, err := base.MaxElementID()
	require.NoError(t, err)
	require.NoError(t, base.Assert(past))

	fork, err := base.Fork(ceiling)
	require.NoError(t, err)
	defer fork.Close()
	requireSameIndexes(t, treeStoreHolding(t, before), fork, "fork before it writes")

	newest, err := fork.MaxElementID()
	require.NoError(t, err)
	require.Equal(t, before[len(before)-1].Tx, newest)

	require.NoError(t, fork.Assert(written))
	want := treeStoreHolding(t, before, written)
	requireSameIndexes(t, want, fork, "fork")
	requireSameIndexes(t, treeStoreHolding(t, before, past), base, "base")

	after, err := fork.DatomsAfter(ceiling)
	require.NoError(t, err)
	require.Len(t, after, len(written))

	session, err := fork.NewReadSession()
	require.NoError(t, err)
	defer session.Close()
	for _, index := range Indices {
		iter, err := session.Scan(ScanBound{Index: index})
		require.NoError(t, err)
		var got []datalog.Datom
		for iter.Next() {
			d, err := iter.Datom()
			require.NoError(t, err)
			got = append(got, *d)
		}
		require.NoError(t, iter.Error())
		require.NoError(t, iter.Close())
		require.Equal(t, scanIndexDatoms(t, want, index), got, "read session: index %v", index)
	}
}

// TestTreeStoreForkOfAFork: a fork of a fork reads what its parent read as of
// its own ceiling — neither what the grandparent held past the first ceiling
// nor what the parent wrote past the second — and what it writes itself.
func TestTreeStoreForkOfAFork(t *testing.T) {
	datoms := treeBatchDatoms(forkBaseSize + 64)
	inherited := datoms[:forkBaseSize]
	pastFirst := datoms[forkBaseSize : forkBaseSize+16]
	parentWrote := datoms[forkBaseSize+16 : forkBaseSize+32]
	pastSecond := datoms[forkBaseSize+32 : forkBaseSize+48]
	childWrote := datoms[forkBaseSize+48:]

	base := NewMemoryTreeStore(&BinaryKeyEncoder{})
	defer base.Close()
	require.NoError(t, base.Assert(inherited))
	first, err := base.MaxElementID()
	require.NoError(t, err)
	require.NoError(t, base.Assert(pastFirst))

	parent, err := base.Fork(first)
	require.NoError(t, err)
	defer parent.Close()
	require.NoError(t, parent.Assert(parentWrote))
	second, err := parent.MaxElementID()
	require.NoError(t, err)
	require.NoError(t, parent.Assert(pastSecond))

	child, err := parent.(*MemoryTreeStore).Fork(second)
	require.NoError(t, err)
	defer child.Close()
	require.NoError(t, child.Assert(childWrote))

	requireSameIndexes(t, treeStoreHolding(t, inherited, parentWrote, pastSecond), parent, "parent")
	requireSameIndexes(t, treeStoreHolding(t, inherited, parentWrote, childWrote), child, "child")
}

// TestTreeStoreForkExcludesAnOpenBatch: a batch AssertEach has left open is not
// published, so a fork taken while it is open starts without it, and finishing
// the batch publishes it to the base alone.
func TestTreeStoreForkExcludesAnOpenBatch(t *testing.T) {
	datoms := treeBatchDatoms(forkBaseSize + 16)
	published, open := datoms[:forkBaseSize], datoms[forkBaseSize:]

	base := NewMemoryTreeStore(&BinaryKeyEncoder{})
	defer base.Close()
	require.NoError(t, base.Assert(published))
	assertEachOf(t, base, open)
	ceiling, err := base.MaxElementID()
	require.NoError(t, err)

	fork, err := base.Fork(ceiling)
	require.NoError(t, err)
	defer fork.Close()
	require.NoError(t, base.FinishBatch())

	requireSameIndexes(t, treeStoreHolding(t, published, open), base, "base")
	requireSameIndexes(t, treeStoreHolding(t, published), fork, "fork")
}

// TestTreeStoreForkWritesStayInTheFork: what a fork asserts and deletes after
// forking changes the fork and leaves the base as it was. The deleted datoms
// are ones the base holds too, so the fork removes them from nodes it shares.
func TestTreeStoreForkWritesStayInTheFork(t *testing.T) {
	datoms := treeBatchDatoms(forkBaseSize + 64)
	shared, written := datoms[:forkBaseSize], datoms[forkBaseSize:]
	deleted, kept := shared[:8], shared[8:]

	base := NewMemoryTreeStore(&BinaryKeyEncoder{})
	defer base.Close()
	require.NoError(t, base.Assert(shared))
	ceiling, err := base.MaxElementID()
	require.NoError(t, err)

	fork, err := base.Fork(ceiling)
	require.NoError(t, err)
	defer fork.Close()
	require.NoError(t, fork.Assert(written))
	removed, err := fork.DeleteDatoms(deleted)
	require.NoError(t, err)
	require.Equal(t, len(deleted), removed)

	requireSameIndexes(t, treeStoreHolding(t, shared), base, "base")
	requireSameIndexes(t, treeStoreHolding(t, kept, written), fork, "fork")
}

// TestTreeStoreBaseWritesStayInTheBase: the base keeps writing after a fork,
// and the fork stays at the version it was forked from.
func TestTreeStoreBaseWritesStayInTheBase(t *testing.T) {
	datoms := treeBatchDatoms(forkBaseSize + 64)
	shared, written := datoms[:forkBaseSize], datoms[forkBaseSize:]
	deleted, kept := shared[:8], shared[8:]

	base := NewMemoryTreeStore(&BinaryKeyEncoder{})
	defer base.Close()
	require.NoError(t, base.Assert(shared))
	ceiling, err := base.MaxElementID()
	require.NoError(t, err)

	fork, err := base.Fork(ceiling)
	require.NoError(t, err)
	defer fork.Close()
	require.NoError(t, base.Assert(written))
	removed, err := base.DeleteDatoms(deleted)
	require.NoError(t, err)
	require.Equal(t, len(deleted), removed)

	requireSameIndexes(t, treeStoreHolding(t, kept, written), base, "base")
	requireSameIndexes(t, treeStoreHolding(t, shared), fork, "fork")
}

// TestTreeStoreSiblingForksWriteConcurrently: forks of one base commit at the
// same time as each other and as the base, one datom per commit so the commits
// interleave, and each store ends holding the datoms it started from and its
// own writes — nothing another writer committed.
func TestTreeStoreSiblingForksWriteConcurrently(t *testing.T) {
	const (
		siblings  = 8
		perWriter = 128
	)
	datoms := treeBatchDatoms(forkBaseSize + (siblings+1)*perWriter)
	shared := datoms[:forkBaseSize]
	writesOf := func(writer int) []datalog.Datom {
		start := forkBaseSize + writer*perWriter
		return datoms[start : start+perWriter]
	}

	base := NewMemoryTreeStore(&BinaryKeyEncoder{})
	defer base.Close()
	require.NoError(t, base.Assert(shared))
	ceiling, err := base.MaxElementID()
	require.NoError(t, err)

	// The siblings are writers 0 through siblings-1; the base writes last.
	writers := make([]Store, siblings, siblings+1)
	for i := range writers {
		fork, err := base.Fork(ceiling)
		require.NoError(t, err)
		defer fork.Close()
		writers[i] = fork
	}
	writers = append(writers, base)

	var wg sync.WaitGroup
	failures := make(chan error, len(writers))
	for i, store := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, d := range writesOf(i) {
				if err := store.Assert([]datalog.Datom{d}); err != nil {
					failures <- fmt.Errorf("writer %d: %w", i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}

	for i, store := range writers {
		requireSameIndexes(t, treeStoreHolding(t, shared, writesOf(i)), store,
			fmt.Sprintf("writer %d", i))
	}
}

// TestTreeStoreForkCarriesNoWriteIdentity: the replica id is a store's write
// identity, and a branch writes as a replica of its own, so a fork starts with
// none of its base's metadata. A value either store sets afterward stays in that
// store.
func TestTreeStoreForkCarriesNoWriteIdentity(t *testing.T) {
	base := NewMemoryTreeStore(&BinaryKeyEncoder{})
	defer base.Close()
	require.NoError(t, base.SetMetadataUint64("replica_id", 7))

	fork, err := base.Fork(datalog.ElementID{})
	require.NoError(t, err)
	defer fork.Close()

	inBase, found, err := base.GetMetadataUint64("replica_id")
	require.NoError(t, err)
	require.True(t, found, "the base lost its replica id")
	require.Equal(t, uint64(7), inBase)
	_, found, err = fork.GetMetadataUint64("replica_id")
	require.NoError(t, err)
	require.False(t, found, "the fork carries its base's replica id")

	require.NoError(t, fork.SetMetadataUint64("replica_id", 9))
	require.NoError(t, base.SetMetadataUint64("replica_id", 11))

	inBase, _, err = base.GetMetadataUint64("replica_id")
	require.NoError(t, err)
	require.Equal(t, uint64(11), inBase)
	inFork, _, err := fork.GetMetadataUint64("replica_id")
	require.NoError(t, err)
	require.Equal(t, uint64(9), inFork)
}

// TestTreeStoreForkClosesIndependently: closing a fork leaves its base and its
// sibling open, closing the base leaves its forks open, and a closed store has
// nothing to fork.
func TestTreeStoreForkClosesIndependently(t *testing.T) {
	datoms := treeBatchDatoms(forkBaseSize + 2)
	shared := datoms[:forkBaseSize]
	baseWrite, forkWrite := datoms[forkBaseSize:forkBaseSize+1], datoms[forkBaseSize+1:]

	base := NewMemoryTreeStore(&BinaryKeyEncoder{})
	require.NoError(t, base.Assert(shared))
	ceiling, err := base.MaxElementID()
	require.NoError(t, err)
	first, err := base.Fork(ceiling)
	require.NoError(t, err)
	second, err := base.Fork(ceiling)
	require.NoError(t, err)
	defer second.Close()

	require.NoError(t, first.Close())
	require.NoError(t, base.Assert(baseWrite), "closing a fork closed its base")

	require.NoError(t, base.Close())
	_, err = base.Fork(ceiling)
	require.ErrorIs(t, err, errMemoryTreeStoreClosed)

	require.NoError(t, second.Assert(forkWrite), "closing the base closed its fork")
	requireSameIndexes(t, treeStoreHolding(t, shared, forkWrite), second, "second fork")
}
