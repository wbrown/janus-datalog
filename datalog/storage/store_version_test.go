package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestStoreVersionCoversEveryIndex closes the domain: a version's tree array is
// fixed-size, so an index added to Indices without widening it would leave a
// nil slot that only fails when something happens to select that order.
func TestStoreVersionCoversEveryIndex(t *testing.T) {
	require.Len(t, Indices, indexCount,
		"give storeVersion a slot for the new index; a missing one is a nil tree at scan time")

	v := emptyStoreVersion()
	for _, index := range Indices {
		require.NotNil(t, v.tree(index), "index %v has no tree", index)
		require.Zero(t, v.tree(index).Len())
	}
	require.Zero(t, v.datomCount())
}

// datomsInTree collects a tree's contents as a pointer set, so two orders can
// be compared for membership without regard to sequence.
func datomsInTree(tree *datomTree) map[*datalog.Datom]bool {
	out := make(map[*datalog.Datom]bool, tree.Len())
	c := tree.cursor()
	for ok := c.seekFirst(); ok; ok = c.next() {
		out[c.datom()] = true
	}
	return out
}

// requireIndexesAgree checks that every order holds exactly the same datoms.
func requireIndexesAgree(t *testing.T, v *storeVersion) {
	t.Helper()
	want := datomsInTree(v.tree(Indices[0]))
	for _, index := range Indices[1:] {
		got := datomsInTree(v.tree(index))
		require.Len(t, got, len(want), "index %v holds a different number of datoms", index)
		for d := range want {
			require.True(t, got[d], "index %v is missing a datom %v holds", index, Indices[0])
		}
	}
}

func TestVersionBuilderWritesEveryIndex(t *testing.T) {
	datoms := sortedTreeDatoms(EAVT, branchingFactor*2+9)

	b := emptyStoreVersion().transient()
	for _, d := range datoms {
		require.True(t, b.addDatom(d))
	}
	v := b.commit()

	require.Equal(t, len(datoms), v.datomCount())
	for _, index := range Indices {
		require.Equal(t, len(datoms), v.tree(index).Len(), "index %v has the wrong count", index)
	}
	requireIndexesAgree(t, v)
}

func TestVersionBuilderIsASet(t *testing.T) {
	datoms := sortedTreeDatoms(EAVT, 32)

	first := emptyStoreVersion().transient()
	for _, d := range datoms {
		first.addDatom(d)
	}
	v := first.commit()

	second := v.transient()
	for _, d := range datoms {
		require.False(t, second.addDatom(d), "re-adding an existing datom reported it as new")
	}
	again := second.commit()

	require.Equal(t, len(datoms), again.datomCount())
	requireIndexesAgree(t, again)
}

func TestVersionBuilderRejectsUseAfterCommit(t *testing.T) {
	b := emptyStoreVersion().transient()
	b.commit()
	require.Panics(t, func() { b.commit() })
	require.Panics(t, func() { b.addDatom(sortedTreeDatoms(EAVT, 1)[0]) })
}

// TestRetainedVersionSurvivesCommits: a version handed out earlier keeps its
// exact contents however many batches land afterward. This is what lets a read
// session be a retained pointer instead of a lock.
func TestRetainedVersionSurvivesCommits(t *testing.T) {
	h := newVersionHolder()

	base := sortedTreeDatoms(EAVT, branchingFactor)
	b := h.begin()
	for _, d := range base {
		b.addDatom(d)
	}
	h.publish(b)

	retained := h.read()
	before := datomsInTree(retained.tree(EAVT))
	require.Len(t, before, len(base))

	for round := 0; round < 4; round++ {
		later := h.begin()
		for i := 0; i < 64; i++ {
			later.addDatom(&datalog.Datom{
				E:  datalog.NewIdentity(fmt.Sprintf("later-%d-%d", round, i)),
				A:  datalog.NewKeyword(":tree/value"),
				V:  int64(i),
				Tx: datalog.ElementID{Lamport: uint64(10_000 + round*100 + i), ReplicaID: 1},
			})
		}
		h.publish(later)
	}

	require.Equal(t, len(base), retained.datomCount(), "retained version's count changed")
	after := datomsInTree(retained.tree(EAVT))
	require.Len(t, after, len(before))
	for d := range before {
		require.True(t, after[d], "retained version lost a datom")
	}
	requireIndexesAgree(t, retained)

	require.Equal(t, len(base)+4*64, h.read().datomCount(), "current version did not advance")
}

// TestAbandonedBatchPublishesNothing: rollback is discarding a root, so a
// batch that is abandoned leaves no trace and the write lock is released for
// the next writer.
func TestAbandonedBatchPublishesNothing(t *testing.T) {
	h := newVersionHolder()
	before := h.read()

	b := h.begin()
	for _, d := range sortedTreeDatoms(EAVT, 16) {
		b.addDatom(d)
	}
	h.abandon(b)

	require.Same(t, before, h.read(), "abandoning a batch changed the published version")
	require.Zero(t, h.read().datomCount())

	// The lock is free: this would deadlock if abandon had not released it.
	next := h.begin()
	h.publish(next)
}

// TestPublishBuiltSwapsInTheBuiltVersion: a bulk batch publishes a version the
// builder never touched, so the swap, the builder's death, and the lock release
// each have to happen anyway.
func TestPublishBuiltSwapsInTheBuiltVersion(t *testing.T) {
	h := newVersionHolder()
	before := h.read()

	datoms := sortedTreeDatoms(EAVT, 24)
	b := h.begin()
	built := versionFromDatoms(datoms)
	h.publishBuilt(b, built)

	require.Same(t, built, h.read(), "published version is not the built one")
	require.NotSame(t, before, h.read())
	require.Equal(t, len(datoms), h.read().datomCount())
	requireIndexesAgree(t, h.read())
	require.Panics(t, func() { b.commit() }, "builder survived publishBuilt")

	// The lock is free: this would deadlock if publishBuilt had not released it.
	next := h.begin()
	h.publish(next)
}

// TestConcurrentReadersNeverSeeATornVersion is why the eight roots are one
// value behind one pointer. A reader takes a version and walks all
// eight orders; if publication were eight separate swaps, it could catch some
// updated and others not, and the orders would disagree about what the store
// holds. Index ordering is CRDT resolution, so that state resolves the same
// datoms two ways.
//
// This belongs in the CI race job's allowlist alongside the other concurrency
// contracts; it asserts consistency without -race, and -race additionally
// covers the publication itself.
func TestConcurrentReadersNeverSeeATornVersion(t *testing.T) {
	const (
		readers    = 4
		readRounds = 200
		commits    = 200
		perCommit  = 8
	)

	h := newVersionHolder()
	seed := h.begin()
	for _, d := range sortedTreeDatoms(EAVT, branchingFactor) {
		seed.addDatom(d)
	}
	h.publish(seed)

	var wg sync.WaitGroup
	failures := make(chan string, readers*readRounds)

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readRounds; i++ {
				v := h.read()

				want := datomsInTree(v.tree(Indices[0]))
				if len(want) != v.datomCount() {
					failures <- fmt.Sprintf("walked %d datoms but the version counts %d",
						len(want), v.datomCount())
					return
				}
				for _, index := range Indices[1:] {
					got := datomsInTree(v.tree(index))
					if len(got) != len(want) {
						failures <- fmt.Sprintf("index %v holds %d datoms, %v holds %d",
							index, len(got), Indices[0], len(want))
						return
					}
					for d := range want {
						if !got[d] {
							failures <- fmt.Sprintf("index %v is missing a datom %v holds",
								index, Indices[0])
							return
						}
					}
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for c := 0; c < commits; c++ {
			b := h.begin()
			for i := 0; i < perCommit; i++ {
				b.addDatom(&datalog.Datom{
					E:  datalog.NewIdentity(fmt.Sprintf("concurrent-%d-%d", c, i)),
					A:  datalog.NewKeyword(":tree/value"),
					V:  int64(c*perCommit + i),
					Tx: datalog.ElementID{Lamport: uint64(100_000 + c*perCommit + i), ReplicaID: 1},
				})
			}
			h.publish(b)
		}
	}()

	wg.Wait()
	close(failures)
	for msg := range failures {
		t.Error(msg)
	}

	require.Equal(t, branchingFactor+commits*perCommit, h.read().datomCount())
}
