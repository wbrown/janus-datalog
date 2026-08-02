package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func treeBatchDatoms(count int) []datalog.Datom {
	attr := datalog.NewKeyword(":batch/value")
	datoms := make([]datalog.Datom, count)
	for i := range datoms {
		datoms[i] = datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("batch:entity:%d", i)),
			A:  attr,
			V:  int64(i),
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 7},
		}
	}
	return datoms
}

func assertEachOf(t *testing.T, store Store, datoms []datalog.Datom) {
	t.Helper()
	require.NoError(t, store.AssertEach(func(add func(*datalog.Datom) error) error {
		for i := range datoms {
			if err := add(&datoms[i]); err != nil {
				return err
			}
		}
		return nil
	}))
}

// TestTreeStoreHoldsOneBatchUntilFinish pins the reason FinishBatch exists.
//
// Successive AssertEach calls share one builder and publish nothing, so a run of
// them costs one version rather than one per call — and a version costs a
// copy-on-write clone of every node it touches, which is what made a thousand-
// chunk dump expensive. Nothing is visible until the run is finished, which is
// what the importer's Finalize does.
func TestTreeStoreHoldsOneBatchUntilFinish(t *testing.T) {
	store := NewMemoryTreeStore(&BinaryKeyEncoder{})
	defer store.Close()

	datoms := treeBatchDatoms(6)
	assertEachOf(t, store, datoms[:2])
	assertEachOf(t, store, datoms[2:4])
	assertEachOf(t, store, datoms[4:])

	require.Zero(t, countStoreIndex(t, store, EAVT),
		"an unfinished batch published")

	require.NoError(t, store.FinishBatch())
	for _, index := range Indices {
		require.Equal(t, len(datoms), countStoreIndex(t, store, index), "index %v", index)
	}

	// Finishing again has nothing to finish, and must not re-publish or wedge the
	// write lock.
	require.NoError(t, store.FinishBatch())
	require.Equal(t, len(datoms), countStoreIndex(t, store, EAVT))
}

// TestTreeStoreAssertPublishesOnReturn pins that holding a batch open is what
// AssertEach does, not what every write does: Assert is a complete operation and
// its datoms are readable when it returns.
func TestTreeStoreAssertPublishesOnReturn(t *testing.T) {
	store := NewMemoryTreeStore(&BinaryKeyEncoder{})
	defer store.Close()

	datoms := treeBatchDatoms(4)
	require.NoError(t, store.Assert(datoms))
	require.Equal(t, len(datoms), countStoreIndex(t, store, EAVT))

	// A second write still works, which it would not if the first left the write
	// lock held.
	more := treeBatchDatoms(6)[4:]
	require.NoError(t, store.Assert(more))
	require.Equal(t, len(datoms)+len(more), countStoreIndex(t, store, EAVT))
}

// TestTreeStoreFailedBatchReleasesTheWriteLock pins that a producer error ends
// the run rather than leaving it open: the store stays writable and the failed
// batch's datoms are gone.
func TestTreeStoreFailedBatchReleasesTheWriteLock(t *testing.T) {
	store := NewMemoryTreeStore(&BinaryKeyEncoder{})
	defer store.Close()

	datoms := treeBatchDatoms(4)
	producerFailed := fmt.Errorf("producer failed")
	err := store.AssertEach(func(add func(*datalog.Datom) error) error {
		if err := add(&datoms[0]); err != nil {
			return err
		}
		return producerFailed
	})
	require.ErrorIs(t, err, producerFailed)
	require.Zero(t, countStoreIndex(t, store, EAVT))

	require.NoError(t, store.Assert(datoms))
	require.Equal(t, len(datoms), countStoreIndex(t, store, EAVT))
}
