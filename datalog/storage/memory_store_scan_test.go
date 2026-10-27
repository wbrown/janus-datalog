package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// storeOfDatoms is a memory store holding n datoms, one entity and one
// Lamport each, under one attribute.
func storeOfDatoms(t *testing.T, n int) *MemoryStore {
	t.Helper()
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })
	attr := datalog.NewKeyword(":memory/run")
	datoms := make([]datalog.Datom, 0, n)
	for i := 0; i < n; i++ {
		datoms = append(datoms, datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("memory:run:%d", i)),
			A:  attr,
			V:  int64(i),
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
		})
	}
	require.NoError(t, store.Assert(datoms))
	return store
}

// A scan on the store pays for what its consumer reads. MaxElementID reads
// one key, so it allocates no more over a store holding tens of chunks of
// keys than over one holding a few. A scan answers the store as of its
// open, so a write after the open is not among its keys. A walk across
// chunk boundaries yields every key of the range, in order.
func TestMemoryStoreScanPaysForWhatItReads(t *testing.T) {
	t.Run("one key read costs one chunk whatever the store holds", func(t *testing.T) {
		few := storeOfDatoms(t, 8*memoryScanChunkKeys)
		many := storeOfDatoms(t, 80*memoryScanChunkKeys)
		allocsOver := func(store *MemoryStore) float64 {
			return testing.AllocsPerRun(20, func() {
				if _, err := store.MaxElementID(); err != nil {
					t.Fatal(err)
				}
			})
		}
		require.Equal(t, allocsOver(few), allocsOver(many), "the allocations do not grow with the store")
	})

	t.Run("a scan answers the store as of its open", func(t *testing.T) {
		store := storeOfDatoms(t, 3)
		iter, err := store.Scan(ScanBound{Index: TAEV})
		require.NoError(t, err)
		defer iter.Close()
		require.NoError(t, store.Assert([]datalog.Datom{{
			E:  datalog.NewIdentity("memory:run:late"),
			A:  datalog.NewKeyword(":memory/run"),
			V:  int64(99),
			Tx: datalog.ElementID{Lamport: 99, ReplicaID: 1},
		}}))
		var lamports []uint64
		for iter.Next() {
			lamports = append(lamports, iter.ElementID().Lamport)
		}
		require.NoError(t, iter.Error())
		require.Equal(t, []uint64{3, 2, 1}, lamports, "the keys are the store's at the open")
	})

	t.Run("a walk across chunk boundaries yields every key in order", func(t *testing.T) {
		n := 3*memoryScanChunkKeys + 7
		store := storeOfDatoms(t, n)
		iter, err := store.Scan(ScanBound{Index: EAVT})
		require.NoError(t, err)
		defer iter.Close()
		var keys [][]byte
		for iter.Next() {
			keys = append(keys, append([]byte(nil), iter.(*memoryIterator).Key()...))
		}
		require.NoError(t, iter.Error())
		require.Len(t, keys, n)
		for i := 1; i < len(keys); i++ {
			require.Equal(t, -1, bytes.Compare(keys[i-1], keys[i]), "the keys are strictly increasing")
		}
	})
}
