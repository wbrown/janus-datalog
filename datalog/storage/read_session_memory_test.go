package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A session scan takes keys from the cloned tree as its consumer pulls them,
// so a scan pulled once allocates the same whatever the store holds. The cache
// bound of every query reads one key through maxElementIDByScan.
func TestMemoryReadSession_ScanAllocatesAsPulled(t *testing.T) {
	small := memorySessionFirstKeyAllocs(t, 1024)
	large := memorySessionFirstKeyAllocs(t, 16384)
	require.Equal(t, small, large,
		"a scan pulled once allocates in proportion to the pull, not to the store")
}

// memorySessionFirstKeyAllocs returns the allocations a session's TAEV scan
// makes when it is opened, pulled once and closed, on a memory store holding n
// datoms.
func memorySessionFirstKeyAllocs(t *testing.T, n int) float64 {
	t.Helper()
	store := NewMemoryStore(&BinaryKeyEncoder{})
	defer store.Close()
	attr := datalog.NewKeyword(":pulled/value")
	datoms := make([]datalog.Datom, 0, n)
	for i := 0; i < n; i++ {
		datoms = append(datoms, datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("pulled:%d", i)),
			A:  attr,
			V:  int64(i),
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
		})
	}
	require.NoError(t, store.Assert(datoms))
	session, err := store.NewReadSession()
	require.NoError(t, err)
	defer session.Close()
	return testing.AllocsPerRun(10, func() {
		iter, err := session.ScanKeysOnly(ScanBound{Index: TAEV})
		if err != nil {
			t.Fatal(err)
		}
		if !iter.Next() {
			t.Fatal("the scan must yield the store's latest key")
		}
		if err := iter.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
