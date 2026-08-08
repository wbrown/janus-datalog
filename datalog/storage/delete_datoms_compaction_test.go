//go:build !(js && wasm)

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestDeleteDatomsCompactsAndLeavesTheStoreUsable pins what the maintenance
// DeleteDatoms performs must not cost.
//
// Flatten stops live compactions for its duration and restarts them on the way
// out, and a value-log rewrite moves entries between files while the keys that
// name them stay put. Both run over the whole store rather than the deleted
// range, so a datom the call never named must survive them, its out-of-line
// value must still resolve, and writes must still land afterwards.
func TestDeleteDatomsCompactsAndLeavesTheStoreUsable(t *testing.T) {
	store, err := NewBadgerStore(t.TempDir(), &BinaryKeyEncoder{CompressionThreshold: 256})
	require.NoError(t, err)
	defer store.Close()

	attr := datalog.NewKeyword(":compaction/payload")
	survivor := datalog.Datom{
		E: datalog.NewIdentity("compaction:survivor"), A: attr,
		V:  tier3Bytes(t, 200000),
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}
	doomed := datalog.Datom{
		E: datalog.NewIdentity("compaction:doomed"), A: attr,
		V:  tier3Bytes(t, 190000),
		Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1},
	}
	require.NoError(t, store.Assert([]datalog.Datom{survivor, doomed}))
	require.Equal(t, 2, countStoreIndex(t, store, EAVT))

	_, err = store.DeleteDatoms([]datalog.Datom{doomed})
	require.NoError(t, err)
	require.Equal(t, 1, countStoreIndex(t, store, EAVT))

	// The survivor's value lives out of line, so reading it back proves the blob
	// still resolves after the value log was rewritten under it.
	iterator, err := store.Scan(ScanBound{Index: EAVT})
	require.NoError(t, err)
	require.True(t, iterator.Next())
	readBack, err := iterator.Datom()
	require.NoError(t, err)
	require.Equal(t, survivor.V, readBack.V)
	require.NoError(t, iterator.Error())
	require.NoError(t, iterator.Close())

	// Live compactions restart when Flatten returns; a store that stayed stopped
	// still accepts writes, so assert the write lands rather than that it returns.
	later := datalog.Datom{
		E: datalog.NewIdentity("compaction:later"), A: attr, V: int64(7),
		Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1},
	}
	require.NoError(t, store.Assert([]datalog.Datom{later}))
	require.Equal(t, 2, countStoreIndex(t, store, EAVT))
}
