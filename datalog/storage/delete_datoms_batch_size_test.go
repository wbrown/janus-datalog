//go:build !(js && wasm)

package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestBadgerDeleteDatomsAboveTransactionLimit pins that a delete batch is bounded
// by the caller's data, not by Badger's per-transaction ceiling.
//
// See BUG_TRUNCATE_DELETES_IN_ONE_TRANSACTION. BadgerStore.DeleteDatoms wraps
// every delete in one db.Update closure, and each datom deletes a key from all
// eight indices, so the pending-write set is eight times the datom count. Badger
// rejects the transaction at maxBatchCount / maxBatchSize, which puts an
// undeclared ceiling of roughly 26,000 datoms on Database.TruncateTo — the sole
// production caller, which hands over the whole post-snapshot tail in one call.
//
// The ceiling is derived from the store rather than computed from MemTableSize
// here: the constants are Badger's and move with its options, and a test that
// restates them stops testing the real boundary the moment either side changes.
//
// The asymmetry with the write side is the point. Assert of the same datoms
// succeeds, because AssertEach writes through a WriteBatch that splits at the
// same ceiling this delete runs into.
func TestBadgerDeleteDatomsAboveTransactionLimit(t *testing.T) {
	store, err := NewBadgerStore(t.TempDir(), &BinaryKeyEncoder{})
	require.NoError(t, err)
	defer store.Close()

	perDatom := int64(len(Indices))
	overshoot := int64(1024)
	datomCount := int(store.db.MaxBatchCount()/perDatom + overshoot)

	attr := datalog.NewKeyword(":batch/value")
	datoms := make([]datalog.Datom, datomCount)
	for i := range datoms {
		datoms[i] = datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("batch:entity-%d", i)),
			A:  attr,
			V:  int64(i),
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
		}
	}

	require.NoError(t, store.Assert(datoms),
		"the write side splits at Badger's ceiling; only the delete side does not")
	require.Equal(t, datomCount, countStoreIndex(t, store, EAVT))

	deleted, err := store.DeleteDatoms(datoms)
	require.NoError(t, err, "a rewind is bounded by its tail, not by Badger's transaction ceiling")
	require.Equal(t, datomCount, deleted)
	for _, index := range Indices {
		require.Zero(t, countStoreIndex(t, store, index), "index %v", index)
	}
}
