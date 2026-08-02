package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// The undo journal has exactly one consumer: Commit restores when a retract
// fails partway through, so a transaction that mixes asserts with a failing
// retract leaves the store as it was. Nothing else reads it — assertMemoryDatom
// cannot fail — which is why an assert-only transaction journals nothing.
//
// No Store method writes a malformed key, so planting one means reaching into
// the backend's own keyspace. The truncated key is a strict prefix of the valid
// one, so it sorts first and the retract fails before deleting anything: the
// journal at that moment holds only the asserts, which is precisely the half
// that assert-only transactions skip.
func TestCommitRestoresAssertsWhenARetractFails(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	doomed := datalog.Datom{
		E:  datalog.NewIdentity("undo:doomed"),
		A:  datalog.NewKeyword(":undo/value"),
		V:  "planted",
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}
	require.NoError(t, store.Assert([]datalog.Datom{doomed}))

	doomedStorage := ToStorageDatom(doomed)
	truncated := string(concatBytes(
		[]byte{byte(EAVT)},
		doomedStorage.E[:],
		doomedStorage.A[:],
		encodeValueForSearch(doomedStorage.V, store.encoder),
	))
	store.mu.Lock()
	store.entries[truncated] = nil
	store.keys.ReplaceOrInsert(truncated)
	store.mu.Unlock()

	added := datalog.Datom{
		E:  datalog.NewIdentity("undo:added"),
		A:  datalog.NewKeyword(":undo/value"),
		V:  "added",
		Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1},
	}

	tx, err := store.BeginTx()
	require.NoError(t, err)
	require.NoError(t, tx.Assert([]datalog.Datom{added}))
	require.NoError(t, tx.Retract([]datalog.Datom{doomed}))
	require.Error(t, tx.Commit(), "the truncated key must fail the retract's decode")

	require.Empty(t, indexKeysFor(store, added), "the failed commit must restore, leaving no trace of its asserts")
	require.Len(t, indexKeysFor(store, doomed), len(Indices), "the datom the retract never reached is untouched")
}

// An assert-only transaction has no error path after it starts writing, so it
// commits whole and needs no journal to fall back on.
func TestAssertOnlyCommitWritesEveryIndex(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	datom := datalog.Datom{
		E:  datalog.NewIdentity("undo:asserted"),
		A:  datalog.NewKeyword(":undo/value"),
		V:  "kept",
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}
	require.NoError(t, store.Assert([]datalog.Datom{datom}))
	require.Len(t, indexKeysFor(store, datom), len(Indices))
}

// indexKeysFor reports which of the eight index keys for datom the store holds.
func indexKeysFor(store *MemoryStore, datom datalog.Datom) []IndexType {
	storageDatom := ToStorageDatom(datom)
	valueBytes, _ := store.encoder.EncodeValueBytes(storageDatom.V)

	store.mu.RLock()
	defer store.mu.RUnlock()
	var present []IndexType
	for _, index := range Indices {
		key := string(store.encoder.encodeKeyWithParts(index, &storageDatom, valueBytes))
		if _, ok := store.entries[key]; ok {
			present = append(present, index)
		}
	}
	return present
}
