package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// Memory-specific construction smoke; public semantics live in the backend matrix.
func TestMemoryStoreConstructs(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	require.NotNil(t, store.Encoder())
	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
}

func TestMemoryStoreTxRetractThenAssertPreservesDatom(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	entity := datalog.NewIdentity("memory:retract-assert")
	attr := datalog.NewKeyword(":memory/value")
	tx1 := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	tx2 := datalog.ElementID{Lamport: 2, ReplicaID: 1}
	require.NoError(t, store.Assert([]datalog.Datom{
		{E: entity, A: attr, V: "X", Tx: tx1},
	}))

	stx, err := store.BeginTx()
	require.NoError(t, err)
	// Same call order Database.Commit uses: Retract then Assert.
	require.NoError(t, stx.Retract([]datalog.Datom{
		{E: entity, A: attr, V: "X", Tx: tx1},
	}))
	require.NoError(t, stx.Assert([]datalog.Datom{
		{E: entity, A: attr, V: "X", Tx: tx2},
	}))
	require.NoError(t, stx.Commit())

	require.Equal(t, 1, countStoreIndex(t, store, EAVT))

	// Scanning the (E, A) run asserts more than a point lookup would: not that
	// a tx2 datom exists, but that the one surviving datom is the tx2 one.
	iter, err := store.Scan(ScanBound{Index: EAVT, Prefix: []datalog.Value{entity, attr}})
	require.NoError(t, err)
	defer iter.Close()

	require.True(t, iter.Next(), "the surviving datom must be present")
	got, err := iter.Datom()
	require.NoError(t, err)
	require.Equal(t, tx2, got.Tx)
	require.False(t, iter.Next(), "exactly one datom must survive")
	require.NoError(t, iter.Error())
}

func TestMemoryStoreRetractRemovesMatchingDatom(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	entity := datalog.NewIdentity("memory:retract-only")
	attr := datalog.NewKeyword(":memory/tag")
	require.NoError(t, store.Assert([]datalog.Datom{
		{E: entity, A: attr, V: "keep", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: entity, A: attr, V: "drop", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}))
	require.NoError(t, store.Retract([]datalog.Datom{
		{E: entity, A: attr, V: "drop", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}))
	require.Equal(t, 1, countStoreIndex(t, store, EAVT))
}

func TestMemoryStoreMaintainsSortedKeys(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	attr := datalog.NewKeyword(":memory/keys")
	for i := 0; i < 20; i++ {
		require.NoError(t, store.Assert([]datalog.Datom{{
			E:  datalog.NewIdentity(fmt.Sprintf("memory:keys:%02d", i)),
			A:  attr,
			V:  int64(i),
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
		}}))
	}
	require.NoError(t, store.Retract([]datalog.Datom{{
		E:  datalog.NewIdentity("memory:keys:05"),
		A:  attr,
		V:  int64(5),
		Tx: datalog.ElementID{Lamport: 6, ReplicaID: 1},
	}}))

	store.mu.RLock()
	defer store.mu.RUnlock()
	require.Equal(t, len(store.entries), store.keys.Len())
	var prev string
	seen := false
	store.keys.Ascend(func(key string) bool {
		_, ok := store.entries[key]
		require.True(t, ok, "ordered key missing from entries: %q", key)
		if seen {
			require.Less(t, prev, key, "keys must stay sorted")
		}
		prev = key
		seen = true
		return true
	})
}

func TestMemoryStoreScanOrdersManyKeys(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	attr := datalog.NewKeyword(":memory/order")
	names := []string{"m0", "m1", "m2", "m3", "m4"}
	var datoms []datalog.Datom
	for i, name := range names {
		datoms = append(datoms, datalog.Datom{
			E:  datalog.NewIdentity("memory:order:" + name),
			A:  attr,
			V:  name,
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
		})
	}
	require.NoError(t, store.Assert(datoms))

	iter, err := store.Scan(ScanBound{Index: EAVT})
	require.NoError(t, err)
	defer iter.Close()
	var keys [][]byte
	for iter.Next() {
		key := iter.(*memoryIterator).Key()
		require.NotNil(t, key)
		keys = append(keys, append([]byte(nil), key...))
	}
	require.NoError(t, iter.Error())
	require.Len(t, keys, len(names))
	for i := 1; i < len(keys); i++ {
		require.Equal(t, -1, bytes.Compare(keys[i-1], keys[i]), "scan keys must be strictly increasing")
	}
}

func TestMemoryStoreBatchedAssertScales(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	attr := datalog.NewKeyword(":memory/bulk")
	const batches = 40
	const batchSize = 100
	for batch := 0; batch < batches; batch++ {
		datoms := make([]datalog.Datom, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			n := batch*batchSize + i
			datoms = append(datoms, datalog.Datom{
				E:  datalog.NewIdentity(fmt.Sprintf("memory:bulk:%d", n)),
				A:  attr,
				V:  int64(n),
				Tx: datalog.ElementID{Lamport: uint64(n + 1), ReplicaID: 1},
			})
		}
		require.NoError(t, store.Assert(datoms))
	}
	require.Equal(t, batches*batchSize, countStoreIndex(t, store, EAVT))
}

func TestInjectedStoreNotClosedOnConstructorError(t *testing.T) {
	inner := NewMemoryStore(nil)
	t.Cleanup(func() { _ = inner.Close() })
	_, err := NewDatabaseWithOptions(DatabaseOptions{
		Store: failMetadataStore{Store: inner},
	})
	require.Error(t, err)
	require.NoError(t, inner.Assert([]datalog.Datom{{
		E:  datalog.NewIdentity("memory:still-open"),
		A:  datalog.NewKeyword(":memory/ok"),
		V:  "yes",
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}}))
}

// CompressionThreshold configures an encoder this constructor builds for a
// backend it opens. An injected store arrived with its encoder already set, and
// the constructor takes that encoder as-is, so the option had nowhere to land:
// it was accepted, discarded, and reported nothing. It is now a rejected
// contradiction, and the store's own encoder is still never mutated.
func TestInjectedStoreRejectsCompressionThreshold(t *testing.T) {
	encoder := &BinaryKeyEncoder{CompressionThreshold: 0}
	store := NewMemoryStore(encoder)
	t.Cleanup(func() { _ = store.Close() })
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Store:                store,
		CompressionThreshold: 512,
	})
	require.Error(t, err)
	require.Nil(t, db)
	require.Contains(t, err.Error(), "CompressionThreshold")
	require.Equal(t, 0, store.Encoder().CompressionThreshold)
}

type failMetadataStore struct {
	Store
}

func (s failMetadataStore) GetMetadataUint64(key string) (uint64, bool, error) {
	return 0, false, fmt.Errorf("injected metadata failure")
}

func TestResolveAllAttributesManySkipsSuccessorDecodeErrors(t *testing.T) {
	store := NewMemoryStore(nil)
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Store:        store,
		DisableCache: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	left := datalog.NewIdentity("pull:left")
	right := datalog.NewIdentity("pull:right")
	first, second := left, right
	if bytes.Compare(left.Bytes(), right.Bytes()) > 0 {
		first, second = right, left
	}
	attr := datalog.NewKeyword(":pull/value")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(first, attr, "ok"))
	require.NoError(t, tx.Add(second, attr, "successor"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Commit also writes :db/txInstant. In EATV order that tx-metadata entity
	// often sits between the two user entities, so corrupting `second` never
	// reaches the shared scan for `first`. Corrupt the true immediate EATV
	// successor of `first` instead — that is the key the pre-decode bound skips.
	successorKey := immediateEATVSuccessorKey(t, store, first)
	corruptEATVKey(t, store, successorKey)
	requireImmediateEATVSuccessorTruncated(t, store, first)

	results, err := db.ResolveAllAttributesMany([]datalog.Identity{first})
	require.NoError(t, err)
	require.Equal(t, "ok", results[0][attr])
}

// immediateEATVSuccessorKey returns the first EATV key after entity's range.
func immediateEATVSuccessorKey(t *testing.T, store *MemoryStore, entity datalog.Identity) string {
	t.Helper()
	store.mu.RLock()
	defer store.mu.RUnlock()
	entityBytes := entity.Bytes()
	prefix := string(append([]byte{byte(EATV)}, entityBytes[:]...))
	var sawOwn bool
	var successor string
	store.keys.AscendGreaterOrEqual(prefix, func(encoded string) bool {
		key := []byte(encoded)
		if len(key) < 21 || key[0] != byte(EATV) {
			return false
		}
		if bytes.Equal(key[1:21], entityBytes[:]) {
			sawOwn = true
			return true
		}
		successor = encoded
		return false
	})
	require.True(t, sawOwn, "entity must have at least one EATV key")
	require.NotEmpty(t, successor, "expected an EATV key after entity (tx metadata or next user)")
	return successor
}

// corruptEATVKey truncates one EATV key in lockstep with the ordered index so
// Datom() decode fails if the pull_batch pre-decode entity bound is skipped.
func corruptEATVKey(t *testing.T, store *MemoryStore, encoded string) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	value, ok := store.entries[encoded]
	require.True(t, ok, "successor key missing from entries")
	require.Greater(t, len(encoded), 22, "successor key must be long enough to truncate")
	delete(store.entries, encoded)
	store.keys.Delete(encoded)
	truncatedKey := string(append([]byte(nil), []byte(encoded)[:22]...))
	store.entries[truncatedKey] = value
	store.keys.ReplaceOrInsert(truncatedKey)
}

// requireImmediateEATVSuccessorTruncated proves the next EATV key after entity
// is the truncated successor — otherwise ResolveAllAttributesMany never hits it.
func requireImmediateEATVSuccessorTruncated(t *testing.T, store *MemoryStore, entity datalog.Identity) {
	t.Helper()
	store.mu.RLock()
	defer store.mu.RUnlock()
	entityBytes := entity.Bytes()
	prefix := string(append([]byte{byte(EATV)}, entityBytes[:]...))
	var next string
	store.keys.AscendGreaterOrEqual(prefix, func(encoded string) bool {
		key := []byte(encoded)
		if len(key) < 21 || key[0] != byte(EATV) {
			return false
		}
		if bytes.Equal(key[1:21], entityBytes[:]) {
			return true
		}
		next = encoded
		return false
	})
	require.NotEmpty(t, next, "missing EATV successor after entity")
	require.Equal(t, 22, len(next), "immediate EATV successor must be the truncated key")
	require.False(t, bytes.Equal([]byte(next)[1:21], entityBytes[:]), "successor must be a different entity")
}
