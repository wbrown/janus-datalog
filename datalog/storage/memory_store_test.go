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
	got, err := store.Get(EAVT, store.Encoder().EncodeKey(EAVT, &datalog.Datom{
		E: entity, A: attr, V: "X", Tx: tx2,
	}))
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, tx2, got.Tx)
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

func TestMemoryStoreMaxElementIDForAttribute(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	attr := datalog.NewKeyword(":memory/hwm")
	low := datalog.ElementID{Lamport: 10, ReplicaID: 1}
	high := datalog.ElementID{Lamport: 20, ReplicaID: 1}
	require.NoError(t, store.Assert([]datalog.Datom{
		{E: datalog.NewIdentity("memory:a"), A: attr, V: "a", Tx: low},
		{E: datalog.NewIdentity("memory:b"), A: attr, V: "b", Tx: high},
	}))

	var attrBytes [32]byte
	copy(attrBytes[:], attr.String())
	got, err := store.MaxElementIDForAttribute(attrBytes[:])
	require.NoError(t, err)
	require.Equal(t, high, got)
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

	iter, err := store.Scan(EAVT, []byte{byte(EAVT)}, []byte{byte(EAVT) + 1})
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

func TestInjectedStoreEncoderThresholdNotMutated(t *testing.T) {
	encoder := &BinaryKeyEncoder{CompressionThreshold: 0}
	store := NewMemoryStore(encoder)
	t.Cleanup(func() { _ = store.Close() })
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Store:                store,
		CompressionThreshold: 512,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
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
	corruptEntityEATVKeys(t, store, second)

	results, err := db.ResolveAllAttributesMany([]datalog.Identity{first})
	require.NoError(t, err)
	require.Equal(t, "ok", results[0][attr])
}

func corruptEntityEATVKeys(t *testing.T, store *MemoryStore, entity datalog.Identity) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	entityBytes := entity.Bytes()
	var keys []string
	for encoded := range store.entries {
		key := []byte(encoded)
		if len(key) > 21 && key[0] == byte(EATV) && bytes.Equal(key[1:21], entityBytes[:]) {
			keys = append(keys, encoded)
		}
	}
	require.NotEmpty(t, keys)
	for _, encoded := range keys {
		value := store.entries[encoded]
		delete(store.entries, encoded)
		// Keep the entity prefix so the shared scan still observes this successor
		// key, but truncate so Datom() decode fails if the boundary check is skipped.
		truncated := append([]byte(nil), []byte(encoded)[:22]...)
		store.entries[string(truncated)] = value
	}
}
