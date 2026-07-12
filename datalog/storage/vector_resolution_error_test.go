package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestLoadRGAElementsPropagatesDatomDecodeError(t *testing.T) {
	store, err := NewBadgerStore(t.TempDir(), &BinaryKeyEncoder{})
	require.NoError(t, err)
	defer store.Close()

	entity := datalog.NewIdentity("vector-decode-error")
	attribute := datalog.NewKeyword(":vector/value")
	datom := datalog.Datom{
		E:        entity,
		A:        attribute,
		V:        "first",
		Tx:       datalog.ElementID{Lamport: 1, ReplicaID: 1},
		Op:       datalog.OpRGAInsert,
		AfterRef: datalog.ElementID{},
	}
	require.NoError(t, store.Assert([]datalog.Datom{datom}))

	validKey := store.encoder.EncodeKey(EATV, &datom)
	malformed := append([]byte(nil), validKey[:54]...)
	rawTx := store.db.NewTransaction(true)
	require.NoError(t, rawTx.Delete(validKey))
	require.NoError(t, rawTx.Set(malformed, nil))
	require.NoError(t, rawTx.Commit())

	entityBytes := entity.Bytes()
	attributeBytes := ToStorageDatom(datalog.Datom{A: attribute}).A
	prefix := append([]byte{byte(EATV)}, entityBytes[:]...)
	prefix = append(prefix, attributeBytes[:]...)
	iter, err := store.Scan(EATV, prefix, prefixEnd(prefix))
	require.NoError(t, err)
	require.True(t, iter.Next())
	_, decodeErr := iter.Datom()
	require.Error(t, decodeErr)
	require.NoError(t, iter.Close())

	matcher := NewBadgerMatcher(store)
	_, err = matcher.loadRGAElements(entityBytes[:], attributeBytes[:])
	require.Error(t, err)
}
