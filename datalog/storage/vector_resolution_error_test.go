//go:build !(js && wasm)

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestLoadRGAElementsPropagatesDatomDecodeError is the reproduction standing
// behind TestResolveVectorAttributeSurfacesScanErrors, which carries the same
// contract on every backend through an injected iterator. Here the key is
// really truncated and the decoder really rejects it. No Store method writes a
// malformed key — no correct writer produces one — so planting it means
// reaching past the interface into a backend's own keyspace, Badger's here.
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

	validKey := store.Encoder().EncodeKey(EATV, &datom)
	malformed := append([]byte(nil), validKey[:54]...)
	rawTx := store.db.NewTransaction(true)
	require.NoError(t, rawTx.Delete(validKey))
	require.NoError(t, rawTx.Set(malformed, nil))
	require.NoError(t, rawTx.Commit())

	entityBytes := entity.Bytes()
	attributeBytes := ToStorageDatom(datalog.Datom{A: attribute}).A
	iter, err := store.Scan(ScanBound{Index: EATV, Prefix: []datalog.Value{entity, attribute}})
	require.NoError(t, err)
	require.True(t, iter.Next(), "malformed key is still a positioned scan entry")
	_, err = iter.Datom()
	require.Error(t, err, "malformed key must fail decode")
	require.False(t, iter.Next())
	require.Error(t, iter.Error())
	require.NoError(t, iter.Close())

	matcher := NewPatternMatcher(store)
	report := &scanReport{}
	_, _, err = matcher.loadRGAElements(entityBytes[:], attributeBytes[:], report)
	require.Error(t, err)

	// The failing resolution still read the index to discover the malformed
	// key, and the report is what keeps that: it is populated independent of
	// whether resolution errors, so a failed resolution still reports the
	// reads it made instead of reporting zero — the reads a trace most needs
	// to see, since they are the ones that bought nothing.
	require.Positive(t, report.scanned,
		"the scan positioned on an entry before decode failed, so it read one")
}
