//go:build !(js && wasm)

package storage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// entityLargerThanOneTransaction builds one entity's datoms sized to overrun a
// single Badger transaction.
//
// Two properties of the write path make this reachable by ordinary data. Every
// datom writes eight index keys, and every index orders V, so the value is
// carried inline in all eight. Values here stay under the 512-byte compression
// threshold so they stay in the keys rather than moving out of line into the
// blob store, which is what makes the keys — and therefore the transaction —
// large. Badger's ceiling is 15% of MemTableSize, so a few thousand datoms on
// one entity clear it.
//
// One entity matters: JDZL closes a chunk only at an entity boundary, so an
// entity is the chunk floor and cannot be split by the exporter's soft budget.
func entityLargerThanOneTransaction(seed string, count int) []datalog.Datom {
	e := datalog.NewIdentity(seed)
	attr := datalog.NewKeyword(":entry/payload")
	filler := strings.Repeat("v", 392)
	datoms := make([]datalog.Datom, count)
	for i := range datoms {
		datoms[i] = datalog.Datom{
			E:  e,
			A:  attr,
			V:  filler + fmt.Sprintf("%08d", i),
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
		}
	}
	return datoms
}

// TestBinaryImportEntityLargerThanOneTransaction pins that import survives an
// entity whose datoms exceed what one Badger transaction can hold.
//
// Before the WriteBatch split in BadgerStore.Assert, ImportBinary asserted each
// decoded chunk in a single transaction, so a dump containing such an entity
// exported cleanly and then refused to import — the round trip was not closed.
func TestBinaryImportEntityLargerThanOneTransaction(t *testing.T) {
	large := entityLargerThanOneTransaction("ceiling:accumulator", 8000)
	small := []datalog.Datom{{
		E:  datalog.NewIdentity("ceiling:neighbour"),
		A:  datalog.NewKeyword(":entry/label"),
		V:  "neighbour",
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 2},
	}}

	// The ceiling is Badger's own, so the backend is this test's subject rather
	// than a parameter: it is named here instead of taken from the mode axis.
	backend, err := BackendNamed("badger")
	require.NoError(t, err)
	badgerOnly := optimizerMode{name: backend.Name, backend: backend}

	// The premise, asserted through StoreTx — the path that does not split,
	// because its caller owns the transaction boundary. If the ceiling ever
	// moves, this fails and says the fixture needs growing, rather than the
	// test quietly ceasing to reproduce anything.
	probe := createOptimizerModeDB(t, badgerOnly, DatabaseOptions{})
	stx, err := probe.store.BeginTx()
	require.NoError(t, err)
	require.ErrorIs(t, stx.Assert(large), badger.ErrTxnTooBig,
		"fixture no longer exceeds one transaction; raise the datom count")
	require.NoError(t, stx.Rollback())

	source := createOptimizerModeDB(t, badgerOnly, DatabaseOptions{})
	require.NoError(t, source.store.Assert(large))
	require.NoError(t, source.store.Assert(small))

	var dump seekBuffer
	require.NoError(t, source.ExportBinary(&dump))

	target := createOptimizerModeDB(t, badgerOnly, DatabaseOptions{})
	require.NoError(t, target.ImportBinary(&dump))

	var reexported seekBuffer
	require.NoError(t, target.ExportBinary(&reexported))
	require.Equal(t, dump.buf, reexported.buf)
}
