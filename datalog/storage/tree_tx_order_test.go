package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A retract names (E, A, V) and not Tx — retractMemoryDatom searches on that
// prefix, and matchingStoredDatoms matches the same components — so a retract
// covers every Tx for that value. What keeps it from taking a datom asserted in
// the same transaction is that it resolves against the state the transaction
// opened on, never against the transaction's own writes.
//
// applyBatch currently enforces that by phasing: every retract resolves against
// b.base and is applied before any assert, whatever order the caller used. A
// transaction that instead applies writes as they arrive has to preserve the
// same outcome, so both orders are pinned here against the phased
// implementation before it changes.
func TestTreeStoreTxRetractAndAssertOrderIndependent(t *testing.T) {
	entity := datalog.NewIdentity("tree-tx:subject")
	attr := datalog.NewKeyword(":tree-tx/value")
	tx1 := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	tx2 := datalog.ElementID{Lamport: 2, ReplicaID: 1}
	superseded := datalog.Datom{E: entity, A: attr, V: "X", Tx: tx1}
	replacement := datalog.Datom{E: entity, A: attr, V: "X", Tx: tx2}

	for _, order := range []struct {
		name  string
		apply func(t *testing.T, stx StoreTx)
	}{
		{
			// The order Database.Commit uses.
			name: "retract then assert",
			apply: func(t *testing.T, stx StoreTx) {
				require.NoError(t, stx.Retract([]datalog.Datom{superseded}))
				require.NoError(t, stx.Assert([]datalog.Datom{replacement}))
			},
		},
		{
			name: "assert then retract",
			apply: func(t *testing.T, stx StoreTx) {
				require.NoError(t, stx.Assert([]datalog.Datom{replacement}))
				require.NoError(t, stx.Retract([]datalog.Datom{superseded}))
			},
		},
	} {
		t.Run(order.name, func(t *testing.T) {
			store := NewMemoryTreeStore(&BinaryKeyEncoder{})
			t.Cleanup(func() { _ = store.Close() })
			require.NoError(t, store.Assert([]datalog.Datom{superseded}))

			stx, err := store.BeginTx()
			require.NoError(t, err)
			order.apply(t, stx)
			require.NoError(t, stx.Commit())

			// Scanning the (E, A) run asserts more than a point lookup would: not
			// that a tx2 datom exists, but that the one surviving datom is it.
			iter, err := store.Scan(ScanBound{Index: EAVT, Prefix: []datalog.Value{entity, attr}})
			require.NoError(t, err)
			defer iter.Close()

			require.True(t, iter.Next(), "the replacement must survive the retract")
			got, err := iter.Datom()
			require.NoError(t, err)
			require.Equal(t, tx2, got.Tx, "the retract must not take the datom asserted beside it")
			require.False(t, iter.Next(), "exactly one datom must survive")
			require.NoError(t, iter.Error())
		})
	}
}
