// Tests for commit atomicity guarantees.
//
// Transaction.Commit wraps the entire logical commit (retracts, asserts,
// transaction metadata) in a single storage transaction so the commit is
// all-or-nothing. The test below locks in the contract that every
// successful Commit() also produces its :db/txInstant metadata datom —
// previously this write was best-effort (logged on failure, ignored), now
// it is part of the same atomic write.
//
// The original bug report covered both atomicity and uniqueness TOCTOU:
// see BUG_TRANSACTION_COMMIT_SPLIT_BADGER_TX.md and
// BUG_UNIQUENESS_VALIDATION_TOCTOU.md. Uniqueness work is
// deferred — the CRDT-aligned redesign lives in
// docs/proposals/CRDT_UNIQUE_SEMANTICS.md.

package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestCommitWritesTxInstantOnSuccess locks in the contract that every
// successful Commit() produces a :db/txInstant datom. Before the
// atomicity refactor this write was best-effort: the storage layer
// logged failures and Commit() still returned success. After the
// refactor, the metadata write joins the same atomic storage txn as
// the rest of the commit, so failure rolls back the entire transaction.
func TestCommitWritesTxInstantOnSuccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-txinstant-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	db, err := NewDatabase(tmpDir)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":user/name")
	txInstantAttr := datalog.NewKeyword(":db/txInstant")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(alice, name, "Alice"))
	txID, err := tx.Commit()
	require.NoError(t, err)

	// The metadata datom uses entity "tx:<lamport>" of the metadata's own
	// ElementID, which is what Commit() returns.
	txEntity := datalog.NewIdentity(fmt.Sprintf("tx:%d", txID.Lamport))

	matcher := NewPatternMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: txEntity},
			query.Constant{Value: txInstantAttr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	found := iter.Next()
	iter.Close()

	if !found {
		t.Errorf("expected :db/txInstant datom for tx %d, none found", txID.Lamport)
	}
}
