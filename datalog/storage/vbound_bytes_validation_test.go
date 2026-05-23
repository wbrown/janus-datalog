package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Reproductions for docs/bugs/BUG_VBOUND_BYTES_VALIDATION_PANIC.md
//
// V-bound queries on a cardinality-one attribute route through
// validatingVBoundIterator.validateCandidate, which compares the EATV winner
// to the bound value with raw interface ==:
//
//	matches := winner.V == it.currentBoundV
//
// For []byte both sides are dynamic type []uint8, which is not comparable, so
// the comparison panics with "comparing uncomparable type []uint8". The fix is
// to use datalog.ValuesEqual / m.valuesEqual.

func newBytesOneDB(t *testing.T) (*Database, datalog.Identity, datalog.Keyword) {
	t.Helper()
	dir := t.TempDir()
	// Cardinality-one, NOT unique: forces the candidate+validate path
	// (validateCandidate), not the unique (A,V)-LWW short-circuit.
	s, err := schema.NewBuilder().
		Attribute(":doc/hash").Type(schema.TypeBytes).One().Add().
		Build()
	require.NoError(t, err)
	db, err := NewDatabaseWithSchema(dir, s)
	require.NoError(t, err)
	return db, datalog.NewIdentity("doc-1"), datalog.NewKeyword(":doc/hash")
}

func queryByHash(t *testing.T, db *Database, v []byte) []datalog.Identity {
	t.Helper()
	rows, err := executor.CollectTuples(db.Query(`[:find ?e :in $ ?v :where [?e :doc/hash ?v]]`, v))
	require.NoError(t, err)
	out := make([]datalog.Identity, 0, len(rows))
	for _, row := range rows {
		id, ok := row[0].(datalog.Identity)
		require.Truef(t, ok, "expected Identity, got %T", row[0])
		out = append(out, id)
	}
	return out
}

// TestVBoundCardinalityOneBytes_NoPanic: a V-bound query on a byte attribute
// must not panic and must return the matching entity.
func TestVBoundCardinalityOneBytes_NoPanic(t *testing.T) {
	db, e, a := newBytesOneDB(t)
	defer db.Close()

	v := []byte{0xde, 0xad, 0xbe, 0xef}
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, v))
	_, err := tx.Commit()
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("V-bound []byte query panicked (bug reproduced): %v", r)
		}
	}()

	got := queryByHash(t, db, v)
	require.Len(t, got, 1)
	require.True(t, got[0].Equal(e))
}

// TestVBoundCardinalityOneBytes_MatchesByContent: a different slice instance
// with identical content must still match (byte-content equality).
func TestVBoundCardinalityOneBytes_MatchesByContent(t *testing.T) {
	db, e, a := newBytesOneDB(t)
	defer db.Close()

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, []byte{0xca, 0xfe, 0xba, 0xbe}))
	_, err := tx.Commit()
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("V-bound []byte content match panicked (bug reproduced): %v", r)
		}
	}()

	// Distinct slice, same content as stored.
	got := queryByHash(t, db, []byte{0xca, 0xfe, 0xba, 0xbe})
	require.Len(t, got, 1)
	require.True(t, got[0].Equal(e))
}

// TestVBoundCardinalityOneBytes_RejectsStaleCandidate: after overwrite, the old
// value still has an AVET candidate row, but the EATV winner differs. The
// candidate must be rejected (and rejection must compare by content, not panic).
func TestVBoundCardinalityOneBytes_RejectsStaleCandidate(t *testing.T) {
	db, e, a := newBytesOneDB(t)
	defer db.Close()

	v1 := []byte{0x01, 0x01}
	v2 := []byte{0x02, 0x02}

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, v1))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Set(e, a, v2)) // LWW overwrite; v1 row remains in AVET
	_, err = tx.Commit()
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("V-bound stale-candidate validation panicked (bug reproduced): %v", r)
		}
	}()

	require.Empty(t, queryByHash(t, db, v1), "stale value must not match the current winner")
}

// TestVBoundCardinalityOneBytes_AfterOverwrite: querying the current value after
// an overwrite returns the entity.
func TestVBoundCardinalityOneBytes_AfterOverwrite(t *testing.T) {
	db, e, a := newBytesOneDB(t)
	defer db.Close()

	v1 := []byte{0x0a}
	v2 := []byte{0x0b}

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, v1))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Set(e, a, v2))
	_, err = tx.Commit()
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("V-bound post-overwrite query panicked (bug reproduced): %v", r)
		}
	}()

	got := queryByHash(t, db, v2)
	require.Len(t, got, 1)
	require.True(t, got[0].Equal(e))
}

// TestVBoundCardinalityOneBytes_AfterRemove: after a tombstone the value must
// not match. (The Op==Remove check precedes the == comparison, so this is a
// contract test that complements the panic reproductions above.)
func TestVBoundCardinalityOneBytes_AfterRemove(t *testing.T) {
	db, e, a := newBytesOneDB(t)
	defer db.Close()

	v := []byte{0xfe, 0xed}

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, v))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Remove(e, a, v))
	_, err = tx.Commit()
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("V-bound post-remove query panicked (bug reproduced): %v", r)
		}
	}()

	require.Empty(t, queryByHash(t, db, v), "removed value must not match")
}
