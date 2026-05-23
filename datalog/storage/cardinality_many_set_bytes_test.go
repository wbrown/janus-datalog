package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Reproductions for docs/bugs/BUG_CARDINALITY_MANY_SET_BYTES_PANIC.md
//
// Transaction.Set for cardinality-many builds map[interface{}]bool keyed
// directly by set members (newSet[val], pendingAdds[v], pendingRemoves[v]).
// []byte is a valid TypeBytes value but is not a valid Go map key, so Set
// panics with "hash of unhashable type []uint8".

func newBytesManyDB(t *testing.T) (*Database, datalog.Identity, datalog.Keyword) {
	t.Helper()
	dir := t.TempDir()
	s, err := schema.NewBuilder().
		Attribute(":file/chunks").Type(schema.TypeBytes).Many().Add().
		Build()
	require.NoError(t, err)
	db, err := NewDatabaseWithSchema(dir, s)
	require.NoError(t, err)
	return db, datalog.NewIdentity("file-1"), datalog.NewKeyword(":file/chunks")
}

func collectByteSet(t *testing.T, db *Database, e datalog.Identity) [][]byte {
	t.Helper()
	rows, err := executor.CollectTuples(db.Query(`[:find ?v :in $ ?e :where [?e :file/chunks ?v]]`, e))
	require.NoError(t, err)
	out := make([][]byte, 0, len(rows))
	for _, row := range rows {
		b, ok := row[0].([]byte)
		require.Truef(t, ok, "expected []byte member, got %T", row[0])
		out = append(out, b)
	}
	return out
}

// TestCardinalityManySet_ByteValues_NoPanic: a plain Set of byte members must
// not panic and must persist the set.
func TestCardinalityManySet_ByteValues_NoPanic(t *testing.T) {
	db, e, a := newBytesManyDB(t)
	defer db.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Set on cardinality-many []byte panicked (bug reproduced): %v", r)
		}
	}()

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, [][]byte{[]byte("chunk-a"), []byte("chunk-b")}))
	_, err := tx.Commit()
	require.NoError(t, err)

	require.ElementsMatch(t, [][]byte{[]byte("chunk-a"), []byte("chunk-b")}, collectByteSet(t, db, e))
}

// TestCardinalityManySet_ByteValues_ReplacesExistingSet: a second Set replaces
// the committed set (add-wins diff against current membership of []byte values).
func TestCardinalityManySet_ByteValues_ReplacesExistingSet(t *testing.T) {
	db, e, a := newBytesManyDB(t)
	defer db.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Set replacement on cardinality-many []byte panicked (bug reproduced): %v", r)
		}
	}()

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, [][]byte{[]byte("a"), []byte("b")}))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Set(e, a, [][]byte{[]byte("b"), []byte("c")}))
	_, err = tx.Commit()
	require.NoError(t, err)

	require.ElementsMatch(t, [][]byte{[]byte("b"), []byte("c")}, collectByteSet(t, db, e))
}

// TestCardinalityManySet_ByteValues_PendingOpsInSameTransaction: Set must see
// earlier Add ops in the same transaction. This exercises the pendingAdds /
// pendingRemoves maps, which use the same direct-[]byte-key pattern as newSet,
// so a fix that only repairs newSet would still panic here.
func TestCardinalityManySet_ByteValues_PendingOpsInSameTransaction(t *testing.T) {
	db, e, a := newBytesManyDB(t)
	defer db.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Set after pending Add on []byte panicked (bug reproduced): %v", r)
		}
	}()

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, []byte("a")))
	require.NoError(t, tx.Add(e, a, []byte("b")))
	require.NoError(t, tx.Set(e, a, [][]byte{[]byte("b"), []byte("c")}))
	_, err := tx.Commit()
	require.NoError(t, err)

	require.ElementsMatch(t, [][]byte{[]byte("b"), []byte("c")}, collectByteSet(t, db, e))
}

// TestCardinalityManySet_ByteValues_DuplicateMembersDedupByContent: two byte
// slices with equal content are the same set member.
func TestCardinalityManySet_ByteValues_DuplicateMembersDedupByContent(t *testing.T) {
	db, e, a := newBytesManyDB(t)
	defer db.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Set with duplicate []byte members panicked (bug reproduced): %v", r)
		}
	}()

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, [][]byte{[]byte("dup"), []byte("dup")}))
	_, err := tx.Commit()
	require.NoError(t, err)

	require.ElementsMatch(t, [][]byte{[]byte("dup")}, collectByteSet(t, db, e))
}
