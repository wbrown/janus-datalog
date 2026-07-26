package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestScanBoundEndIsTheExclusiveSuccessor pins the arithmetic a prefix range
// rests on: the end key must be the least key strictly greater than every key
// under the prefix. For a prefix ending in 0xFF that means carrying — the byte
// that overflowed becomes 0x00 and the carry lands to its left. Leaving the
// 0xFF in place instead yields a key above the true successor, and the range
// then covers the whole of the *next* sibling subtree below that byte.
//
// This is not hypothetical arithmetic: orderedInt64 encodes -1 as
// 0x7FFFFFFFFFFFFFFF, so any bound naming a negative long ends in 0xFF, and one
// entity hash in 256 does too.
func TestScanBoundEndIsTheExclusiveSuccessor(t *testing.T) {
	encoder := &BinaryKeyEncoder{}
	attr := datalog.NewKeyword(":bound/count")

	// A bound on a value whose encoding ends 0xFF.
	run, err := encoder.EncodeScanBound(ScanBound{
		Index:  AVET,
		Prefix: []datalog.Value{attr, int64(-1)},
	})
	start, end := run.Start, run.End
	require.NoError(t, err)

	// The successor of the prefix: every key under it sorts below, and the
	// first key of the next sibling sorts at or above.
	require.Positive(t, bytes.Compare(end, start))

	// Construct the neighbouring value's own key. int64(7) encodes to
	// 0x8000000000000007, the sibling immediately after -1's 0x7FFF..FF in the
	// ordered encoding, so its key must NOT fall inside [start, end).
	neighbour := encoder.EncodeKey(AVET, &datalog.Datom{
		E:  datalog.NewIdentity("bound:neighbour"),
		A:  attr,
		V:  int64(7),
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	})
	require.GreaterOrEqual(t, bytes.Compare(neighbour, end), 0,
		"a key for a different value must sort at or above the bound's end; "+
			"end=%x neighbour=%x", end, neighbour)
}

// TestScanBoundOnNegativeLongDoesNotMatchItsNeighbour is the end-to-end form of
// the same defect, on the default path with no options set: a query for one
// value must not return entities carrying a different value.
func TestScanBoundOnNegativeLongDoesNotMatchItsNeighbour(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":person/count").Type(schema.TypeLong).Many().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir(), Schema: s})
	require.NoError(t, err)
	defer db.Close()

	count := datalog.NewKeyword(":person/count")
	negative := datalog.NewIdentity("person:negative")
	positive := datalog.NewIdentity("person:positive")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(negative, count, int64(-1)))
	require.NoError(t, tx.Add(positive, count, int64(7)))
	_, err = tx.Commit()
	require.NoError(t, err)

	result, err := db.Query(`[:find ?e :where [?e :person/count -1]]`)
	require.NoError(t, err)
	rows, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)

	require.Len(t, rows, 1, "only the entity holding -1 matches; got %v", rows)
	require.True(t, rows[0][0].(datalog.Identity).Equal(negative))
}

// TestScanBoundOnStringDoesNotMatchItsExtension pins the second half of the
// same contract: a V-terminated bound must name its own value, not every value
// that has it as a byte prefix. The V component is [type][raw bytes] with no
// length and no terminator, so "abc" and "abcd" differ only by length and no
// end key separates them — and the AVET consumers never re-compare datom.V.
//
// Unlike the successor defect above this predates the typed bound: the byte
// form was already [A][type][value] with the same exclusive end. The typed
// bound inherits it, and the doc comment on ScanBound asserting that a bound
// names a contiguous run of exactly the bound values is wrong for a
// variable-length V until it is fixed.
func TestScanBoundOnStringDoesNotMatchItsExtension(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":person/tag").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir(), Schema: s})
	require.NoError(t, err)
	defer db.Close()

	tag := datalog.NewKeyword(":person/tag")
	short := datalog.NewIdentity("person:short")
	long := datalog.NewIdentity("person:long")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(short, tag, "abc"))
	require.NoError(t, tx.Add(long, tag, "abcd"))
	_, err = tx.Commit()
	require.NoError(t, err)

	result, err := db.Query(`[:find ?e :where [?e :person/tag "abc"]]`)
	require.NoError(t, err)
	rows, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)

	require.Len(t, rows, 1, `only the entity tagged "abc" matches; got %v`, rows)
	require.True(t, rows[0][0].(datalog.Identity).Equal(short))
}
