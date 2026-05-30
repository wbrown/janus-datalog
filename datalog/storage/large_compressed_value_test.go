package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// jsonishStorage mirrors the codec test's densest realistic content: a JSON-like
// array of small uniform records, which produces more than 65535 LZ77 sequences
// at a couple of MB. Used to exercise the widened (v2/uint32) compression header
// end-to-end through Tier-3 blob routing, not just the codec in isolation.
func jsonishStorage(n int) string {
	var b bytes.Buffer
	b.WriteByte('[')
	for i := 0; b.Len() < n; i++ {
		fmt.Fprintf(&b, `{"id":%d,"name":"alice","active":true,"score":%d},`,
			100000+i, i%97)
	}
	return b.String()[:n]
}

// TestLargeCompressedValue_RoundTripsThroughStorage writes a ~2 MB densely
// structured string (well past the old uint16 sequence cap), commits, closes,
// reopens the database from disk, and reads the value back byte-for-byte. Under
// the v1 (uint16) header this value committed cleanly and then failed to
// decompress on read — write-accept / read-fail data loss. With the v2 header it
// must round-trip exactly.
func TestLargeCompressedValue_RoundTripsThroughStorage(t *testing.T) {
	path := t.TempDir()
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":doc/body"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	e := datalog.NewIdentity("doc1")
	body := jsonishStorage(2 * 1024 * 1024)

	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: path, Schema: s, ReplicaID: 1})
	require.NoError(t, err)
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, datalog.NewKeyword(":doc/body"), body))
	_, err = tx.Commit()
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Reopen from disk and read the value back through the full decode path
	// (Tier-3 blob fetch + Decompress).
	db2, err := NewDatabaseWithOptions(DatabaseOptions{Path: path, Schema: s, ReplicaID: 1})
	require.NoError(t, err)
	defer db2.Close()

	rel, err := db2.Query(`[:find ?v :where [?e :doc/body ?v]]`)
	require.NoError(t, err)
	it := rel.Iterator()
	defer it.Close()

	require.True(t, it.Next(), "expected the stored value")
	got, ok := it.Tuple()[0].(string)
	require.True(t, ok, "value should decode as a string")
	// Compare without dumping 2 MB on mismatch.
	assert.Equal(t, len(body), len(got), "decoded length must match")
	assert.True(t, body == got, "2 MB value must round-trip through storage")
	assert.False(t, it.Next(), "expected exactly one row")
	require.NoError(t, it.Error())
}
