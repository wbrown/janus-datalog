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
// structured string (well past the old uint16 sequence cap), commits, ends
// the session, reopens over the same stored state, and reads the value back
// byte-for-byte through the full decode path (Tier-3 blob fetch +
// Decompress) with nothing warm from the write session. Under the v1
// (uint16) header this value committed cleanly and then failed to decompress
// on read — write-accept / read-fail data loss. With the v2 header it must
// round-trip exactly, on every backend. CompressionThreshold is explicit so
// the injected-store backends route through Tier 3 identically.
func TestLargeCompressedValue_RoundTripsThroughStorage(t *testing.T) {
	for i, c := range reopenBackendCases() {
		t.Run(c.name, func(t *testing.T) {
			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					// Fresh backend case per mode: each case's stored state
					// lives in its closure, and the writes must not leak
					// across optimizer legs.
					c := reopenBackendCases()[i]
					popts := mode.plannerOptions()

					s := schema.NewSchema()
					s.Add(&schema.AttributeDefinition{
						Ident:       datalog.NewKeyword(":doc/body"),
						ValueType:   schema.TypeString,
						Cardinality: schema.CardinalityOne,
					})
					e := datalog.NewIdentity("doc1")
					body := jsonishStorage(2 * 1024 * 1024)

					db := c.open(t, DatabaseOptions{Schema: s, ReplicaID: 1, CompressionThreshold: 256, PlannerOptions: &popts})
					tx := db.NewTransaction()
					require.NoError(t, tx.Set(e, datalog.NewKeyword(":doc/body"), body))
					_, err := tx.Commit()
					require.NoError(t, err)

					// Reopen over the same stored state and read the value back
					// through the full decode path.
					db2 := c.open(t, DatabaseOptions{Schema: s, ReplicaID: 1, CompressionThreshold: 256, PlannerOptions: &popts})

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
					assert.False(t, it.Next(), "expected exactly one tuple")
					require.NoError(t, it.Error())
				})
			}
		})
	}
}
