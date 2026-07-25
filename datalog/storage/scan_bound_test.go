package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// scanBoundComponentOrder restates the key layouts from encodeKeyWithParts.
// It is deliberately a second, independent copy: if it read the production
// order table, a wrong table would agree with itself and this test would pass
// while every scan bound addressed the wrong run of the index.
var scanBoundComponentOrder = map[IndexType][]string{
	EAVT: {"E", "A", "V", "Tx"},
	EATV: {"E", "A", "Tx", "V"},
	AEVT: {"A", "E", "V", "Tx"},
	AETV: {"A", "E", "Tx", "V"},
	ATEV: {"A", "Tx", "E", "V"},
	AVET: {"A", "V", "E", "Tx"},
	VAET: {"V", "A", "E", "Tx"},
	TAEV: {"Tx", "A", "E", "V"},
}

// TestScanBoundEncodesAsPrefixRange is the transcription proof: for every
// index and every prefix length, a typed ScanBound must encode to the exact
// bytes EncodePrefixRange produces for the same logical bound. The conversion
// of the byte-range call sites rests on this equality.
func TestScanBoundEncodesAsPrefixRange(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	entity := datalog.NewIdentity("scanbound:entity")
	attr := datalog.NewKeyword(":scanbound/attr")
	value := int64(0x5150)
	tx := datalog.ElementID{Lamport: 0x1234, ReplicaID: 0x5678}

	typed := map[string]datalog.Value{"E": entity, "A": attr, "V": value, "Tx": tx}

	// The four components encode to distinct byte runs of distinct lengths, so
	// a transposed component order cannot coincidentally produce equal bytes.
	sd := ToStorageDatom(datalog.Datom{E: entity, A: attr})
	encodedPart := map[string][]byte{
		"E":  sd.E[:],
		"A":  sd.A[:],
		"V":  encodeValueForSearch(value, encoder),
		"Tx": encoder.EncodeTxForPrefix(NewTxFromElementID(tx)),
	}

	for _, index := range Indices {
		order, ok := scanBoundComponentOrder[index]
		require.True(t, ok, "index %v missing from the expected component order", index)
		require.Len(t, order, 4)

		for n := 0; n <= len(order); n++ {
			t.Run(fmt.Sprintf("%s/prefix%d", indexName(index), n), func(t *testing.T) {
				prefix := make([]datalog.Value, 0, n)
				parts := make([][]byte, 0, n)
				for _, name := range order[:n] {
					prefix = append(prefix, typed[name])
					parts = append(parts, encodedPart[name])
				}

				wantStart, wantEnd := encoder.EncodePrefixRange(index, parts...)

				gotStart, gotEnd, err := encoder.EncodeScanBound(ScanBound{Index: index, Prefix: prefix})
				require.NoError(t, err)
				require.Equal(t, wantStart, gotStart, "start bytes")
				require.Equal(t, wantEnd, gotEnd, "end bytes")
			})
		}
	}
}

// TestScanBoundContainsItsDatomKey ties the component order to the real key
// layout rather than to the test's own table. TestScanBoundEncodesAsPrefixRange
// compares two orderings that both feed EncodePrefixRange, so a misreading of
// the layout shared by both tables would agree with itself. Here the bound is
// checked against a key that encodeKeyWithParts actually laid down: for every
// index and prefix length, the bound built from a datom's own components must
// start that datom's key, and the key must fall inside the range.
func TestScanBoundContainsItsDatomKey(t *testing.T) {
	// Compression off, so EncodeKey's value encoding and encodeValueForSearch
	// take the same branch and the comparison is meaningful.
	encoder := &BinaryKeyEncoder{}

	entity := datalog.NewIdentity("scanbound:entity")
	attr := datalog.NewKeyword(":scanbound/attr")
	value := int64(0x5150)
	tx := datalog.ElementID{Lamport: 0x1234, ReplicaID: 0x5678}

	datom := datalog.Datom{E: entity, A: attr, V: value, Tx: tx}
	typed := map[string]datalog.Value{"E": entity, "A": attr, "V": value, "Tx": tx}

	for _, index := range Indices {
		order := scanBoundComponentOrder[index]
		key := encoder.EncodeKey(index, &datom)

		for n := 0; n <= len(order); n++ {
			t.Run(fmt.Sprintf("%s/prefix%d", indexName(index), n), func(t *testing.T) {
				prefix := make([]datalog.Value, 0, n)
				for _, name := range order[:n] {
					prefix = append(prefix, typed[name])
				}

				start, end, err := encoder.EncodeScanBound(ScanBound{Index: index, Prefix: prefix})
				require.NoError(t, err)
				require.True(t, bytes.HasPrefix(key, start),
					"bound start %x must prefix the datom's key %x", start, key)
				require.Negative(t, bytes.Compare(key, end),
					"datom's key %x must sort below the range end %x", key, end)
			})
		}
	}
}

// TestScanBoundOverlongPrefixRejected pins loud failure over silent
// truncation: an index has exactly four components, so a fifth prefix element
// binds nothing and is a caller bug.
func TestScanBoundOverlongPrefixRejected(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	entity := datalog.NewIdentity("scanbound:entity")
	attr := datalog.NewKeyword(":scanbound/attr")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	_, _, err := encoder.EncodeScanBound(ScanBound{
		Index:  EAVT,
		Prefix: []datalog.Value{entity, attr, int64(1), tx, int64(2)},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "prefix")
}

// TestScanBoundRejectsMistypedComponent pins the other loud arm: each position
// is inhabited by exactly one kind, so a value of the wrong kind is a caller
// bug rather than something to coerce.
func TestScanBoundRejectsMistypedComponent(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	// EAVT's leading component is E, inhabited only by Identity.
	_, _, err := encoder.EncodeScanBound(ScanBound{
		Index:  EAVT,
		Prefix: []datalog.Value{"not-an-identity"},
	})
	require.Error(t, err)

	// ATEV's second component is Tx, inhabited only by ElementID.
	attr := datalog.NewKeyword(":scanbound/attr")
	_, _, err = encoder.EncodeScanBound(ScanBound{
		Index:  ATEV,
		Prefix: []datalog.Value{attr, int64(7)},
	})
	require.Error(t, err)
}

// TestScanBoundUnknownIndexRejected pins the taxonomy's default arm.
func TestScanBoundUnknownIndexRejected(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	_, _, err := encoder.EncodeScanBound(ScanBound{Index: IndexType(200)})
	require.Error(t, err)
}
