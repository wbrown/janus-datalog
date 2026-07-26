package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
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

// TestScanBoundThroughSpansToSecondPrefix pins the span form: the run starts
// at Prefix and ends at Through's exclusive successor, so every datom under
// Through's own prefix is included.
func TestScanBoundThroughSpansToSecondPrefix(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	first := datalog.NewIdentity("scanbound:span-a")
	last := datalog.NewIdentity("scanbound:span-z")

	gotStart, gotEnd, err := encoder.EncodeScanBound(ScanBound{
		Index:   EAVT,
		Prefix:  []datalog.Value{first},
		Through: []datalog.Value{last},
	})
	require.NoError(t, err)

	wantStart, _ := encoder.EncodePrefixRange(EAVT, entityBytesFor(first))
	_, wantEnd := encoder.EncodePrefixRange(EAVT, entityBytesFor(last))
	require.Equal(t, wantStart, gotStart, "start is the first prefix's start")
	require.Equal(t, wantEnd, gotEnd, "end is the last prefix's exclusive successor")

	// The span must contain both endpoints' own keys.
	require.Negative(t, bytes.Compare(gotStart, gotEnd))
}

// TestScanBoundThroughEqualToPrefixIsThePlainPrefix pins that the span form
// degenerates exactly, so the two shapes are one concept rather than two.
func TestScanBoundThroughEqualToPrefixIsThePlainPrefix(t *testing.T) {
	encoder := &BinaryKeyEncoder{}
	entity := datalog.NewIdentity("scanbound:degenerate")

	plainStart, plainEnd, err := encoder.EncodeScanBound(ScanBound{
		Index:  EAVT,
		Prefix: []datalog.Value{entity},
	})
	require.NoError(t, err)

	spanStart, spanEnd, err := encoder.EncodeScanBound(ScanBound{
		Index:   EAVT,
		Prefix:  []datalog.Value{entity},
		Through: []datalog.Value{entity},
	})
	require.NoError(t, err)

	require.Equal(t, plainStart, spanStart)
	require.Equal(t, plainEnd, spanEnd)
}

// TestScanBoundInvertedThroughRejected pins loud failure over a silent empty
// result: a Through that sorts below Prefix names an empty range, which every
// scan would report as "no matches" rather than as the caller bug it is.
func TestScanBoundInvertedThroughRejected(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	low := datalog.NewIdentity("scanbound:span-a")
	high := datalog.NewIdentity("scanbound:span-z")

	// Establish which of the two actually sorts lower, so the test asserts
	// inversion rather than an assumption about hash ordering.
	lowBytes, highBytes := entityBytesFor(low), entityBytesFor(high)
	if bytes.Compare(lowBytes, highBytes) > 0 {
		low, high = high, low
	}

	_, _, err := encoder.EncodeScanBound(ScanBound{
		Index:   EAVT,
		Prefix:  []datalog.Value{high},
		Through: []datalog.Value{low},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Through")
}

// TestScanBoundThroughValidatedLikePrefix pins that Through is held to the
// same component rules as Prefix rather than being a second, laxer path.
func TestScanBoundThroughValidatedLikePrefix(t *testing.T) {
	encoder := &BinaryKeyEncoder{}
	entity := datalog.NewIdentity("scanbound:through-validation")
	attr := datalog.NewKeyword(":scanbound/attr")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	// Over-long Through.
	_, _, err := encoder.EncodeScanBound(ScanBound{
		Index:   EAVT,
		Prefix:  []datalog.Value{entity},
		Through: []datalog.Value{entity, attr, int64(1), tx, int64(2)},
	})
	require.Error(t, err)

	// Mistyped Through component: EAVT's leading component is E.
	_, _, err = encoder.EncodeScanBound(ScanBound{
		Index:   EAVT,
		Prefix:  []datalog.Value{entity},
		Through: []datalog.Value{"not-an-identity"},
	})
	require.Error(t, err)
}

// TestIndexSelectionEventReportsBound pins the observability contract of the
// typed seam: pattern/index-selection reports the run it addressed — the index,
// the positions bound in that index's component order, and the values bound to
// them. It must never report an encoded key range; that is one backend's
// projection of the bound, which a handler cannot interpret and a typed backend
// never produces.
func TestIndexSelectionEventReportsBound(t *testing.T) {
	var events []annotations.Event
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:              t.TempDir(),
		AnnotationHandler: func(e annotations.Event) { events = append(events, e) },
	})
	require.NoError(t, err)
	defer db.Close()

	name := datalog.NewKeyword(":person/name")
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(datalog.NewIdentity("p1"), name, "Alice"))
	require.NoError(t, tx.Add(datalog.NewIdentity("p2"), name, "Bob"))
	_, err = tx.Commit()
	require.NoError(t, err)

	eventFor := func(t *testing.T, name, query string) annotations.Event {
		t.Helper()
		events = nil
		result, err := db.Query(query)
		require.NoError(t, err)
		_, err = executor.CollectTuples(result, nil)
		require.NoError(t, err)
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].Name == name {
				return events[i]
			}
		}
		t.Fatalf("no %s event; saw %d events", name, len(events))
		return annotations.Event{}
	}

	// Every scan-reporting event must describe its run the same way, so the
	// assertions below are shared: no encoded range, and parallel position and
	// value slices naming what the run binds.
	requireBound := func(t *testing.T, e annotations.Event, index string, positions, values []string) {
		t.Helper()
		require.Equal(t, index, e.Data["index"])
		require.Equal(t, positions, e.Data["bound"])
		require.Equal(t, values, e.Data["bound.values"])
		for _, byteField := range []string{"scan.start", "scan.end", "start", "end", "value_bytes"} {
			require.NotContains(t, e.Data, byteField,
				"an encoded key range is one backend's projection, not an annotation")
		}
	}

	t.Run("A bound, unbound scan path", func(t *testing.T) {
		e := eventFor(t, "pattern/index-selection", `[:find ?e ?n :where [?e :person/name ?n]]`)
		requireBound(t, e, "AETV", []string{"A"}, []string{":person/name"})
	})

	t.Run("A and V bound, V-validation path", func(t *testing.T) {
		e := eventFor(t, "v-validation/open-scan", `[:find ?e :where [?e :person/name "Alice"]]`)
		requireBound(t, e, "AVET",
			[]string{"A", "V"}, []string{":person/name", "Alice"})
	})

	t.Run("nothing bound", func(t *testing.T) {
		e := eventFor(t, "pattern/index-selection", `[:find ?e ?a ?v :where [?e ?a ?v]]`)
		require.Empty(t, e.Data["bound"],
			"a whole-index scan binds no component, and says so rather than omitting the field")
		require.Empty(t, e.Data["bound.values"])
		require.Contains(t, e.Data, "bound", "the field is present even when the run binds nothing")
	})

	// The producer and the formatter are pinned separately — here they meet.
	// Separate pins agreeing on a payload shape is exactly what failed before:
	// the formatter read fields no emitter produced, and every scan line
	// rendered "bound: ?" because nothing ran the two together.
	t.Run("the formatter renders what the matcher emits", func(t *testing.T) {
		events = nil
		result, err := db.Query(`[:find ?e ?n :where [?e :person/name ?n]]`)
		require.NoError(t, err)
		_, err = executor.CollectTuples(result, nil)
		require.NoError(t, err)

		var out bytes.Buffer
		formatter := annotations.NewPlainTextFormatter(&out)
		for _, e := range events {
			formatter.Handle(e)
		}
		require.Contains(t, out.String(), "AETV, bound: A",
			"the scan line must name the run the matcher actually addressed")
	})
}

// encodeScanBoundForTest renders a bound as the byte range it addresses, for
// the tests that assert on key layout rather than on the bound's components.
// It lives here rather than beside those tests because they are spread across
// both Badger-only and portable test files.
func encodeScanBoundForTest(t *testing.T, m *PatternMatcher, bound ScanBound) (start, end []byte) {
	t.Helper()
	start, end, err := m.encoder.EncodeScanBound(bound)
	if err != nil {
		t.Fatalf("encode scan bound on %v: %v", bound.Index, err)
	}
	return start, end
}

// entityBytesFor renders an Identity in the storage form the E component uses.
func entityBytesFor(id datalog.Identity) []byte {
	var e Entity
	copy(e[:], id.Bytes())
	return e[:]
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
