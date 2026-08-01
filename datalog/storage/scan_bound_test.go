package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

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

// scanBoundValueFixtures gives the tables below a mid-range V and one whose
// ordered encoding ends 0xFF. The second is what makes the exclusion assertion
// in TestScanBoundContainsItsDatomKey a pin on the end key's carry: orderedInt64
// encodes -1 as 0x7FFFFFFFFFFFFFFF, so its prefix's exclusive successor can only
// be reached by carrying, and int64(7) — 0x8000000000000007 — is the first key
// of the sibling subtree an end that skipped the carry would swallow.
var scanBoundValueFixtures = []struct {
	name  string
	value int64
	other int64
}{
	{"midrange", 0x5150, 0x5151},
	{"carry", -1, 7},
}

// TestScanBoundEncodesAsPrefixRange is the transcription proof: for every
// index and every prefix length, a typed ScanBound must encode to the exact
// bytes EncodePrefixRange produces for the same logical bound. The conversion
// of the byte-range call sites rests on this equality.
//
// Equality is all it proves. Both sides derive their end from the same
// incrementLastByte call, so this table cannot tell a correct exclusive
// successor from an incorrect one — it can only tell that the typed form and
// the byte form agree. The end arithmetic itself is pinned by
// TestScanBoundEndIsTheExclusiveSuccessor and by the exclusion assertion in
// TestScanBoundContainsItsDatomKey, both of which compare against keys the
// encoder actually laid down rather than against a second copy of the
// arithmetic.
func TestScanBoundEncodesAsPrefixRange(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	entity := datalog.NewIdentity("scanbound:entity")
	attr := datalog.NewKeyword(":scanbound/attr")
	tx := datalog.ElementID{Lamport: 0x1234, ReplicaID: 0x5678}

	for _, fixture := range scanBoundValueFixtures {
		value := fixture.value
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
				t.Run(fmt.Sprintf("%s/%s/prefix%d", fixture.name, index.String(), n), func(t *testing.T) {
					prefix := make([]datalog.Value, 0, n)
					parts := make([][]byte, 0, n)
					for _, name := range order[:n] {
						prefix = append(prefix, typed[name])
						parts = append(parts, encodedPart[name])
					}

					wantStart, wantEnd := encoder.EncodePrefixRange(index, parts...)

					run, err := encoder.EncodeScanBound(ScanBound{Index: index, Prefix: prefix})
					gotStart := run.Start
					gotEnd := run.End
					require.NoError(t, err)
					require.Equal(t, wantStart, gotStart, "start bytes")
					require.Equal(t, wantEnd, gotEnd, "end bytes")
				})
			}
		}
	}
}

// TestScanBoundContainsItsDatomKey ties the bound to the real key layout rather
// than to the test's own table. TestScanBoundEncodesAsPrefixRange compares two
// orderings that both feed EncodePrefixRange, so a misreading of the layout
// shared by both tables — or a wrong exclusive end computed the same way twice —
// would agree with itself. Here the bound is checked against keys that
// encodeKeyWithParts actually laid down, in both directions:
//
//   - containment: the bound built from a datom's own components starts that
//     datom's key, and the key sorts inside the range;
//   - exclusion: a datom differing in the deepest bound component is not under
//     the prefix, so its key sorts outside the range. With the carry fixture
//     this is what a non-carrying end key fails.
func TestScanBoundContainsItsDatomKey(t *testing.T) {
	// Compression off, so EncodeKey's value encoding and encodeValueForSearch
	// take the same branch and the comparison is meaningful.
	encoder := &BinaryKeyEncoder{}

	entity := datalog.NewIdentity("scanbound:entity")
	attr := datalog.NewKeyword(":scanbound/attr")
	tx := datalog.ElementID{Lamport: 0x1234, ReplicaID: 0x5678}

	// Alternates for the exclusion assertion. None is a byte extension of the
	// component it replaces, so this table reports the end-key carry rather
	// than the missing V length delimiter, which has its own reproducer.
	otherEntity := datalog.NewIdentity("scanbound:other-entity")
	otherAttr := datalog.NewKeyword(":scanbound/other")
	otherTx := datalog.ElementID{Lamport: 0x4321, ReplicaID: 0x8765}

	for _, fixture := range scanBoundValueFixtures {
		value := fixture.value
		datom := datalog.Datom{E: entity, A: attr, V: value, Tx: tx}
		typed := map[string]datalog.Value{"E": entity, "A": attr, "V": value, "Tx": tx}

		for _, index := range Indices {
			order := scanBoundComponentOrder[index]
			key := encoder.EncodeKey(index, &datom)

			for n := 0; n <= len(order); n++ {
				t.Run(fmt.Sprintf("%s/%s/prefix%d", fixture.name, index.String(), n), func(t *testing.T) {
					prefix := make([]datalog.Value, 0, n)
					for _, name := range order[:n] {
						prefix = append(prefix, typed[name])
					}

					run, err := encoder.EncodeScanBound(ScanBound{Index: index, Prefix: prefix})
					start := run.Start
					end := run.End
					require.NoError(t, err)
					require.True(t, bytes.HasPrefix(key, start),
						"bound start %x must prefix the datom's key %x", start, key)
					require.Negative(t, bytes.Compare(key, end),
						"datom's key %x must sort below the range end %x", key, end)

					if n == 0 {
						return
					}

					deepest := order[n-1]
					neighbour := datom
					switch deepest {
					case "E":
						neighbour.E = otherEntity
					case "A":
						neighbour.A = otherAttr
					case "V":
						neighbour.V = fixture.other
					case "Tx":
						neighbour.Tx = otherTx
					}
					neighbourKey := encoder.EncodeKey(index, &neighbour)
					require.True(t,
						bytes.Compare(neighbourKey, start) < 0 || bytes.Compare(neighbourKey, end) >= 0,
						"a datom differing in %s is not under the prefix, so its key must sort "+
							"outside [start, end); start=%x end=%x key=%x",
						deepest, start, end, neighbourKey)
				})
			}
		}
	}
}

// TestIndexSelectionEventReportsBound pins the observability contract of the
// typed seam: pattern/index-selection reports the run it addressed — the index,
// the positions bound in that index's component order, and the values bound to
// them. It must never report an encoded key range; that is one backend's
// projection of the bound, which a handler cannot interpret and a typed backend
// never produces.
func TestIndexSelectionEventReportsBound(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var events []annotations.Event
			opts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:              t.TempDir(),
				PlannerOptions:    &opts,
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

			// Every scan-reporting event must describe its run the same way, so
			// the assertions below are shared: no encoded range, and parallel
			// position and value slices naming what the run binds.
			requireBound := func(t *testing.T, e annotations.Event, index IndexType, positions []string, values []datalog.Value) {
				t.Helper()
				// The IndexType and the bound values themselves, not renderings of
				// them: the producer carries values and the formatter renders, so
				// an emitter that went back to flattening fails here rather than
				// passing on strings that happen to read the same.
				require.Equal(t, index, e.Data[annotations.KeyIndex])
				require.Equal(t, positions, e.Data[annotations.KeyBound])
				require.Equal(t, values, e.Data[annotations.KeyBoundValues])
				for _, byteField := range []string{"scan.start", "scan.end", "start", "end", "value_bytes"} {
					require.NotContains(t, e.Data, byteField,
						"an encoded key range is one backend's projection, not an annotation")
				}
			}

			t.Run("A bound, unbound scan path", func(t *testing.T) {
				e := eventFor(t, "pattern/index-selection", `[:find ?e ?n :where [?e :person/name ?n]]`)
				requireBound(t, e, AETV, []string{"A"},
					[]datalog.Value{datalog.NewKeyword(":person/name")})
			})

			t.Run("A and V bound, V-validation path", func(t *testing.T) {
				e := eventFor(t, "v-validation/open-scan", `[:find ?e :where [?e :person/name "Alice"]]`)
				requireBound(t, e, AVET, []string{"A", "V"},
					[]datalog.Value{datalog.NewKeyword(":person/name"), "Alice"})
			})

			t.Run("nothing bound", func(t *testing.T) {
				e := eventFor(t, "pattern/index-selection", `[:find ?e ?a ?v :where [?e ?a ?v]]`)
				require.Empty(t, e.Data[annotations.KeyBound],
					"a whole-index scan binds no component, and says so rather than omitting the field")
				require.Empty(t, e.Data[annotations.KeyBoundValues])
				require.Contains(t, e.Data, annotations.KeyBound,
					"the field is present even when the run binds nothing")
			})

			// The producer and the formatter are pinned separately — here they
			// meet. Separate pins can agree on a payload shape that neither
			// side produces: a formatter reading keys no emitter writes still
			// renders, and every scan line comes out "bound: ?".
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
		})
	}
}

// encodeScanBoundForTest renders a bound as the byte range it addresses, for
// the tests that assert on key layout rather than on the bound's components.
// It lives here rather than beside those tests because they are spread across
// both Badger-only and portable test files.
func encodeScanBoundForTest(t *testing.T, m *PatternMatcher, bound ScanBound) (start, end []byte) {
	t.Helper()
	run, err := m.store.Encoder().EncodeScanBound(bound)
	if err != nil {
		t.Fatalf("encode scan bound on %v: %v", bound.Index, err)
	}
	return run.Start, run.End
}

// TestScanBoundOverlongPrefixRejected pins loud failure over silent
// truncation: an index has exactly four components, so a fifth prefix element
// binds nothing and is a caller bug.
func TestScanBoundOverlongPrefixRejected(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	entity := datalog.NewIdentity("scanbound:entity")
	attr := datalog.NewKeyword(":scanbound/attr")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	_, err := encoder.EncodeScanBound(ScanBound{
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
	_, err := encoder.EncodeScanBound(ScanBound{
		Index:  EAVT,
		Prefix: []datalog.Value{"not-an-identity"},
	})
	require.Error(t, err)

	// ATEV's second component is Tx, inhabited only by ElementID.
	attr := datalog.NewKeyword(":scanbound/attr")
	_, err = encoder.EncodeScanBound(ScanBound{
		Index:  ATEV,
		Prefix: []datalog.Value{attr, int64(7)},
	})
	require.Error(t, err)
}

// TestScanBoundUnknownIndexRejected pins the taxonomy's default arm.
func TestScanBoundUnknownIndexRejected(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	_, err := encoder.EncodeScanBound(ScanBound{Index: IndexType(200)})
	require.Error(t, err)
}

// TestScanBoundMembershipFollowsThePayloadClassification pins every entry of
// PayloadIsFixedWidth through the field that consumes it. A bound ending on V
// produces an exact run when the payload's length follows from its tag and a
// length-narrowed one when it does not, so moving any tag between the two arms
// of the classification reds this table.
//
// This is the pin a behavioural test cannot be. Answering "variable" for a
// fixed-width tag computes size + tail to exactly the length the exact arm
// admits, so a scan returns the same datoms under either rule and only the run's
// own membership records which one produced them. In the other direction a
// behavioural test needs a fixture whose value is a byte prefix of another
// stored value, which is a property of the fixture rather than of the
// classification.
func TestScanBoundMembershipFollowsThePayloadClassification(t *testing.T) {
	const threshold = 32
	encoder := &BinaryKeyEncoder{CompressionThreshold: threshold}
	attr := datalog.NewKeyword(":classify/attr")

	// Tier 3 needs a compressed form above maxKeyValueSize. The incompressible
	// region keeps it above; the compressible one makes Compress report a net
	// benefit, so it returns bytes rather than falling back to the raw tag.
	oversize := make([]byte, 80*1024)
	_, err := rand.New(rand.NewSource(1)).Read(oversize)
	require.NoError(t, err)
	oversize = append(oversize, make([]byte, 80*1024)...)

	// fixed is a deliberate second, independent copy of PayloadIsFixedWidth's
	// two arms. Reading the production classification here instead would make
	// the assertion self-consistent: encodeBoundEndpoint decides exactness by
	// calling that same function, so both sides of the comparison would move
	// together and a tag switched between arms would pass.
	cases := []struct {
		name  string
		tag   datalog.ValueType
		fixed bool
		value datalog.Value
	}{
		{"string", datalog.TypeString, false, "abc"},
		{"int", datalog.TypeInt, true, int64(7)},
		{"float", datalog.TypeFloat, true, 1.5},
		{"bool", datalog.TypeBool, true, true},
		{"time", datalog.TypeTime, true, time.Unix(1_700_000_000, 0).UTC()},
		{"bytes", datalog.TypeBytes, false, []byte("abc")},
		{"reference", datalog.TypeReference, true, datalog.NewIdentity("classify:entity")},
		{"keyword", datalog.TypeKeyword, false, datalog.NewKeyword(":classify/value")},
		{"symbol", datalog.TypeSymbol, false, datalog.NewSymbol("classify-value")},
		{"elementid", datalog.TypeElementID, true, datalog.ElementID{Lamport: 3, ReplicaID: 4}},
		{"compressed-string", datalog.TypeCompressedString, false, strings.Repeat("compressible-", 8)},
		{"compressed-bytes", datalog.TypeCompressedBytes, false, bytes.Repeat([]byte("0123456789"), 8)},
		{"hashed-string", datalog.TypeHashedString, true, string(oversize)},
		{"hashed-bytes", datalog.TypeHashedBytes, true, oversize},
	}

	// Enumerate the domain: a tag with no case here would be classified by
	// PayloadIsFixedWidth and pinned by nothing.
	covered := map[datalog.ValueType]bool{}
	for _, tc := range cases {
		covered[tc.tag] = true
	}
	for tag := datalog.TypeString; tag <= datalog.TypeHashedBytes; tag++ {
		require.True(t, covered[tag], "value type %d has no case in this table", tag)
	}
	require.Len(t, covered, int(datalog.TypeHashedBytes)+1,
		"the value-type block grew without this table")

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A fixture that stops landing on the tag it names would exercise a
			// different arm and say nothing about this one.
			gotTag, _, _ := datalog.EncodeValue(tc.value, threshold)
			require.Equal(t, tc.tag, gotTag, "fixture no longer encodes to the tag it names")

			require.Equal(t, tc.fixed, datalog.PayloadIsFixedWidth(tc.tag),
				"the classification moved this tag between arms")

			// AVET orders V second, so this bound ends on V and every component
			// behind it is fixed width.
			run, err := encoder.EncodeScanBound(ScanBound{
				Index:  AVET,
				Prefix: []datalog.Value{attr, tc.value},
			})
			require.NoError(t, err)
			require.Equal(t, tc.fixed, run.Membership.exact,
				"a run is exact exactly when the bound V's payload width follows from its tag")
		})
	}
}
