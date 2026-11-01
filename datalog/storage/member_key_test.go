package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// An add-wins set holds one member per value under datalog.ValuesEqual, so
// the key a value is held under is one key for every representation of the
// value: an instant in any location, or carrying a clock reading, keys as the
// instant; two byte slices of one content key together; an ElementID keys by
// value whether given as a value or a pointer; and every other value keys as
// itself. Two values that differ under ValuesEqual key apart, a byte slice and
// a string of the same bytes among them.
func TestMemberKey_IsOneKeyPerValueUnderValuesEqual(t *testing.T) {
	noon := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	dusk := time.Date(2026, 10, 29, 18, 0, 0, 0, time.UTC)
	east := time.FixedZone("east", 3*60*60)
	reading := time.Now()
	quire := datalog.NewIdentity("press:quire")
	sheet := datalog.NewIdentity("press:sheet")
	stamp := datalog.ElementID{Lamport: 7, ReplicaID: 3}

	same := []struct {
		name string
		a, b any
	}{
		{"an instant in UTC and in another zone", noon, noon.In(east)},
		{"an instant carrying a clock reading and the same instant without one", reading, reading.Round(0)},
		{"an instant carrying a clock reading and its UTC form", reading, reading.UTC()},
		{"two byte slices of one content", []byte("quire"), []byte("quire")},
		{"an ElementID by value and by pointer", stamp, &stamp},
		{"a string", "quire", "quire"},
		{"an int64", int64(7), int64(7)},
		{"an identity", quire, quire},
		{"a keyword", datalog.NewKeyword(":press/sewn"), datalog.NewKeyword(":press/sewn")},
	}
	for _, tc := range same {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, datalog.ValuesEqual(tc.a, tc.b), "the pair is one value under ValuesEqual")
			set := map[string]any{memberKey(tc.a): tc.a}
			_, held := set[memberKey(tc.b)]
			require.True(t, held, "a set keyed by %v is found by %v", tc.a, tc.b)
			require.Equal(t, memberKey(tc.a), memberKey(tc.b), "one key for both representations")
		})
	}

	apart := []struct {
		name string
		a, b any
	}{
		{"two instants", noon, dusk},
		{"two instants an hour apart in two zones", noon.In(east), noon.Add(time.Hour)},
		{"two byte slices", []byte("quire"), []byte("sheet")},
		{"a byte slice and a string of the same bytes", []byte("quire"), "quire"},
		{"two identities", quire, sheet},
		{"an int64 and a string", int64(7), "7"},
	}
	for _, tc := range apart {
		t.Run(tc.name, func(t *testing.T) {
			require.False(t, datalog.ValuesEqual(tc.a, tc.b), "the pair is two values under ValuesEqual")
			set := map[string]any{memberKey(tc.a): tc.a}
			_, held := set[memberKey(tc.b)]
			require.False(t, held, "a set keyed by %v is not found by %v", tc.a, tc.b)
		})
	}
}
