package datalog

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Reproducer for BUGS_MIXED_TYPE_COMPARISON_ORDERING: CompareValues returned -1
// for every type mismatch, so for mixed-type pairs BOTH CompareValues(a,b) < 0
// and CompareValues(b,a) < 0 could be true — non-antisymmetric, which violates
// the strict-weak-ordering contract sort.Slice/min/max/order-by depend on.
//
// The fix gives CompareValues a custom type-rank total ordering: numeric
// (int/float together) < bool < time < string < bytes < keyword < symbol <
// identity < elementID; same-rank values compare by value, different-rank by
// rank. These tests pin the comparator laws (antisymmetry, sort-safety,
// predicate consistency) rather than any single direction outcome.

// mixedTypeValues spans every rank class, for the antisymmetry matrix.
func mixedTypeValues() []Value {
	return []Value{
		nil,
		int64(-5),
		int64(42),
		3.14,
		true,
		false,
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		"apple",
		"zzz",
		[]byte{0x01, 0x02},
		NewKeyword(":k/one"),
		NewSymbol("sym"),
		NewIdentity("ent-1"),
		ElementID{Lamport: 1, ReplicaID: 1},
	}
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

// TestCompareValues_Antisymmetric pins cmp(a,b) == -cmp(b,a) across every
// ordered pair of mixed types. This is the core defect: previously many pairs
// had cmp(a,b) == cmp(b,a) == -1.
func TestCompareValues_Antisymmetric(t *testing.T) {
	vals := mixedTypeValues()
	for i, a := range vals {
		for j, b := range vals {
			ab := sign(CompareValues(a, b))
			ba := sign(CompareValues(b, a))
			require.Equalf(t, ab, -ba,
				"antisymmetry violated: CompareValues(vals[%d]=%v, vals[%d]=%v)=%d but reverse=%d",
				i, a, j, b, ab, ba)
		}
	}
}

// TestCompareValues_EqualOnlyWithinSameValue pins that cmp(a,b)==0 implies the
// values are equal under ValuesEqual (so distinct-type pairs never compare equal
// by accident).
func TestCompareValues_ZeroIsEquality(t *testing.T) {
	vals := mixedTypeValues()
	for _, a := range vals {
		for _, b := range vals {
			if CompareValues(a, b) == 0 {
				require.Truef(t, ValuesEqual(a, b),
					"cmp(%v,%v)==0 but ValuesEqual is false", a, b)
			}
		}
	}
}

// TestCompareValues_Transitive samples transitivity: for any a<b and b<c, a<c.
func TestCompareValues_Transitive(t *testing.T) {
	vals := mixedTypeValues()
	for _, a := range vals {
		for _, b := range vals {
			if sign(CompareValues(a, b)) >= 0 {
				continue
			}
			for _, c := range vals {
				if sign(CompareValues(b, c)) < 0 {
					require.Truef(t, sign(CompareValues(a, c)) < 0,
						"transitivity violated: %v < %v < %v but not %v < %v", a, b, c, a, c)
				}
			}
		}
	}
}

// TestCompareValues_SortStable confirms a heterogeneous slice sorts without
// violating Go's strict-weak-ordering contract (a non-antisymmetric comparator
// produces an unstable / order-dependent result; modern Go can even panic).
// Sorting twice from different initial orders must yield the same sequence.
func TestCompareValues_SortStable(t *testing.T) {
	base := mixedTypeValues()

	a := append([]Value(nil), base...)
	sort.SliceStable(a, func(i, j int) bool { return CompareValues(a[i], a[j]) < 0 })

	b := append([]Value(nil), base...)
	// Reverse b first so it starts from a different order.
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	sort.SliceStable(b, func(i, j int) bool { return CompareValues(b[i], b[j]) < 0 })

	require.Equal(t, len(a), len(b))
	for i := range a {
		require.Truef(t, ValuesEqual(a[i], b[i]) || (a[i] == nil && b[i] == nil),
			"sort order differs at %d: %v vs %v", i, a[i], b[i])
	}
}

// TestCompareValues_PredicateNotBothDirections is the direct user-visible
// reproducer from the bug report: a mixed-type pair must not satisfy `<` in both
// directions.
func TestCompareValues_PredicateNotBothDirections(t *testing.T) {
	less := func(x, y Value) bool { return CompareValues(x, y) < 0 }
	require.Falsef(t, less("zzz", int64(1)) && less(int64(1), "zzz"),
		"string<int and int<string both passed")
	require.Falsef(t, less(NewSymbol("s"), "a") && less("a", NewSymbol("s")),
		"symbol<string and string<symbol both passed")
}

// TestCompareValues_NumericCrossTypeUnchanged pins that int and float still
// compare by numeric value (not separated by rank) — the existing, correct
// behavior the custom rank must preserve.
func TestCompareValues_NumericCrossTypeUnchanged(t *testing.T) {
	require.Equal(t, -1, sign(CompareValues(int64(1), 2.5)))
	require.Equal(t, 1, sign(CompareValues(3.0, int64(2))))
	require.Equal(t, 0, sign(CompareValues(int64(2), 2.0)))
	// Int-width normalization unchanged.
	require.Equal(t, 0, sign(CompareValues(int32(5), int64(5))))
}

// TestCompareValues_KnownPairs pins concrete outcomes for representative
// pairs: same-type orderings, mixed numeric widths, and the specific rank
// direction between classes (numeric < bool < string), which the law tests
// above cannot pin.
func TestCompareValues_KnownPairs(t *testing.T) {
	tests := []struct {
		name     string
		left     interface{}
		right    interface{}
		expected int
	}{
		// Integer comparisons
		{"int64 less", int64(10), int64(20), -1},
		{"int64 equal", int64(20), int64(20), 0},
		{"int64 greater", int64(30), int64(20), 1},

		// Float comparisons
		{"float less", 10.5, 20.5, -1},
		{"float equal", 20.5, 20.5, 0},
		{"float greater", 30.5, 20.5, 1},

		// String comparisons
		{"string less", "Alice", "Bob", -1},
		{"string equal", "Bob", "Bob", 0},
		{"string greater", "Charlie", "Bob", 1},

		// Boolean comparisons
		{"bool false < true", false, true, -1},
		{"bool equal", true, true, 0},
		{"bool true > false", true, false, 1},

		// Mixed numeric types
		{"int to int64", int(10), int64(20), -1},
		{"int64 to float", int64(10), 20.5, -1},
		{"float to int", 10.5, int(10), 1},

		// Mixed types order by type rank (numeric=1 < bool=2 < string=4).
		{"string vs int", "test", 123, 1},    // string rank > numeric rank
		{"bool vs string", true, "test", -1}, // bool rank < string rank
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareValues(tt.left, tt.right)
			if result != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, result)
			}
		})
	}
}
