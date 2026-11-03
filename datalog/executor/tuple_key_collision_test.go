// Tests for tuple-key collision behavior in NOT/OR/union dedup paths.
//
// A dedup key built by joining fmt.Sprintf("%v",...) results with "|" is not
// injective: distinct tuples can map to the same key, silently dropping valid
// tuples or matching the wrong anti-join tuples.
//
// These tests pin the TupleKey/TupleKeyMap semantics that avoid this and
// prevent the string-key collision behavior from returning.
//
// Each adversarial pair is constructed so that
//   fmt.Sprintf("%v", a[0]) + "|" + fmt.Sprintf("%v", a[1])
// produces the same string for both tuples, while datalog.ValuesEqual
// correctly distinguishes them.

package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// adversarialTuplePair is two distinct tuples whose stringified forms
// (under fmt.Sprintf("%v",...) joined by "|") are identical.
type adversarialTuplePair struct {
	name string
	a, b Tuple
}

var adversarialTuplePairs = []adversarialTuplePair{
	{
		// "a|b" + "|" + "c" == "a" + "|" + "b|c"
		name: "pipe-shifts-delimiter",
		a:    Tuple{"a|b", "c"},
		b:    Tuple{"a", "b|c"},
	},
	{
		// fmt.Sprintf("%v", int64(5)) == "5"
		name: "int-vs-string-numeric",
		a:    Tuple{int64(5), "x"},
		b:    Tuple{"5", "x"},
	},
	{
		// fmt.Sprintf("%v", true) == "true"
		name: "bool-vs-string-true",
		a:    Tuple{true, "x"},
		b:    Tuple{"true", "x"},
	},
	{
		// fmt.Sprintf("%v", 3.14) == "3.14"
		name: "float-vs-string-numeric",
		a:    Tuple{float64(3.14), "x"},
		b:    Tuple{"3.14", "x"},
	},
}

// TestGetUniqueCombinations_NoCollisionAcrossTupleBoundaries verifies that
// distinct tuples whose stringified forms collide under "|" concatenation
// are still returned as distinct combinations.
//
// Exercises: relation_ops.go getUniqueCombinations
func TestGetUniqueCombinations_NoCollisionAcrossTupleBoundaries(t *testing.T) {
	syms := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

	for _, tc := range adversarialTuplePairs {
		t.Run(tc.name, func(t *testing.T) {
			rel := NewMaterializedRelation(syms, []Tuple{tc.a, tc.b})
			combos, err := getUniqueCombinations(rel, syms)
			if err != nil {
				t.Fatalf("getUniqueCombinations failed: %v", err)
			}
			if len(combos) != 2 {
				t.Errorf("expected 2 distinct combos for %+v and %+v, got %d: %+v",
					tc.a, tc.b, len(combos), combos)
			}
		})
	}
}

// TestUnionRelations_NoCollisionAcrossTupleBoundaries verifies that the OR
// branch dedup (unionRelations) preserves distinct tuples whose stringified
// forms collide.
//
// Exercises: relation_ops.go unionRelations
func TestUnionRelations_NoCollisionAcrossTupleBoundaries(t *testing.T) {
	syms := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	opts := ExecutorOptions{}

	for _, tc := range adversarialTuplePairs {
		t.Run(tc.name, func(t *testing.T) {
			rel1 := NewMaterializedRelation(syms, []Tuple{tc.a})
			rel2 := NewMaterializedRelation(syms, []Tuple{tc.b})
			union := unionRelations([]Relation{rel1, rel2}, syms, opts)
			if union.Size() != 2 {
				t.Errorf("expected 2 distinct tuples in union of %+v and %+v, got %d",
					tc.a, tc.b, union.Size())
			}
		})
	}
}

// TestTupleKeyMap_DistinguishesAdversarialPairs is a baseline test verifying
// that TupleKeyMap correctly distinguishes the same adversarial pairs that
// collide under the stringification-based key.
//
// Exercises: tuple_key.go TupleKeyMap
func TestTupleKeyMap_DistinguishesAdversarialPairs(t *testing.T) {
	for _, tc := range adversarialTuplePairs {
		t.Run(tc.name, func(t *testing.T) {
			positions := allPositions(len(tc.a))
			m := NewTupleKeyMap(positions, positions, 0)

			m.Put(tc.a, struct{}{})
			if _, ok := m.Get(tc.a); !ok {
				t.Fatal("first key should exist after Put")
			}

			if _, ok := m.Get(tc.b); ok {
				t.Errorf("TupleKeyMap conflated distinct tuples %+v and %+v",
					tc.a, tc.b)
			}

			m.Put(tc.b, struct{}{})
			if m.Len() != 2 {
				t.Errorf("both keys should exist after both Puts, held %d", m.Len())
			}
		})
	}
}

// TestTupleKeyMap_BytesAreDistinguishedByContent verifies that []byte values are
// keyed by content in a TupleKeyMap: equal content hashes equally (so it lands in
// the same slot run and is found), and different content does not match.
// HashValue has a dedicated []byte case that hashes by content (hashBytes).
// Without it, []byte fell through to a pointer-address hash, so equal slices
// hashed to DIFFERENT slots and the ValuesEqual comparison — which only runs on
// the slots a hash reaches — never ran. That made byte-valued joins/dedup miss;
// it surfaced as resolved/BUG_VBOUND_BYTES_WRONG_RESULTS_UNDER_RACE (a join on a
// byte key dropped tuples, only reliably under -race because the bogus address
// hash happened to collide otherwise).
//
// Exercises: datalog.HashValue and tuple_key.go's TupleKeyMap with []byte values
func TestTupleKeyMap_BytesAreDistinguishedByContent(t *testing.T) {
	a := []byte{1, 2, 3}
	bSameContent := []byte{1, 2, 3} // distinct slice, equal content
	cDifferent := []byte{1, 2, 4}
	positions := allPositions(1)

	// Equal content must hash equally — reaching the same slots by design,
	// not by an accidental pointer-address collision.
	if ha, hb := hashTuplePositions(Tuple{a}, positions), hashTuplePositions(Tuple{bSameContent}, positions); ha != hb {
		t.Fatalf("equal []byte content must hash equally: got %d != %d", ha, hb)
	}

	m := NewTupleKeyMap(positions, positions, 0)
	m.Put(Tuple{a}, struct{}{})
	if _, ok := m.Get(Tuple{bSameContent}); !ok {
		t.Error("equal-content []byte must be found")
	}
	if _, ok := m.Get(Tuple{cDifferent}); ok {
		t.Error("different-content []byte must not match")
	}
}
