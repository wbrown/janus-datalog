// Tests for tuple-key collision behavior in NOT/OR/union dedup paths.
//
// Background: notOrTupleKey (relation_ops.go) builds dedup keys via
// fmt.Sprintf("%v",...) joined by "|". This stringification is not
// injective — distinct tuples can map to the same key — and the
// dedup logic silently drops valid distinct tuples or matches the
// wrong rows for anti-join filtering.
//
// These tests assert correct dedup semantics. They are expected to FAIL
// against the current notOrTupleKey-based implementation and PASS once
// the call sites in relation_ops.go and query_executor.go migrate to the
// existing TupleKey / TupleKeyMap primitive.
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
// that the existing TupleKey / TupleKeyMap primitive (the migration target)
// correctly distinguishes the same adversarial pairs that collide under the
// stringification-based key. This test should PASS today and continue to
// pass after migration.
//
// Exercises: tuple_key.go TupleKey, TupleKeyMap
func TestTupleKeyMap_DistinguishesAdversarialPairs(t *testing.T) {
	for _, tc := range adversarialTuplePairs {
		t.Run(tc.name, func(t *testing.T) {
			m := NewTupleKeyMap()
			ka := NewTupleKeyFull(tc.a)
			kb := NewTupleKeyFull(tc.b)

			m.Put(ka, struct{}{})
			if !m.Exists(ka) {
				t.Fatal("first key should exist after Put")
			}

			if m.Exists(kb) {
				t.Errorf("TupleKeyMap conflated distinct tuples %+v and %+v",
					tc.a, tc.b)
			}

			m.Put(kb, struct{}{})
			if !m.Exists(ka) || !m.Exists(kb) {
				t.Error("both keys should exist after both Puts")
			}
		})
	}
}

// TestTupleKeyMap_BytesAreDistinguishedByContent verifies that []byte values are
// keyed by content in a TupleKeyMap: equal content hashes equally (so it lands in
// the same bucket and is found), and different content does not match. hashValue
// has a dedicated []byte case that hashes by content (hashBytes). Without it,
// []byte fell through to a pointer-address hash, so equal slices landed in
// DIFFERENT buckets and the ValuesEqual fallback — which only resolves collisions
// WITHIN a bucket — never ran. That made byte-valued joins/dedup miss; it surfaced
// as resolved/BUG_VBOUND_BYTES_WRONG_RESULTS_UNDER_RACE (a join on a byte key
// dropped rows, only reliably under -race because the bogus address hash happened
// to collide otherwise).
//
// Exercises: tuple_key.go hashValue/TupleKey/TupleKeyMap with []byte values
func TestTupleKeyMap_BytesAreDistinguishedByContent(t *testing.T) {
	a := []byte{1, 2, 3}
	bSameContent := []byte{1, 2, 3} // distinct slice, equal content
	cDifferent := []byte{1, 2, 4}

	ka := NewTupleKeyFull(Tuple{a})
	kb := NewTupleKeyFull(Tuple{bSameContent})
	kc := NewTupleKeyFull(Tuple{cDifferent})

	// Equal content must hash equally — landing in the same bucket by design,
	// not by an accidental pointer-address collision.
	if ka.hash != kb.hash {
		t.Fatalf("equal []byte content must hash equally: got %d != %d", ka.hash, kb.hash)
	}

	m := NewTupleKeyMap()
	m.Put(ka, struct{}{})
	if !m.Exists(kb) {
		t.Error("equal-content []byte must be found")
	}
	if m.Exists(kc) {
		t.Error("different-content []byte must not match")
	}
}

// TestNotJoinClause_PipeInValue_E2E verifies that NOT-JOIN does not
// erroneously filter outer tuples whose join-key values collide under
// stringification with values matched by the inner clause.
//
// Setup:
//
//	a: (name="x|y", tag="z",   archived)
//	b: (name="x",   tag="y|z")
//
// Outer tuples for (?name, ?tag):
//
//	("x|y", "z")  — stringifies to "x|y|z"
//	("x",   "y|z") — also stringifies to "x|y|z"  (collision)
//
// The NOT-JOIN over [?name ?tag] checks whether ANY entity with the same
// (name, tag) is archived. Only the first combo matches an archived entity
// (a). With correct keying, the result is exactly ("x", "y|z") for entity b.
// With the stringification collision, both combos share a key — depending
// on iteration order, either both are filtered (0 results) or neither is
// (2 results).
//
// Exercises: query_executor.go filterWithNotJoinClause end-to-end
func TestNotJoinClause_PipeInValue_E2E(t *testing.T) {
	nameAttr := datalog.NewKeyword(":item/name")
	tagAttr := datalog.NewKeyword(":item/tag")
	archivedAttr := datalog.NewKeyword(":item/archived")

	a := datalog.NewIdentity("a")
	b := datalog.NewIdentity("b")

	datoms := []datalog.Datom{
		{E: a, A: nameAttr, V: "x|y", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: a, A: tagAttr, V: "z", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: a, A: archivedAttr, V: true, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},

		{E: b, A: nameAttr, V: "x", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: b, A: tagAttr, V: "y|z", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher, nil)

	// [:find ?name ?tag
	//  :where [?e :item/name ?name]
	//         [?e :item/tag ?tag]
	//         (not-join [?name ?tag]
	//                   [?other :item/name ?name]
	//                   [?other :item/tag ?tag]
	//                   [?other :item/archived true])]
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?name")},
			query.FindVariable{Symbol: datalog.NewSymbol("?tag")},
		},
		Where: []query.Clause{
			&query.DataPattern{Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: nameAttr},
				query.Variable{Name: datalog.NewSymbol("?name")},
			}},
			&query.DataPattern{Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: tagAttr},
				query.Variable{Name: datalog.NewSymbol("?tag")},
			}},
			&query.NotJoinClause{
				JoinVars: []query.Symbol{
					datalog.NewSymbol("?name"),
					datalog.NewSymbol("?tag"),
				},
				Clauses: []query.Clause{
					&query.DataPattern{Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?other")},
						query.Constant{Value: nameAttr},
						query.Variable{Name: datalog.NewSymbol("?name")},
					}},
					&query.DataPattern{Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?other")},
						query.Constant{Value: tagAttr},
						query.Variable{Name: datalog.NewSymbol("?tag")},
					}},
					&query.DataPattern{Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?other")},
						query.Constant{Value: archivedAttr},
						query.Constant{Value: true},
					}},
				},
			},
		},
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	if result.Size() != 1 {
		t.Fatalf("expected 1 result (name=x, tag=y|z) for entity b, got %d", result.Size())
	}

	tuple := result.Get(0)
	name, _ := tuple[0].(string)
	tag, _ := tuple[1].(string)
	if name != "x" || tag != "y|z" {
		t.Errorf("expected (x, y|z), got (%q, %q)", name, tag)
	}
}
