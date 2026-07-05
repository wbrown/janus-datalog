package executor

import (
	"fmt"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Vector values ([]interface{}, and typed slices per the equality layer's
// polymorphic slice comparison) are datalog values: cardinality-vector reads
// bind them. These tests pin content-based hashing — equal vectors must
// deduplicate, join, and hash identically, regardless of representation.
// See docs/bugs/resolved/BUG_VECTOR_VALUES_DEGENERATE_HASHING.md.

// Two tuples whose only difference is the identity (not content) of their
// vector values must collapse under set semantics.
func TestEqualVectorValuesDeduplicate(t *testing.T) {
	v := datalog.NewSymbol("?v")
	vecA := []interface{}{"a", int64(1)}
	vecB := []interface{}{"a", int64(1)} // equal content, distinct backing array

	rel := NewMaterializedRelation([]query.Symbol{v}, []Tuple{{vecA}, {vecB}})
	if rel.Size() != 1 {
		t.Fatalf("equal vector values must deduplicate: expected 1 tuple, got %d", rel.Size())
	}
}

// Equal vector values must match across a hash join's build and probe sides.
func TestEqualVectorValuesJoin(t *testing.T) {
	x := datalog.NewSymbol("?x")
	v := datalog.NewSymbol("?v")
	y := datalog.NewSymbol("?y")

	left := NewMaterializedRelation([]query.Symbol{x, v}, []Tuple{
		{"left-row", []interface{}{"a", int64(1)}},
	})
	right := NewMaterializedRelation([]query.Symbol{v, y}, []Tuple{
		{[]interface{}{"a", int64(1)}, "right-row"},
	})

	joined := left.HashJoin(right, []query.Symbol{v})
	if joined.Size() != 1 {
		t.Fatalf("equal vector join keys must match: expected 1 row, got %d", joined.Size())
	}
}

// Distinct vector values must hash with reasonable spread — content-based
// hashing, not identity- or address-based. Degenerate constant hashing
// collapses every TupleKeyMap keyed on vectors into a single linear
// equality chain.
func TestDistinctVectorValuesHashWithSpread(t *testing.T) {
	hashes := make(map[uint64]bool)
	for i := 0; i < 100; i++ {
		vec := []interface{}{int64(i), "tag"}
		key := NewTupleKeyFull(Tuple{vec})
		hashes[key.hash] = true
	}
	// Content hashing should give ~100 distinct hashes; the address-constant
	// default gives exactly 1. Any threshold between rules out the defect
	// without demanding a perfect hash.
	if len(hashes) < 90 {
		t.Fatalf("expected ≥90 distinct hashes for 100 distinct vectors, got %d (degenerate hashing)", len(hashes))
	}
}

// The value domain is closed: a type outside it reaching the hash layer is
// a layering violation (e.g. a pulled map — result presentation, never a
// relational value) and must fail loudly naming the type, mirroring
// datalog.Type()'s convention — never hash by address (silent wrong answers
// in joins and deduplication).
func TestNonValueTypeHashPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected value-domain panic for map value in hash layer")
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, "not a datalog value") {
			t.Fatalf("panic should name the value-domain violation, got: %v", r)
		}
	}()
	NewTupleKeyFull(Tuple{map[string]interface{}{"k": "v"}})
}

// Typed slices are part of the value domain by the equality layer's own
// definition: ValuesEqual handles slices polymorphically (reflect.Slice,
// element-wise), so []string{"a"} equals []interface{}{"a"} — and the
// storage vector-matching path (matchVectorWithBindings) puts []string
// values into relation tuples. The consistency invariant (ValuesEqual ⇒
// equal hashes) therefore requires hashValue to hash every slice kind via
// the same generic element iteration — including across representations.
func TestTypedSliceValuesHashLikeVectors(t *testing.T) {
	typed := []string{"a", "b"}
	generic := []interface{}{"a", "b"}

	if !datalog.ValuesEqual(typed, generic) {
		t.Fatal("precondition: ValuesEqual treats []string and []interface{} with equal elements as equal")
	}

	keyTyped := NewTupleKeyFull(Tuple{typed})
	keyGeneric := NewTupleKeyFull(Tuple{generic})
	if keyTyped.hash != keyGeneric.hash {
		t.Fatalf("cross-representation equal slices must hash equally: %d != %d", keyTyped.hash, keyGeneric.hash)
	}

	// And distinct typed slices must not collapse onto one hash.
	hashes := make(map[uint64]bool)
	for i := 0; i < 100; i++ {
		key := NewTupleKeyFull(Tuple{[]string{"tag", string(rune('a' + i%26)), string(rune('0' + i/26))}})
		hashes[key.hash] = true
	}
	if len(hashes) < 90 {
		t.Fatalf("expected ≥90 distinct hashes for 100 distinct typed slices, got %d", len(hashes))
	}
}

// The hash-consistency property behind both cases: ValuesEqual(a, b) must
// imply equal tuple-key hashes. Read the hashes directly (same package).
func TestEqualVectorValuesHashConsistently(t *testing.T) {
	vecA := []interface{}{"a", int64(1), []interface{}{"nested"}}
	vecB := []interface{}{"a", int64(1), []interface{}{"nested"}}

	if !datalog.ValuesEqual(vecA, vecB) {
		t.Fatal("precondition: vectors must be ValuesEqual")
	}

	keyA := NewTupleKeyFull(Tuple{vecA})
	keyB := NewTupleKeyFull(Tuple{vecB})
	if keyA.hash != keyB.hash {
		t.Fatalf("ValuesEqual values must hash equally: %d != %d", keyA.hash, keyB.hash)
	}
}
