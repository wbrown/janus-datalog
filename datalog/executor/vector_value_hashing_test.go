package executor

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

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

func TestSignedZeroHashesConsistentlyAcrossSetOperations(t *testing.T) {
	positive := float64(0)
	negative := math.Copysign(0, -1)
	if !datalog.ValuesEqual(positive, negative) {
		t.Fatal("precondition: signed zero values must be equal")
	}

	positiveKey := NewTupleKeyFull(Tuple{positive})
	negativeKey := NewTupleKeyFull(Tuple{negative})
	if positiveKey.hash != negativeKey.hash {
		t.Fatalf("equal signed zeros must hash equally: %d != %d", positiveKey.hash, negativeKey.hash)
	}

	value := datalog.NewSymbol("?value")
	if got := NewMaterializedRelation(
		[]query.Symbol{value},
		[]Tuple{{positive}, {negative}},
	).Size(); got != 1 {
		t.Fatalf("signed zeros must deduplicate to one tuple, got %d", got)
	}

	left := NewMaterializedRelation(
		[]query.Symbol{value, datalog.NewSymbol("?left")},
		[]Tuple{{positive, "left"}},
	)
	right := NewMaterializedRelation(
		[]query.Symbol{value, datalog.NewSymbol("?right")},
		[]Tuple{{negative, "right"}},
	)
	if got := HashJoin(left, right, []query.Symbol{value}).Size(); got != 1 {
		t.Fatalf("signed-zero join keys must match, got %d rows", got)
	}

	grouped := executeGroupedAggregation(
		NewMaterializedRelation(
			[]query.Symbol{value},
			[]Tuple{{positive}, {negative}},
		),
		[]query.Symbol{value},
		[]query.FindAggregate{{Function: "count", Arg: value}},
	)
	if got := grouped.Size(); got != 1 {
		t.Fatalf("signed zeros must form one aggregate group, got %d", got)
	}
}

func TestValuesEqualImpliesEqualTupleHashRandomized(t *testing.T) {
	random := rand.New(rand.NewSource(0x51a9ed))
	for caseIndex := 0; caseIndex < 2_000; caseIndex++ {
		var left, right interface{}
		switch random.Intn(7) {
		case 0:
			value := int64(random.Intn(2_000) - 1_000)
			left, right = value, int(value)
		case 1:
			value := make([]byte, 1+random.Intn(32))
			_, _ = random.Read(value)
			left, right = value, append([]byte(nil), value...)
		case 2:
			values := []interface{}{int64(random.Intn(50)), fmt.Sprintf("v-%d", random.Intn(50))}
			left = values
			right = []interface{}{values[0], values[1]}
		case 3:
			values := []string{fmt.Sprintf("v-%d", random.Intn(50)), "tail"}
			left = values
			right = []interface{}{values[0], values[1]}
		case 4:
			instant := time.Unix(int64(random.Intn(10_000)), int64(random.Intn(1_000))).UTC()
			left = instant
			right = instant.In(time.FixedZone("random", random.Intn(12)*3600))
		case 5:
			left = float64(0)
			right = math.Copysign(0, -1)
		case 6:
			seed := fmt.Sprintf("identity-%d", random.Intn(100))
			left, right = datalog.NewIdentity(seed), datalog.NewIdentity(seed)
		}
		if !datalog.ValuesEqual(left, right) {
			t.Fatalf("case %d: generated values are not equal: %T(%v), %T(%v)",
				caseIndex, left, left, right, right)
		}
		leftHash := NewTupleKeyFull(Tuple{left}).hash
		rightHash := NewTupleKeyFull(Tuple{right}).hash
		if leftHash != rightHash {
			t.Fatalf("case %d: equal values hash differently: %d != %d (%T, %T)",
				caseIndex, leftHash, rightHash, left, right)
		}
	}
}
