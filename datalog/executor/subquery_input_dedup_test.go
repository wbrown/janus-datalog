package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestGetUniqueInputCombinations_NoStringKeyCollisions is a regression test for
// BUG_SUBQUERY_INPUT_DEDUP_STRING_COLLISION.
//
// Correlated-subquery input dedup must treat two input combinations as equal
// only when every value compares equal under typed value identity — never by
// string rendering. The old fmt.Sprintf("%v")+"|" key is not injective: distinct
// combinations stringify to the same key and collapse into one subquery
// execution, silently dropping results for the others.
func TestGetUniqueInputCombinations_NoStringKeyCollisions(t *testing.T) {
	syms := []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")}

	cases := []struct {
		name   string
		tuples []Tuple
	}{
		// Delimiter ambiguity: "a|b"+"c" vs "a"+"b|c" both render "a|b|c".
		{"delimiter", []Tuple{{"a|b", "c"}, {"a", "b|c"}}},
		// Cross-type: int64(5) vs string "5" both render "5".
		{"int64-vs-string", []Tuple{{int64(5), "x"}, {"5", "x"}}},
		// Cross-type: bool true vs string "true".
		{"bool-vs-string", []Tuple{{true, "x"}, {"true", "x"}}},
		// Cross-type: float64 3.14 vs string "3.14".
		{"float-vs-string", []Tuple{{float64(3.14), "x"}, {"3.14", "x"}}},
		// []byte values must be compared by content, not pointer identity.
		{"bytes-distinct", []Tuple{{[]byte("ab"), "x"}, {[]byte("cd"), "x"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rel := NewMaterializedRelation(syms, tc.tuples)
			combos, err := getUniqueInputCombinations(rel, syms)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(combos) != 2 {
				t.Fatalf("expected 2 distinct combinations, got %d: %v "+
					"(distinct typed tuples collapsed by string-key collision)", len(combos), combos)
			}
		})
	}
}

// TestGetUniqueInputCombinations_SourceMarkerDoesNotCollide ensures a source
// marker ($) in the input symbols — constant execution context, not in the outer
// relation — neither breaks dedup of the real data values nor causes accidental
// collisions.
func TestGetUniqueInputCombinations_SourceMarkerDoesNotCollide(t *testing.T) {
	source := datalog.NewSymbol("$")
	if !source.IsSource() {
		t.Fatalf("expected %q to be a source symbol", source)
	}
	inputSymbols := []query.Symbol{source, datalog.NewSymbol("?a")}

	// Outer relation contains only the data symbol ?a; the source is not a symbol.
	rel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?a")},
		[]Tuple{{int64(5)}, {"5"}, {int64(5)}}, // int64(5) and "5" are distinct; the second int64(5) is a real duplicate
	)

	combos, err := getUniqueInputCombinations(rel, inputSymbols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(combos) != 2 {
		t.Fatalf("expected 2 distinct combinations (int64(5) and \"5\"; the repeated int64(5) deduped), got %d: %v",
			len(combos), combos)
	}
	// Every combination must carry the source marker through unchanged.
	for _, c := range combos {
		if c[source] != source {
			t.Fatalf("source marker not preserved in combination: %v", c)
		}
	}
}
