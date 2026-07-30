package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestSubqueryInputProjection_NoStringKeyCollisions is a regression test for
// BUG_SUBQUERY_INPUT_DEDUP_STRING_COLLISION.
//
// Correlated-subquery input dedup must treat two input combinations as equal
// only when every value compares equal under typed value identity — never by
// string rendering. The old fmt.Sprintf("%v")+"|" key is not injective: distinct
// combinations stringify to the same key and collapse into one subquery
// execution, silently dropping results for the others.
//
// Unique input combinations ARE the outer relation projected onto the data
// input symbols: Project's set semantics is the dedup, backed by the same
// TupleKeyMap typed-value-identity machinery.
func TestSubqueryInputProjection_NoStringKeyCollisions(t *testing.T) {
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
			dataSymbols := filterSourceSymbols(syms)
			combos, err := rel.Project(dataSymbols)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if combos.Size() != 2 {
				t.Fatalf("expected 2 distinct combinations, got %d: %s "+
					"(distinct typed tuples collapsed by string-key collision)", combos.Size(), combos.Table())
			}
		})
	}
}

// TestSubqueryInputProjection_SourceMarkerExcluded ensures a source marker
// ($) in the input symbols — execution context, never data — is dropped
// before projecting: it neither appears in the projected combinations nor
// breaks dedup of the real data values.
func TestSubqueryInputProjection_SourceMarkerExcluded(t *testing.T) {
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

	// The source marker is execution context, not a relation symbol: it must
	// be excluded from the data symbols before projecting, never carried
	// through as part of a combination.
	dataSymbols := filterSourceSymbols(inputSymbols)
	if len(dataSymbols) != 1 || dataSymbols[0] != datalog.NewSymbol("?a") {
		t.Fatalf("expected filterSourceSymbols to drop the source marker, got %v", dataSymbols)
	}

	combos, err := rel.Project(dataSymbols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if combos.Size() != 2 {
		t.Fatalf("expected 2 distinct combinations (int64(5) and \"5\"; the repeated int64(5) deduped), got %d: %s",
			combos.Size(), combos.Table())
	}
}
