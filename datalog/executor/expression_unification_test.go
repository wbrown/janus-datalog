package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestEvaluateExpressionUnifyPassThroughAnswersItsSourcesCopyAnswer pins the
// RequiresCopy discipline on the unify pass-through: when every binding
// symbol is bound and the tuple survives unification unchanged, the result
// answers the source's tuple itself, so its copy answer is the source's, and
// a consumer retaining a tuple from a workspace-reusing source copies it —
// the same boundary rule every other retain-site honors. Without the copy,
// every retained tuple aliases the iterator workspace and reads as the last
// tuple.
func TestEvaluateExpressionUnifyPassThroughAnswersItsSourcesCopyAnswer(t *testing.T) {
	xSym := datalog.NewSymbol("?x")
	ySym := datalog.NewSymbol("?y")
	srcSymbols := []query.Symbol{xSym, ySym}
	src := NewStreamingRelation(srcSymbols, newWorkspaceReusingIterator(len(srcSymbols),
		[][]interface{}{
			{int64(1), int64(2)},
			{int64(2), int64(3)},
			{int64(3), int64(4)},
		}, // every tuple unifies: ?y = ?x + 1
	))

	q, err := parser.ParseQuery(`[:find ?x ?y
	  :where [?e :test/x ?x]
	         [?e :test/y ?y]
	         [(+ ?x 1) ?y]]`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	expr := q.Where[2].(*query.Expression)

	result := evaluateExpressionWithLookup(src, expr, nil, nil)

	var got []Tuple
	it := result.Iterator()
	if !it.RequiresCopy() {
		t.Fatalf("the pass-through answers the workspace-reusing source's tuple, so its copy answer is true")
	}
	for it.Next() {
		got = append(got, copyTuple(it.Tuple()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iteration failed: %v", err)
	}
	if closeErr := it.Close(); closeErr != nil {
		t.Fatalf("close failed: %v", closeErr)
	}

	want := []Tuple{
		{int64(1), int64(2)},
		{int64(2), int64(3)},
		{int64(3), int64(4)},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d tuples, want %d", len(got), len(want))
	}
	for i := range want {
		for j := range want[i] {
			if !datalog.ValuesEqual(got[i][j], want[i][j]) {
				t.Fatalf("tuple %d = %v, want %v (workspace aliasing corrupts retained tuples)", i, got[i], want[i])
			}
		}
	}
}

// TestEvaluateExpressionUnifiesBoundBindings pins the per-tuple evaluation
// path directly, one case per write site: scalar binding with the symbol
// bound, multi-tuple expansion (enumerate) with every binding symbol bound,
// and multi-tuple expansion with a partial overlap (one symbol bound, one
// new). In every case a bound symbol filters; only genuinely new symbols
// extend the tuple.
func TestEvaluateExpressionUnifiesBoundBindings(t *testing.T) {
	xSym := datalog.NewSymbol("?x")
	ySym := datalog.NewSymbol("?y")
	vecSym := datalog.NewSymbol("?vec")
	idxSym := datalog.NewSymbol("?idx")
	tagSym := datalog.NewSymbol("?tag")
	vecAB := []interface{}{"a", "b"}

	cases := map[string]struct {
		queryText   string
		exprIndex   int
		symbols     []query.Symbol
		tuples      []Tuple
		wantSymbols []query.Symbol
		wantTuples  []Tuple
	}{
		"scalar binding, symbol bound": {
			queryText: `[:find ?x ?y
			  :where [?e :test/x ?x]
			         [?e :test/y ?y]
			         [(+ ?x 1) ?y]]`,
			exprIndex: 2,
			symbols:   []query.Symbol{xSym, ySym},
			tuples: []Tuple{
				{int64(1), int64(2)}, // 1+1 = 2 — survives unchanged
				{int64(3), int64(7)}, // 3+1 ≠ 7 — dropped
			},
			wantSymbols: []query.Symbol{xSym, ySym},
			wantTuples:  []Tuple{{int64(1), int64(2)}},
		},
		"expansion, every binding symbol bound": {
			queryText: `[:find ?idx ?tag
			  :where [?e :test/vec ?vec]
			         [?e :test/idx ?idx]
			         [?e :test/tag ?tag]
			         [(enumerate ?vec) [[?idx ?tag]]]]`,
			exprIndex: 3,
			symbols:   []query.Symbol{vecSym, idxSym, tagSym},
			tuples: []Tuple{
				{vecAB, int64(0), "a"}, // (0 "a") is an expansion tuple — survives once
				{vecAB, int64(0), "b"}, // (0 "b") is not — dropped
			},
			wantSymbols: []query.Symbol{vecSym, idxSym, tagSym},
			wantTuples:  []Tuple{{vecAB, int64(0), "a"}},
		},
		"expansion, partial overlap": {
			queryText: `[:find ?idx ?tag
			  :where [?e :test/vec ?vec]
			         [?e :test/tag ?tag]
			         [(enumerate ?vec) [[?idx ?tag]]]]`,
			exprIndex: 2,
			symbols:   []query.Symbol{vecSym, tagSym},
			tuples: []Tuple{
				// ?tag bound to "b": only the (1 "b") expansion tuple unifies,
				// extending the tuple with ?idx = 1. The (0 "a") tuple does not.
				{vecAB, "b"},
			},
			wantSymbols: []query.Symbol{vecSym, tagSym, idxSym},
			wantTuples:  []Tuple{{vecAB, "b", int64(1)}},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			q, err := parser.ParseQuery(tc.queryText)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			expr, ok := q.Where[tc.exprIndex].(*query.Expression)
			if !ok {
				t.Fatalf("clause %d is %T, expected *query.Expression", tc.exprIndex, q.Where[tc.exprIndex])
			}

			rel := NewMaterializedRelation(tc.symbols, tc.tuples)
			result := evaluateExpressionWithLookup(rel, expr, nil, nil)

			gotSymbols := result.Symbols()
			if len(gotSymbols) != len(tc.wantSymbols) {
				t.Fatalf("symbols = %v, want %v", gotSymbols, tc.wantSymbols)
			}
			for i, sym := range tc.wantSymbols {
				if gotSymbols[i] != sym {
					t.Fatalf("symbols = %v, want %v", gotSymbols, tc.wantSymbols)
				}
			}

			var got []Tuple
			it := result.Iterator()
			for it.Next() {
				got = append(got, it.Tuple())
			}
			if err := it.Error(); err != nil {
				t.Fatalf("iteration failed: %v", err)
			}
			if closeErr := it.Close(); closeErr != nil {
				t.Fatalf("close failed: %v", closeErr)
			}

			if len(got) != len(tc.wantTuples) {
				for i, tuple := range got {
					t.Logf("tuple %d: %v", i, tuple)
				}
				t.Fatalf("got %d tuples, want %d", len(got), len(tc.wantTuples))
			}
			for i, want := range tc.wantTuples {
				if len(got[i]) != len(want) {
					t.Fatalf("tuple %d = %v, want %v", i, got[i], want)
				}
				for j := range want {
					if !datalog.ValuesEqual(got[i][j], want[j]) {
						t.Fatalf("tuple %d = %v, want %v", i, got[i], want)
					}
				}
			}
		})
	}
}
