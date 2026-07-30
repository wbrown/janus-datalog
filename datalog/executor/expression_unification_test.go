package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// An expression whose binding variable is already bound is a unification
// constraint (Datomic semantics): the tuple survives when the computed value
// equals the existing value and is dropped otherwise; the existing value is
// never replaced. The executor's ground-constants path already implements
// exactly this ("filter (unify) instead of extending", executeExpression);
// these tests pin the same contract for the per-tuple path, which today
// overwrites the bound value instead. Overwriting silently corrupts tuples:
// the OHLC decorrelation failure produced tuples whose ?datetime came from the
// outer bar while the aggregates belonged to a different group, because the
// hour expressions overwrote the join keys instead of filtering.
//
// These pins involve no subquery, so the planner's subquery scheduling
// cannot affect them: the expression-provided query case and every
// function-level case stay red until the executor defect itself is fixed.
// (The pattern-provided query case is enforced by pattern deferral today and
// pins that adjacent behavior.) See
// BUG_EXPRESSION_BINDING_OVERWRITES_BOUND_VARIABLE.md and its
// planner sibling
// BUG_UNCORRELATED_SUBQUERY_SCHEDULES_BEFORE_BINDING_PROVIDERS.md.

// TestExpressionOntoBoundVariableUnifies pins the query-level contract under
// both planner modes: an expression targeting a bound ?y keeps exactly the
// entities where ?y equals the computed value.
//
// The provider of ?y matters for WHERE the contract is enforced today.
// Pattern-provided: patternDependsOnPendingExpression defers the ?y pattern
// behind the expression, so the join itself unifies — correct results by
// scheduling. Expression-provided: nothing reorders two expressions, so the
// second one evaluates with ?y bound and takes the per-tuple path — the
// direct query-level reproducer of the overwrite defect.
func TestExpressionOntoBoundVariableUnifies(t *testing.T) {
	xAttr := datalog.NewKeyword(":test/x")
	yAttr := datalog.NewKeyword(":test/y")
	zAttr := datalog.NewKeyword(":test/z")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	e1 := datalog.NewIdentity("test:e1")
	e2 := datalog.NewIdentity("test:e2")

	cases := map[string]struct {
		datoms    []datalog.Datom
		queryText string
	}{
		"bound by pattern": {
			datoms: []datalog.Datom{
				{E: e1, A: xAttr, V: int64(1), Tx: tx},
				{E: e1, A: yAttr, V: int64(2), Tx: tx}, // 1+1 = 2 — unifies
				{E: e2, A: xAttr, V: int64(3), Tx: tx},
				{E: e2, A: yAttr, V: int64(7), Tx: tx}, // 3+1 ≠ 7 — dropped
			},
			queryText: `[:find ?x ?y
			  :where [?e :test/x ?x]
			         [?e :test/y ?y]
			         [(+ ?x 1) ?y]]`,
		},
		"bound by expression": {
			datoms: []datalog.Datom{
				{E: e1, A: xAttr, V: int64(1), Tx: tx},
				{E: e1, A: zAttr, V: int64(2), Tx: tx}, // ?y = 2*1; 1+1 = 2 — unifies
				{E: e2, A: xAttr, V: int64(3), Tx: tx},
				{E: e2, A: zAttr, V: int64(7), Tx: tx}, // ?y = 7*1; 3+1 ≠ 7 — dropped
			},
			queryText: `[:find ?x ?y
			  :where [?e :test/x ?x]
			         [?e :test/z ?z]
			         [(* ?z 1) ?y]
			         [(+ ?x 1) ?y]]`,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			matcher := NewMemoryPatternMatcher(tc.datoms)
			q, err := parser.ParseQuery(tc.queryText)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}

			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
					result, err := exec.Execute(q)
					if err != nil {
						t.Fatalf("execute failed: %v", err)
					}
					if result.Size() != 1 {
						for i := 0; i < result.Size(); i++ {
							t.Logf("tuple %d: %v", i, result.Get(i))
						}
						t.Fatalf("expected exactly the unifying entity (1 tuple), got %d", result.Size())
					}
					tuple := result.Get(0)
					if !datalog.ValuesEqual(tuple[0], int64(1)) || !datalog.ValuesEqual(tuple[1], int64(2)) {
						t.Fatalf("expected [1 2], got %v", tuple)
					}
				})
			}
		})
	}
}

// TestEvaluateExpressionUnifyPassThroughCopiesFromUnsafeSource pins the
// RequiresCopy discipline on the unify pass-through: when every binding
// symbol is bound and the tuple survives unification unchanged, the
// retained tuple must be copied out of a workspace-reusing source — the
// same boundary rule every other retain-site honors. Without the copy,
// every retained tuple aliases the iterator workspace and reads as the last
// tuple.
func TestEvaluateExpressionUnifyPassThroughCopiesFromUnsafeSource(t *testing.T) {
	xSym := datalog.NewSymbol("?x")
	ySym := datalog.NewSymbol("?y")
	src := newMockUnsafeRelation(
		[]query.Symbol{xSym, ySym},
		[][]interface{}{
			{int64(1), int64(2)},
			{int64(2), int64(3)},
			{int64(3), int64(4)},
		}, // every tuple unifies: ?y = ?x + 1
	)

	q, err := parser.ParseQuery(`[:find ?x ?y
	  :where [?e :test/x ?x]
	         [?e :test/y ?y]
	         [(+ ?x 1) ?y]]`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	expr := q.Where[2].(*query.Expression)

	result, err := evaluateExpressionWithLookup(src, expr, nil, nil)
	if err != nil {
		t.Fatalf("evaluation failed: %v", err)
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
			         [(enumerate ?vec) [?idx ?tag]]]`,
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
			         [(enumerate ?vec) [?idx ?tag]]]`,
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
			result, err := evaluateExpressionWithLookup(rel, expr, nil, nil)
			if err != nil {
				t.Fatalf("evaluation failed: %v", err)
			}

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
