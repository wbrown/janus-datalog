package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// catItemDatoms: two categories; A has 5 items, B has 2. Used to distinguish
// per-invocation vs global vs dropped limit semantics for subqueries.
func catItemDatoms() []datalog.Datom {
	d := []datalog.Datom{
		{E: datalog.NewIdentity("catA"), A: datalog.NewKeyword(":cat/name"), V: "A", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("catB"), A: datalog.NewKeyword(":cat/name"), V: "B", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}
	add := func(seed, cat string, val int64) {
		e := datalog.NewIdentity(seed)
		d = append(d,
			datalog.Datom{E: e, A: datalog.NewKeyword(":item/cat"), V: cat, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			datalog.Datom{E: e, A: datalog.NewKeyword(":item/val"), V: val, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		)
	}
	add("a1", "A", 1)
	add("a2", "A", 2)
	add("a3", "A", 3)
	add("a4", "A", 4)
	add("a5", "A", 5)
	add("b1", "B", 6)
	add("b2", "B", 7)
	return d
}

// TestSubqueryLimitIsPerInvocation pins the contract for :limit inside a
// subquery. Two end states are acceptable; silently-ignored caps (wrong
// results) are not:
//   - Interim: parsing rejects the subquery :limit (no silent wrong results).
//   - Future: parsing succeeds AND the cap is applied per invocation —
//     A=5 items -> 2, B=2 items -> 2, total 4 rows.
//
// A global cap (2) or a dropped limit (7) must fail this test. When per-
// invocation support lands, simply removing the parse rejection makes the
// correctness assertion below the active gate — no test edit required.
func TestSubqueryLimitIsPerInvocation(t *testing.T) {
	matcher := NewMemoryPatternMatcher(catItemDatoms())
	exec := NewExecutorWithOptions(matcher, nil, planner.PlannerOptions{})

	q, err := parser.ParseQuery(`[:find ?c ?v
	                              :where [?cat :cat/name ?c]
	                                     [(q [:find ?val
	                                          :in $ ?c
	                                          :where [?i :item/cat ?c]
	                                                 [?i :item/val ?val]
	                                          :limit 2]
	                                         $ ?c) [[?v] ...]]]`)
	if err != nil {
		// Interim behavior: :limit inside a subquery is rejected at parse time.
		t.Logf("subquery :limit rejected at parse (interim): %v", err)
		return
	}
	// Rejection removed => execution must honor the per-invocation cap.
	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Size() != 4 {
		t.Errorf("expected 4 rows (per-invocation limit 2: A->2, B->2), got %d", result.Size())
		dumpRelationTest(t, result)
	}
}

// TestSubqueryLimitTopPerGroup pins the "top per group" idiom — order-by desc +
// limit 1 in a correlated subquery bound as a tuple. Acceptable end states:
//   - Interim: parsing rejects the subquery :limit.
//   - Future: parsing succeeds AND it yields the max val per category:
//     (A,5), (B,7).
//
// When per-invocation support lands, removing the parse rejection makes the
// correctness assertion below the active gate — no test edit required.
func TestSubqueryLimitTopPerGroup(t *testing.T) {
	matcher := NewMemoryPatternMatcher(catItemDatoms())
	exec := NewExecutorWithOptions(matcher, nil, planner.PlannerOptions{})

	q, err := parser.ParseQuery(`[:find ?c ?v
	                              :where [?cat :cat/name ?c]
	                                     [(q [:find ?val
	                                          :in $ ?c
	                                          :where [?i :item/cat ?c]
	                                                 [?i :item/val ?val]
	                                          :order-by [[?val :desc]]
	                                          :limit 1]
	                                         $ ?c) [[?v]]]]`)
	if err != nil {
		// Interim behavior: :limit inside a subquery is rejected at parse time.
		t.Logf("subquery :limit rejected at parse (interim): %v", err)
		return
	}
	// Rejection removed => execution must honor order-by + limit per invocation.
	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Size() != 2 {
		t.Fatalf("expected 2 rows (top-1 per group), got %d", result.Size())
	}
	got := map[string]int64{}
	it := result.Iterator()
	defer it.Close()
	for it.Next() {
		tup := it.Tuple()
		got[tup[0].(string)] = tup[1].(int64)
	}
	if got["A"] != 5 || got["B"] != 7 {
		t.Errorf("expected A->5, B->7, got %v", got)
	}
}

// TestPureAggregateWithLimit: a pure (ungrouped) aggregate yields one row;
// limit 1 keeps it, limit 0 yields none.
func TestPureAggregateWithLimit(t *testing.T) {
	matcher := NewMemoryPatternMatcher(catItemDatoms())
	exec := NewExecutor(matcher, nil)

	t.Run("limit 1 keeps the single aggregate row", func(t *testing.T) {
		q, err := parser.ParseQuery(`[:find (count ?e) .
		                              :where [?e :item/val ?v]
		                              :limit 1]`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		if result.Size() != 1 {
			t.Fatalf("expected 1 row, got %d", result.Size())
		}
		if result.Get(0)[0].(int64) != 7 {
			t.Errorf("expected count 7, got %v", result.Get(0)[0])
		}
	})

	t.Run("limit 0 drops the aggregate row", func(t *testing.T) {
		q, err := parser.ParseQuery(`[:find (count ?e)
		                              :where [?e :item/val ?v]
		                              :limit 0]`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		if result.Size() != 0 {
			t.Errorf("expected 0 rows, got %d", result.Size())
		}
	})
}

// TestPullWithLimit: :limit caps the number of pulled entities (rows).
func TestPullWithLimit(t *testing.T) {
	matcher := NewMemoryPatternMatcher(catItemDatoms())
	exec := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find (pull ?e [:item/val])
	                              :where [?e :item/val ?v]
	                              :limit 1]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Size() != 1 {
		t.Errorf("expected 1 pulled row, got %d", result.Size())
	}
}
