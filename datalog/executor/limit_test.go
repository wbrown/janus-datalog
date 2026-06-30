package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// countingSliceIterator (defined in lazy_seq_relation_test.go) tracks how many
// tuples the source produced via its nextCalls pointer, which lets these tests
// prove that a limit stops pulling from the source instead of draining it.

// TestLimitRelationStopsScanAfterN is the core streaming-early-termination
// contract: wrapping a streaming relation of 1000 tuples in a limit of 5 must
// pull the source iterator at most 5 times, not drain it.
func TestLimitRelationStopsScanAfterN(t *testing.T) {
	const total = 1000

	build := func(calls *int) *countingSliceIterator {
		tuples := make([]Tuple, total)
		for i := range tuples {
			tuples[i] = Tuple{int64(i)}
		}
		return &countingSliceIterator{tuples: tuples, nextCalls: calls}
	}

	t.Run("limit below size stops the scan early", func(t *testing.T) {
		calls := 0
		sr := NewStreamingRelation([]query.Symbol{datalog.NewSymbol("?x")}, build(&calls))
		lr := NewLimitRelation(sr, 5)

		if got := lr.Size(); got != 5 {
			t.Errorf("expected Size 5, got %d", got)
		}
		if calls != 5 {
			t.Errorf("expected source pulled exactly 5 times (early termination), got %d", calls)
		}
	})

	t.Run("limit zero pulls nothing", func(t *testing.T) {
		calls := 0
		sr := NewStreamingRelation([]query.Symbol{datalog.NewSymbol("?x")}, build(&calls))
		lr := NewLimitRelation(sr, 0)

		if got := lr.Size(); got != 0 {
			t.Errorf("expected Size 0, got %d", got)
		}
		if calls != 0 {
			t.Errorf("expected source pulled 0 times, got %d", calls)
		}
	})

	t.Run("limit above size returns all without error", func(t *testing.T) {
		calls := 0
		small := &countingSliceIterator{tuples: []Tuple{{int64(1)}, {int64(2)}, {int64(3)}}, nextCalls: &calls}
		sr := NewStreamingRelation([]query.Symbol{datalog.NewSymbol("?x")}, small)
		lr := NewLimitRelation(sr, 10)

		if got := lr.Size(); got != 3 {
			t.Errorf("expected Size 3, got %d", got)
		}
	})

	t.Run("iterator yields exactly the limit", func(t *testing.T) {
		calls := 0
		sr := NewStreamingRelation([]query.Symbol{datalog.NewSymbol("?x")}, build(&calls))
		lr := NewLimitRelation(sr, 7)

		it := lr.Iterator()
		defer it.Close()
		count := 0
		for it.Next() {
			count++
		}
		if count != 7 {
			t.Errorf("expected iterator to yield 7 tuples, got %d", count)
		}
	})
}

// telemetryDatoms builds a small dataset: 5 entities, each with a value and a
// monotonically increasing tx-like ordinal, for top-N / limit testing.
func telemetryDatoms() []datalog.Datom {
	mk := func(seed string, ord int64) []datalog.Datom {
		e := datalog.NewIdentity(seed)
		return []datalog.Datom{
			{E: e, A: datalog.NewKeyword(":n/ord"), V: ord, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			{E: e, A: datalog.NewKeyword(":n/kind"), V: "telemetry", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		}
	}
	var datoms []datalog.Datom
	datoms = append(datoms, mk("e1", 10)...)
	datoms = append(datoms, mk("e2", 20)...)
	datoms = append(datoms, mk("e3", 30)...)
	datoms = append(datoms, mk("e4", 40)...)
	datoms = append(datoms, mk("e5", 50)...)
	return datoms
}

func TestQueryLimitNoOrderBy(t *testing.T) {
	matcher := NewMemoryPatternMatcher(telemetryDatoms())
	exec := NewExecutor(matcher, nil)

	cases := []struct {
		name  string
		limit string
		want  int
	}{
		{"limit below size", "1", 1},
		{"limit equal size", "5", 5},
		{"limit above size", "100", 5},
		{"limit zero", "0", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := parser.ParseQuery(`[:find ?e ?ord
			                              :where [?e :n/kind "telemetry"]
			                                     [?e :n/ord ?ord]
			                              :limit ` + tc.limit + `]`)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if result.Size() != tc.want {
				t.Errorf("expected %d rows, got %d", tc.want, result.Size())
			}
		})
	}
}

func TestQueryLimitWithOrderByTopN(t *testing.T) {
	matcher := NewMemoryPatternMatcher(telemetryDatoms())
	exec := NewExecutor(matcher, nil)

	// Latest-2 by ord descending: should be 50, 40 in that order.
	q, err := parser.ParseQuery(`[:find ?ord
	                              :where [?e :n/kind "telemetry"]
	                                     [?e :n/ord ?ord]
	                              :order-by [[?ord :desc]]
	                              :limit 2]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Size() != 2 {
		t.Fatalf("expected 2 rows, got %d", result.Size())
	}
	want := []int64{50, 40}
	for i := 0; i < result.Size(); i++ {
		got := result.Get(i)[0].(int64)
		if got != want[i] {
			t.Errorf("row %d: expected %d, got %d", i, want[i], got)
		}
	}
}

func TestQueryLimitAfterAggregationCapsGroups(t *testing.T) {
	// Four cities; limit 2 must cap the number of grouped rows to 2.
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("p1"), A: datalog.NewKeyword(":person/city"), V: "NYC", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("p2"), A: datalog.NewKeyword(":person/city"), V: "LA", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("p3"), A: datalog.NewKeyword(":person/city"), V: "SF", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("p4"), A: datalog.NewKeyword(":person/city"), V: "BOS", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}
	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find ?city (count ?p)
	                              :where [?p :person/city ?city]
	                              :limit 2]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Size() != 2 {
		t.Errorf("expected 2 grouped rows, got %d", result.Size())
	}
}

// TestQueryLimitWithRelationInputIsGlobal verifies the cap is applied to the
// combined output across all RelationInput tuples, not per input tuple.
func TestQueryLimitWithRelationInputIsGlobal(t *testing.T) {
	// Two keys, each with 3 values: 6 rows total without a limit.
	datoms := []datalog.Datom{}
	add := func(seed, key string, val int64) {
		e := datalog.NewIdentity(seed)
		datoms = append(datoms,
			datalog.Datom{E: e, A: datalog.NewKeyword(":item/key"), V: key, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			datalog.Datom{E: e, A: datalog.NewKeyword(":item/val"), V: val, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		)
	}
	add("a1", "k1", 1)
	add("a2", "k1", 2)
	add("a3", "k1", 3)
	add("b1", "k2", 4)
	add("b2", "k2", 5)
	add("b3", "k2", 6)

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find ?v
	                              :in $ [[?k] ...]
	                              :where [?e :item/key ?k]
	                                     [?e :item/val ?v]
	                              :limit 2]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	keySym := datalog.NewSymbol("?k")
	inputRel := NewMaterializedRelation([]query.Symbol{keySym}, []Tuple{{"k1"}, {"k2"}})

	result, err := exec.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	// Global cap of 2; a per-tuple cap would yield up to 4 (2 per key).
	if result.Size() != 2 {
		t.Errorf("expected global limit of 2 rows, got %d", result.Size())
	}
}

// TestOrderByAndLimitWithRelationInputIsGlobal verifies finalization runs over
// the UNION of per-tuple executions: :order-by sorts the whole result and
// :limit takes a global top-N. The old per-tuple-sort-then-concatenate behavior
// would return an order-dependent (wrong) slice here.
func TestOrderByAndLimitWithRelationInputIsGlobal(t *testing.T) {
	// keyA -> vals {1,2,3}, keyB -> vals {4,5,6,7}. Global top-3 desc = 7,6,5.
	datoms := []datalog.Datom{}
	add := func(seed, key string, val int64) {
		e := datalog.NewIdentity(seed)
		datoms = append(datoms,
			datalog.Datom{E: e, A: datalog.NewKeyword(":item/key"), V: key, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			datalog.Datom{E: e, A: datalog.NewKeyword(":item/val"), V: val, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		)
	}
	add("a1", "A", 1)
	add("a2", "A", 2)
	add("a3", "A", 3)
	add("b1", "B", 4)
	add("b2", "B", 5)
	add("b3", "B", 6)
	add("b4", "B", 7)

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find ?v
	                              :in $ [[?k] ...]
	                              :where [?e :item/key ?k]
	                                     [?e :item/val ?v]
	                              :order-by [[?v :desc]]
	                              :limit 3]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	keySym := datalog.NewSymbol("?k")
	inputRel := NewMaterializedRelation([]query.Symbol{keySym}, []Tuple{{"A"}, {"B"}})

	result, err := exec.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Size() != 3 {
		t.Fatalf("expected 3 rows, got %d", result.Size())
	}
	want := []int64{7, 6, 5}
	for i := 0; i < 3; i++ {
		got := result.Get(i)[0].(int64)
		if got != want[i] {
			t.Errorf("row %d: expected %d, got %d (global top-3 desc)", i, want[i], got)
		}
	}
}
