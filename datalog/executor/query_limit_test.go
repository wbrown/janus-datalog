package executor_test

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

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
	matcher := factsDatabase(t, telemetryDatoms())

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

	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())

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
						t.Errorf("expected %d tuples, got %d", tc.want, result.Size())
					}
				})
			}
		})
	}
}

func TestQueryLimitWithOrderByTopN(t *testing.T) {
	matcher := factsDatabase(t, telemetryDatoms())

	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())

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
				t.Fatalf("expected 2 tuples, got %d", result.Size())
			}
			want := []int64{50, 40}
			for i := 0; i < result.Size(); i++ {
				got := result.Get(i)[0].(int64)
				if got != want[i] {
					t.Errorf("tuple %d: expected %d, got %d", i, want[i], got)
				}
			}
		})
	}
}

func TestQueryLimitAfterAggregationCapsGroups(t *testing.T) {
	// Four cities; limit 2 must cap the number of grouped tuples to 2.
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("p1"), A: datalog.NewKeyword(":person/city"), V: "NYC", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("p2"), A: datalog.NewKeyword(":person/city"), V: "LA", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("p3"), A: datalog.NewKeyword(":person/city"), V: "SF", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("p4"), A: datalog.NewKeyword(":person/city"), V: "BOS", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}
	matcher := factsDatabase(t, datoms)

	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())

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
				t.Errorf("expected 2 grouped tuples, got %d", result.Size())
			}
		})
	}
}

// TestQueryLimitWithRelationInputIsGlobal verifies the cap is applied to the
// combined output across all RelationInput tuples, not per input tuple.
func TestQueryLimitWithRelationInputIsGlobal(t *testing.T) {
	// Two keys, each with 3 values: 6 tuples total without a limit.
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

	matcher := factsDatabase(t, datoms)

	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())

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

			result, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			// Global cap of 2; a per-tuple cap would yield up to 4 (2 per key).
			if result.Size() != 2 {
				t.Errorf("expected global limit of 2 tuples, got %d", result.Size())
			}
		})
	}
}

// TestOrderByAndLimitWithRelationInputIsGlobal verifies finalization runs over
// the UNION of per-tuple executions: :order-by sorts the whole result and
// :limit takes a global top-N. A per-tuple-sort-then-concatenate approach
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

	matcher := factsDatabase(t, datoms)

	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())

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

			result, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if result.Size() != 3 {
				t.Fatalf("expected 3 tuples, got %d", result.Size())
			}
			want := []int64{7, 6, 5}
			for i := 0; i < 3; i++ {
				got := result.Get(i)[0].(int64)
				if got != want[i] {
					t.Errorf("tuple %d: expected %d, got %d (global top-3 desc)", i, want[i], got)
				}
			}
		})
	}
}
