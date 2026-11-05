package executor_test

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// chainsDatoms links pages into chains of ten :page/next edges each, edges
// in all. The closure holds fifty-five pairs per chain, and every chain ends
// after ten iterations whatever the edge count.
func chainsDatoms(edges int) []datalog.Datom {
	datoms := make([]datalog.Datom, 0, edges)
	for chain := 0; chain < edges/10; chain++ {
		base := chain * 11
		for i := 0; i < 10; i++ {
			tx := datalog.ElementID{Lamport: uint64(base + i + 1), ReplicaID: 1}
			datoms = append(datoms, datalog.Datom{E: Page(base + i), A: PageNext, V: Page(base + i + 1), Tx: tx})
		}
	}
	return datoms
}

// BenchmarkTransitiveClosure measures the closure of :page/next over a
// thousand, ten thousand and a hundred thousand edges under both optimizer
// modes: semi-naive, the engine's evaluation through ExecuteWithRelations,
// and naive, the same step driven with the full relation at every occurrence.
func BenchmarkTransitiveClosure(b *testing.B) {
	rs := MustRules(b, FollowsRulesText)
	q := MustParse(b, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, edges := range []int{1000, 10000, 100000} {
		matcher := factsDatabase(b, chainsDatoms(edges))
		want := edges / 10 * 55
		for _, mode := range OptimizerModes {
			b.Run(fmt.Sprintf("%d-edges/%s/semi-naive", edges, mode.Name()), func(b *testing.B) {
				benchmarkSemiNaiveClosure(b, matcher, rs, q, mode.PlannerOptions(), want)
			})
			b.Run(fmt.Sprintf("%d-edges/%s/naive", edges, mode.Name()), func(b *testing.B) {
				benchmarkNaiveClosure(b, matcher, rs, mode.PlannerOptions(), want)
			})
		}
	}
}

// benchmarkNaiveClosure times the stratum's step with every occurrence
// reading the full relation, each pass re-deriving every path, until a pass
// derives nothing new.
func benchmarkNaiveClosure(b *testing.B, matcher PatternMatcher, rs *query.RuleSet, opts planner.PlannerOptions, want int) {
	exec := NewExecutorWithOptions(matcher, nil, opts)
	follows := datalog.NewSymbol("follows")
	ctx := NewContext().WithRules(rs)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stratum := rs.Strata()[0]
		bodies, err := planner.PlanStratum(exec.GetPlanner(), rs, stratum, nil)
		if err != nil {
			b.Fatal(err)
		}
		eval := exec.PrepareStratum(rs, planner.PlannedStratum{Stratum: stratum, Bodies: bodies})
		full := NewDerivedSet(eval.Symbols(follows))
		seed, err := eval.Run(ctx, nil, eval.Bodies().Plain)
		if err != nil {
			b.Fatal(err)
		}
		for _, rel := range seed[follows] {
			if _, err := full.Absorb(rel); err != nil {
				b.Fatal(err)
			}
		}
		for grew := true; grew; {
			whole := full.Relation(exec.Options())
			view := map[query.Symbol]Relation{follows: whole, query.DeltaSymbol(follows): whole}
			got, err := eval.Run(ctx, view, eval.Bodies().Delta)
			if err != nil {
				b.Fatal(err)
			}
			grew = false
			for _, rel := range got[follows] {
				added, err := full.Absorb(rel)
				if err != nil {
					b.Fatal(err)
				}
				if len(added) > 0 {
					grew = true
				}
			}
		}
		if full.Len() != want {
			b.Fatalf("got %d pairs, want %d", full.Len(), want)
		}
	}
}

// BenchmarkBoundInvocation measures the closure with its start bound to the
// first page, on the same graphs: the rewrite seeds the fixpoint with the
// start, so the derivation is the chain the start reaches, whatever the edge
// count.
func BenchmarkBoundInvocation(b *testing.B) {
	rs := MustRules(b, FollowsRulesText)
	q := MustParse(b, `[:find ?y :in $ % ?x :where (follows ?x ?y)]`)
	for _, edges := range []int{1000, 10000, 100000} {
		matcher := factsDatabase(b, chainsDatoms(edges))
		for _, mode := range OptimizerModes {
			b.Run(fmt.Sprintf("%d-edges/%s", edges, mode.Name()), func(b *testing.B) {
				exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
				inputs, err := CreateInputRelationsFromValuesWithOptions(q,
					[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), Page(0)}, exec.Options())
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					tuples, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs))
					if err != nil {
						b.Fatal(err)
					}
					if len(tuples) != 10 {
						b.Fatalf("got %d pages, want the ten page0 reaches", len(tuples))
					}
				}
			})
		}
	}
}

// benchmarkSemiNaiveClosure times the closure query as the engine runs it.
func benchmarkSemiNaiveClosure(b *testing.B, matcher PatternMatcher, rs *query.RuleSet, q *query.Query, opts planner.PlannerOptions, want int) {
	exec := NewExecutorWithOptions(matcher, nil, opts)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tuples, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil))
		if err != nil {
			b.Fatal(err)
		}
		if len(tuples) != want {
			b.Fatalf("got %d pairs, want %d", len(tuples), want)
		}
	}
}
