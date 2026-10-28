package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The binding-driven match selects its bindings under typedPositionBindingCheck
// once at its entry, one closure call per tuple of the binding relation. The
// two benchmarks put that selection's cost beside the match it precedes: the
// selection alone over a relation of n identities, and the whole match of
// [?e :bench/n ?v] driven by the same relation and drained, on the cache
// route and, with the cache off, on the join route the size selects.

var benchN = datalog.NewKeyword(":bench/n")

// benchBindingPattern is [?e :bench/n ?v].
func benchBindingPattern() *query.DataPattern {
	return &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: benchN},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
}

// benchBindingRelation is n identities, bench:0 to bench:n-1, bound to ?e.
func benchBindingRelation(n int) executor.Relation {
	tuples := make([]executor.Tuple, n)
	for i := range tuples {
		tuples[i] = executor.Tuple{datalog.NewIdentity(fmt.Sprintf("bench:%d", i))}
	}
	return executor.NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?e")}, tuples)
}

// benchBindingStore holds bench:i with :bench/n i for every identity the
// relation binds.
func benchBindingStore(b *testing.B, n int, opts DatabaseOptions) *Database {
	b.Helper()
	db := createOptimizerModeDB(b, optimizerModes[0], opts)
	tx := db.NewTransaction()
	for i := 0; i < n; i++ {
		if err := tx.Add(datalog.NewIdentity(fmt.Sprintf("bench:%d", i)), benchN, int64(i)); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	return db
}

func BenchmarkTypedBindingSelection(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			pattern := benchBindingPattern()
			bindings := benchBindingRelation(n)
			typed := typedPositionBindingCheck(pattern, bindings.Symbols())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if selected := executor.Select(bindings, typed); selected.Size() != n {
					b.Fatalf("selected %d of %d typed bindings", selected.Size(), n)
				}
			}
		})
	}
}

// BenchmarkTypedBindingCheck is the check alone, one call per tuple, with no
// relation built from the survivors.
func BenchmarkTypedBindingCheck(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			pattern := benchBindingPattern()
			bindings := benchBindingRelation(n)
			typed := typedPositionBindingCheck(pattern, bindings.Symbols())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				passed := 0
				it := bindings.Iterator()
				for it.Next() {
					if typed(it.Tuple()) {
						passed++
					}
				}
				if err := it.Close(); err != nil {
					b.Fatal(err)
				}
				if passed != n {
					b.Fatalf("%d of %d bindings passed the check", passed, n)
				}
			}
		})
	}
}

func BenchmarkBindingDrivenMatch(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		for _, cache := range []struct {
			name string
			opts DatabaseOptions
		}{
			{"cache", DatabaseOptions{}},
			{"no_cache", DatabaseOptions{DisableCache: true}},
		} {
			b.Run(fmt.Sprintf("n=%d/%s", n, cache.name), func(b *testing.B) {
				db := benchBindingStore(b, n, cache.opts)
				matcher := db.matcherWithExecOptions(db.effectivePlannerOptions()).(*PatternMatcher)
				q := query.PatternQuery(benchBindingPattern())
				bindings := benchBindingRelation(n)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tuples, err := executor.CollectTuples(matcher.MatchWithConstraints(q, executor.Relations{bindings}, nil))
					if err != nil {
						b.Fatal(err)
					}
					if len(tuples) != n {
						b.Fatalf("matched %d of %d bindings", len(tuples), n)
					}
				}
			})
		}
	}
}
