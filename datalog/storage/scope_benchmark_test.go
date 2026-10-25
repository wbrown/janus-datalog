package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// scopeBenchmarkSizes are the outer relation's sizes for the scope
// benchmarks: a thousand entities and a hundred thousand.
var scopeBenchmarkSizes = []int{1000, 100000}

// scopeSubject is one subject a scope benchmark runs its form over: the
// clauses that bind it, under the name the benchmark's path carries.
type scopeSubject struct {
	name    string
	clauses string
}

// scopeSubjects are the subjects the three forms run over: the entity
// pattern, which streams, and a subquery over it, whose relation binding is
// materialized.
var scopeSubjects = []scopeSubject{
	{"streaming", "[?e :t/n _]"},
	{"materialized", "[(q [:find ?e :where [?e :t/n _]] $) [[?e] ...]]"},
}

// writeScopeBenchmarkFixture commits n entities into d, each carrying :t/n,
// every second one :t/a and every third one :t/b, graphBatch datoms per
// transaction.
func writeScopeBenchmarkFixture(b testing.TB, d *Database, n int) {
	b.Helper()
	tx := d.NewTransaction()
	written := 0
	add := func(e datalog.Identity, a datalog.Keyword, v interface{}) {
		if err := tx.Add(e, a, v); err != nil {
			b.Fatal(err)
		}
		written++
		if written == graphBatch {
			if _, err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
			tx = d.NewTransaction()
			written = 0
		}
	}
	for i := 0; i < n; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("scope/%d", i))
		add(e, kw(":t/n"), int64(i))
		if i%2 == 0 {
			add(e, kw(":t/a"), int64(i))
		}
		if i%3 == 0 {
			add(e, kw(":t/b"), int64(i+n))
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

// withA and withB count the fixture's entities carrying :t/a and :t/b at
// size n.
func withA(n int) int { return (n + 1) / 2 }
func withB(n int) int { return (n + 2) / 3 }

// scopeWindowCaps are the window caps a scope benchmark runs under at size
// n (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §8): one tuple,
// thirty-two, the default, and the subject's size where that is past the
// default, so the subject also runs as one window.
func scopeWindowCaps(n int) []int {
	caps := []int{1, 32, executor.SmallBindingBoundary}
	if n > executor.SmallBindingBoundary {
		caps = append(caps, n)
	}
	return caps
}

// benchmarkScope runs a query finding find over each subject followed by
// form, at every size, every window cap and every optimizer mode, and checks
// that each run yields want(n) tuples.
func benchmarkScope(b *testing.B, subjects []scopeSubject, find, form string, want func(n int) int) {
	for _, subject := range subjects {
		q, err := parser.ParseQuery(fmt.Sprintf("[:find %s :where %s %s]", find, subject.clauses, form))
		if err != nil {
			b.Fatal(err)
		}
		for _, n := range scopeBenchmarkSizes {
			for _, windowCap := range scopeWindowCaps(n) {
				for _, mode := range optimizerModes {
					b.Run(fmt.Sprintf("%d/%s/%s/cap%d", n, subject.name, mode.name, windowCap), func(b *testing.B) {
						opts := mode.plannerOptions()
						opts.ScopeWindowCap = windowCap
						d := openBackendDB(b, mode.backend, DatabaseOptions{PlannerOptions: &opts})
						writeScopeBenchmarkFixture(b, d, n)
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							tuples, err := executor.CollectTuples(d.Query(q))
							if err != nil {
								b.Fatal(err)
							}
							if len(tuples) != want(n) {
								b.Fatalf("got %d tuples, want %d", len(tuples), want(n))
							}
						}
					})
				}
			}
		}
	}
}

// BenchmarkOrJoinScope measures an or-join over the subject, required on the
// entity, each branch binding the output from one attribute: the entities
// carrying :t/a and those carrying :t/b each yield a pair.
func BenchmarkOrJoinScope(b *testing.B) {
	benchmarkScope(b, scopeSubjects, "?e ?v", `(or-join [[?e] ?v] [?e :t/a ?v] [?e :t/b ?v])`, func(n int) int { return withA(n) + withB(n) })
}

// BenchmarkFlatOrJoinScope measures a flat or-join over a streaming subject
// that carries both header symbols, the entity and its :t/a value, so each
// branch seeks on both: the entities carrying :t/a each yield their pair.
func BenchmarkFlatOrJoinScope(b *testing.B) {
	subject := []scopeSubject{{"streaming", "[?e :t/n _] [?e :t/a ?v]"}}
	benchmarkScope(b, subject, "?e ?v", `(or-join [?e ?v] [?e :t/a ?v] [?e :t/b ?v])`, withA)
}

// BenchmarkOrDefaultJoinScope measures an or-default-join over the subject,
// required on the entity: every entity yields its :t/a or the default.
func BenchmarkOrDefaultJoinScope(b *testing.B) {
	benchmarkScope(b, scopeSubjects, "?e ?v", `(or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])`, func(n int) int { return n })
}

// BenchmarkNotJoinScope measures a not-join over the subject, required on
// the entity: the entities without :t/a remain.
func BenchmarkNotJoinScope(b *testing.B) {
	benchmarkScope(b, scopeSubjects, "?e", `(not-join [?e] [?e :t/a _])`, func(n int) int { return n - withA(n) })
}
