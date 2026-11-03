package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// BenchmarkClausesAfterAnEvaluation measures queries whose clauses follow an
// evaluation, over the scope fixture's hundred thousand entities: a pattern
// after an expression over its entity, after a missing? filter, and after a
// comparison over an expression's output, and a ground expression and a
// get-else on a constant entity extending the entities' group. Each run
// checks the count its query answers.
func BenchmarkClausesAfterAnEvaluation(b *testing.B) {
	const n = 100000
	shapes := []struct {
		name  string
		query string
		want  int
	}{
		{"a pattern after an expression",
			`[:find ?e ?z :where [?e :t/n ?x] [(+ ?x 1) ?y] [?e :t/a ?z]]`, withA(n)},
		{"a pattern after a missing? filter",
			`[:find ?e ?z :where [?e :t/n _] [(missing? $ ?e :t/b)] [?e :t/a ?z]]`, withA(n) - (n+5)/6},
		{"a pattern after a comparison over an expression",
			fmt.Sprintf(`[:find ?e ?z :where [?e :t/n ?x] [(+ ?x 1) ?y] [(< ?y %d)] [?e :t/a ?z]]`, n/2+1), (n/2 + 1) / 2},
		{"a ground expression extending a group",
			`[:find ?e ?c :where [?e :t/n _] [(ground 7) ?c]]`, n},
		{"a get-else on a constant entity extending a group",
			`[:find ?e ?c :where [?e :t/n _] [(get-else $ #id "scope/0" :t/a -1) ?c]]`, n},
	}
	for _, shape := range shapes {
		q, err := parser.ParseQuery(shape.query)
		if err != nil {
			b.Fatal(err)
		}
		for _, mode := range optimizerModes {
			b.Run(shape.name+"/"+mode.name, func(b *testing.B) {
				opts := mode.plannerOptions()
				d := openBackendDB(b, mode.backend, DatabaseOptions{PlannerOptions: &opts})
				writeScopeBenchmarkFixture(b, d, n)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tuples, err := executor.CollectTuples(d.Query(q))
					if err != nil {
						b.Fatal(err)
					}
					if len(tuples) != shape.want {
						b.Fatalf("got %d tuples, want %d", len(tuples), shape.want)
					}
				}
			})
		}
	}
}
