package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/executor"
)

// chains is the graph of edges/10 chains of ten :edge datoms each. The
// closure holds fifty-five pairs per chain, and every chain ends after ten
// iterations whatever the edge count.
func chains(edges int) graphSpec {
	var spec graphSpec
	for chain := 0; chain < edges/10; chain++ {
		base := chain*11 + 1
		for i := 0; i <= 10; i++ {
			spec.nodes = append(spec.nodes, nodeName(base+i))
		}
		for i := 0; i < 10; i++ {
			spec.edges = append(spec.edges, [2]string{nodeName(base + i), nodeName(base + i + 1)})
		}
	}
	return spec
}

// BenchmarkTransitiveClosure measures the closure of :edge through
// Database.Query on every leg of the matrix, over a thousand, ten thousand
// and a hundred thousand edges.
func BenchmarkTransitiveClosure(b *testing.B) {
	for _, edges := range []int{1000, 10000, 100000} {
		want := edges / 10 * 55
		for _, mode := range optimizerModes {
			d := createOptimizerModeDB(b, mode, DatabaseOptions{})
			writeGraph(b, d, chains(edges))
			rules := mustRuleSet(b, closureRules)
			b.Run(fmt.Sprintf("%d-edges/%s", edges, mode.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					tuples, err := executor.CollectTuples(d.Query(closureQuery, rules))
					if err != nil {
						b.Fatal(err)
					}
					if len(tuples) != want {
						b.Fatalf("got %d pairs, want %d", len(tuples), want)
					}
				}
			})
		}
	}
}

// BenchmarkBoundInvocation measures the closure through Database.Query with
// its start bound to the first node, on the same graphs as the closure
// benchmark: the derivation is the chain the start reaches, whatever the
// edge count.
func BenchmarkBoundInvocation(b *testing.B) {
	const bound = `[:find ?y :in $ % ?start :where (reach ?start ?y)]`
	for _, edges := range []int{1000, 10000, 100000} {
		for _, mode := range optimizerModes {
			d := createOptimizerModeDB(b, mode, DatabaseOptions{})
			writeGraph(b, d, chains(edges))
			rules := mustRuleSet(b, closureRules)
			b.Run(fmt.Sprintf("%d-edges/%s", edges, mode.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					tuples, err := executor.CollectTuples(d.Query(bound, rules, nodeID("n1")))
					if err != nil {
						b.Fatal(err)
					}
					if len(tuples) != 10 {
						b.Fatalf("got %d nodes, want the ten n1 reaches", len(tuples))
					}
				}
			})
		}
	}
}

// BenchmarkBoundInvocationThenScan measures the closure through Database.Query
// with its start bound to the first node and followed by a scan of :edge on
// what the start reaches, on the same graphs: the rewrite is for the start
// alone, so the derivation is the chain the start reaches whatever the edge
// count (REWRITE_ORDER_PROOF.md T5).
func BenchmarkBoundInvocationThenScan(b *testing.B) {
	const bound = `[:find ?c :in $ % ?start :where (reach ?start ?b) [?b :edge ?c]]`
	for _, edges := range []int{1000, 10000, 100000} {
		for _, mode := range optimizerModes {
			d := createOptimizerModeDB(b, mode, DatabaseOptions{})
			writeGraph(b, d, chains(edges))
			rules := mustRuleSet(b, closureRules)
			b.Run(fmt.Sprintf("%d-edges/%s", edges, mode.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					tuples, err := executor.CollectTuples(d.Query(bound, rules, nodeID("n1")))
					if err != nil {
						b.Fatal(err)
					}
					if len(tuples) != 9 {
						b.Fatalf("got %d nodes, want the nine n1's chain reaches past its first edge", len(tuples))
					}
				}
			})
		}
	}
}
