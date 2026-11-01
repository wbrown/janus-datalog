package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// perKeySeekKeyCounts are the binding sizes BenchmarkPerKeySeek runs: one
// key, thirty-two, the boundary at and below which the hash join seeks each
// key in one scan of the extent, and one past it, where it scans the extent
// once and probes.
var perKeySeekKeyCounts = []int{1, 32, executor.SmallBindingBoundary, executor.SmallBindingBoundary + 1}

// BenchmarkPerKeySeek measures a pattern bound at its entity position by a
// collection input of k keys over an attribute carried by n entities, at
// every key count, size and optimizer mode. One seek's cost is the difference
// between two key counts on one extent, and the per-key seek at the boundary
// stands beside the one-scan arm one key past it. The keys name the
// fixture's first k entities, so a count past the extent carries one key the
// index does not hold. The EA cache is off, as in the seek tests, so the
// binding-driven pattern reaches the matcher's scan paths and not the cache.
func BenchmarkPerKeySeek(b *testing.B) {
	q, err := parser.ParseQuery(`[:find ?e ?v :in $ [?e ...] :where [?e :t/n ?v]]`)
	if err != nil {
		b.Fatal(err)
	}
	for _, n := range scopeBenchmarkSizes {
		for _, k := range perKeySeekKeyCounts {
			for _, mode := range optimizerModes {
				b.Run(fmt.Sprintf("%d/%s/keys%d", n, mode.name, k), func(b *testing.B) {
					opts := mode.plannerOptions()
					d := openBackendDB(b, mode.backend, DatabaseOptions{PlannerOptions: &opts, DisableCache: true})
					writeScopeBenchmarkFixture(b, d, n)
					keys := make([]interface{}, k)
					for i := range keys {
						keys[i] = datalog.NewIdentity(fmt.Sprintf("scope/%d", i))
					}
					want := k
					if k > n {
						want = n
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						tuples, err := executor.CollectTuples(d.Query(q, keys))
						if err != nil {
							b.Fatal(err)
						}
						if len(tuples) != want {
							b.Fatalf("got %d tuples, want %d", len(tuples), want)
						}
					}
				})
			}
		}
	}
}
