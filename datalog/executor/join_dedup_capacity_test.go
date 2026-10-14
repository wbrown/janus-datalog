package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A hash join sizes its dedup map from the build tuples it has collected,
// which it holds before the probe begins, an empty build side included: a
// join of an empty streaming relation with a three-tuple one, whose sizes it
// cannot read ahead, yields nothing and allocates less than a thousand-entry
// dedup map costs on its own, on the materialized path and on the streaming
// one. The semi-naive fixpoint joins an empty delta on every iteration that
// converges.
func TestHashJoinSizesItsDedupMapFromTheBuildTuples(t *testing.T) {
	x, y, z := datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")
	streaming := func(symbols []query.Symbol, tuples []Tuple) Relation {
		return NewStreamingRelation(symbols, NewMaterializedRelation(symbols, tuples).Iterator())
	}
	left := func() Relation {
		return streaming([]query.Symbol{x, y}, nil)
	}
	right := func() Relation {
		return streaming([]query.Symbol{x, z}, []Tuple{{int64(1), "p"}, {int64(2), "q"}, {int64(3), "r"}})
	}
	thousand := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			NewTupleKeyMapWithCapacity(1000)
		}
	}).AllocedBytesPerOp()
	for _, path := range []struct {
		name string
		opts ExecutorOptions
	}{
		{"materialized", ExecutorOptions{}},
		{"streaming", ExecutorOptions{EnableStreamingJoins: true}},
	} {
		t.Run(path.name, func(t *testing.T) {
			joined := testing.Benchmark(func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					tuples, err := CollectTuples(HashJoinWithOptions(left(), right(), []query.Symbol{x}, path.opts), nil)
					if err != nil {
						b.Fatal(err)
					}
					if len(tuples) != 0 {
						b.Fatalf("joined %d tuples against an empty build side, want none", len(tuples))
					}
				}
			}).AllocedBytesPerOp()
			if joined >= thousand {
				t.Errorf("a join against an empty build side allocated %d bytes, at least the %d bytes a thousand-entry dedup map costs alone", joined, thousand)
			}
		})
	}
}
