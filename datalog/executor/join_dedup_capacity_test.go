package executor

import (
	"runtime"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// bytesAllocatedBy returns the bytes one call of f allocates, read from the
// runtime's total allocation before and after the call, after a collection
// so that nothing pending from before lands inside the window.
func bytesAllocatedBy(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

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
	var held *TupleKeyMap
	thousand := bytesAllocatedBy(func() {
		held = NewTupleKeyMapWithCapacity(1000)
	})
	runtime.KeepAlive(held)
	for _, path := range []struct {
		name string
		opts ExecutorOptions
	}{
		{"materialized", ExecutorOptions{}},
		{"streaming", ExecutorOptions{EnableStreamingJoins: true}},
	} {
		t.Run(path.name, func(t *testing.T) {
			var tuples [][]interface{}
			var err error
			joined := bytesAllocatedBy(func() {
				tuples, err = CollectTuples(HashJoinWithOptions(left(), right(), []query.Symbol{x}, path.opts), nil)
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(tuples) != 0 {
				t.Fatalf("joined %d tuples against an empty build side, want none", len(tuples))
			}
			t.Logf("a thousand-entry dedup map costs %d bytes; the join allocated %d", thousand, joined)
			if joined >= thousand {
				t.Errorf("a join against an empty build side allocated %d bytes, at least the %d bytes a thousand-entry dedup map costs alone", joined, thousand)
			}
		})
	}
}
