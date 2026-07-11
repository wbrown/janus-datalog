package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestTupleKeyMapPutIfAbsent(t *testing.T) {
	m := NewTupleKeyMap()
	key := NewTupleKeyFull(Tuple{datalog.NewIdentity("entity:1"), "value"})

	if existed := m.PutIfAbsent(key, "first"); existed {
		t.Fatal("first insertion reported an existing key")
	}
	if existed := m.PutIfAbsent(key, "second"); !existed {
		t.Fatal("repeated insertion reported a new key")
	}

	got, ok := m.Get(key)
	if !ok {
		t.Fatal("inserted key was not found")
	}
	if got != "first" {
		t.Fatalf("PutIfAbsent replaced the original value: got %v", got)
	}
}

func BenchmarkDedupInsertionPaths(b *testing.B) {
	const tupleCount = 10_000

	cases := []struct {
		name        string
		uniqueCount int
	}{
		{name: "unique-heavy", uniqueCount: tupleCount},
		{name: "duplicate-heavy", uniqueCount: 100},
	}

	for _, bc := range cases {
		tuples := dedupBenchmarkTuples(tupleCount, bc.uniqueCount)

		b.Run(bc.name+"/materialized", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result := deduplicateTuples(tuples)
				if len(result) != bc.uniqueCount {
					b.Fatalf("got %d tuples, want %d", len(result), bc.uniqueCount)
				}
			}
		})

		b.Run(bc.name+"/streaming", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				source := &sliceIterator{tuples: tuples, pos: -1}
				iter := NewDedupIterator(source, len(tuples))
				count := 0
				for iter.Next() {
					count++
				}
				if err := iter.Error(); err != nil {
					b.Fatal(err)
				}
				if err := iter.Close(); err != nil {
					b.Fatal(err)
				}
				if count != bc.uniqueCount {
					b.Fatalf("got %d tuples, want %d", count, bc.uniqueCount)
				}
			}
		})
	}
}

func dedupBenchmarkTuples(tupleCount, uniqueCount int) []Tuple {
	unique := make([]Tuple, uniqueCount)
	for i := range unique {
		unique[i] = Tuple{
			datalog.NewIdentity(fmt.Sprintf("entity:%d", i)),
			fmt.Sprintf("value-%d", i),
		}
	}

	tuples := make([]Tuple, tupleCount)
	for i := range tuples {
		tuples[i] = unique[i%uniqueCount]
	}
	return tuples
}
