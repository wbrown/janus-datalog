package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Allocation-free microbenchmarks for the tuple-key hash path, including the
// int8/int16/int32 cases in HashValue. These isolate the hash computation
// from the GC-dominated macro hash-join benchmarks.

var tupleHashSink uint64

func BenchmarkHashTuplePositions_Int64(b *testing.B) {
	tu := Tuple{int64(1234567)}
	positions := allPositions(1)
	for i := 0; i < b.N; i++ {
		tupleHashSink = hashTuplePositions(tu, positions)
	}
}

func BenchmarkHashTuplePositions_Identity(b *testing.B) {
	tu := Tuple{datalog.NewIdentity("entity-1234567")}
	positions := allPositions(1)
	for i := 0; i < b.N; i++ {
		tupleHashSink = hashTuplePositions(tu, positions)
	}
}
