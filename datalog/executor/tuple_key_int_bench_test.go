package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Allocation-free microbenchmarks for the TupleKey hash path touched by the
// integer-width change (hashValue gained int8/int16/int32 cases). These isolate
// the hash computation from the GC-dominated macro hash-join benchmarks.

var tupleKeySink TupleKey

func BenchmarkTupleKeyFull_Int64(b *testing.B) {
	tu := Tuple{int64(1234567)}
	for i := 0; i < b.N; i++ {
		tupleKeySink = NewTupleKeyFull(tu)
	}
}

func BenchmarkTupleKeyFull_Identity(b *testing.B) {
	tu := Tuple{datalog.NewIdentity("entity-1234567")}
	for i := 0; i < b.N; i++ {
		tupleKeySink = NewTupleKeyFull(tu)
	}
}
