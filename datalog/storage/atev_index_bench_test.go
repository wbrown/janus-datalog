//go:build !(js && wasm)

package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// BenchmarkAssert_WriteCost measures the cost of writing N datoms with the
// 8-index ATEV configuration. Each additional index is roughly 1/8 of the
// total key-write cost, so the ATEV tax is approximately ns_per_datom / 8.
// Run alongside removing ATEV from Indices on a comparison branch to validate
// the ~14% extra-cost estimate empirically.
func BenchmarkAssert_WriteCost(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir, err := os.MkdirTemp("", "atev-bench-write-*")
				if err != nil {
					b.Fatal(err)
				}
				store, err := NewBadgerStore(dir, &BinaryKeyEncoder{})
				if err != nil {
					b.Fatal(err)
				}
				datoms := make([]datalog.Datom, n)
				a := datalog.NewKeyword(":bench/write")
				for j := 0; j < n; j++ {
					datoms[j] = datalog.Datom{
						E:  datalog.NewIdentity(fmt.Sprintf("we-%d-%d", i, j)),
						A:  a,
						V:  int64(j),
						Tx: datalog.ElementID{Lamport: uint64(j + 1), ReplicaID: 1},
					}
				}
				b.StartTimer()

				if err := store.Assert(datoms); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				store.Close()
				os.RemoveAll(dir)
				b.StartTimer()
			}
		})
	}
}
