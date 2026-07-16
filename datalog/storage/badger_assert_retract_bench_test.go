//go:build !(js && wasm)

package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Badger mirrors of BenchmarkMemoryAssertBulk / BenchmarkMemoryRetract for
// same-host Store-path comparison (8 keys/datom, keys-only values).

func BenchmarkBadgerAssertBulk(b *testing.B) {
	for _, size := range []int{1000, 4000} {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			attr := datalog.NewKeyword(":bench/assert-bulk")
			datoms := make([]datalog.Datom, size)
			for i := 0; i < size; i++ {
				datoms[i] = datalog.Datom{
					E:  datalog.NewIdentity(fmt.Sprintf("assert-entity-%d", i)),
					A:  attr,
					V:  int64(i),
					Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				dir, err := os.MkdirTemp("", "badger-assert-bulk-*")
				if err != nil {
					b.Fatal(err)
				}
				store, err := NewBadgerStore(dir, &BinaryKeyEncoder{})
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if err := store.Assert(datoms); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				_ = store.Close()
				_ = os.RemoveAll(dir)
				b.StartTimer()
			}
		})
	}
}

func BenchmarkBadgerRetract(b *testing.B) {
	for _, size := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			dir, err := os.MkdirTemp("", "badger-retract-*")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = os.RemoveAll(dir) })

			store, err := NewBadgerStore(dir, &BinaryKeyEncoder{})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })

			attr := datalog.NewKeyword(":bench/retract")
			datoms := make([]datalog.Datom, size)
			for i := 0; i < size; i++ {
				datoms[i] = datalog.Datom{
					E:  datalog.NewIdentity(fmt.Sprintf("retract-entity-%d", i)),
					A:  attr,
					V:  int64(i),
					Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
				}
			}
			if err := store.Assert(datoms); err != nil {
				b.Fatal(err)
			}
			target := datoms[size/2]

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := store.Assert([]datalog.Datom{target}); err != nil {
					b.Fatal(err)
				}
				if err := store.Retract([]datalog.Datom{target}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
