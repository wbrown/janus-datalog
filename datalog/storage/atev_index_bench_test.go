package storage

import (
	"fmt"
	"os"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
)

// maxElementIDViaAEVTScan replicates the pre-ATEV implementation of
// MaxElementIDForAttribute: forward-scan every AEVT entry under the attribute
// prefix and track the maximum Tx. Kept here purely so the benchmark can
// compare against the post-ATEV single-seek path on identical data.
func maxElementIDViaAEVTScan(s *BadgerStore, a []byte) (datalog.ElementID, error) {
	var maxID datalog.ElementID
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		aevtPrefix := make([]byte, 1+32)
		aevtPrefix[0] = byte(AEVT)
		copy(aevtPrefix[1:33], a)

		it.Seek(aevtPrefix)
		for it.Valid() {
			key := it.Item().Key()
			if key[0] != byte(AEVT) || !bytesEqual(key[1:33], aevtPrefix[1:33]) {
				break
			}
			_, _, _, tx, _, _, err := s.encoder.DecodeKey(AEVT, key)
			if err != nil {
				it.Next()
				continue
			}
			elemID := Tx(tx).ToElementID()
			if elemID.Compare(maxID) > 0 {
				maxID = elemID
			}
			it.Next()
		}
		return nil
	})
	return maxID, err
}

// populateAttribute writes `count` distinct (E, A, V, Tx) datoms for attribute
// `a`. Each entity gets a unique Identity and a monotonically increasing Tx so
// the freshness check has a clear answer (max Tx == count) and so the scan
// must actually traverse the full attribute range.
func populateAttribute(b *testing.B, store *BadgerStore, a datalog.Keyword, count int) {
	b.Helper()
	datoms := make([]datalog.Datom, count)
	for i := 0; i < count; i++ {
		datoms[i] = datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("entity-%d", i)),
			A:  a,
			V:  int64(i),
			Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1},
		}
	}
	if err := store.Assert(datoms); err != nil {
		b.Fatalf("assert: %v", err)
	}
}

// BenchmarkMaxElementIDForAttribute_ATEVSeek_vs_AEVTScan measures the
// freshness-path win at several attribute cardinalities. The ATEV column is
// the production path (single forward seek); the AEVT column is the
// pre-existing forward scan.
//
// Expectation: ATEV is roughly constant-time; AEVT scales linearly with the
// number of datoms for the attribute.
func BenchmarkMaxElementIDForAttribute_ATEVSeek_vs_AEVTScan(b *testing.B) {
	cardinalities := []int{10, 100, 1000, 10000}
	for _, n := range cardinalities {
		b.Run(fmt.Sprintf("ATEV_seek/N=%d", n), func(b *testing.B) {
			dir, err := os.MkdirTemp("", "atev-bench-seek-*")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(dir)
			store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()

			a := datalog.NewKeyword(":bench/attr")
			populateAttribute(b, store, a, n)
			var aBytes [32]byte
			copy(aBytes[:], a.String())

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := store.MaxElementIDForAttribute(aBytes[:]); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("AEVT_scan/N=%d", n), func(b *testing.B) {
			dir, err := os.MkdirTemp("", "atev-bench-scan-*")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(dir)
			store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()

			a := datalog.NewKeyword(":bench/attr")
			populateAttribute(b, store, a, n)
			var aBytes [32]byte
			copy(aBytes[:], a.String())

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := maxElementIDViaAEVTScan(store, aBytes[:]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

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
				store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
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
