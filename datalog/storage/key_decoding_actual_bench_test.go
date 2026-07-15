//go:build !(js && wasm)

package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// BenchmarkKeyOnlyScanning uses ScanKeysOnly which actually decodes keys
func BenchmarkKeyOnlyScanning(b *testing.B) {
	sizes := []int{1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			encoders := []struct {
				name    string
				encoder *BinaryKeyEncoder
			}{
				{"Binary", &BinaryKeyEncoder{}},
			}

			for _, tc := range encoders {
				b.Run(tc.name, func(b *testing.B) {
					// Create temp database
					dir, err := os.MkdirTemp("", "bench-keyonly-*")
					if err != nil {
						b.Fatal(err)
					}
					defer os.RemoveAll(dir)

					encoder := tc.encoder
					store, err := NewBadgerStore(dir, encoder)
					if err != nil {
						b.Fatal(err)
					}
					defer store.Close()

					// Insert test data
					datoms := make([]datalog.Datom, size)
					attr := datalog.NewKeyword(":test/value")
					for i := 0; i < size; i++ {
						entity := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
						datoms[i] = datalog.Datom{
							E:  entity,
							A:  attr,
							V:  int64(i),
							Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
						}
					}

					if err := store.Assert(datoms); err != nil {
						b.Fatal(err)
					}

					attrHash := ToStorageDatom(datoms[0]).A
					start, end := encoder.EncodePrefixRange(AEVT, attrHash[:])

					b.ResetTimer()
					b.ReportAllocs()

					for i := 0; i < b.N; i++ {
						// Use ScanKeysOnly which decodes from keys
						it, err := store.ScanKeysOnly(AEVT, start, end)
						if err != nil {
							b.Fatal(err)
						}

						count := 0
						for it.Next() {
							d, err := it.Datom()
							if err != nil {
								b.Fatal(err)
							}
							// Force materialization
							_ = d.V
							count++
						}
						it.Close()

						if count != size {
							b.Fatalf("expected %d datoms, got %d", size, count)
						}
					}
				})
			}
		})
	}
}

// BenchmarkDatomFromKeyDirect directly benchmarks key decoding
func BenchmarkDatomFromKeyDirect(b *testing.B) {
	sizes := []int{100, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			encoders := []struct {
				name    string
				encoder *BinaryKeyEncoder
			}{
				{"Binary", &BinaryKeyEncoder{}},
			}

			for _, tc := range encoders {
				b.Run(tc.name, func(b *testing.B) {
					encoder := tc.encoder

					// Pre-generate keys
					keys := make([][]byte, size)
					attr := datalog.NewKeyword(":test/value")
					for i := 0; i < size; i++ {
						entity := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
						datom := &datalog.Datom{
							E:  entity,
							A:  attr,
							V:  int64(i),
							Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
						}
						keys[i] = encoder.EncodeKey(EAVT, datom)
					}

					b.ResetTimer()
					b.ReportAllocs()

					totalDecoded := 0
					for i := 0; i < b.N; i++ {
						for _, key := range keys {
							d, err := DatomFromKey(EAVT, key, encoder, nil)
							if err != nil {
								b.Fatal(err)
							}
							// Force materialization
							_ = d.V
							totalDecoded++
						}
					}

					b.ReportMetric(float64(totalDecoded)/float64(b.N), "datoms/op")
				})
			}
		})
	}
}

// BenchmarkValueDeserializationVsKeyDecoding compares both approaches
func BenchmarkValueDeserializationVsKeyDecoding(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-compare-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	encoder := &BinaryKeyEncoder{}
	store, err := NewBadgerStore(dir, encoder)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Insert 1000 datoms
	size := 1000
	datoms := make([]datalog.Datom, size)
	attr := datalog.NewKeyword(":test/value")
	for i := 0; i < size; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		datoms[i] = datalog.Datom{
			E:  entity,
			A:  attr,
			V:  int64(i),
			Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
		}
	}

	if err := store.Assert(datoms); err != nil {
		b.Fatal(err)
	}

	attrHash := ToStorageDatom(datoms[0]).A
	start, end := encoder.EncodePrefixRange(AEVT, attrHash[:])

	b.Run("ValueDeserialization", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it, _ := store.Scan(AEVT, start, end)
			count := 0
			for it.Next() {
				d, _ := it.Datom()
				_ = d.V
				count++
			}
			it.Close()
		}
	})

	b.Run("KeyDecoding", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it, _ := store.ScanKeysOnly(AEVT, start, end)
			count := 0
			for it.Next() {
				d, _ := it.Datom()
				_ = d.V
				count++
			}
			it.Close()
		}
	})
}
