package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// BenchmarkEncoderComparison compares Binary vs L85 key encoding performance
func BenchmarkEncoderComparison(b *testing.B) {
	strategies := []struct {
		name     string
		strategy KeyEncodingStrategy
	}{
		{"Binary", BinaryStrategy},
		{"L85", L85Strategy},
	}

	// Create test datoms
	entity := datalog.NewIdentity("test-entity")
	attr := datalog.NewKeyword(":test/attribute")

	for _, strat := range strategies {
		b.Run(strat.name+"/Encode", func(b *testing.B) {
			encoder := NewKeyEncoder(strat.strategy)
			datom := &datalog.Datom{
				E:  entity,
				A:  attr,
				V:  int64(42),
				Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				encoder.EncodeKey(EAVT, datom)
			}
		})

		b.Run(strat.name+"/DatomFromKey", func(b *testing.B) {
			encoder := NewKeyEncoder(strat.strategy)
			datom := &datalog.Datom{
				E:  entity,
				A:  attr,
				V:  int64(42),
				Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
			}
			key := encoder.EncodeKey(EAVT, datom)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				DatomFromKey(EAVT, key, encoder, nil)
			}
		})
	}
}

// BenchmarkScanWithEncodings compares full scan performance with different encoders
func BenchmarkScanWithEncodings(b *testing.B) {
	datasetSizes := []int{1000, 10000}

	for _, size := range datasetSizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			strategies := []struct {
				name     string
				strategy KeyEncodingStrategy
			}{
				{"Binary", BinaryStrategy},
				{"L85", L85Strategy},
			}

			for _, strat := range strategies {
				b.Run(strat.name, func(b *testing.B) {
					// Create temp database
					dir, err := os.MkdirTemp("", "bench-encoding-*")
					if err != nil {
						b.Fatal(err)
					}
					defer os.RemoveAll(dir)

					encoder := NewKeyEncoder(strat.strategy)
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

					// Benchmark scan
					attrHash := ToStorageDatom(datoms[0]).A
					start, end := encoder.EncodePrefixRange(AEVT, attrHash[:])

					b.ResetTimer()
					b.ReportAllocs()

					for i := 0; i < b.N; i++ {
						it, err := store.Scan(AEVT, start, end)
						if err != nil {
							b.Fatal(err)
						}

						count := 0
						for it.Next() {
							_, err := it.Datom()
							if err != nil {
								b.Fatal(err)
							}
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

// BenchmarkStorageSize compares storage size overhead
func BenchmarkStorageSize(b *testing.B) {
	entity := datalog.NewIdentity("test-entity")
	attr := datalog.NewKeyword(":test/attribute")
	datom := &datalog.Datom{
		E:  entity,
		A:  attr,
		V:  int64(42),
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}

	binaryEncoder := NewKeyEncoder(BinaryStrategy)
	l85Encoder := NewKeyEncoder(L85Strategy)

	binaryKey := binaryEncoder.EncodeKey(EAVT, datom)
	l85Key := l85Encoder.EncodeKey(EAVT, datom)

	b.Logf("Binary key size: %d bytes", len(binaryKey))
	b.Logf("L85 key size: %d bytes", len(l85Key))
	b.Logf("L85 overhead: %.1f%%", float64(len(l85Key)-len(binaryKey))/float64(len(binaryKey))*100)
}
