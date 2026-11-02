//go:build !(js && wasm)

package storage

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestProfileBinaryDecoding profiles query execution with binary encoding
func TestProfileBinaryDecoding(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping profile in short mode")
	}

	profileQueryExecution(t, "cpu_binary_decode.prof", 10000)
}

func profileQueryExecution(t *testing.T, profileName string, datasetSize int) {
	encoder := &BinaryKeyEncoder{}
	store, err := NewBadgerStore(t.TempDir(), encoder)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close the store: %v", err)
		}
	}()

	// Insert test data with multiple attributes to exercise different access patterns
	t.Logf("Inserting %d datoms...", datasetSize)
	datoms := make([]datalog.Datom, datasetSize)
	for i := 0; i < datasetSize; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		attr := datalog.NewKeyword(":test/value")
		datoms[i] = datalog.Datom{
			E:  entity,
			A:  attr,
			V:  int64(i % 1000),                                           // Modulo to get some duplicate values
			Tx: datalog.ElementID{Lamport: uint64(i / 100), ReplicaID: 1}, // Group into transactions
		}
	}

	if err := store.Assert(datoms); err != nil {
		t.Fatal(err)
	}

	// Start CPU profiling
	f, err := os.Create(profileName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("close %s: %v", profileName, err)
		}
	}()

	t.Logf("Starting CPU profile: %s", profileName)
	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatal(err)
	}
	defer pprof.StopCPUProfile()

	// Run queries to stress the decoding path
	bound := ScanBound{Index: AEVT, Prefix: []datalog.Value{datoms[0].A}}
	written := make(map[datalog.Identity]interface{}, datasetSize)
	for _, d := range datoms {
		written[d.E] = d.V
	}

	// Every scan reads every datom written; the first also checks each
	// decoded value against the one written for its entity.
	totalScanned := 0
	for i := 0; i < 100; i++ {
		it, err := store.Scan(bound)
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for it.Next() {
			d, err := it.Datom()
			if err != nil {
				t.Fatal(err)
			}
			if i == 0 && d.V != written[d.E] {
				t.Fatalf("entity %v decoded %v, written %v", d.E, d.V, written[d.E])
			}
			count++
		}
		if err := it.Error(); err != nil {
			t.Fatal(err)
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		if count != datasetSize {
			t.Fatalf("scan %d read %d datoms, want %d", i, count, datasetSize)
		}
		totalScanned += count
	}

	pprof.StopCPUProfile()
	t.Logf("Profile complete: %s (scanned %d datoms across 100 iterations)", profileName, totalScanned)
	t.Logf("View with: go tool pprof -http=:8080 %s", profileName)
}

// BenchmarkDecodingInPipeline measures decoding overhead in realistic query execution
func BenchmarkDecodingInPipeline(b *testing.B) {
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
					encoder := tc.encoder
					store, err := NewBadgerStore(b.TempDir(), encoder)
					if err != nil {
						b.Fatal(err)
					}
					defer func() {
						if err := store.Close(); err != nil {
							b.Errorf("close the store: %v", err)
						}
					}()

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

					bound := ScanBound{Index: AEVT, Prefix: []datalog.Value{datoms[0].A}}

					b.ResetTimer()
					b.ReportAllocs()

					for i := 0; i < b.N; i++ {
						it, err := store.Scan(bound)
						if err != nil {
							b.Fatal(err)
						}

						count := 0
						for it.Next() {
							d, err := it.Datom()
							if err != nil {
								b.Fatal(err)
							}
							if d.A != attr {
								b.Fatalf("scan of %v decoded a datom of %v", attr, d.A)
							}
							count++
						}
						if err := it.Error(); err != nil {
							b.Fatal(err)
						}
						if err := it.Close(); err != nil {
							b.Fatal(err)
						}
						if count != size {
							b.Fatalf("scan read %d datoms, want %d", count, size)
						}
					}
				})
			}
		})
	}
}
