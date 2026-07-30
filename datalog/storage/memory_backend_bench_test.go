package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Portable MemoryStore benches for native vs js/wasm comparison.
// These intentionally avoid Badger and path-backed databases so the same
// binary can run under GOOS=js GOARCH=wasm via the Node runner.

func openMemoryBenchDB(b *testing.B) *Database {
	b.Helper()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Store:        NewMemoryStore(nil),
		DisableCache: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	})
	return db
}

func BenchmarkMemoryKeyOnlyScanning(b *testing.B) {
	for _, size := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			store := NewMemoryStore(&BinaryKeyEncoder{})
			b.Cleanup(func() { _ = store.Close() })

			attr := datalog.NewKeyword(":test/value")
			datoms := make([]datalog.Datom, size)
			for i := 0; i < size; i++ {
				datoms[i] = datalog.Datom{
					E:  datalog.NewIdentity(fmt.Sprintf("entity-%d", i)),
					A:  attr,
					V:  int64(i),
					Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
				}
			}
			if err := store.Assert(datoms); err != nil {
				b.Fatal(err)
			}

			bound := ScanBound{Index: AEVT, Prefix: []datalog.Value{datoms[0].A}}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				it, err := store.ScanKeysOnly(bound)
				if err != nil {
					b.Fatal(err)
				}
				count := 0
				for it.Next() {
					d, err := it.Datom()
					if err != nil {
						b.Fatal(err)
					}
					_ = d.V
					count++
				}
				if err := it.Error(); err != nil {
					b.Fatal(err)
				}
				if err := it.Close(); err != nil {
					b.Fatal(err)
				}
				if count != size {
					b.Fatalf("expected %d datoms, got %d", size, count)
				}
			}
		})
	}
}

func BenchmarkMemoryResolveAllAttributesMany(b *testing.B) {
	// Batch-only: cache-disabled per-entity walks are pathological on
	// MemoryStore (minutes/op at 3899) and drown the native/wasm comparison.
	for _, entityCount := range []int{230, 3899} {
		b.Run(fmt.Sprintf("entities=%d/batch", entityCount), func(b *testing.B) {
			db, entities := setupMemoryBatchWildcardDB(b, entityCount, 5)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := db.ResolveAllAttributesMany(entities); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func setupMemoryBatchWildcardDB(b *testing.B, entityCount, attrsPerEntity int) (*Database, []datalog.Identity) {
	b.Helper()
	db := openMemoryBenchDB(b)
	attrs := make([]datalog.Keyword, attrsPerEntity)
	for i := range attrs {
		attrs[i] = datalog.NewKeyword(fmt.Sprintf(":entity/attr%d", i))
	}
	entities := make([]datalog.Identity, entityCount)
	tx := db.NewTransaction()
	for i := range entities {
		entities[i] = datalog.NewIdentity(fmt.Sprintf("batch-entity:%d", i))
		for attrIndex, attr := range attrs {
			if err := tx.Set(entities[i], attr, fmt.Sprintf("value-%d-%d", i, attrIndex)); err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	return db, entities
}

func BenchmarkMemoryStorageHashJoinCompiledMatching(b *testing.B) {
	db := openMemoryBenchDB(b)

	groupAttr := datalog.NewKeyword(":item/group")
	groups := make([]datalog.Identity, 100)
	for i := range groups {
		groups[i] = datalog.NewIdentity(fmt.Sprintf("group-%d", i))
	}
	tx := db.NewTransaction()
	for i := 0; i < 10_000; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("item-%d", i))
		if err := tx.Add(entity, groupAttr, groups[i%len(groups)]); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	groupSymbol := datalog.NewSymbol("?group")
	noiseSymbol := datalog.NewSymbol("?noise")
	entitySymbol := datalog.NewSymbol("?e")
	bindingTuples := make([]executor.Tuple, 50)
	for i := range bindingTuples {
		bindingTuples[i] = executor.Tuple{int64(i), groups[i]}
	}
	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{noiseSymbol, groupSymbol},
		bindingTuples,
	)
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entitySymbol},
		query.Constant{Value: groupAttr},
		query.Variable{Name: groupSymbol},
	}}
	resultSymbols := []query.Symbol{entitySymbol, groupSymbol}
	matcher := NewPatternMatcher(db.Store())

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := matcher.matchWithHashJoin(
			pattern,
			bindingRel,
			resultSymbols,
			2,
			AVET,
			nil,
		)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		it := result.Iterator()
		for it.Next() {
			count++
		}
		if err := it.Error(); err != nil {
			b.Fatal(err)
		}
		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
		if count != 5_000 {
			b.Fatalf("got %d matches, want 5000", count)
		}
	}
}

func BenchmarkMemorySimpleQuery(b *testing.B) {
	db := openMemoryBenchDB(b)
	nameAttr := datalog.NewKeyword(":person/name")
	ageAttr := datalog.NewKeyword(":person/age")
	tx := db.NewTransaction()
	for i := 0; i < 2000; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("person-%d", i))
		if err := tx.Add(e, nameAttr, fmt.Sprintf("name-%d", i)); err != nil {
			b.Fatal(err)
		}
		if err := tx.Add(e, ageAttr, int64(20+i%50)); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	q := `[:find ?e ?name :where [?e :person/age 30] [?e :person/name ?name]]`
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rel, err := db.Query(q)
		if err != nil {
			b.Fatal(err)
		}
		tuples, err := executor.CollectTuples(rel, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(tuples) == 0 {
			b.Fatal("expected matches")
		}
	}
}

func BenchmarkMemoryRetract(b *testing.B) {
	for _, size := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			store := NewMemoryStore(&BinaryKeyEncoder{})
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

// BenchmarkMemoryAssertBulk tracks Import-shaped growth: N×4 datoms must not
// produce ~N² wall time (the old sorted-[]string insert pathology).
func BenchmarkMemoryAssertBulk(b *testing.B) {
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
				store := NewMemoryStore(&BinaryKeyEncoder{})
				if err := store.Assert(datoms); err != nil {
					b.Fatal(err)
				}
				_ = store.Close()
			}
		})
	}
}
