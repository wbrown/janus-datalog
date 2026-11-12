package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// The paths a memory-tree store takes a datom, a seek or a commit at a time,
// on a store of the default branching factor. They use nothing a store of
// before the branching factor lacks, so the same file measures a tree as it is
// against a tree as it was:
//
//	go test -run '^$' -bench TreeStoreHotPaths -count 10 ./datalog/storage
//
// with -benchtime 200000x for the commit, so that every run grows the same
// store.

const hotPathsHeld = 200_000

// hotPathsStore is a default store grown to hotPathsHeld datoms a commit at a
// time, so that its trees are made by inserts and splits and not packed full
// by one sorted build.
func hotPathsStore(b *testing.B) (*MemoryTreeStore, []datalog.Datom) {
	datoms := treeBatchDatoms(hotPathsHeld)
	store := NewMemoryTreeStore(nil)
	for i := range datoms {
		if err := store.Assert(datoms[i : i+1]); err != nil {
			b.Fatal(err)
		}
	}
	return store, datoms
}

// BenchmarkTreeStoreHotPathsScan walks every datom of one index: what a scan
// costs a datom.
func BenchmarkTreeStoreHotPathsScan(b *testing.B) {
	store, _ := hotPathsStore(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan, err := store.Scan(ScanBound{Index: TAEV})
		if err != nil {
			b.Fatal(err)
		}
		walked := 0
		for scan.Next() {
			walked++
		}
		if err := scan.Close(); err != nil {
			b.Fatal(err)
		}
		if walked != hotPathsHeld {
			b.Fatalf("walked %d datoms of %d", walked, hotPathsHeld)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/hotPathsHeld, "ns/datom")
}

// BenchmarkTreeStoreHotPathsSeek opens a scan at one entity, takes its first
// datom and closes it: a cursor made, a descent, one step.
func BenchmarkTreeStoreHotPathsSeek(b *testing.B) {
	store, datoms := hotPathsStore(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan, err := store.Scan(ScanBound{Index: EAVT, Prefix: []datalog.Value{datoms[i%hotPathsHeld].E}})
		if err != nil {
			b.Fatal(err)
		}
		if !scan.Next() {
			b.Fatal("the entity's scan found no datom")
		}
		if err := scan.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTreeStoreHotPathsCommit asserts b.N datoms into an empty store, a
// commit each: what a commit of one datom costs, in time and in bytes.
func BenchmarkTreeStoreHotPathsCommit(b *testing.B) {
	datoms := treeBatchDatoms(b.N)
	store := NewMemoryTreeStore(nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range datoms {
		if err := store.Assert(datoms[i : i+1]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := store.Close(); err != nil {
		b.Fatal(err)
	}
}
