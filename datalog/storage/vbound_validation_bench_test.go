package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// =============================================================================
// V-bound validation fast-path benchmark
// =============================================================================
//
// Measures the validateCandidate cache fast path: a V-bound query
// [?e :place/type "room"] (A+V constant, E unbound, CardinalityOne non-unique —
// a type/category-tag lookup where many entities share one value) drives one
// validateCandidate call per emitted candidate. With the EA cache enabled, each
// validation is an O(1) cache lookup;
// with DisableCache, each is an EATV point seek (Badger ConcatIterator
// open/close + IncrRef/DecrRef). Both modes pay the AVET candidate enumeration,
// so the cache-vs-nocache delta isolates the validation cost — the share the
// downstream profile attributed to validatingVBoundIterator.validateCandidate.
//
// Run:
//   go test ./datalog/storage/ -run '^$' -bench 'BenchmarkVBoundValidation' -benchmem
//
// allocs/op is the cleanest signal: on the cache side the per-candidate Badger
// iterator allocations disappear.

// benchmarkVBoundValidation loads n entities that all hold :place/type = "room",
// optionally re-typing the first half to "cave" so the "room" AVET group also
// carries stale candidates that validateCandidate must reject. The measured loop
// re-runs the V-bound query after a warm-up, matching a long-running process
// whose EA cache stays warm.
func benchmarkVBoundValidation(b *testing.B, n int, disableCache, supersede bool) {
	a := datalog.NewKeyword(":place/type")

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:         b.TempDir(),
		Schema:       placeTypeSchema(),
		ReplicaID:    1,
		DisableCache: disableCache,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	entities := make([]datalog.Identity, n)
	tx := db.NewTransaction()
	for i := 0; i < n; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("place-%d", i))
		entities[i] = e
		if err := tx.Add(e, a, "room"); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	expect := n
	if supersede {
		// Re-type the first half to "cave": they leave stale "room" AVET keys
		// (still emitted as candidates) but are no longer live, so the candidate
		// count stays n while only n-n/2 are valid — exercising the reject path.
		tx2 := db.NewTransaction()
		for i := 0; i < n/2; i++ {
			if err := tx2.Add(entities[i], a, "cave"); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := tx2.Commit(); err != nil {
			b.Fatal(err)
		}
		expect = n - n/2
	}

	const query = `[:find ?e :where [?e :place/type "room"]]`

	// Warm the EA and parse caches so the measured loop is steady-state.
	if got := countVBoundQuery(b, db, query); got != expect {
		b.Fatalf("warmup: expected %d results, got %d", expect, got)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := countVBoundQuery(b, db, query); got != expect {
			b.Fatalf("expected %d results, got %d", expect, got)
		}
	}
}

// countVBoundQuery runs the query and drains the iterator fully (so every
// candidate is validated), returning the result count.
func countVBoundQuery(b *testing.B, db *Database, query string) int {
	rel, err := db.Query(query)
	if err != nil {
		b.Fatal(err)
	}
	count := 0
	it := rel.Iterator()
	for it.Next() {
		count++
	}
	it.Close()
	return count
}

// BenchmarkVBoundValidation: all candidates live (set-once), the steady-state
// case. Each query validates n candidates; cache vs nocache isolates the
// per-candidate seek cost.
func BenchmarkVBoundValidation(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("N=%d/cache", n), func(b *testing.B) {
			benchmarkVBoundValidation(b, n, false, false)
		})
		b.Run(fmt.Sprintf("N=%d/nocache", n), func(b *testing.B) {
			benchmarkVBoundValidation(b, n, true, false)
		})
	}
}

// BenchmarkVBoundValidationSupersession: half the entities re-typed away from
// "room", so the candidate count stays n while only half are live — amplifying
// validation work and exercising the reject path.
func BenchmarkVBoundValidationSupersession(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("N=%d/cache", n), func(b *testing.B) {
			benchmarkVBoundValidation(b, n, false, true)
		})
		b.Run(fmt.Sprintf("N=%d/nocache", n), func(b *testing.B) {
			benchmarkVBoundValidation(b, n, true, true)
		})
	}
}
