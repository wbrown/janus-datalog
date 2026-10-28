package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// BenchmarkCardinalityManyUnboundScan measures a pattern over a
// cardinality-many attribute with the entity open, [?e :person/tags ?tag],
// on the tree store: the scan enumerates every entity carrying the attribute
// and answers each one's current set. The cache arm runs with the EA cache,
// the nocache arm with DisableCache, so the delta is what resolving each
// entity's set costs when the set is already resolved. The measured loop
// re-runs the query after a warm-up, so the cache arm is steady state.
//
// Run:
//   go test ./datalog/storage/ -run '^$' -bench 'BenchmarkCardinalityManyUnboundScan' -benchmem
//
// allocs/op is the signal: a set served from the cache allocates no cursor,
// accumulator, key or state per entity.
func BenchmarkCardinalityManyUnboundScan(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("N=%d/cache", n), func(b *testing.B) {
			benchmarkCardinalityManyUnboundScan(b, n, 4, false)
		})
		b.Run(fmt.Sprintf("N=%d/nocache", n), func(b *testing.B) {
			benchmarkCardinalityManyUnboundScan(b, n, 4, true)
		})
	}
}

// benchmarkCardinalityManyUnboundScan loads n entities each carrying members
// tags and times the unbound scan, which answers n*members tuples.
func benchmarkCardinalityManyUnboundScan(b *testing.B, n, members int, disableCache bool) {
	tags := datalog.NewKeyword(":person/tags")
	s, err := schema.NewBuilder().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	if err != nil {
		b.Fatal(err)
	}
	db := openBackendDB(b, engineBackends()[0], DatabaseOptions{Schema: s, DisableCache: disableCache})

	tx := db.NewTransaction()
	for i := 0; i < n; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("person-%d", i))
		for j := 0; j < members; j++ {
			if err := tx.Add(e, tags, fmt.Sprintf("tag-%d", j)); err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	const query = `[:find ?e ?tag :where [?e :person/tags ?tag]]`
	expect := n * members

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
