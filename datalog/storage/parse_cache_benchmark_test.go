package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

const benchQuery = `[:find ?e ?name ?age
  :where [?e :person/name ?name]
         [?e :person/age ?age]
         [(> ?age 18)]]`

// BenchmarkResolveQueryNoCache measures resolveQuery with parse cache disabled
// (re-parses every call).
func BenchmarkResolveQueryNoCache(b *testing.B) {
	db, err := NewDatabase(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.SetParseCache(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.resolveQuery(benchQuery)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkResolveQueryWithCache measures resolveQuery with parse cache enabled
// (cache hit after first call).
func BenchmarkResolveQueryWithCache(b *testing.B) {
	db, err := NewDatabase(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Warm cache
	db.resolveQuery(benchQuery)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.resolveQuery(benchQuery)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkResolveQueryPreParsed measures resolveQuery with pre-parsed *query.Query
// (baseline — no parsing, no cache lookup).
func BenchmarkResolveQueryPreParsed(b *testing.B) {
	db, err := NewDatabase(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	q, err := parser.ParseQuery(benchQuery)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.resolveQuery(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- End-to-end benchmarks: full Query() path including execution ---

func setupParseCacheBenchDB(b *testing.B) *Database {
	b.Helper()
	db, err := NewDatabase(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		tx := db.NewTransaction()
		e := datalog.NewIdentity(fmt.Sprintf("person%d", i))
		tx.Add(e, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
		tx.Add(e, datalog.NewKeyword(":person/age"), int64(10+i))
		tx.Commit()
	}
	return db
}

// BenchmarkQueryStringNoParseCache measures full Query() with string input
// and parse cache disabled.
func BenchmarkQueryStringNoParseCache(b *testing.B) {
	db := setupParseCacheBenchDB(b)
	defer db.Close()
	db.SetParseCache(nil) // disable parse cache

	// Warm plan cache
	executor.CollectTuples(db.Query(benchQuery))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := executor.CollectTuples(db.Query(benchQuery))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryStringWithParseCache measures full Query() with string input
// and parse cache enabled (default behavior).
func BenchmarkQueryStringWithParseCache(b *testing.B) {
	db := setupParseCacheBenchDB(b)
	defer db.Close()

	// Warm both caches
	executor.CollectTuples(db.Query(benchQuery))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := executor.CollectTuples(db.Query(benchQuery))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryPreParsed measures full Query() with pre-parsed *query.Query
// (baseline — no parsing at all).
func BenchmarkQueryPreParsed(b *testing.B) {
	db := setupParseCacheBenchDB(b)
	defer db.Close()

	q, err := parser.ParseQuery(benchQuery)
	if err != nil {
		b.Fatal(err)
	}

	// Warm plan cache
	executor.CollectTuples(db.Query(q))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := executor.CollectTuples(db.Query(q))
		if err != nil {
			b.Fatal(err)
		}
	}
}
