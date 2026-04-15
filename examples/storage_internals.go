//go:build example

// storage_internals.go demonstrates advanced/escape-hatch APIs for
// query plan inspection, execution analysis, and direct storage access.
//
// These are internal APIs that may change between versions.
//
// Run from the repository root:
//
//	go run -tags example examples/storage_internals.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog/db"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "internals-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(tmpDir + "/internals.db")
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	f, err := os.Open("examples/data/people.edn")
	if err != nil {
		log.Fatal(err)
	}
	if err := d.Import(f); err != nil {
		log.Fatal(err)
	}
	f.Close()

	queryStr := `[:find ?name ?dept_name
	              :where [?e :person/name ?name]
	                     [?e :person/department ?dept]
	                     [?dept :dept/name ?dept_name]]`

	fmt.Println("=== 1. Explain: query plan without execution ===")
	// Explain shows how the engine will execute a query:
	// which phases, which indices, which joins.
	plan, err := d.Explain(queryStr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(plan.String())

	fmt.Println("=== 2. Analyze: plan + execution statistics ===")
	// Analyze actually runs the query and captures timing at each step.
	// Like EXPLAIN ANALYZE in PostgreSQL.
	result, err := d.Analyze(queryStr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.String())

	fmt.Println("=== 3. Plan cache ===")
	// Parsed queries and plans are cached automatically.
	// Running the same query string again reuses the cached plan.
	cache := d.PlanCache()
	hits, misses, size := cache.Stats()
	fmt.Printf("  Plan cache: %d entries, %d hits, %d misses\n", size, hits, misses)

	// Clear the cache (useful for benchmarking).
	d.ClearPlanCache()
	_, _, size = d.PlanCache().Stats()
	fmt.Printf("  After clear: %d entries\n", size)

	fmt.Println("\n=== 4. Parse cache ===")
	// EDN query strings are parsed and cached separately.
	parseCache := d.ParseCache()
	_, _, parseSize := parseCache.Stats()
	fmt.Printf("  Parse cache entries: %d\n", parseSize)

	fmt.Println("\n=== 5. Direct storage access ===")
	// d.Store() gives direct access to the BadgerDB store.
	// This is the lowest-level escape hatch -- use with caution.
	store := d.Store()
	fmt.Printf("  BadgerDB store: %T\n", store)
	// Available low-level operations: Assert, Retract, Scan, Get, Close
	// See datalog/storage/badger_store.go for the full API.
}
