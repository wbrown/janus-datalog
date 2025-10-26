//go:build example
// +build example

package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func main() {
	// Create test database
	dbPath := "/tmp/binding-test"
	os.RemoveAll(dbPath)
	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Add test data
	tx := db.NewTransaction()
	
	// Two symbols
	sym1 := datalog.NewIdentity("symbol-1")
	sym2 := datalog.NewIdentity("symbol-2")
	
	tx.Add(sym1, datalog.NewKeyword(":symbol/name"), "SYM1")
	tx.Add(sym2, datalog.NewKeyword(":symbol/name"), "SYM2")
	
	// Add prices for each
	for i := 0; i < 3; i++ {
		price1 := datalog.NewIdentity(fmt.Sprintf("price-1-%d", i))
		price2 := datalog.NewIdentity(fmt.Sprintf("price-2-%d", i))
		
		tx.Add(price1, datalog.NewKeyword(":price/symbol"), sym1)
		tx.Add(price1, datalog.NewKeyword(":price/value"), float64(100+i))
		
		tx.Add(price2, datalog.NewKeyword(":price/symbol"), sym2)
		tx.Add(price2, datalog.NewKeyword(":price/value"), float64(200+i))
	}
	
	_, err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Created test data: 2 symbols, 3 prices each")
	
	// Test direct matching with bindings
	matcher := db.Matcher()
	
	// Create a pattern [?p :price/symbol <sym1>]
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?p"},
			query.Constant{Value: datalog.NewKeyword(":price/symbol")},
			query.Variable{Name: "?s"},
		},
	}
	
	// Test 1: Match without bindings
	fmt.Println("\nTest 1: Match without bindings")
	results, err := matcher.Match(pattern, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d datoms\n", results.Size())
	
	// Test 2: Match with sym1 binding
	fmt.Println("\nTest 2: Match with ?s bound to symbol-1")
	// Create a single-row relation with the binding
	rel := executor.NewMaterializedRelation([]query.Symbol{"?s"}, []executor.Tuple{{sym1}})
	results, err = matcher.Match(pattern, []executor.Relation{rel})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d datoms (expected 3)\n", results.Size())
	iter := results.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		fmt.Printf("  ?s=%v\n", tuple[0])
	}

	// Test 3: Match with sym2 binding
	fmt.Println("\nTest 3: Match with ?s bound to symbol-2")
	// Create a single-row relation with the binding
	rel = executor.NewMaterializedRelation([]query.Symbol{"?s"}, []executor.Tuple{{sym2}})
	results, err = matcher.Match(pattern, []executor.Relation{rel})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d datoms (expected 3)\n", results.Size())
	iter = results.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		fmt.Printf("  ?s=%v\n", tuple[0])
	}
}
