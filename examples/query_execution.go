//go:build example
// +build example

package main

import (
	"fmt"
	"log"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

func main() {
	// Create some test data
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	charlie := datalog.NewIdentity("user:charlie")
	
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	friendAttr := datalog.NewKeyword(":user/friend")
	
	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: alice, A: ageAttr, V: int64(30), Tx: 1},
		{E: alice, A: friendAttr, V: bob, Tx: 2},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: bob, A: ageAttr, V: int64(25), Tx: 1},
		{E: bob, A: friendAttr, V: charlie, Tx: 2},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		{E: charlie, A: ageAttr, V: int64(35), Tx: 1},
	}

	// Create a pattern matcher with the data
	matcher := executor.NewMemoryPatternMatcher(datoms)
	
	// Create the executor
	exec := executor.NewExecutor(matcher)

	// Example 1: Simple query
	fmt.Println("=== Example 1: Find all names ===")
	runQuery(exec, `[:find ?name :where [?e :user/name ?name]]`)

	// Example 2: Join query - this actually joins across different entities
	fmt.Println("\n=== Example 2: Find friends and their ages ===")
	runQuery(exec, `[:find ?name ?friend-name ?friend-age
                  :where [?person :user/name ?name]
                         [?person :user/friend ?friend]
                         [?friend :user/name ?friend-name]
                         [?friend :user/age ?friend-age]]`)

	// Example 3: Query with filter
	fmt.Println("\n=== Example 3: Find people younger than 30 ===")
	runQuery(exec, `[:find ?name ?age
                  :where [?e :user/name ?name]
                         [?e :user/age ?age]
                         [(< ?age 30)]]`)

	// Example 4: Transitive query
	fmt.Println("\n=== Example 4: Find friends of friends ===")
	runQuery(exec, `[:find ?person ?friend-of-friend
                  :where [?p1 :user/name ?person]
                         [?p1 :user/friend ?p2]
                         [?p2 :user/friend ?p3]
                         [?p3 :user/name ?friend-of-friend]]`)
}

func runQuery(exec *executor.Executor, queryStr string) {
	// Parse the query
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		log.Fatalf("Failed to parse query: %v", err)
	}
	
	// Display the formatted query
	fmt.Printf("\nQuery:\n%s\n\n", parser.FormatQuery(q))

	// Execute the query
	result, err := exec.Execute(q)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}

	// Print results
	fmt.Printf("Columns: %v\n", result.Columns)
	fmt.Printf("Results (%d rows):\n", result.Size())
	for i := 0; i < result.Size(); i++ {
		fmt.Printf("  %v\n", result.Get(i))
	}
}
