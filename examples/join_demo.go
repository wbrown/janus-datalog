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
	// Create entities
	alice := datalog.NewIdentity("person:alice")
	bob := datalog.NewIdentity("person:bob")
	charlie := datalog.NewIdentity("person:charlie")
	
	eng := datalog.NewIdentity("dept:engineering")
	sales := datalog.NewIdentity("dept:sales")
	
	// Attributes
	nameAttr := datalog.NewKeyword(":name")
	deptAttr := datalog.NewKeyword(":person/department")
	managerAttr := datalog.NewKeyword(":dept/manager")
	budgetAttr := datalog.NewKeyword(":dept/budget")
	
	// Create datoms that require joining
	datoms := []datalog.Datom{
		// People and their names
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		
		// People's departments
		{E: alice, A: deptAttr, V: eng, Tx: 1},
		{E: bob, A: deptAttr, V: sales, Tx: 1},
		{E: charlie, A: deptAttr, V: eng, Tx: 1},
		
		// Department names
		{E: eng, A: nameAttr, V: "Engineering", Tx: 1},
		{E: sales, A: nameAttr, V: "Sales", Tx: 1},
		
		// Department managers
		{E: eng, A: managerAttr, V: alice, Tx: 1},
		{E: sales, A: managerAttr, V: bob, Tx: 1},
		
		// Department budgets
		{E: eng, A: budgetAttr, V: int64(1000000), Tx: 1},
		{E: sales, A: budgetAttr, V: int64(500000), Tx: 1},
	}

	matcher := executor.NewMemoryPatternMatcher(datoms)
	exec := executor.NewExecutor(matcher)

	fmt.Println("=== Join Demo ===")
	fmt.Println("\nData model:")
	fmt.Println("- People have names and departments")
	fmt.Println("- Departments have names, managers, and budgets")
	fmt.Println("- This requires joining across entity boundaries")

	// Query 1: Simple join - person to department
	fmt.Println("\n1. Find people and their department names:")
	runQuery(exec, `[:find ?person-name ?dept-name
	                 :where [?person :name ?person-name]
	                        [?person :person/department ?dept]
	                        [?dept :name ?dept-name]]`)

	// Query 2: Multi-hop join - person to department to manager
	fmt.Println("\n2. Find people and their department manager names:")
	runQuery(exec, `[:find ?person-name ?manager-name
	                 :where [?person :name ?person-name]
	                        [?person :person/department ?dept]
	                        [?dept :dept/manager ?manager]
	                        [?manager :name ?manager-name]]`)

	// Query 3: Complex join with filter
	fmt.Println("\n3. Find people in departments with budget > 600k:")
	runQuery(exec, `[:find ?person-name ?dept-name ?budget
	                 :where [?person :name ?person-name]
	                        [?person :person/department ?dept]
	                        [?dept :name ?dept-name]
	                        [?dept :dept/budget ?budget]
	                        [(> ?budget 600000)]]`)

	// Query 4: Self-join - find coworkers
	fmt.Println("\n4. Find coworkers (people in same department):")
	runQuery(exec, `[:find ?name1 ?name2
	                 :where [?p1 :name ?name1]
	                        [?p2 :name ?name2]
	                        [?p1 :person/department ?dept]
	                        [?p2 :person/department ?dept]
	                        [(< ?name1 ?name2)]]`) // Avoid duplicates

	// Query 5: Demonstrating the join process
	fmt.Println("\n5. Step-by-step join demonstration:")
	fmt.Println("\nStep 1 - People pattern [?person :name ?person-name]:")
	runQuery(exec, `[:find ?person ?person-name
	                 :where [?person :name ?person-name]]`)
	
	fmt.Println("\nStep 2 - Department pattern [?person :person/department ?dept]:")
	runQuery(exec, `[:find ?person ?dept
	                 :where [?person :person/department ?dept]]`)
	
	fmt.Println("\nStep 3 - Joined result:")
	runQuery(exec, `[:find ?person ?person-name ?dept
	                 :where [?person :name ?person-name]
	                        [?person :person/department ?dept]]`)
}

func runQuery(exec *executor.Executor, queryStr string) {
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		log.Fatalf("Failed to parse query: %v", err)
	}
	
	// Display the formatted query
	fmt.Printf("\nQuery:\n%s\n\n", parser.FormatQuery(q))

	result, err := exec.Execute(q)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}

	fmt.Printf("Results (%d rows):\n", result.Size())
	for i := 0; i < result.Size(); i++ {
		fmt.Printf("  %v\n", result.Get(i))
	}
}
