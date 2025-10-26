//go:build example
// +build example

package main

import (
	"fmt"
	"log"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// MockMatcher for testing
type MockMatcher struct {
	datoms []datalog.Datom
}

func (m *MockMatcher) Match(pattern *query.DataPattern, bindings executor.Relations) (executor.Relation, error) {
	// Find matching datoms
	var matchedDatoms []datalog.Datom
	for _, d := range m.datoms {
		if matchesDatom(d, pattern) {
			matchedDatoms = append(matchedDatoms, d)
		}
	}

	// Extract pattern variables to determine columns
	var columns []query.Symbol
	if e := pattern.GetE(); e != nil && e.IsVariable() {
		if v, ok := e.(*query.Variable); ok {
			columns = append(columns, v.Name)
		}
	}
	if a := pattern.GetA(); a != nil && a.IsVariable() {
		if v, ok := a.(*query.Variable); ok {
			columns = append(columns, v.Name)
		}
	}
	if v := pattern.GetV(); v != nil && v.IsVariable() {
		if vr, ok := v.(*query.Variable); ok {
			columns = append(columns, vr.Name)
		}
	}

	// Convert datoms to tuples
	var tuples []executor.Tuple
	for _, d := range matchedDatoms {
		var tuple executor.Tuple
		if e := pattern.GetE(); e != nil && e.IsVariable() {
			tuple = append(tuple, d.E)
		}
		if a := pattern.GetA(); a != nil && a.IsVariable() {
			tuple = append(tuple, d.A)
		}
		if v := pattern.GetV(); v != nil && v.IsVariable() {
			tuple = append(tuple, d.V)
		}
		tuples = append(tuples, tuple)
	}

	return executor.NewMaterializedRelation(columns, tuples), nil
}

func matchesDatom(d datalog.Datom, pattern *query.DataPattern) bool {
	if e := pattern.GetE(); e != nil && !e.IsBlank() {
		if e.IsVariable() {
			// Variables always match
		} else if c, ok := e.(query.Constant); ok {
			if !matchesValue(d.E, c.Value) {
				return false
			}
		}
	}

	if a := pattern.GetA(); a != nil && !a.IsBlank() {
		if a.IsVariable() {
			// Variables always match
		} else if c, ok := a.(query.Constant); ok {
			if !matchesValue(d.A, c.Value) {
				return false
			}
		}
	}

	if v := pattern.GetV(); v != nil && !v.IsBlank() {
		if v.IsVariable() {
			// Variables always match
		} else if c, ok := v.(query.Constant); ok {
			if !matchesValue(d.V, c.Value) {
				return false
			}
		}
	}

	return true
}

func matchesValue(actual, expected interface{}) bool {
	// Simple equality for now
	return actual == expected
}

func runQuery(name string, queryStr string, datoms []datalog.Datom) {
	fmt.Printf("\n=== %s ===\n", name)
	
	// Parse the query
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		log.Printf("Failed to parse query: %v", err)
		return
	}

	// Display the formatted query
	fmt.Printf("Query:\n%s\n\n", parser.FormatQuery(q))

	// Create executor with mock matcher
	matcher := &MockMatcher{datoms: datoms}
	exec := executor.NewExecutor(matcher)

	// Execute query
	result, err := exec.Execute(q)
	if err != nil {
		log.Printf("Query execution failed: %v", err)
		return
	}

	// Display results
	columns := result.Columns()
	fmt.Printf("Results (%d):\n", result.Size())

	iter := result.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		fmt.Printf("  ")
		for j, col := range columns {
			if j > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("%s = %v", col, tuple[j])
		}
		fmt.Printf("\n")
	}
}

func main() {
	fmt.Println("Datalog Expression Clauses and Variadic Comparators Demo")
	fmt.Println("======================================================")

	// Create some test data
	datoms := []datalog.Datom{
		// Product data
		{E: datalog.NewIdentity("product1"), A: datalog.NewKeyword(":name"), V: "Widget"},
		{E: datalog.NewIdentity("product1"), A: datalog.NewKeyword(":price"), V: int64(100)},
		{E: datalog.NewIdentity("product1"), A: datalog.NewKeyword(":tax"), V: int64(10)},
		
		{E: datalog.NewIdentity("product2"), A: datalog.NewKeyword(":name"), V: "Gadget"},
		{E: datalog.NewIdentity("product2"), A: datalog.NewKeyword(":price"), V: int64(200)},
		{E: datalog.NewIdentity("product2"), A: datalog.NewKeyword(":tax"), V: int64(20)},
		
		{E: datalog.NewIdentity("product3"), A: datalog.NewKeyword(":name"), V: "Doohickey"},
		{E: datalog.NewIdentity("product3"), A: datalog.NewKeyword(":price"), V: int64(50)},
		{E: datalog.NewIdentity("product3"), A: datalog.NewKeyword(":tax"), V: int64(5)},
		
		// Person data
		{E: datalog.NewIdentity("person1"), A: datalog.NewKeyword(":first"), V: "John"},
		{E: datalog.NewIdentity("person1"), A: datalog.NewKeyword(":last"), V: "Doe"},
		{E: datalog.NewIdentity("person1"), A: datalog.NewKeyword(":age"), V: int64(25)},
		
		{E: datalog.NewIdentity("person2"), A: datalog.NewKeyword(":first"), V: "Jane"},
		{E: datalog.NewIdentity("person2"), A: datalog.NewKeyword(":last"), V: "Smith"},
		{E: datalog.NewIdentity("person2"), A: datalog.NewKeyword(":age"), V: int64(30)},
		
		// Score data for range testing
		{E: datalog.NewIdentity("score1"), A: datalog.NewKeyword(":student"), V: "Alice"},
		{E: datalog.NewIdentity("score1"), A: datalog.NewKeyword(":value"), V: int64(85)},
		
		{E: datalog.NewIdentity("score2"), A: datalog.NewKeyword(":student"), V: "Bob"},
		{E: datalog.NewIdentity("score2"), A: datalog.NewKeyword(":value"), V: int64(92)},
		
		{E: datalog.NewIdentity("score3"), A: datalog.NewKeyword(":student"), V: "Charlie"},
		{E: datalog.NewIdentity("score3"), A: datalog.NewKeyword(":value"), V: int64(105)}, // Out of range
	}

	// Demo 1: Arithmetic expression - calculate total price
	runQuery("Arithmetic Expression - Total Price",
		`[:find ?product ?total
		  :where [?p :name ?product]
		         [?p :price ?price]
		         [?p :tax ?tax]
		         [(+ ?price ?tax) ?total]]`,
		datoms)

	// Demo 2: String concatenation - full names
	runQuery("String Concatenation - Full Names",
		`[:find ?fullname
		  :where [?p :first ?first]
		         [?p :last ?last]
		         [(str ?first " " ?last) ?fullname]]`,
		datoms)

	// Demo 3: Variadic comparison - range check
	runQuery("Variadic Comparison - Valid Scores",
		`[:find ?student ?score
		  :where [?s :student ?student]
		         [?s :value ?score]
		         [(<= 0 ?score 100)]]`,
		datoms)

	// Demo 4: Complex expression with multiplication
	runQuery("Complex Expression - Discount Calculation",
		`[:find ?product ?original ?discounted
		  :where [?p :name ?product]
		         [?p :price ?original]
		         [(* ?original 0.8) ?discounted]]`,
		datoms)

	// Demo 5: Chained comparison - age ranges
	runQuery("Chained Comparison - Age Groups",
		`[:find ?first ?last ?age
		  :where [?p :first ?first]
		         [?p :last ?last]
		         [?p :age ?age]
		         [(< 20 ?age 35)]]`,
		datoms)

	// Demo 6: Ground values
	runQuery("Ground Values - Constants",
		`[:find ?product ?price ?vat_rate
		  :where [?p :name ?product]
		         [?p :price ?price]
		         [(ground 0.2) ?vat_rate]]`,
		datoms)

	// Demo 7: Identity binding
	runQuery("Identity Binding",
		`[:find ?name ?entity
		  :where [?e :name ?name]
		         [(identity ?e) ?entity]]`,
		datoms)
}
