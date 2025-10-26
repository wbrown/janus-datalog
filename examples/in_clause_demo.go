//go:build example
// +build example

package main

import (
	"fmt"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func main() {
	// Example 1: Basic :in clause
	query1 := `[:find ?e ?name
	            :in $ ?age
	            :where [?e :person/name ?name]
	                   [?e :person/age ?age]]`
	
	fmt.Println("=== Example 1: Basic :in clause ===")
	fmt.Println("Original:")
	fmt.Println(query1)
	
	q1, err := parser.ParseQuery(query1)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	
	fmt.Println("\nFormatted:")
	fmt.Println(parser.FormatQuery(q1))
	
	// Example 2: Multiple input types
	query2 := `[:find ?e ?name ?food
	            :in $ [?food ...] [[?min-age ?max-age]]
	            :where [?e :person/name ?name]
	                   [?e :person/age ?age]
	                   [(>= ?age ?min-age)]
	                   [(<= ?age ?max-age)]
	                   [?e :person/likes ?food]]`
	
	fmt.Println("\n=== Example 2: Multiple input types ===")
	fmt.Println("Original:")
	fmt.Println(query2)
	
	q2, err := parser.ParseQuery(query2)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	
	fmt.Println("\nFormatted:")
	fmt.Println(parser.FormatQuery(q2))
	
	// Example 3: Subquery with :in clause
	query3 := `[:find ?date ?high ?low
	            :where 
	              [?s :symbol/ticker "CRWV"]
	              [(ground "2025-06-02") ?date]
	              
	              [(q [:find (max ?h)
	                   :in $ ?symbol ?date
	                   :where [?p :price/symbol ?symbol]
	                          [?p :price/time ?t]
	                          [(same-date? ?t ?date)]
	                          [?p :price/high ?h]]
	                  ?s ?date) [[?high]]]
	                  
	              [(q [:find (min ?l)
	                   :in $ ?symbol ?date
	                   :where [?p :price/symbol ?symbol]
	                          [?p :price/time ?t]
	                          [(same-date? ?t ?date)]
	                          [?p :price/low ?l]]
	                  ?s ?date) [[?low]]]]`
	
	fmt.Println("\n=== Example 3: Subquery with :in clause ===")
	fmt.Println("Original:")
	fmt.Println(query3)
	
	q3, err := parser.ParseQuery(query3)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	
	fmt.Println("\nFormatted:")
	fmt.Println(parser.FormatQuery(q3))
	
	// Show input specs
	fmt.Println("\n=== Input Specifications ===")
	for _, q := range []*query.Query{q1, q2, q3} {
		if len(q.In) > 0 {
			fmt.Printf("Query has %d inputs: ", len(q.In))
			for i, input := range q.In {
				if i > 0 {
					fmt.Print(", ")
				}
				fmt.Printf("%T(%s)", input, input.String())
			}
			fmt.Println()
		}
	}
}
