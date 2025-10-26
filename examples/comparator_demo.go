//go:build example
// +build example

package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func main() {
	fmt.Println("=== Datalog Query Parser with Comparators Demo ===\n")

	examples := []struct {
		name        string
		query       string
		description string
	}{
		{
			name: "Age Filter",
			query: `[:find ?person ?name ?age
                     :where [?person :person/name ?name]
                            [?person :person/age ?age]
                            [(< ?age 30)]]`,
			description: "Find all people younger than 30",
		},
		{
			name: "Price Range",
			query: `[:find ?item ?name ?price
                     :where [?item :item/name ?name]
                            [?item :item/price ?price]
                            [(>= ?price 10.00)]
                            [(<= ?price 50.00)]]`,
			description: "Find items priced between $10 and $50",
		},
		{
			name: "String Matching",
			query: `[:find ?person ?email
                     :where [?person :person/email ?email]
                            [(str/ends-with? ?email "@company.com")]]`,
			description: "Find all people with company email addresses",
		},
		{
			name: "Inequality Check",
			query: `[:find ?order ?status
                     :where [?order :order/status ?status]
                            [(!= ?status "cancelled")]
                            [(!= ?status "refunded")]]`,
			description: "Find orders that are not cancelled or refunded",
		},
		{
			name: "Comparison Between Variables",
			query: `[:find ?emp1 ?emp2 ?salary1 ?salary2
                     :where [?emp1 :employee/salary ?salary1]
                            [?emp2 :employee/salary ?salary2]
                            [(< ?salary1 ?salary2)]
                            [(!= ?emp1 ?emp2)]]`,
			description: "Find pairs of employees where first earns less than second",
		},
		{
			name: "Complex Financial Query",
			query: `[:find ?stock ?symbol ?price ?volume
                     :where [?stock :stock/symbol ?symbol]
                            [?stock :stock/price ?price]
                            [?stock :stock/volume ?volume]
                            [(> ?price 100)]
                            [(> ?volume 1000000)]
                            [(str/starts-with? ?symbol "A")]]`,
			description: "Find high-priced, high-volume stocks starting with 'A'",
		},
		{
			name: "Date Comparison",
			query: `[:find ?event ?name ?date
                     :where [?event :event/name ?name]
                            [?event :event/date ?date]
                            [(> ?date "2024-01-01")]
                            [(< ?date "2024-12-31")]]`,
			description: "Find events in 2024",
		},
		{
			name: "Mathematical Operations",
			query: `[:find ?product ?price ?tax
                     :where [?product :product/price ?price]
                            [?product :product/tax-rate ?tax]
                            [(* ?price ?tax)]
                            [(> ?price 50)]]`,
			description: "Find products over $50 with tax calculations",
		},
	}

	for _, example := range examples {
		fmt.Printf("=== %s ===\n", example.name)
		fmt.Printf("Description: %s\n", example.description)
		fmt.Printf("\nQuery:\n%s\n\n", example.query)
		
		q, err := parser.ParseQuery(example.query)
		if err != nil {
			log.Printf("Parse error: %v\n", err)
			continue
		}

		// Validate the query
		if err := parser.ValidateQuery(q); err != nil {
			log.Printf("Validation error: %v\n", err)
			continue
		}

		fmt.Printf("Parsed successfully!\n")
		fmt.Printf("Find variables: %v\n", q.Find)
		fmt.Printf("Pattern breakdown:\n")
		
		for i, pattern := range q.Where {
			switch p := pattern.(type) {
			case *query.DataPattern:
				fmt.Printf("  [%d] Data pattern: %s\n", i+1, p.String())
			case query.Predicate:
				fmt.Printf("  [%d] Predicate: %s\n", i+1, p.String())
			}
		}

		fmt.Println("\n" + strings.Repeat("-", 60) + "\n")
	}
	
	// Show some error cases
	fmt.Println("=== Error Cases ===\n")
	
	errorCases := []struct {
		name  string
		query string
	}{
		{
			name:  "Unbound variable in comparator",
			query: `[:find ?x :where [(< ?y 10)]]`,
		},
		{
			name:  "Invalid comparator syntax",
			query: `[:find ?x :where [?x :foo ?y] [< ?y 10]]`,
		},
		{
			name:  "Empty function",
			query: `[:find ?x :where [?x :foo ?y] [()]]`,
		},
	}
	
	for _, tc := range errorCases {
		fmt.Printf("=== %s ===\n", tc.name)
		fmt.Printf("Query: %s\n", tc.query)
		
		q, err := parser.ParseQuery(tc.query)
		if err != nil {
			fmt.Printf("Parse error (as expected): %v\n", err)
		} else {
			// Try validation too
			if valErr := parser.ValidateQuery(q); valErr != nil {
				fmt.Printf("Validation error (as expected): %v\n", valErr)
			} else {
				fmt.Printf("Unexpected: parsed and validated successfully!\n")
			}
		}
		fmt.Println()
	}
}
