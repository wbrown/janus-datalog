//go:build example
// +build example

package main

import (
	"fmt"
	"log"

	"github.com/wbrown/janus-datalog/datalog/parser"
)

func main() {
	fmt.Println("=== Query Parser Error Handling Demo ===\n")

	errorCases := []struct {
		name  string
		query string
	}{
		{
			name:  "Missing find clause",
			query: `[:where [?e :person/name ?name]]`,
		},
		{
			name:  "Missing where clause",
			query: `[:find ?e ?name]`,
		},
		{
			name:  "Non-variable in find",
			query: `[:find ?e "literal" :where [?e :foo ?bar]]`,
		},
		{
			name:  "Invalid pattern (too short)",
			query: `[:find ?e :where [?e ?a]]`,
		},
		{
			name:  "Invalid pattern (too long)",
			query: `[:find ?e :where [?e ?a ?v ?t ?extra]]`,
		},
		{
			name:  "Unbound variable in find",
			query: `[:find ?e ?unbound :where [?e :person/name ?name]]`,
		},
		{
			name:  "Invalid EDN syntax",
			query: `[:find ?e :where [?e :person/name`,
		},
		{
			name:  "Invalid symbol (starts with number)",
			query: `[:find ?e :where [?e :person/name 123abc]]`,
		},
		{
			name:  "Query not a vector",
			query: `{:find ?e :where [?e :foo ?bar]}`,
		},
	}

	for _, tc := range errorCases {
		fmt.Printf("=== %s ===\n", tc.name)
		fmt.Printf("Query: %s\n", tc.query)
		
		q, err := parser.ParseQuery(tc.query)
		if err != nil {
			fmt.Printf("Parse Error: %v\n", err)
		} else {
			// If parsing succeeded, try validation
			if valErr := parser.ValidateQuery(q); valErr != nil {
				fmt.Printf("Validation Error: %v\n", valErr)
			} else {
				fmt.Printf("Unexpected: Query parsed and validated successfully!\n")
			}
		}
		fmt.Println()
	}

	// Show a complex valid query for contrast
	fmt.Println("=== Valid Complex Query ===")
	validQuery := `[:find ?person ?name ?age ?city
                    :where [?person :person/name ?name]
                           [?person :person/age ?age]
                           [?person :person/address ?addr]
                           [?addr :address/city ?city]
                           [?addr :address/country "USA"]]`
	
	fmt.Printf("Query:\n%s\n\n", validQuery)
	
	q, err := parser.ParseQuery(validQuery)
	if err != nil {
		log.Fatalf("Unexpected parse error: %v", err)
	}
	
	if err := parser.ValidateQuery(q); err != nil {
		log.Fatalf("Unexpected validation error: %v", err)
	}
	
	fmt.Println("✓ Query parsed and validated successfully!")
	fmt.Printf("\nExtracted variables: %v\n", parser.ExtractVariables(q.Where))
}
