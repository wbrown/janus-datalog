//go:build example
// +build example

package main

import (
	"fmt"
	"log"

	"github.com/wbrown/janus-datalog/datalog/edn"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func main() {
	fmt.Println("=== EDN Parser Demo ===\n")
	demoEDNParser()

	fmt.Println("\n=== Query Parser Demo ===\n")
	demoQueryParser()
}

func demoEDNParser() {
	// Demo various EDN types
	examples := []string{
		`42`,
		`3.14`,
		`"hello world"`,
		`:keyword`,
		`:namespace/keyword`,
		`true`,
		`nil`,
		`[1 2 3]`,
		`{:name "Alice" :age 30}`,
		`#{1 2 3}`,
		`(+ 1 2)`,
		`[:find ?x :where [?x :foo "bar"]]`,
	}

	for _, input := range examples {
		fmt.Printf("Input: %s\n", input)

		node, err := edn.Parse(input)
		if err != nil {
			log.Printf("Error: %v\n", err)
			continue
		}

		fmt.Printf("Type: %v\n", nodeTypeName(node.Type))
		fmt.Printf("Parsed: %s\n", node.String())
		fmt.Println("---")
	}
}

func demoQueryParser() {
	queries := []struct {
		name  string
		query string
	}{
		{
			name: "Simple query",
			query: `[:find ?e ?name
                     :where [?e :person/name ?name]]`,
		},
		{
			name: "Query with multiple patterns",
			query: `[:find ?e ?name ?age
                     :where [?e :person/name ?name]
                            [?e :person/age ?age]
                            [?e :person/active true]]`,
		},
		{
			name: "Query with blank",
			query: `[:find ?name ?age
                     :where [_ :person/name ?name]
                            [_ :person/age ?age]]`,
		},
		{
			name: "Query with literals",
			query: `[:find ?e ?age
                     :where [?e :person/name "Alice"]
                            [?e :person/age ?age]]`,
		},
		{
			name: "Complex financial query",
			query: `[:find ?e ?symbol ?price ?time
                     :where [?e :stock/symbol ?symbol]
                            [?e :stock/price ?price]
                            [?e :stock/time ?time]
                            [?e :stock/exchange "NYSE"]]`,
		},
	}

	for _, example := range queries {
		fmt.Printf("=== %s ===\n", example.name)
		fmt.Printf("Input:\n%s\n\n", example.query)

		q, err := parser.ParseQuery(example.query)
		if err != nil {
			log.Printf("Error: %v\n", err)
			continue
		}

		fmt.Printf("Parsed Query:\n")
		fmt.Printf("  Find: %v\n", q.Find)
		fmt.Printf("  Where (%d patterns):\n", len(q.Where))
		
		for i, clause := range q.Where {
			fmt.Printf("    Clause %d: ", i+1)
			// Type-assert to DataPattern if it is one
			if pattern, ok := clause.(*query.DataPattern); ok {
				for j, elem := range pattern.Elements {
					if j > 0 {
						fmt.Print(" ")
					}
					fmt.Printf("%s", elem.String())
					if elem.IsVariable() {
						fmt.Print("(var)")
					} else if elem.IsBlank() {
						fmt.Print("(blank)")
					} else {
						fmt.Print("(const)")
					}
				}
			} else {
				// For non-DataPattern clauses (predicates, etc.)
				fmt.Printf("%v", clause)
			}
			fmt.Println()
		}
		
		// Validate the query
		if err := parser.ValidateQuery(q); err != nil {
			fmt.Printf("  Validation Error: %v\n", err)
		} else {
			fmt.Printf("  Validation: ✓ Valid\n")
		}

		// Show formatted output
		fmt.Printf("\nFormatted:\n%s\n", parser.FormatQuery(q))
		fmt.Println()
	}
}

func nodeTypeName(t edn.NodeType) string {
	switch t {
	case edn.NodeNil:
		return "nil"
	case edn.NodeBool:
		return "boolean"
	case edn.NodeInt:
		return "integer"
	case edn.NodeFloat:
		return "float"
	case edn.NodeString:
		return "string"
	case edn.NodeChar:
		return "character"
	case edn.NodeSymbol:
		return "symbol"
	case edn.NodeKeyword:
		return "keyword"
	case edn.NodeList:
		return "list"
	case edn.NodeVector:
		return "vector"
	case edn.NodeMap:
		return "map"
	case edn.NodeSet:
		return "set"
	case edn.NodeTagged:
		return "tagged"
	default:
		return "unknown"
	}
}
