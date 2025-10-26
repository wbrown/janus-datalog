//go:build example
// +build example

package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func main() {
	fmt.Println("=== Datalog Query Planner Demo ===\n")

	// Create planner with some statistics
	stats := &planner.Statistics{
		AttributeCardinality: map[string]int{
			":person/name":       10000,
			":person/age":        100,
			":person/email":      10000,
			":company/name":      1000,
			":company/employees": 50,
			":stock/symbol":      5000,
			":stock/price":       1000,
		},
		EntityCount: 100000,
	}

	p := planner.NewPlanner(stats, planner.PlannerOptions{
		EnableDynamicReordering: true,
		EnablePredicatePushdown: true,
	})

	examples := []struct {
		name  string
		query string
		desc  string
	}{
		{
			name: "Simple lookup",
			query: `[:find ?e ?name
                     :where [?e :person/name ?name]]`,
			desc: "Find all people and their names",
		},
		{
			name: "Entity-bound query",
			query: `[:find ?name ?age ?email
                     :where ["person-123" :person/name ?name]
                            ["person-123" :person/age ?age]
                            ["person-123" :person/email ?email]]`,
			desc: "Get all attributes for a specific person",
		},
		{
			name: "Reverse lookup",
			query: `[:find ?person
                     :where [?person :person/email "alice@example.com"]]`,
			desc: "Find person by email (uses AVET index)",
		},
		{
			name: "Join query",
			query: `[:find ?company ?employee ?name
                     :where [?company :company/employees ?employee]
                            [?employee :person/name ?name]]`,
			desc: "Find companies and their employees' names",
		},
		{
			name: "Multi-way join",
			query: `[:find ?p1 ?p2 ?company
                     :where [?p1 :works-at ?company]
                            [?p2 :works-at ?company]
                            [(!= ?p1 ?p2)]]`,
			desc: "Find pairs of people who work at the same company",
		},
		{
			name: "Query with filters",
			query: `[:find ?person ?name ?age
                     :where [?person :person/name ?name]
                            [?person :person/age ?age]
                            [(>= ?age 21)]
                            [(<= ?age 65)]
                            [(str/starts-with? ?name "J")]]`,
			desc: "Find working-age adults whose names start with J",
		},
		{
			name: "Complex financial query",
			query: `[:find ?stock ?symbol ?price ?company ?ceo
                     :where [?stock :stock/symbol ?symbol]
                            [?stock :stock/price ?price]
                            [(> ?price 100)]
                            [?stock :stock/company ?company]
                            [?company :company/ceo ?ceo]
                            [?ceo :person/name ?ceo-name]
                            [(str/contains? ?ceo-name "Tech")]]`,
			desc: "Find high-priced stocks whose CEOs have 'Tech' in their name",
		},
	}

	for _, example := range examples {
		fmt.Printf("=== %s ===\n", example.name)
		fmt.Printf("Description: %s\n\n", example.desc)
		fmt.Printf("Query:\n%s\n\n", example.query)

		// Parse query
		q, err := parser.ParseQuery(example.query)
		if err != nil {
			log.Printf("Parse error: %v\n", err)
			continue
		}

		// Create plan
		plan, err := p.Plan(q)
		if err != nil {
			log.Printf("Planning error: %v\n", err)
			continue
		}

		fmt.Printf("Execution Plan:\n")
		fmt.Printf("  Find variables: %v\n", plan.Query.Find)
		fmt.Printf("  Number of phases: %d\n\n", len(plan.Phases))

		for i, phase := range plan.Phases {
			fmt.Printf("  Phase %d:\n", i+1)
			if len(phase.Available) > 0 {
				fmt.Printf("    Input symbols: %v\n", phase.Available)
			}

			fmt.Printf("    Patterns (%d):\n", len(phase.Patterns))
			for j, pattern := range phase.Patterns {
				dp := pattern.Pattern.(*query.DataPattern)
				fmt.Printf("      [%d] %s\n", j+1, dp.String())
				fmt.Printf("          Index: %s\n", indexName(pattern.Index))
				fmt.Printf("          Bound: E=%v A=%v V=%v\n",
					pattern.BoundMask.E, pattern.BoundMask.A, pattern.BoundMask.V)
				fmt.Printf("          Binds: %v\n", getBindings(pattern.Bindings))
				fmt.Printf("          Selectivity score: %d\n", pattern.Selectivity)
			}
			
			if len(phase.Predicates) > 0 {
				fmt.Printf("    Predicates (%d):\n", len(phase.Predicates))
				for j, pred := range phase.Predicates {
					fmt.Printf("      [%d] %s\n", j+1, pred.Predicate.String())
					fmt.Printf("          Uses: %v\n", pred.RequiredVars)
				}
			}

			fmt.Printf("    Output symbols: %v\n", phase.Provides)
			if len(phase.Keep) > 0 {
				fmt.Printf("    Keep for later: %v\n", phase.Keep)
			}
		}
		
		fmt.Println("\n" + strings.Repeat("-", 70) + "\n")
	}
}

func indexName(idx planner.IndexType) string {
	switch idx {
	case planner.EAVT:
		return "EAVT"
	case planner.AEVT:
		return "AEVT"
	case planner.AVET:
		return "AVET"
	case planner.VAET:
		return "VAET"
	case planner.TAEV:
		return "TAEV"
	default:
		return fmt.Sprintf("Unknown(%d)", idx)
	}
}

func getBindings(bindings map[query.Symbol]bool) []query.Symbol {
	var symbols []query.Symbol
	for sym := range bindings {
		symbols = append(symbols, sym)
	}
	// Sort for consistent output
	for i := 0; i < len(symbols); i++ {
		for j := i + 1; j < len(symbols); j++ {
			if symbols[i] > symbols[j] {
				symbols[i], symbols[j] = symbols[j], symbols[i]
			}
		}
	}
	return symbols
}
