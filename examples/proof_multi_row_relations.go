//go:build example
// +build example

package main

import (
	"fmt"
	"log"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Helper to collect datoms from a Relation
func collectDatoms(rel executor.Relation) []datalog.Datom {
	var datoms []datalog.Datom
	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()
		// For simple patterns like [?person :person/age ?age],
		// tuple has [person_entity, age_value]
		// We need to reconstruct the datom
		if len(tuple) >= 2 {
			// This is a simplification - in reality the pattern structure
			// determines how to interpret the tuple
			datom := datalog.Datom{
				E:  tuple[0].(datalog.Identity),
				A:  datalog.NewKeyword(":person/age"), // Pattern-dependent
				V:  tuple[1],
				Tx: 1,
			}
			datoms = append(datoms, datom)
		}
	}
	return datoms
}

func main() {
	fmt.Println("=== PROOF: Relations-Based Binding Works ===\n")

	// Create test data
	alice := datalog.NewIdentity("person:alice")
	bob := datalog.NewIdentity("person:bob")
	charlie := datalog.NewIdentity("person:charlie")
	david := datalog.NewIdentity("person:david")

	nameAttr := datalog.NewKeyword(":person/name")
	ageAttr := datalog.NewKeyword(":person/age")
	cityAttr := datalog.NewKeyword(":person/city")

	// Create a comprehensive dataset
	datoms := []datalog.Datom{
		// Names
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		{E: david, A: nameAttr, V: "David", Tx: 1},

		// Ages
		{E: alice, A: ageAttr, V: int64(25), Tx: 1},
		{E: bob, A: ageAttr, V: int64(30), Tx: 1},
		{E: charlie, A: ageAttr, V: int64(35), Tx: 1},
		{E: david, A: ageAttr, V: int64(40), Tx: 1},

		// Cities
		{E: alice, A: cityAttr, V: "NYC", Tx: 1},
		{E: bob, A: cityAttr, V: "SF", Tx: 1},
		{E: charlie, A: cityAttr, V: "NYC", Tx: 1},
		{E: david, A: cityAttr, V: "LA", Tx: 1},
	}

	matcher := executor.NewMemoryPatternMatcher(datoms)

	// PROOF 1: Relations filter pattern matches
	fmt.Println("PROOF 1: Relations-based filtering")
	fmt.Println("-----------------------------------")
	{
		// Create a relation with just Alice and Bob
		bindingRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?person"},
			[]executor.Tuple{
				{alice},
				{bob},
			},
		)
		fmt.Printf("Binding relation has %d rows: Alice and Bob\n", bindingRel.Size())

		// Pattern: [?person :person/age ?age]
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?person"},
				query.Constant{Value: ageAttr},
				query.Variable{Name: "?age"},
			},
		}

		// Without binding relation - should get all 4 ages
		allResults, err := matcher.Match(pattern, executor.Relations{})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("\nWithout binding relation: %d results (all people)\n", allResults.Size())

		// With binding relation - should get only 2 ages
		filteredResults, err := matcher.Match(pattern, executor.Relations{bindingRel})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("With binding relation: %d results (only Alice and Bob)\n", filteredResults.Size())

		// Iterate and display
		it := filteredResults.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			person := tuple[0].(datalog.Identity)
			age := tuple[1]
			fmt.Printf("  - %s age %v\n", person, age)
		}
		it.Close()

		if filteredResults.Size() != 2 {
			fmt.Println("❌ FAILED: Expected exactly 2 results")
		} else {
			fmt.Println("✅ SUCCESS: Relations correctly filtered results")
		}
	}

	// PROOF 2: Multi-column relation binds multiple variables
	fmt.Println("\n\nPROOF 2: Multi-column relation binds multiple variables")
	fmt.Println("-------------------------------------------------------")
	{
		// Create a multi-column relation with person-attribute pairs
		bindingRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?p", "?attr"},
			[]executor.Tuple{
				{alice, nameAttr},    // Get Alice's name
				{bob, ageAttr},       // Get Bob's age
				{charlie, cityAttr},  // Get Charlie's city
			},
		)
		fmt.Printf("Binding relation has %d rows with 2 columns each\n", bindingRel.Size())
		fmt.Println("  Row 1: (alice, :person/name)")
		fmt.Println("  Row 2: (bob, :person/age)")
		fmt.Println("  Row 3: (charlie, :person/city)")

		// Pattern: [?p ?attr ?value]
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?p"},
				query.Variable{Name: "?attr"},
				query.Variable{Name: "?value"},
			},
		}

		// Match with multi-column binding
		results, err := matcher.Match(pattern, executor.Relations{bindingRel})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("\nResults: %d datoms (exactly matching our 3 specifications)\n", results.Size())
		it := results.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			fmt.Printf("  - %s %s = %v\n", tuple[0], tuple[1], tuple[2])
		}
		it.Close()

		if results.Size() == 3 {
			fmt.Println("✅ SUCCESS: Multi-column relation correctly bound multiple variables")
		} else {
			fmt.Println("❌ FAILED: Expected exactly 3 results")
		}
	}

	// PROOF 3: Empty relation returns no results
	fmt.Println("\n\nPROOF 3: Empty relation returns no results")
	fmt.Println("------------------------------------------")
	{
		// Create an empty relation
		emptyRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?person"},
			[]executor.Tuple{}, // No tuples!
		)
		fmt.Printf("Empty relation has %d rows\n", emptyRel.Size())

		// Pattern that would normally match all names
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?person"},
				query.Constant{Value: nameAttr},
				query.Variable{Name: "?name"},
			},
		}

		// Match with empty relation
		results, err := matcher.Match(pattern, executor.Relations{emptyRel})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Results: %d datoms\n", results.Size())

		if results.Size() == 0 {
			fmt.Println("✅ SUCCESS: Empty relation correctly returns no results")
		} else {
			fmt.Println("❌ FAILED: Empty relation should return no results")
		}
	}

	fmt.Println("\n=== ALL PROOFS COMPLETE ===")
}
