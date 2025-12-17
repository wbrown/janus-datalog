//go:build example
// +build example

package main

import (
	"fmt"
	"os"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// ============================================================================
// Define your domain types with struct tags
// ============================================================================

// Person represents a person in our domain.
// The `datalog` tag maps struct fields to database attributes.
type Person struct {
	// ID field marked with "-,id" - holds the entity identity
	ID datalog.Identity `datalog:"-,id"`

	// Simple attributes - namespace derived from struct name (person)
	Name  string `datalog:"name"`  // → :person/name
	Age   int64  `datalog:"age"`   // → :person/age
	Email string `datalog:"email"` // → :person/email

	// Cardinality-many: slice of strings
	Tags []string `datalog:"tags"` // → :person/tags (many)

	// Reference to another entity
	Manager *Person `datalog:"manager"` // → :person/manager (ref)

	// Cardinality-many references
	Friends []*Person `datalog:"friends"` // → :person/friends (many refs)
}

// Company demonstrates a different namespace
type Company struct {
	ID       datalog.Identity `datalog:"-,id"`
	Name     string           `datalog:"name"`     // → :company/name
	Industry string           `datalog:"industry"` // → :company/industry
}

func main() {
	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "reflect-demo")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Println("=== Struct Reflection API Demo ===")
	fmt.Println()

	// =========================================================================
	// Part 1: Generate Schema from Structs
	// =========================================================================
	fmt.Println("1. Generating Schema from Structs")
	fmt.Println("----------------------------------")

	// Generate schema from one struct
	schema, err := reflect.SchemaFromStruct(Person{})
	if err != nil {
		panic(err)
	}

	fmt.Println("Schema generated from Person struct:")
	fmt.Println("  :person/name    → string, one")
	fmt.Println("  :person/age     → long, one")
	fmt.Println("  :person/email   → string, one")
	fmt.Println("  :person/tags    → string, many")
	fmt.Println("  :person/manager → ref, one")
	fmt.Println("  :person/friends → ref, many")
	fmt.Println()

	// Can also generate from multiple structs
	_, err = reflect.SchemaFromStructs(Person{}, Company{})
	if err != nil {
		panic(err)
	}
	fmt.Println("Combined schema generated from Person and Company")
	fmt.Println()

	// =========================================================================
	// Part 2: Create Database with Schema
	// =========================================================================
	fmt.Println("2. Creating Database with Schema")
	fmt.Println("---------------------------------")

	db, err := storage.NewDatabaseWithSchema(tmpDir, schema)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Println("Database created with schema validation")
	fmt.Println()

	// =========================================================================
	// Part 3: Write Structs to Database
	// =========================================================================
	fmt.Println("3. Writing Structs to Database")
	fmt.Println("-------------------------------")

	// Create some people
	alice := &Person{
		Name:  "Alice",
		Age:   30,
		Email: "alice@example.com",
		Tags:  []string{"developer", "team-lead", "mentor"},
	}

	bob := &Person{
		Name:  "Bob",
		Age:   25,
		Email: "bob@example.com",
		Tags:  []string{"developer", "junior"},
	}

	carol := &Person{
		Name:  "Carol",
		Age:   35,
		Email: "carol@example.com",
		Tags:  []string{"manager", "senior"},
	}

	// Write using AddStructAuto - generates ID automatically
	tx := db.NewTransaction()

	aliceID, err := tx.AddStructAuto(alice)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Added Alice with auto-generated ID: %s\n", aliceID.String()[:20]+"...")

	bobID, err := tx.AddStructAuto(bob)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Added Bob with auto-generated ID: %s\n", bobID.String()[:20]+"...")

	carolID, err := tx.AddStructAuto(carol)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Added Carol with auto-generated ID: %s\n", carolID.String()[:20]+"...")

	// Commit the transaction
	if _, err := tx.Commit(); err != nil {
		panic(err)
	}

	// The struct's ID field is now populated
	fmt.Printf("\nAlice's ID field was set: %v\n", alice.ID.Hash() != [20]byte{})
	fmt.Println()

	// =========================================================================
	// Part 4: Add Relationships
	// =========================================================================
	fmt.Println("4. Adding Relationships")
	fmt.Println("------------------------")

	// Update Alice to have Bob and Carol as friends
	tx = db.NewTransaction()

	// Add friend relationships manually (or could update struct and re-add)
	tx.Add(aliceID, datalog.NewKeyword(":person/friends"), bobID)
	tx.Add(aliceID, datalog.NewKeyword(":person/friends"), carolID)

	// Set Carol as Alice's manager
	tx.Add(aliceID, datalog.NewKeyword(":person/manager"), carolID)

	if _, err := tx.Commit(); err != nil {
		panic(err)
	}

	fmt.Println("Added Alice's friends (Bob, Carol) and manager (Carol)")
	fmt.Println()

	// =========================================================================
	// Part 5: Read Structs from Database
	// =========================================================================
	fmt.Println("5. Reading Structs from Database")
	fmt.Println("---------------------------------")

	// PullInto reads entity into a struct
	var loadedAlice Person
	if err := db.PullInto(aliceID, &loadedAlice); err != nil {
		panic(err)
	}

	fmt.Printf("Loaded Alice:\n")
	fmt.Printf("  Name:  %s\n", loadedAlice.Name)
	fmt.Printf("  Age:   %d\n", loadedAlice.Age)
	fmt.Printf("  Email: %s\n", loadedAlice.Email)
	fmt.Printf("  Tags:  %v\n", loadedAlice.Tags)
	fmt.Println()

	// =========================================================================
	// Part 6: Read Multiple Structs
	// =========================================================================
	fmt.Println("6. Reading Multiple Structs")
	fmt.Println("----------------------------")

	var allPeople []Person
	if err := db.PullIntoMany([]datalog.Identity{aliceID, bobID, carolID}, &allPeople); err != nil {
		panic(err)
	}

	fmt.Printf("Loaded %d people:\n", len(allPeople))
	for _, p := range allPeople {
		fmt.Printf("  - %s (age %d): %v\n", p.Name, p.Age, p.Tags)
	}
	fmt.Println()

	// =========================================================================
	// Part 7: Using Explicit Entity IDs
	// =========================================================================
	fmt.Println("7. Using Explicit Entity IDs")
	fmt.Println("-----------------------------")

	// You can provide your own IDs
	dave := &Person{
		ID:    datalog.NewIdentity("dave-unique-id"),
		Name:  "Dave",
		Age:   40,
		Email: "dave@example.com",
		Tags:  []string{"architect"},
	}

	tx = db.NewTransaction()
	returnedID, err := tx.AddStructAuto(dave)
	if err != nil {
		panic(err)
	}
	if _, err := tx.Commit(); err != nil {
		panic(err)
	}

	// The returned ID matches the one we provided
	fmt.Printf("Dave's explicit ID was used: %v\n", returnedID.Hash() == dave.ID.Hash())
	fmt.Println()

	// =========================================================================
	// Part 8: Pattern Generation
	// =========================================================================
	fmt.Println("8. Pattern Generation")
	fmt.Println("----------------------")

	// You can see what pull pattern is generated from a struct
	pattern := reflect.GeneratePullPattern(Person{}, schema)
	fmt.Printf("Generated pull pattern for Person:\n  %s\n", pattern)

	// For nested structs with refs, patterns include nested attributes
	simplePattern := reflect.GenerateSimplePullPattern(Person{})
	fmt.Printf("Simple (flat) pull pattern:\n  %s\n", simplePattern)
	fmt.Println()

	// =========================================================================
	// Part 9: Struct Tag Reference
	// =========================================================================
	fmt.Println("9. Struct Tag Reference")
	fmt.Println("------------------------")
	fmt.Println(`
Tag format: datalog:"name" or datalog:"namespace/name"

Special tags:
  datalog:"-"      Skip this field
  datalog:"-,id"   This field holds the entity Identity

Namespace derivation:
  type Person → namespace "person"
  type HTTPServer → namespace "http-server"

Cardinality inference:
  string         → cardinality one
  []string       → cardinality many
  *Person        → cardinality one (optional)
  []*Person      → cardinality many refs

Examples:
  Name string           → :person/name (string, one)
  Tags []string         → :person/tags (string, many)
  Manager *Person       → :person/manager (ref, one)
  Friends []*Person     → :person/friends (ref, many)
`)

	fmt.Println("=== Demo Complete ===")
}
