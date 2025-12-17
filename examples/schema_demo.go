//go:build example
// +build example

package main

import (
	"fmt"
	"os"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func main() {
	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "schema-demo")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Println("=== Schema Demo ===")
	fmt.Println()

	// =========================================================================
	// Part 1: Define Schema
	// =========================================================================
	fmt.Println("1. Defining Schema")
	fmt.Println("-------------------")

	s, err := schema.NewBuilder().
		// Simple string attribute
		Attribute(":person/name").Type(schema.TypeString).Add().
		// Integer attribute
		Attribute(":person/age").Type(schema.TypeLong).Add().
		// Unique email
		Attribute(":person/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		// Cardinality-many tags
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		// Cardinality-many refs to friends
		Attribute(":person/friends").Type(schema.TypeRef).Many().Doc("References to friend entities").Add().
		Build()

	if err != nil {
		panic(err)
	}

	fmt.Println("Schema defined with attributes:")
	fmt.Println("  :person/name    - string, cardinality-one")
	fmt.Println("  :person/age     - long, cardinality-one")
	fmt.Println("  :person/email   - string, unique")
	fmt.Println("  :person/tags    - string, cardinality-many")
	fmt.Println("  :person/friends - ref, cardinality-many")
	fmt.Println()

	// =========================================================================
	// Part 2: Create Database with Schema
	// =========================================================================
	fmt.Println("2. Creating Database with Schema")
	fmt.Println("---------------------------------")

	db, err := storage.NewDatabaseWithSchema(tmpDir, s)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Println("Database created with schema validation enabled")
	fmt.Println()

	// =========================================================================
	// Part 3: Add Data with Type Validation
	// =========================================================================
	fmt.Println("3. Adding Data with Type Validation")
	fmt.Println("------------------------------------")

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	carol := datalog.NewIdentity("carol")

	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")
	email := datalog.NewKeyword(":person/email")
	tags := datalog.NewKeyword(":person/tags")
	friends := datalog.NewKeyword(":person/friends")

	// Add Alice with multiple tags and friends
	tx := db.NewTransaction()
	tx.Add(alice, name, "Alice Smith")
	tx.Add(alice, age, int64(30))
	tx.Add(alice, email, "alice@example.com")
	tx.Add(alice, tags, "developer")
	tx.Add(alice, tags, "team-lead")
	tx.Add(alice, tags, "mentor")
	tx.Add(alice, friends, bob)
	tx.Add(alice, friends, carol)

	// Add Bob
	tx.Add(bob, name, "Bob Jones")
	tx.Add(bob, age, int64(25))
	tx.Add(bob, email, "bob@example.com")
	tx.Add(bob, tags, "developer")
	tx.Add(bob, tags, "backend")

	// Add Carol
	tx.Add(carol, name, "Carol White")
	tx.Add(carol, age, int64(28))
	tx.Add(carol, email, "carol@example.com")
	tx.Add(carol, tags, "designer")

	_, err = tx.Commit()
	if err != nil {
		panic(err)
	}

	fmt.Println("Added 3 people with tags and friendships")
	fmt.Println()

	// =========================================================================
	// Part 4: Demonstrate Type Validation Failure
	// =========================================================================
	fmt.Println("4. Type Validation (Failure Case)")
	fmt.Println("----------------------------------")

	tx2 := db.NewTransaction()
	err = tx2.Add(alice, age, "not a number") // Wrong type!
	if err != nil {
		fmt.Printf("Type validation caught error: %v\n", err)
	}
	fmt.Println()

	// =========================================================================
	// Part 5: Demonstrate Uniqueness Validation
	// =========================================================================
	fmt.Println("5. Uniqueness Validation (Failure Case)")
	fmt.Println("----------------------------------------")

	dave := datalog.NewIdentity("dave")
	tx3 := db.NewTransaction()
	tx3.Add(dave, name, "Dave Brown")
	tx3.Add(dave, email, "alice@example.com") // Duplicate email!
	_, err = tx3.Commit()
	if err != nil {
		fmt.Printf("Uniqueness validation caught error: %v\n", err)
	}
	fmt.Println()

	// =========================================================================
	// Part 6: Pull API with Cardinality-Many
	// =========================================================================
	fmt.Println("6. Pull API with Cardinality-Many")
	fmt.Println("----------------------------------")

	// Create pull pattern
	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullAttribute{Attr: name},
			&query.PullAttribute{Attr: age},
			&query.PullAttribute{Attr: tags}, // cardinality-many
			&query.PullMapSpec{
				Attr: friends, // cardinality-many refs
				Pattern: &query.PullPattern{
					Specs: []query.PullAttrSpec{
						&query.PullAttribute{Attr: name},
					},
				},
			},
		},
	}

	// Resolve pattern with schema (enables cardinality-many handling)
	resolved := schema.ResolvePullPattern(pattern, s)

	// Execute pull
	matcher := storage.NewBadgerMatcher(db.Store())
	puller := executor.NewPullExecutor(matcher)
	result, err := puller.PullResolved(alice, resolved)
	if err != nil {
		panic(err)
	}

	fmt.Println("Pull result for Alice:")
	fmt.Printf("  Name: %v\n", result["person/name"])
	fmt.Printf("  Age: %v\n", result["person/age"])

	// Tags should be an array
	if tagsVal, ok := result["person/tags"].([]interface{}); ok {
		fmt.Printf("  Tags: %v (array with %d items)\n", tagsVal, len(tagsVal))
	}

	// Friends should be an array of nested objects
	if friendsVal, ok := result["person/friends"].([]interface{}); ok {
		fmt.Printf("  Friends: %d friends\n", len(friendsVal))
		for i, f := range friendsVal {
			if friendMap, ok := f.(map[string]interface{}); ok {
				fmt.Printf("    [%d] %v\n", i, friendMap["person/name"])
			}
		}
	}
	fmt.Println()

	// =========================================================================
	// Part 7: EDN Schema Parsing
	// =========================================================================
	fmt.Println("7. EDN Schema Parsing")
	fmt.Println("---------------------")

	ednSchema := `{:product/name   {:db/valueType   :db.type/string
                      :db/cardinality :db.cardinality/one}
 :product/price  {:db/valueType   :db.type/double}
 :product/tags   {:db/valueType   :db.type/string
                  :db/cardinality :db.cardinality/many}
 :product/sku    {:db/valueType   :db.type/string
                  :db/unique      :db.unique/identity
                  :db/doc         "Stock keeping unit"}}`

	parsedSchema, err := schema.ParseSchema(ednSchema)
	if err != nil {
		panic(err)
	}

	fmt.Println("Parsed EDN schema with attributes:")
	fmt.Printf("  Count: %d attributes\n", parsedSchema.Count())
	fmt.Printf("  :product/tags is many? %v\n", parsedSchema.IsMany(datalog.NewKeyword(":product/tags")))
	fmt.Printf("  :product/name is many? %v\n", parsedSchema.IsMany(datalog.NewKeyword(":product/name")))
	fmt.Println()

	fmt.Println("=== Demo Complete ===")
}
