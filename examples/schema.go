//go:build example

// schema.go demonstrates schema definition, type validation, cardinality,
// and uniqueness constraints.
//
// Run from the repository root:
//
//	go run -tags example examples/schema.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "schema-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Println("=== 1. Define a schema ===")
	// Schemas define attribute types, cardinality, and constraints.
	// They are optional -- Janus Datalog works without them -- but
	// they enable validation and proper CRDT semantics.
	s := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Doc("Full name").Add().
		Attribute(":person/age").Type(schema.TypeLong).Add().
		Attribute(":person/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Doc("Free-form tags").Add().
		Attribute(":person/active").Type(schema.TypeBoolean).Add().
		MustBuild()

	fmt.Println("  Schema defined with 5 attributes")
	emailDef := s.GetAttribute(datalog.NewKeyword(":person/email"))
	fmt.Printf("  :person/email unique constraint: %s\n", emailDef.Unique)
	fmt.Printf("  :person/tags cardinality: %s\n", s.Cardinality(datalog.NewKeyword(":person/tags")))

	// Open the database with the schema
	d, err := db.Open(tmpDir+"/schema.db", db.WithSchema(s))
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	alice := datalog.NewIdentity("person:alice")
	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")
	email := datalog.NewKeyword(":person/email")
	tags := datalog.NewKeyword(":person/tags")

	fmt.Println("\n=== 2. Write valid data ===")
	tx := d.NewTransaction()
	tx.Add(alice, name, "Alice Chen")
	tx.Add(alice, age, int64(30))
	tx.Add(alice, email, "alice@example.com")

	// Cardinality-many: multiple values for the same entity+attribute
	tx.Add(alice, tags, "engineer")
	tx.Add(alice, tags, "team-lead")
	tx.Add(alice, tags, "go-developer")

	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Committed Alice with 3 tags")

	fmt.Println("\n=== 3. Type validation ===")
	// The schema enforces types. Passing a string for :person/age will fail.
	tx = d.NewTransaction()
	bob := datalog.NewIdentity("person:bob")
	err = tx.Add(bob, age, "not a number")
	if err != nil {
		fmt.Printf("  Type error caught: %v\n", err)
	}
	tx.Rollback()

	fmt.Println("\n=== 4. Uniqueness constraint ===")
	// :person/email is UniqueValue. Uniqueness here is a read-time property,
	// not a write-time rejection: there is no single transactor to serialize a
	// check-then-write, so both writes land and the value resolves to exactly
	// one owner when read. LookupByUnique is that read, and the entity view
	// agrees with it -- the entity that lost the value stops reporting it.
	// See docs/reference/CRDT_UNIQUE_SEMANTICS.md.
	tx = d.NewTransaction()
	tx.Add(bob, name, "Bob Smith")
	tx.Add(bob, email, "alice@example.com") // already claimed by Alice
	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Commit succeeded -- the duplicate is not rejected")

	owner, err := d.LookupByUnique(email, "alice@example.com")
	if err != nil {
		log.Fatal(err)
	}
	ownerName, _, err := d.GetString(owner, name)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  LookupByUnique: the value belongs to %s\n", ownerName)

	aliceEmail, found, err := d.GetString(alice, email)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Alice's email now reads %q (present: %t)\n", aliceEmail, found)

	fmt.Println("\n=== 5. Cardinality-one: last write wins ===")
	// Updating a cardinality-one attribute replaces the old value.
	tx = d.NewTransaction()
	tx.Add(alice, name, "Alice M. Chen")
	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	updatedName, _, _ := d.GetString(alice, name)
	fmt.Printf("  Updated name: %s\n", updatedName)

	fmt.Println("\n=== 6. Cardinality-many: add-wins set ===")
	// Adding another tag keeps all previous tags (set semantics).
	tx = d.NewTransaction()
	tx.Add(alice, tags, "mentor")
	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// Query all tags
	var tagValues []string
	err = d.QueryInto(&tagValues,
		`[:find ?tag
		  :where [?e :person/name "Alice M. Chen"]
		         [?e :person/tags ?tag]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Alice's tags: %v\n", tagValues)

	fmt.Println("\n=== 7. Query across cardinalities ===")
	// Add more people with tags
	tx = d.NewTransaction()
	tx.Add(bob, name, "Bob Smith")
	tx.Add(bob, email, "bob@example.com")
	tx.Add(bob, tags, "engineer")
	tx.Add(bob, tags, "backend")

	carol := datalog.NewIdentity("person:carol")
	tx.Add(carol, name, "Carol Davis")
	tx.Add(carol, email, "carol@example.com")
	tx.Add(carol, tags, "engineer")
	tx.Add(carol, tags, "frontend")
	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// Find all engineers
	type TagResult struct {
		Name string `datalog:"?name"`
	}
	var engineers []TagResult
	err = d.QueryInto(&engineers,
		`[:find ?name
		  :where [?e :person/name ?name]
		         [?e :person/tags "engineer"]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Engineers:")
	for _, r := range engineers {
		fmt.Printf("    %s\n", r.Name)
	}
}
