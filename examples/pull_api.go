//go:build example

// pull_api.go demonstrates the Pull API for entity-centric data retrieval
// using Go struct tags, including nested references and cardinality-many.
//
// Run from the repository root:
//
//	go run -tags example examples/pull_api.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Domain structs with datalog tags for PullInto.
// Tag format: `datalog:"attribute-name"` (without leading colon).
// The special tag `datalog:"-,id"` maps to the entity's Identity.
type Department struct {
	ID       datalog.Identity `datalog:"-,id"`
	Name     string           `datalog:"dept/name"`
	Location string           `datalog:"dept/location"`
	Budget   float64          `datalog:"dept/budget"`
}

type Person struct {
	ID         datalog.Identity `datalog:"-,id"`
	Name       string           `datalog:"person/name"`
	Age        int64            `datalog:"person/age"`
	Email      string           `datalog:"person/email"`
	City       string           `datalog:"person/city"`
	Active     bool             `datalog:"person/active"`
	Tags       []string         `datalog:"person/tags"`       // cardinality-many
	Department *Department      `datalog:"person/department"` // ref -> nested pull
}

func main() {
	tmpDir, err := os.MkdirTemp("", "pull-api-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Schema is required for cardinality-many and ref handling in PullInto.
	s := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Attribute(":person/age").Type(schema.TypeLong).Add().
		Attribute(":person/email").Type(schema.TypeString).Add().
		Attribute(":person/city").Type(schema.TypeString).Add().
		Attribute(":person/salary").Type(schema.TypeDouble).Add().
		Attribute(":person/active").Type(schema.TypeBoolean).Add().
		Attribute(":person/nickname").Type(schema.TypeString).Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Attribute(":person/joined").Type(schema.TypeInstant).Add().
		Attribute(":person/department").Type(schema.TypeRef).Add().
		Attribute(":person/manager").Type(schema.TypeRef).Add().
		Attribute(":dept/name").Type(schema.TypeString).Add().
		Attribute(":dept/location").Type(schema.TypeString).Add().
		Attribute(":dept/budget").Type(schema.TypeDouble).Add().
		MustBuild()

	d, err := db.Open(tmpDir+"/pull.db", db.WithSchema(s))
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	// Import the shared dataset
	f, err := os.Open("examples/data/people.edn")
	if err != nil {
		log.Fatal(err)
	}
	if err := d.Import(f); err != nil {
		log.Fatal(err)
	}
	f.Close()

	fmt.Println("=== 1. PullInto: single entity ===")
	// Pull all mapped attributes for Alice.
	alice := datalog.NewIdentity("person:alice")
	var person Person
	if err := d.PullInto(alice, &person); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Name: %s\n", person.Name)
	fmt.Printf("  Age: %d\n", person.Age)
	fmt.Printf("  City: %s\n", person.City)
	fmt.Printf("  Active: %v\n", person.Active)
	fmt.Printf("  Tags: %v\n", person.Tags)
	if person.Department != nil {
		fmt.Printf("  Department: %s (%s)\n", person.Department.Name, person.Department.Location)
	}

	fmt.Println("\n=== 2. PullIntoMany: batch retrieval ===")
	// Pull multiple entities at once.
	ids := []datalog.Identity{
		datalog.NewIdentity("person:alice"),
		datalog.NewIdentity("person:bob"),
		datalog.NewIdentity("person:carol"),
	}

	// PullIntoMany needs a slice of the struct type.
	var people []Person
	if err := d.PullIntoMany(ids, &people); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Pulled %d people:\n", len(people))
	for _, p := range people {
		dept := "none"
		if p.Department != nil {
			dept = p.Department.Name
		}
		fmt.Printf("    %s (dept: %s, tags: %v)\n", p.Name, dept, p.Tags)
	}

	fmt.Println("\n=== 3. Convenience getters ===")
	// For simple attribute lookups, GetString/GetInt/etc. are more direct.
	name, ok, err := d.GetString(alice, datalog.NewKeyword(":person/name"))
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		fmt.Printf("  GetString: %s\n", name)
	}

	age, ok, err := d.GetInt(alice, datalog.NewKeyword(":person/age"))
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		fmt.Printf("  GetInt: %d\n", age)
	}

	active, ok, err := d.GetBool(alice, datalog.NewKeyword(":person/active"))
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		fmt.Printf("  GetBool: %v\n", active)
	}

	// GetRef returns the referenced entity's Identity.
	deptRef, ok, err := d.GetRef(alice, datalog.NewKeyword(":person/department"))
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		deptName, _, _ := d.GetString(deptRef, datalog.NewKeyword(":dept/name"))
		fmt.Printf("  GetRef -> GetString: department = %s\n", deptName)
	}

	// GetStrings returns all values for a cardinality-many attribute.
	tags, err := d.GetStrings(alice, datalog.NewKeyword(":person/tags"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  GetStrings: %v\n", tags)

	fmt.Println("\n=== 4. Pull with query: find then pull ===")
	// A common pattern: query for entity IDs, then PullInto for details.
	var entityIDs []datalog.Identity
	err = d.QueryInto(&entityIDs,
		`[:find ?e
		  :where [?e :person/city "San Francisco"]
		         [?e :person/active true]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Found %d active people in SF:\n", len(entityIDs))

	var sfPeople []Person
	if err := d.PullIntoMany(entityIDs, &sfPeople); err != nil {
		log.Fatal(err)
	}
	for _, p := range sfPeople {
		fmt.Printf("    %s (age %d)\n", p.Name, p.Age)
	}
}
