//go:build example

// getting_started.go demonstrates the core Janus Datalog workflow:
// opening a database, writing facts, and querying them.
//
// Run from the repository root:
//
//	go run -tags example examples/getting_started.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

func main() {
	// Create a temporary database
	tmpDir, err := os.MkdirTemp("", "getting-started-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(tmpDir + "/example.db")
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	// --- Writing facts ---
	// Entities are identified by hashing a unique string.
	// Attributes are keywords like :person/name.
	// Values can be strings, int64, float64, bool, time.Time, or references.
	alice := datalog.NewIdentity("person:alice")
	bob := datalog.NewIdentity("person:bob")
	carol := datalog.NewIdentity("person:carol")

	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")
	city := datalog.NewKeyword(":person/city")

	tx := d.NewTransaction()
	tx.Add(alice, name, "Alice")
	tx.Add(alice, age, int64(30))
	tx.Add(alice, city, "San Francisco")

	tx.Add(bob, name, "Bob")
	tx.Add(bob, age, int64(25))
	tx.Add(bob, city, "New York")

	tx.Add(carol, name, "Carol")
	tx.Add(carol, age, int64(35))
	tx.Add(carol, city, "San Francisco")

	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("=== 1. Query with EDN string ===")
	// The EDN query language follows Datomic conventions.
	// ?e, ?name, ?city are logic variables that get bound during matching.
	rel, err := d.Query(`[:find ?name ?city
	                      :where [?e :person/name ?name]
	                             [?e :person/city ?city]]`)
	if err != nil {
		log.Fatal(err)
	}
	iter := rel.Iterator()
	defer iter.Close()
	for iter.Next() {
		t := iter.Tuple()
		fmt.Printf("  %s lives in %s\n", t[0], t[1])
	}

	fmt.Println("\n=== 2. Same query with qb (query builder) ===")
	// The qb package builds queries programmatically.
	// Pointer identity on *Var creates implicit joins:
	// the same variable 'e' in two patterns means "same entity".
	e := qb.NewVar("e")
	qName := qb.NewVar("name")
	qCity := qb.NewVar("city")

	q := qb.Query().
		Find(qName, qCity).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), qName),
			qb.Pat(e, qb.Kw(":person/city"), qCity),
		).MustBuild()

	rel, err = d.Query(q)
	if err != nil {
		log.Fatal(err)
	}
	iter2 := rel.Iterator()
	defer iter2.Close()
	for iter2.Next() {
		t := iter2.Tuple()
		fmt.Printf("  %s lives in %s\n", t[0], t[1])
	}

	fmt.Println("\n=== 3. QueryInto: typed results ===")
	// QueryInto maps query results directly into Go structs.
	// Struct tags match the ?variable names from the :find clause.
	type PersonResult struct {
		Name string `datalog:"?name"`
		City string `datalog:"?city"`
	}
	var results []PersonResult
	err = d.QueryInto(&results, `[:find ?name ?city
	                               :where [?e :person/name ?name]
	                                      [?e :person/city ?city]]`)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range results {
		fmt.Printf("  %s lives in %s\n", r.Name, r.City)
	}

	fmt.Println("\n=== 4. QueryOneInto: single result ===")
	// QueryOneInto returns the first matching result.
	var single PersonResult
	found, err := d.QueryOneInto(&single,
		`[:find ?name ?city
		  :where [?e :person/name ?name]
		         [?e :person/city ?city]
		         [(= ?name "Alice")]]`)
	if err != nil {
		log.Fatal(err)
	}
	if found {
		fmt.Printf("  Found: %s in %s\n", single.Name, single.City)
	}

	fmt.Println("\n=== 5. Convenience getters ===")
	// GetString, GetInt, etc. retrieve single attribute values directly.
	aliceName, ok, err := d.GetString(alice, name)
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		fmt.Printf("  Alice's name: %s\n", aliceName)
	}

	aliceAge, ok, err := d.GetInt(alice, age)
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		fmt.Printf("  Alice's age: %d\n", aliceAge)
	}

	fmt.Println("\n=== 6. Query with filter ===")
	// Find people in San Francisco older than 28.
	type AgeResult struct {
		Name string `datalog:"?name"`
		Age  int64  `datalog:"?age"`
	}
	var sfPeople []AgeResult
	err = d.QueryInto(&sfPeople,
		`[:find ?name ?age
		  :where [?e :person/name ?name]
		         [?e :person/age ?age]
		         [?e :person/city "San Francisco"]
		         [(> ?age 28)]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  People in SF older than 28:")
	for _, r := range sfPeople {
		fmt.Printf("    %s (age %d)\n", r.Name, r.Age)
	}

	fmt.Println("\n=== 7. Blank (_) wildcard and entity references ===")
	// Use _ (blank) when you don't need to bind a variable.
	// Here we don't care which entity has the name, just give us all names.
	var allNames []string
	err = d.QueryInto(&allNames,
		`[:find ?name :where [_ :person/name ?name]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  All names (blank entity): %v\n", allNames)

	// Count entities with a given attribute using blank for the value.
	rel, err = d.Query(`[:find (count ?e) :where [?e :person/name _]]`)
	if err != nil {
		log.Fatal(err)
	}
	iter3 := rel.Iterator()
	defer iter3.Close()
	if iter3.Next() {
		fmt.Printf("  Total people (blank value): %v\n", iter3.Tuple()[0])
	}

	// Values can be references to other entities.
	// Alice follows Bob; a post references Alice as its author.
	follows := datalog.NewKeyword(":user/follows")
	postAuthor := datalog.NewKeyword(":post/author")
	postContent := datalog.NewKeyword(":post/content")
	post1 := datalog.NewIdentity("post:alice:1")

	tx = d.NewTransaction()
	tx.Add(alice, follows, bob) // reference: alice -> bob
	tx.Add(post1, postAuthor, alice) // reference: post -> alice
	tx.Add(post1, postContent, "Hello Datalog!")
	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// Follow the reference chain: post -> author -> name
	type PostResult struct {
		Author  string `datalog:"?author"`
		Content string `datalog:"?content"`
	}
	var posts []PostResult
	err = d.QueryInto(&posts,
		`[:find ?author ?content
		  :where [?p :post/author ?a]
		         [?a :person/name ?author]
		         [?p :post/content ?content]]`)
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range posts {
		fmt.Printf("  Post by %s: %q\n", p.Author, p.Content)
	}
}
