//go:build example

// query_builder.go demonstrates the qb (query builder) package for
// constructing queries programmatically instead of writing EDN strings.
//
// Run from the repository root:
//
//	go run -tags example examples/query_builder.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

// Define attributes as constants. This prevents typos and enables IDE support.
var (
	PersonName   = qb.Kw(":person/name")
	PersonAge    = qb.Kw(":person/age")
	PersonCity   = qb.Kw(":person/city")
	PersonSalary = qb.Kw(":person/salary")
	PersonEmail  = qb.Kw(":person/email")
	PersonNick   = qb.Kw(":person/nickname")
	PersonDept   = qb.Kw(":person/department")
	DeptName     = qb.Kw(":dept/name")
)

func main() {
	tmpDir, err := os.MkdirTemp("", "query-builder-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(tmpDir + "/qb.db")
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	// Load the shared people dataset
	f, err := os.Open("examples/data/people.edn")
	if err != nil {
		log.Fatal(err)
	}
	if err := d.Import(f); err != nil {
		log.Fatal(err)
	}
	f.Close()

	fmt.Println("=== 1. Basic pattern matching ===")
	// Variables are created with qb.NewVar. Same pointer = join.
	e := qb.NewVar("e")
	name := qb.NewVar("name")

	q := qb.Query().
		Find(name).
		Where(qb.Pat(e, PersonName, name)).
		MustBuild()

	var names []string
	if err := d.QueryInto(&names, q); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Found %d people\n", len(names))

	fmt.Println("\n=== 2. Joins via shared variables ===")
	// Using the same *Var in multiple patterns creates an implicit join.
	// Here 'e' joins person name to person city.
	city := qb.NewVar("city")

	q = qb.Query().
		Find(name, city).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonCity, city),
		).MustBuild()

	type personCityResult struct {
		Name string `datalog:"?name"`
		City string `datalog:"?city"`
	}
	var pcs []personCityResult
	if err := d.QueryInto(&pcs, q); err != nil {
		log.Fatal(err)
	}
	for _, pc := range pcs[:3] {
		fmt.Printf("  %s -> %s\n", pc.Name, pc.City)
	}
	fmt.Printf("  ... and %d more\n", len(pcs)-3)

	fmt.Println("\n=== 3. Predicates: filtering ===")
	// qb.Gt, qb.Lt, qb.Eq, qb.Ne filter results.
	age := qb.NewVar("age")

	q = qb.Query().
		Find(name, age).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonAge, age),
			qb.Gt(age, qb.V(int64(40))),
		).MustBuild()

	type NameAge struct {
		Name string `datalog:"?name"`
		Age  int64  `datalog:"?age"`
	}
	var older []NameAge
	if err := d.QueryInto(&older, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  People over 40:")
	for _, r := range older {
		fmt.Printf("    %s (age %d)\n", r.Name, r.Age)
	}

	fmt.Println("\n=== 4. Range predicates ===")
	// qb.Range creates chained comparisons: min < var < max
	q = qb.Query().
		Find(name, age).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonAge, age),
			qb.Range(qb.V(int64(30)), age, qb.V(int64(40))),
		).
		OrderBy(qb.Asc(age)).
		MustBuild()

	var thirties []NameAge
	if err := d.QueryInto(&thirties, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  People in their 30s (exclusive):")
	for _, r := range thirties {
		fmt.Printf("    %s (age %d)\n", r.Name, r.Age)
	}

	fmt.Println("\n=== 5. Constants in patterns ===")
	// qb.V wraps a constant value for use in patterns.
	q = qb.Query().
		Find(name).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonCity, qb.V("New York")),
		).MustBuild()

	var nyNames []string
	if err := d.QueryInto(&nyNames, q); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  People in New York: %v\n", nyNames)

	fmt.Println("\n=== 6. Database functions: GetElse, Missing, GetSome ===")
	// GetElse provides a default value when an attribute is missing.
	nick := qb.NewVar("nick")

	q = qb.Query().
		Find(name, nick).
		Where(
			qb.Pat(e, PersonName, name),
			qb.GetElse(e, PersonNick, "none").As(nick),
		).
		OrderBy(qb.Asc(name)).
		MustBuild()

	type NameNick struct {
		Name string `datalog:"?name"`
		Nick string `datalog:"?nick"`
	}
	var nicks []NameNick
	if err := d.QueryInto(&nicks, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Names with nicknames (defaulting to 'none'):")
	for _, r := range nicks[:5] {
		fmt.Printf("    %s -> %s\n", r.Name, r.Nick)
	}
	fmt.Printf("    ... and %d more\n", len(nicks)-5)

	fmt.Println("\n=== 7. Missing: filter for absent attributes ===")
	// Missing filters for entities that lack an attribute.
	q = qb.Query().
		Find(name).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Missing(e, PersonEmail),
		).
		OrderBy(qb.Asc(name)).
		MustBuild()

	var noEmail []string
	if err := d.QueryInto(&noEmail, q); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  People without email: %v\n", noEmail)

	fmt.Println("\n=== 8. Ordering ===")
	salary := qb.NewVar("salary")

	q = qb.Query().
		Find(name, salary).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonSalary, salary),
		).
		OrderBy(qb.Desc(salary)).
		MustBuild()

	type NameSalary struct {
		Name   string  `datalog:"?name"`
		Salary float64 `datalog:"?salary"`
	}
	var topPaid []NameSalary
	if err := d.QueryInto(&topPaid, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Top 5 salaries:")
	for _, r := range topPaid[:5] {
		fmt.Printf("    %s: $%.0f\n", r.Name, r.Salary)
	}

	fmt.Println("\n=== 9. Wildcard (Blank) ===")
	// qb.Blank() matches any value without binding it.
	// Useful when you need to assert an attribute exists but don't need its value.
	q = qb.Query().
		Find(name).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonNick, qb.Blank()), // has a nickname
		).MustBuild()

	var withNick []string
	if err := d.QueryInto(&withNick, q); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  People with nicknames: %v\n", withNick)
}
