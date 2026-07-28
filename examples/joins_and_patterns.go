//go:build example

// joins_and_patterns.go demonstrates how shared variables create joins
// across multiple data patterns, including multi-hop joins and
// cross-product prevention.
//
// Run from the repository root:
//
//	go run -tags example examples/joins_and_patterns.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "joins-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(tmpDir + "/joins.db")
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	f, err := os.Open("examples/data/people.edn")
	if err != nil {
		log.Fatal(err)
	}
	if err := d.Import(f); err != nil {
		log.Fatal(err)
	}
	f.Close()

	fmt.Println("=== 1. Two-way join: person -> department ===")
	// The variable 'dept' appears in both patterns, creating a join.
	// Pattern 1 binds ?dept to the person's department reference.
	// Pattern 2 binds the same ?dept to the department's name.
	type PersonDept struct {
		Name     string `datalog:"?name"`
		DeptName string `datalog:"?dept_name"`
	}
	var pd []PersonDept
	err = d.QueryInto(&pd,
		`[:find ?name ?dept_name
		  :where [?e :person/name ?name]
		         [?e :person/department ?dept]
		         [?dept :dept/name ?dept_name]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Person -> Department:")
	for _, r := range pd[:5] {
		fmt.Printf("    %s -> %s\n", r.Name, r.DeptName)
	}
	fmt.Printf("    ... and %d more\n", len(pd)-5)

	fmt.Println("\n=== 2. Three-way join: person -> department -> location ===")
	// Each shared variable adds another join condition.
	// ?e links name to department, ?dept links department to location.
	e := qb.NewVar("e")
	name := qb.NewVar("name")
	dept := qb.NewVar("dept")
	deptName := qb.NewVar("dept_name")
	location := qb.NewVar("location")

	q := qb.Query().
		Find(name, deptName, location).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Pat(e, qb.Kw(":person/department"), dept),
			qb.Pat(dept, qb.Kw(":dept/name"), deptName),
			qb.Pat(dept, qb.Kw(":dept/location"), location),
		).
		OrderBy(qb.Asc(location), qb.Asc(name)).
		MustBuild()

	type PersonDeptLoc struct {
		Name     string `datalog:"?name"`
		DeptName string `datalog:"?dept_name"`
		Location string `datalog:"?location"`
	}
	var pdl []PersonDeptLoc
	if err := d.QueryInto(&pdl, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Person -> Department -> Location:")
	for _, r := range pdl[:6] {
		fmt.Printf("    %s (%s) in %s\n", r.Name, r.DeptName, r.Location)
	}
	fmt.Printf("    ... and %d more\n", len(pdl)-6)

	fmt.Println("\n=== 3. Join with filter: people in San Francisco departments ===")
	q = qb.Query().
		Find(name, deptName).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Pat(e, qb.Kw(":person/department"), dept),
			qb.Pat(dept, qb.Kw(":dept/name"), deptName),
			qb.Pat(dept, qb.Kw(":dept/location"), qb.V("San Francisco")),
		).
		OrderBy(qb.Asc(name)).
		MustBuild()

	var sfPeople []PersonDept
	if err := d.QueryInto(&sfPeople, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  People in SF departments:")
	for _, r := range sfPeople {
		fmt.Printf("    %s (%s)\n", r.Name, r.DeptName)
	}

	fmt.Println("\n=== 4. Self-join: person -> manager ===")
	// Both ?e and ?mgr are entity variables. ?mgr appears as a value
	// in :person/manager and as an entity in :person/name.
	mgr := qb.NewVar("mgr")
	mgrName := qb.NewVar("mgr_name")

	q = qb.Query().
		Find(name, mgrName).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Pat(e, qb.Kw(":person/manager"), mgr),
			qb.Pat(mgr, qb.Kw(":person/name"), mgrName),
		).
		OrderBy(qb.Asc(mgrName), qb.Asc(name)).
		MustBuild()

	type PersonMgr struct {
		Name    string `datalog:"?name"`
		Manager string `datalog:"?mgr_name"`
	}
	var pms []PersonMgr
	if err := d.QueryInto(&pms, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Person -> Manager:")
	for _, r := range pms[:6] {
		fmt.Printf("    %s reports to %s\n", r.Name, r.Manager)
	}
	fmt.Printf("    ... and %d more\n", len(pms)-6)

	fmt.Println("\n=== 5. Cross-product prevention ===")
	// When patterns share NO variables, the engine detects the disjoint
	// groups and raises an error instead of silently creating a Cartesian
	// product (18 people x 4 departments = 72 meaningless tuples).
	personName := qb.NewVar("pname")
	deptN := qb.NewVar("dname")
	p := qb.NewVar("p")
	dep := qb.NewVar("d")

	q = qb.Query().
		Find(personName, deptN).
		Where(
			qb.Pat(p, qb.Kw(":person/name"), personName),
			// No shared variable between these two patterns!
			qb.Pat(dep, qb.Kw(":dept/name"), deptN),
		).MustBuild()

	_, err = d.Query(q)
	if err != nil {
		fmt.Printf("  Disjoint patterns detected: %v\n", err)
	} else {
		fmt.Println("  Note: query succeeded (planner may optimize small cross-products)")
		fmt.Println("  Best practice: always join through shared variables")
	}
}
