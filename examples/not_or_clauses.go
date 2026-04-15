//go:build example

// not_or_clauses.go demonstrates logical negation (not, not-join)
// and disjunction (or, or-join, or-default) in queries.
//
// Run from the repository root:
//
//	go run -tags example examples/not_or_clauses.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "not-or-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(tmpDir + "/notor.db")
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

	e := qb.NewVar("e")
	name := qb.NewVar("name")

	fmt.Println("=== 1. NOT: exclude matching entities ===")
	// Find people who are NOT in New York.
	// EDN: (not [?e :person/city "New York"])
	q := qb.Query().
		Find(name).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Not(
				qb.Pat(e, qb.Kw(":person/city"), qb.V("New York")),
			),
		).
		OrderBy(qb.Asc(name)).
		MustBuild()

	var notNY []string
	if err := d.QueryInto(&notNY, q); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  People NOT in New York (%d):\n", len(notNY))
	for _, n := range notNY {
		fmt.Printf("    %s\n", n)
	}

	fmt.Println("\n=== 2. NOT: inactive people ===")
	// Find people who are not active.
	q = qb.Query().
		Find(name).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Not(
				qb.Pat(e, qb.Kw(":person/active"), qb.V(true)),
			),
		).
		OrderBy(qb.Asc(name)).
		MustBuild()

	var inactive []string
	if err := d.QueryInto(&inactive, q); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Inactive people (%d):\n", len(inactive))
	for _, n := range inactive {
		fmt.Printf("    %s\n", n)
	}

	fmt.Println("\n=== 3. NOT-JOIN: explicit join variables ===")
	// not-join explicitly declares which variables link the outer
	// query to the negation. Here ?e is the join variable.
	q = qb.Query().
		Find(name).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.NotJoin([]*qb.Var{e},
				qb.Pat(e, qb.Kw(":person/manager"), qb.Blank()),
			),
		).
		OrderBy(qb.Asc(name)).
		MustBuild()

	var noManager []string
	if err := d.QueryInto(&noManager, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  People without a manager (department heads):")
	for _, n := range noManager {
		fmt.Printf("    %s\n", n)
	}

	fmt.Println("\n=== 4. OR: match any branch ===")
	// Find people in San Francisco OR Chicago.
	// EDN: (or [?e :person/city "San Francisco"]
	//          [?e :person/city "Chicago"])
	q = qb.Query().
		Find(name).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Or().
				Branch(qb.Pat(e, qb.Kw(":person/city"), qb.V("San Francisco"))).
				Branch(qb.Pat(e, qb.Kw(":person/city"), qb.V("Chicago"))),
		).
		OrderBy(qb.Asc(name)).
		MustBuild()

	var sfOrChi []string
	if err := d.QueryInto(&sfOrChi, q); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  People in SF or Chicago (%d):\n", len(sfOrChi))
	for _, n := range sfOrChi {
		fmt.Printf("    %s\n", n)
	}

	fmt.Println("\n=== 5. OR-DEFAULT: fallback values ===")
	// or-default tries each branch and uses the last as a fallback.
	// Find each person's nickname, defaulting to "N/A" if they don't have one.
	nick := qb.NewVar("nick")

	q = qb.Query().
		Find(name, nick).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.OrDefault().
				Branch(qb.Pat(e, qb.Kw(":person/nickname"), nick)).
				Branch(qb.Ground("N/A").As(nick)),
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
	fmt.Println("  Nicknames with fallback:")
	for _, r := range nicks[:8] {
		fmt.Printf("    %s -> %s\n", r.Name, r.Nick)
	}
	fmt.Printf("    ... and %d more\n", len(nicks)-8)

	fmt.Println("\n=== 6. EDN equivalents ===")
	// The same NOT/OR queries as EDN strings.
	var ednResult []string

	// NOT in EDN
	err = d.QueryInto(&ednResult,
		`[:find ?name
		  :where [?e :person/name ?name]
		         (not [?e :person/city "New York"])]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  NOT (EDN): %d people not in NY\n", len(ednResult))

	// OR in EDN
	ednResult = nil
	err = d.QueryInto(&ednResult,
		`[:find ?name
		  :where [?e :person/name ?name]
		         (or [?e :person/city "San Francisco"]
		             [?e :person/city "Chicago"])]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  OR (EDN): %d people in SF or Chicago\n", len(ednResult))
}
