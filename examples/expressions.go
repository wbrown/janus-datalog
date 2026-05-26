//go:build example

// expressions.go demonstrates computed values in queries: arithmetic,
// string concatenation, ground values, and time extraction functions.
//
// Run from the repository root:
//
//	go run -tags example examples/expressions.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "expressions-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(tmpDir + "/expr.db")
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

	fmt.Println("=== 1. Arithmetic: salary after raise ===")
	// Expressions compute new values from existing ones.
	// EDN equivalent: [(* ?salary 1.1) ?raised]
	e := qb.NewVar("e")
	name := qb.NewVar("name")
	salary := qb.NewVar("salary")
	raised := qb.NewVar("raised")

	q := qb.Query().
		Find(name, salary, raised).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Pat(e, qb.Kw(":person/salary"), salary),
			qb.Mul(salary, qb.V(1.1)).As(raised),
		).
		OrderBy(qb.Desc(salary)).
		MustBuild()

	type RaiseResult struct {
		Name   string  `datalog:"?name"`
		Salary float64 `datalog:"?salary"`
		Raised float64 `datalog:"?raised"`
	}
	var raises []RaiseResult
	if err := d.QueryInto(&raises, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Top 5 with 10% raise:")
	for _, r := range raises[:5] {
		fmt.Printf("    %s: $%.0f -> $%.0f\n", r.Name, r.Salary, r.Raised)
	}

	fmt.Println("\n=== 2. Subtraction: years until retirement ===")
	// EDN equivalent: [(- 65 ?age) ?years_left]
	age := qb.NewVar("age")
	yearsLeft := qb.NewVar("years_left")

	q = qb.Query().
		Find(name, age, yearsLeft).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Pat(e, qb.Kw(":person/age"), age),
			qb.Sub(qb.V(int64(65)), age).As(yearsLeft),
			qb.Lt(yearsLeft, qb.V(int64(20))),
		).
		OrderBy(qb.Asc(yearsLeft)).
		MustBuild()

	type RetireResult struct {
		Name      string `datalog:"?name"`
		Age       int64  `datalog:"?age"`
		YearsLeft int64  `datalog:"?years_left"`
	}
	var retire []RetireResult
	if err := d.QueryInto(&retire, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  People within 20 years of retirement:")
	for _, r := range retire {
		fmt.Printf("    %s (age %d): %d years left\n", r.Name, r.Age, r.YearsLeft)
	}

	fmt.Println("\n=== 3. Ground: inject constants ===")
	// Ground binds a constant value to a variable.
	// EDN equivalent: [(ground 0.30) ?tax_rate]
	taxRate := qb.NewVar("tax_rate")
	afterTax := qb.NewVar("after_tax")

	q = qb.Query().
		Find(name, salary, afterTax).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Pat(e, qb.Kw(":person/salary"), salary),
			qb.Ground(0.30).As(taxRate),
			qb.Mul(salary, taxRate).As(afterTax),  // this computes salary * tax_rate, not after-tax
			qb.Sub(salary, afterTax).As(afterTax), // net pay
		).
		OrderBy(qb.Desc(salary)).
		MustBuild()

	// Actually, we can't reuse afterTax for both bindings. Let me fix:
	taxAmt := qb.NewVar("tax_amt")
	netPay := qb.NewVar("net_pay")

	q = qb.Query().
		Find(name, salary, netPay).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Pat(e, qb.Kw(":person/salary"), salary),
			qb.Ground(0.30).As(taxRate),
			qb.Mul(salary, taxRate).As(taxAmt),
			qb.Sub(salary, taxAmt).As(netPay),
		).
		OrderBy(qb.Desc(netPay)).
		MustBuild()

	type NetPayResult struct {
		Name   string  `datalog:"?name"`
		Salary float64 `datalog:"?salary"`
		NetPay float64 `datalog:"?net_pay"`
	}
	var netPays []NetPayResult
	if err := d.QueryInto(&netPays, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Top 5 by net pay (30% tax):")
	for _, r := range netPays[:5] {
		fmt.Printf("    %s: $%.0f gross -> $%.0f net\n", r.Name, r.Salary, r.NetPay)
	}

	fmt.Println("\n=== 4. Time extraction: join year ===")
	// Extract year/month from :person/joined timestamps.
	// EDN equivalent: [(year ?joined) ?year]
	joined := qb.NewVar("joined")
	year := qb.NewVar("year")

	q = qb.Query().
		Find(name, year).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Pat(e, qb.Kw(":person/joined"), joined),
			qb.Year(joined).As(year),
			qb.Lte(year, qb.V(int64(2019))),
		).
		OrderBy(qb.Asc(year)).
		MustBuild()

	type JoinYear struct {
		Name string `datalog:"?name"`
		Year int64  `datalog:"?year"`
	}
	var earlyJoiners []JoinYear
	if err := d.QueryInto(&earlyJoiners, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  People who joined in 2019 or earlier:")
	for _, r := range earlyJoiners {
		fmt.Printf("    %s (joined %d)\n", r.Name, r.Year)
	}

	fmt.Println("\n=== 5. Chained comparison: salary range ===")
	// Range creates min < var < max (exclusive).
	// RangeInclusive creates min <= var <= max.
	// EDN equivalent: [(< 120000.0 ?salary 180000.0)]
	q = qb.Query().
		Find(name, salary).
		Where(
			qb.Pat(e, qb.Kw(":person/name"), name),
			qb.Pat(e, qb.Kw(":person/salary"), salary),
			qb.Range(qb.V(120000.0), salary, qb.V(180000.0)),
		).
		OrderBy(qb.Asc(salary)).
		MustBuild()

	type SalaryRange struct {
		Name   string  `datalog:"?name"`
		Salary float64 `datalog:"?salary"`
	}
	var midRange []SalaryRange
	if err := d.QueryInto(&midRange, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  People with salary $120k-$180k (exclusive):")
	for _, r := range midRange {
		fmt.Printf("    %s: $%.0f\n", r.Name, r.Salary)
	}
}
