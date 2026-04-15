//go:build example

// subqueries.go demonstrates nested queries with tuple and relation
// binding for correlated computations like "max price per security".
//
// Run from the repository root:
//
//	go run -tags example examples/subqueries.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "subqueries-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(tmpDir + "/sub.db")
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	f, err := os.Open("examples/data/securities.edn")
	if err != nil {
		log.Fatal(err)
	}
	if err := d.Import(f); err != nil {
		log.Fatal(err)
	}
	f.Close()

	fmt.Println("=== 1. Tuple binding: max close per security ===")
	// A subquery computes the max close for a given security.
	// The outer query passes ?sec as input; the inner query returns
	// a single tuple bound to ?max_close.
	//
	// EDN equivalent:
	//   [:find ?ticker ?max_close
	//    :where [?sec :security/ticker ?ticker]
	//           [(q [:find (max ?c)
	//                :in $ ?s
	//                :where [?p :price/security ?s]
	//                       [?p :price/close ?c]]
	//               ?sec)
	//            [[?max_close]]]]

	sec := qb.NewVar("sec")
	ticker := qb.NewVar("ticker")
	maxClose := qb.NewVar("max_close")

	// Inner query: given a security, find its max closing price
	innerSec := qb.NewVar("s")
	innerPrice := qb.NewVar("p")
	innerClose := qb.NewVar("c")

	inner := qb.Query().
		Find(qb.Max(innerClose)).
		In(qb.DB, qb.Scalar(innerSec)).
		Where(
			qb.Pat(innerPrice, qb.Kw(":price/security"), innerSec),
			qb.Pat(innerPrice, qb.Kw(":price/close"), innerClose),
		)

	q := qb.Query().
		Find(ticker, maxClose).
		Where(
			qb.Pat(sec, qb.Kw(":security/ticker"), ticker),
			qb.Subquery(inner, sec).BindTuple(maxClose),
		).
		OrderBy(qb.Desc(maxClose)).
		MustBuild()

	type MaxResult struct {
		Ticker   string  `datalog:"?ticker"`
		MaxClose float64 `datalog:"?max_close"`
	}
	var maxResults []MaxResult
	if err := d.QueryInto(&maxResults, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Maximum closing price per security:")
	for _, r := range maxResults {
		fmt.Printf("    %s: $%.2f\n", r.Ticker, r.MaxClose)
	}

	fmt.Println("\n=== 2. Tuple binding: price range per security ===")
	// Subquery returning multiple aggregates as a single tuple.
	innerMin := qb.NewVar("min_c")
	innerMax := qb.NewVar("max_c")
	minClose := qb.NewVar("min_close")

	rangeInner := qb.Query().
		Find(qb.Min(innerClose), qb.Max(innerClose)).
		In(qb.DB, qb.Scalar(innerSec)).
		Where(
			qb.Pat(innerPrice, qb.Kw(":price/security"), innerSec),
			qb.Pat(innerPrice, qb.Kw(":price/close"), innerClose),
		)

	q = qb.Query().
		Find(ticker, minClose, maxClose).
		Where(
			qb.Pat(sec, qb.Kw(":security/ticker"), ticker),
			qb.Subquery(rangeInner, sec).BindTuple(minClose, maxClose),
		).
		OrderBy(qb.Asc(ticker)).
		MustBuild()

	// Suppress unused variable warnings
	_ = innerMin
	_ = innerMax

	type RangeResult struct {
		Ticker   string  `datalog:"?ticker"`
		MinClose float64 `datalog:"?min_close"`
		MaxClose float64 `datalog:"?max_close"`
	}
	var rangeResults []RangeResult
	if err := d.QueryInto(&rangeResults, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Price range (min-max close) per security:")
	for _, r := range rangeResults {
		fmt.Printf("    %s: $%.2f - $%.2f (spread: $%.2f)\n",
			r.Ticker, r.MinClose, r.MaxClose, r.MaxClose-r.MinClose)
	}

	fmt.Println("\n=== 3. EDN subquery ===")
	// Subqueries can also be written directly in EDN.
	type AvgResult struct {
		Ticker   string  `datalog:"?ticker"`
		AvgClose float64 `datalog:"?avg_close"`
	}
	var avgResults []AvgResult
	err = d.QueryInto(&avgResults,
		`[:find ?ticker ?avg_close
		  :where [?sec :security/ticker ?ticker]
		         [(q [:find (avg ?c)
		              :in $ ?s
		              :where [?p :price/security ?s]
		                     [?p :price/close ?c]]
		             ?sec)
		          [[?avg_close]]]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Average close via EDN subquery:")
	for _, r := range avgResults {
		fmt.Printf("    %s: $%.2f\n", r.Ticker, r.AvgClose)
	}
}
