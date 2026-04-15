//go:build example

// aggregations.go demonstrates aggregate functions (count, sum, avg,
// min, max) with grouping and ordering.
//
// Run from the repository root:
//
//	go run -tags example examples/aggregations.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "aggregations-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(tmpDir + "/agg.db")
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

	fmt.Println("=== 1. Count: total securities ===")
	// EDN: [:find (count ?s) :where [?s :security/ticker _]]
	s := qb.NewVar("s")
	ticker := qb.NewVar("ticker")

	q := qb.Query().
		Find(qb.Count(s)).
		Where(qb.Pat(s, qb.Kw(":security/ticker"), ticker)).
		MustBuild()

	rel, err := d.Query(q)
	if err != nil {
		log.Fatal(err)
	}
	iter := rel.Iterator()
	defer iter.Close()
	if iter.Next() {
		fmt.Printf("  Total securities: %v\n", iter.Tuple()[0])
	}

	fmt.Println("\n=== 2. Avg: average closing price per security ===")
	// Non-aggregated variables become group-by keys.
	// EDN: [:find ?ticker (avg ?close) :where ...]
	p := qb.NewVar("p")
	sec := qb.NewVar("sec")
	close_ := qb.NewVar("close")

	q = qb.Query().
		Find(ticker, qb.Avg(close_)).
		Where(
			qb.Pat(p, qb.Kw(":price/security"), sec),
			qb.Pat(p, qb.Kw(":price/close"), close_),
			qb.Pat(sec, qb.Kw(":security/ticker"), ticker),
		).
		OrderBy(qb.Asc(ticker)).
		MustBuild()

	type AvgClose struct {
		Ticker   string  `datalog:"?ticker"`
		AvgClose float64 `datalog:"(avg ?close)"`
	}
	var avgCloses []AvgClose
	if err := d.QueryInto(&avgCloses, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Average closing price by ticker:")
	for _, r := range avgCloses {
		fmt.Printf("    %s: $%.2f\n", r.Ticker, r.AvgClose)
	}

	fmt.Println("\n=== 3. Multiple aggregations in one query ===")
	// Sum total volume and find min/max close per security.
	volume := qb.NewVar("volume")

	q = qb.Query().
		Find(ticker, qb.Sum(volume), qb.Min(close_), qb.Max(close_)).
		Where(
			qb.Pat(p, qb.Kw(":price/security"), sec),
			qb.Pat(p, qb.Kw(":price/close"), close_),
			qb.Pat(p, qb.Kw(":price/volume"), volume),
			qb.Pat(sec, qb.Kw(":security/ticker"), ticker),
		).
		OrderBy(qb.Desc(qb.NewVar("(sum ?volume)"))).
		MustBuild()

	type SecurityStats struct {
		Ticker   string  `datalog:"?ticker"`
		TotalVol int64   `datalog:"(sum ?volume)"`
		MinClose float64 `datalog:"(min ?close)"`
		MaxClose float64 `datalog:"(max ?close)"`
	}
	var stats []SecurityStats
	if err := d.QueryInto(&stats, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Security stats (volume, min/max close):")
	for _, r := range stats {
		fmt.Printf("    %s: vol=%dM, range=$%.2f-$%.2f\n",
			r.Ticker, r.TotalVol/1_000_000, r.MinClose, r.MaxClose)
	}

	fmt.Println("\n=== 4. Group by sector ===")
	sector := qb.NewVar("sector")

	q = qb.Query().
		Find(sector, qb.Count(sec), qb.Sum(volume)).
		Where(
			qb.Pat(p, qb.Kw(":price/security"), sec),
			qb.Pat(p, qb.Kw(":price/volume"), volume),
			qb.Pat(sec, qb.Kw(":security/sector"), sector),
		).
		MustBuild()

	type SectorStats struct {
		Sector   string `datalog:"?sector"`
		Count    int64  `datalog:"(count ?sec)"`
		TotalVol int64  `datalog:"(sum ?volume)"`
	}
	var sectorStats []SectorStats
	if err := d.QueryInto(&sectorStats, q); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Volume by sector:")
	for _, r := range sectorStats {
		fmt.Printf("    %s: %d securities, total volume %dM\n",
			r.Sector, r.Count, r.TotalVol/1_000_000)
	}

	fmt.Println("\n=== 5. EDN string query with aggregation ===")
	// Same query written as an EDN string instead of qb.
	var ednResults []AvgClose
	err = d.QueryInto(&ednResults,
		`[:find ?ticker (avg ?close)
		  :where [?p :price/security ?sec]
		         [?p :price/close ?close]
		         [?sec :security/ticker ?ticker]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Average close (via EDN query):")
	for _, r := range ednResults {
		fmt.Printf("    %s: $%.2f\n", r.Ticker, r.AvgClose)
	}
}
