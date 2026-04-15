//go:build example

// multi_source.go demonstrates cross-source queries: joining database
// entities with in-memory Go data using SliceSource and NewMemorySource.
//
// Run from the repository root:
//
//	go run -tags example examples/multi_source.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

// PortfolioRule is a Go struct that will be exposed as a queryable source.
type PortfolioRule struct {
	Ticker    string
	MaxWeight float64
	Rating    string
}

func main() {
	tmpDir, err := os.MkdirTemp("", "multi-source-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(tmpDir + "/multi.db")
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	// Load securities from the shared dataset
	f, err := os.Open("examples/data/securities.edn")
	if err != nil {
		log.Fatal(err)
	}
	if err := d.Import(f); err != nil {
		log.Fatal(err)
	}
	f.Close()

	fmt.Println("=== 1. SliceSource: Go structs as a query source ===")
	// SliceSource wraps a Go slice so it can be queried alongside
	// database entities. Each struct field is mapped to a keyword
	// via an AttributeSchema.
	rules := []PortfolioRule{
		{"AAPL", 0.25, "Buy"},
		{"GOOGL", 0.20, "Buy"},
		{"MSFT", 0.15, "Hold"},
		{"AMZN", 0.20, "Buy"},
		{"TSLA", 0.10, "Sell"},
		{"NVDA", 0.10, "Buy"}, // not in our database
	}

	ruleSource := db.NewSliceSource(rules, db.AttributeSchema[PortfolioRule]{
		datalog.NewKeyword(":rule/ticker"):     func(r PortfolioRule) any { return r.Ticker },
		datalog.NewKeyword(":rule/max-weight"): func(r PortfolioRule) any { return r.MaxWeight },
		datalog.NewKeyword(":rule/rating"):     func(r PortfolioRule) any { return r.Rating },
	})

	// Query from the rule source alone (no database needed).
	type RuleResult struct {
		Ticker string  `datalog:"?ticker"`
		Weight float64 `datalog:"?weight"`
		Rating string  `datalog:"?rating"`
	}
	var ruleResults []RuleResult
	err = d.QueryInto(&ruleResults,
		`[:find ?ticker ?weight ?rating
		  :in $rules
		  :where [$rules ?r :rule/ticker ?ticker]
		         [$rules ?r :rule/max-weight ?weight]
		         [$rules ?r :rule/rating ?rating]]`,
		db.WithSources(map[datalog.Symbol]db.PatternMatcher{
			datalog.NewSymbol("$rules"): ruleSource,
		}))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Portfolio rules:")
	for _, r := range ruleResults {
		fmt.Printf("    %s: max %.0f%%, rating=%s\n", r.Ticker, r.Weight*100, r.Rating)
	}

	fmt.Println("\n=== 2. Cross-source join: database + in-memory ===")
	// Join securities from the database ($) with rules from memory ($rules).
	// The ?ticker variable bridges the two sources.
	type EnrichedResult struct {
		Ticker string  `datalog:"?ticker"`
		Name   string  `datalog:"?name"`
		Sector string  `datalog:"?sector"`
		Rating string  `datalog:"?rating"`
		Weight float64 `datalog:"?weight"`
	}
	var enriched []EnrichedResult
	err = d.QueryInto(&enriched,
		`[:find ?ticker ?name ?sector ?rating ?weight
		  :in $ $rules
		  :where [?s :security/ticker ?ticker]
		         [?s :security/name ?name]
		         [?s :security/sector ?sector]
		         [$rules ?r :rule/ticker ?ticker]
		         [$rules ?r :rule/rating ?rating]
		         [$rules ?r :rule/max-weight ?weight]]`,
		db.WithSources(map[datalog.Symbol]db.PatternMatcher{
			datalog.NewSymbol("$rules"): ruleSource,
		}))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Securities enriched with portfolio rules:")
	for _, r := range enriched {
		fmt.Printf("    %s (%s) - %s, rating=%s, max=%.0f%%\n",
			r.Ticker, r.Name, r.Sector, r.Rating, r.Weight*100)
	}
	// Note: NVDA doesn't appear because it's not in the database.

	fmt.Println("\n=== 3. Query builder with sources ===")
	// The qb package supports multi-source queries with Source() and PatFrom().
	rs := qb.Source("$rules")
	r := qb.NewVar("r")
	s := qb.NewVar("s")
	tickerVar := qb.NewVar("ticker")
	nameVar := qb.NewVar("name")
	rating := qb.NewVar("rating")

	q := qb.Query().
		Find(tickerVar, nameVar, rating).
		In(qb.DB, rs).
		Where(
			qb.Pat(s, qb.Kw(":security/ticker"), tickerVar),
			qb.Pat(s, qb.Kw(":security/name"), nameVar),
			qb.PatFrom(rs, r, qb.Kw(":rule/ticker"), tickerVar),
			qb.PatFrom(rs, r, qb.Kw(":rule/rating"), rating),
			qb.Eq(rating, qb.V("Buy")),
		).
		OrderBy(qb.Asc(tickerVar)).
		MustBuild()

	type BuyResult struct {
		Ticker string `datalog:"?ticker"`
		Name   string `datalog:"?name"`
		Rating string `datalog:"?rating"`
	}
	var buys []BuyResult
	err = d.QueryInto(&buys, q,
		db.WithSources(map[datalog.Symbol]db.PatternMatcher{
			datalog.NewSymbol("$rules"): ruleSource,
		}))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  'Buy' rated securities (via qb):")
	for _, r := range buys {
		fmt.Printf("    %s (%s)\n", r.Ticker, r.Name)
	}

	fmt.Println("\n=== 4. NewMemorySource: ad-hoc datoms ===")
	// NewMemorySource creates a source from raw datoms.
	// Useful for injecting computed or external data.
	alerts := db.NewMemorySource([]datalog.Datom{
		{E: datalog.NewIdentity("alert:1"), A: datalog.NewKeyword(":alert/ticker"), V: "TSLA"},
		{E: datalog.NewIdentity("alert:1"), A: datalog.NewKeyword(":alert/message"), V: "Volatility spike detected"},
		{E: datalog.NewIdentity("alert:2"), A: datalog.NewKeyword(":alert/ticker"), V: "AAPL"},
		{E: datalog.NewIdentity("alert:2"), A: datalog.NewKeyword(":alert/message"), V: "Earnings report tomorrow"},
	})

	type AlertResult struct {
		Ticker  string `datalog:"?ticker"`
		Name    string `datalog:"?name"`
		Message string `datalog:"?msg"`
	}
	var alertResults []AlertResult
	err = d.QueryInto(&alertResults,
		`[:find ?ticker ?name ?msg
		  :in $ $alerts
		  :where [?s :security/ticker ?ticker]
		         [?s :security/name ?name]
		         [$alerts ?a :alert/ticker ?ticker]
		         [$alerts ?a :alert/message ?msg]]`,
		db.WithSources(map[datalog.Symbol]db.PatternMatcher{
			datalog.NewSymbol("$alerts"): alerts,
		}))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Alerts joined with securities:")
	for _, r := range alertResults {
		fmt.Printf("    %s (%s): %s\n", r.Ticker, r.Name, r.Message)
	}
}
