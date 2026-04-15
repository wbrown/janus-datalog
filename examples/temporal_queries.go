//go:build example

// temporal_queries.go demonstrates time-travel queries using AsOf and
// History to view past database states.
//
// Run from the repository root:
//
//	go run -tags example examples/temporal_queries.go
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
)

func main() {
	tmpDir, err := os.MkdirTemp("", "temporal-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(tmpDir + "/temporal.db")
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	stock := datalog.NewIdentity("stock:AAPL")
	price := datalog.NewKeyword(":stock/price")
	name := datalog.NewKeyword(":stock/name")

	fmt.Println("=== 1. Multiple transactions updating the same entity ===")
	// Each Commit() returns an ElementID that identifies the transaction.
	// We'll track AAPL's price across 4 transactions.

	tx := d.NewTransactionAt(time.Date(2025, 1, 6, 16, 0, 0, 0, time.UTC))
	tx.Add(stock, name, "Apple Inc.")
	tx.Add(stock, price, 243.50)
	tx1ID, err := tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Tx1 (Jan 6): price=$243.50, txID=%s\n", tx1ID)

	tx = d.NewTransactionAt(time.Date(2025, 1, 7, 16, 0, 0, 0, time.UTC))
	tx.Add(stock, price, 246.80)
	tx2ID, err := tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Tx2 (Jan 7): price=$246.80, txID=%s\n", tx2ID)

	tx = d.NewTransactionAt(time.Date(2025, 1, 8, 16, 0, 0, 0, time.UTC))
	tx.Add(stock, price, 248.50)
	tx3ID, err := tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Tx3 (Jan 8): price=$248.50, txID=%s\n", tx3ID)

	tx = d.NewTransactionAt(time.Date(2025, 1, 9, 16, 0, 0, 0, time.UTC))
	tx.Add(stock, price, 252.30)
	_, err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Tx4 (Jan 9): price=$252.30")

	fmt.Println("\n=== 2. Current state: latest values ===")
	// Without AsOf, queries see the latest state.
	currentPrice, ok, err := d.GetFloat(stock, price)
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		fmt.Printf("  Current price: $%.2f\n", currentPrice)
	}

	fmt.Println("\n=== 3. AsOf: query past state ===")
	// AsOf returns a database handle filtered to a specific transaction.
	// Later writes are invisible through this handle.
	asOfTx1 := d.AsOf(tx1ID)
	p1, ok, _ := asOfTx1.GetFloat(stock, price)
	if ok {
		fmt.Printf("  As of Tx1 (Jan 6): $%.2f\n", p1)
	}

	asOfTx2 := d.AsOf(tx2ID)
	p2, ok, _ := asOfTx2.GetFloat(stock, price)
	if ok {
		fmt.Printf("  As of Tx2 (Jan 7): $%.2f\n", p2)
	}

	asOfTx3 := d.AsOf(tx3ID)
	p3, ok, _ := asOfTx3.GetFloat(stock, price)
	if ok {
		fmt.Printf("  As of Tx3 (Jan 8): $%.2f\n", p3)
	}

	fmt.Println("\n=== 4. AsOf queries are full database views ===")
	// You can run any query on an AsOf handle, not just getters.
	type StockPrice struct {
		Name  string  `datalog:"?name"`
		Price float64 `datalog:"?price"`
	}
	var result StockPrice
	found, err := asOfTx2.QueryOneInto(&result,
		`[:find ?name ?price
		  :where [?s :stock/name ?name]
		         [?s :stock/price ?price]]`)
	if err != nil {
		log.Fatal(err)
	}
	if found {
		fmt.Printf("  Query as of Tx2: %s at $%.2f\n", result.Name, result.Price)
	}

	fmt.Println("\n=== 5. History: see all versions ===")
	// History() returns all raw datoms without CRDT resolution.
	// Every write is visible, including superseded values.
	hist := d.History()
	var allPrices []float64
	err = hist.QueryInto(&allPrices,
		`[:find ?price
		  :where [?s :stock/price ?price]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  All historical prices: %v\n", allPrices)

	fmt.Println("\n=== 6. Multiple entities with temporal queries ===")
	// Add a second stock to show AsOf works across entities.
	msft := datalog.NewIdentity("stock:MSFT")
	tx = d.NewTransactionAt(time.Date(2025, 1, 6, 16, 0, 0, 0, time.UTC))
	tx.Add(msft, name, "Microsoft Corp.")
	tx.Add(msft, price, 420.10)
	msftTx1, err := tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	tx = d.NewTransactionAt(time.Date(2025, 1, 7, 16, 0, 0, 0, time.UTC))
	tx.Add(msft, price, 425.90)
	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// AsOf the first MSFT transaction: AAPL should show its latest
	// price at that time too (all entities are filtered together).
	asOfMsft1 := d.AsOf(msftTx1)
	var stocks []StockPrice
	err = asOfMsft1.QueryInto(&stocks,
		`[:find ?name ?price
		  :where [?s :stock/name ?name]
		         [?s :stock/price ?price]]`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Prices as of MSFT's first transaction:")
	for _, s := range stocks {
		fmt.Printf("    %s: $%.2f\n", s.Name, s.Price)
	}
}
