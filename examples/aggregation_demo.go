//go:build example
// +build example

package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func main() {
	// Create a database for financial data
	dbPath := "/tmp/datalog-aggregation"
	os.RemoveAll(dbPath)

	db, err := storage.NewDatabaseWithTimeTx(dbPath)
	if err != nil {
		log.Fatal("Failed to create database:", err)
	}
	defer db.Close()

	// Create entities for securities
	aapl := datalog.NewIdentity("security:AAPL")
	googl := datalog.NewIdentity("security:GOOGL")
	msft := datalog.NewIdentity("security:MSFT")

	// Add security metadata
	tx := db.NewTransaction()
	tx.Add(aapl, datalog.NewKeyword(":security/ticker"), "AAPL")
	tx.Add(aapl, datalog.NewKeyword(":security/name"), "Apple Inc.")
	tx.Add(aapl, datalog.NewKeyword(":security/sector"), "Technology")

	tx.Add(googl, datalog.NewKeyword(":security/ticker"), "GOOGL")
	tx.Add(googl, datalog.NewKeyword(":security/name"), "Alphabet Inc.")
	tx.Add(googl, datalog.NewKeyword(":security/sector"), "Technology")

	tx.Add(msft, datalog.NewKeyword(":security/ticker"), "MSFT")
	tx.Add(msft, datalog.NewKeyword(":security/name"), "Microsoft Corp.")
	tx.Add(msft, datalog.NewKeyword(":security/sector"), "Technology")
	tx.Commit()

	// Add trading data for a specific date
	date := time.Date(2024, 1, 15, 16, 0, 0, 0, time.UTC)

	tx = db.NewTransactionAt(date)
	// AAPL trades
	tx.Add(aapl, datalog.NewKeyword(":trade/volume"), int64(65432100))
	tx.Add(aapl, datalog.NewKeyword(":trade/close"), 185.92)
	tx.Add(aapl, datalog.NewKeyword(":trade/market-cap"), 2870000000000.0) // $2.87T

	// GOOGL trades
	tx.Add(googl, datalog.NewKeyword(":trade/volume"), int64(23456789))
	tx.Add(googl, datalog.NewKeyword(":trade/close"), 142.65)
	tx.Add(googl, datalog.NewKeyword(":trade/market-cap"), 1820000000000.0) // $1.82T

	// MSFT trades
	tx.Add(msft, datalog.NewKeyword(":trade/volume"), int64(18234567))
	tx.Add(msft, datalog.NewKeyword(":trade/close"), 384.25)
	tx.Add(msft, datalog.NewKeyword(":trade/market-cap"), 2850000000000.0) // $2.85T
	tx.Commit()

	// Create executor
	exec := executor.NewExecutor(db.Matcher())

	// Test 1: Count securities
	fmt.Println("Test 1: Count all securities")
	q1, _ := parser.ParseQuery(`
		[:find (count ?s)
		 :where [?s :security/ticker _]]`)

	result1, _ := exec.Execute(q1)
	fmt.Printf("Total securities: %v\n\n", result1.Get(0)[0])

	// Test 2: Sum of trading volumes
	fmt.Println("Test 2: Total trading volume")
	q2, _ := parser.ParseQuery(`
		[:find (sum ?volume)
		 :where [?s :trade/volume ?volume]]`)

	result2, _ := exec.Execute(q2)
	fmt.Printf("Total volume: %v\n\n", result2.Get(0)[0])

	// Test 3: Average close price
	fmt.Println("Test 3: Average closing price")
	q3, _ := parser.ParseQuery(`
		[:find (avg ?close)
		 :where [?s :trade/close ?close]]`)

	result3, _ := exec.Execute(q3)
	fmt.Printf("Average close: $%.2f\n\n", result3.Get(0)[0])

	// Test 4: Min and max prices
	fmt.Println("Test 4: Price range")
	q4, _ := parser.ParseQuery(`
		[:find (min ?close) (max ?close)
		 :where [?s :trade/close ?close]]`)

	result4, _ := exec.Execute(q4)
	fmt.Printf("Min price: $%.2f, Max price: $%.2f\n\n", 
		result4.Get(0)[0], result4.Get(0)[1])

	// Test 5: Group by sector with aggregations
	fmt.Println("Test 5: Sector analysis")
	q5, _ := parser.ParseQuery(`
		[:find ?sector (count ?s) (sum ?volume) (avg ?close)
		 :where [?s :security/sector ?sector]
		        [?s :trade/volume ?volume]
		        [?s :trade/close ?close]]`)

	result5, _ := exec.Execute(q5)
	for i := 0; i < result5.Size(); i++ {
		tuple := result5.Get(i)
		fmt.Printf("Sector: %s\n", tuple[0])
		fmt.Printf("  Count: %v\n", tuple[1])
		fmt.Printf("  Total Volume: %v\n", tuple[2])
		fmt.Printf("  Avg Price: $%.2f\n", tuple[3])
	}

	// Test 6: Multiple grouping variables
	fmt.Println("\nTest 6: Market cap by ticker")
	q6, _ := parser.ParseQuery(`
		[:find ?ticker ?name (sum ?cap)
		 :where [?s :security/ticker ?ticker]
		        [?s :security/name ?name]
		        [?s :trade/market-cap ?cap]]`)

	result6, _ := exec.Execute(q6)
	fmt.Println("Market Caps:")
	for i := 0; i < result6.Size(); i++ {
		tuple := result6.Get(i)
		cap := tuple[2].(float64) / 1e12 // Convert to trillions
		fmt.Printf("  %s (%s): $%.2fT\n", tuple[0], tuple[1], cap)
	}

	// Test 7: Total sector market cap
	fmt.Println("\nTest 7: Total sector market cap")
	q7, _ := parser.ParseQuery(`
		[:find ?sector (sum ?cap)
		 :where [?s :security/sector ?sector]
		        [?s :trade/market-cap ?cap]]`)

	result7, _ := exec.Execute(q7)
	for i := 0; i < result7.Size(); i++ {
		tuple := result7.Get(i)
		cap := tuple[1].(float64) / 1e12 // Convert to trillions
		fmt.Printf("%s sector: $%.2fT\n", tuple[0], cap)
	}
}
