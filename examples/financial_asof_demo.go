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
	// Create a database with time-based transactions
	dbPath := "/tmp/datalog-asof"
	os.RemoveAll(dbPath)

	db, err := storage.NewDatabaseWithTimeTx(dbPath)
	if err != nil {
		log.Fatal("Failed to create database:", err)
	}
	defer db.Close()

	// Security entity
	aapl := datalog.NewIdentity("security:AAPL")

	// Add security metadata with a timestamp before all price data
	baseTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	tx := db.NewTransactionAt(baseTime)
	tx.Add(aapl, datalog.NewKeyword(":security/ticker"), "AAPL")
	tx.Add(aapl, datalog.NewKeyword(":security/name"), "Apple Inc.")
	tx.Add(aapl, datalog.NewKeyword(":security/exchange"), "NASDAQ")
	txID, _ := tx.Commit()
	fmt.Printf("Security metadata added at Tx=%d\n", txID)

	// Add price history with specific timestamps
	prices := []struct {
		date   time.Time
		open   float64
		close  float64
		volume int64
	}{
		{time.Date(2024, 1, 2, 16, 0, 0, 0, time.UTC), 186.50, 185.64, 82488700},
		{time.Date(2024, 1, 3, 16, 0, 0, 0, time.UTC), 185.50, 184.25, 58414500},
		{time.Date(2024, 1, 4, 16, 0, 0, 0, time.UTC), 183.90, 181.91, 71885400},
		{time.Date(2024, 1, 5, 16, 0, 0, 0, time.UTC), 181.50, 182.68, 62375100},
	}

	fmt.Println("Loading price history:")
	for _, p := range prices {
		tx := db.NewTransactionAt(p.date)
		tx.Add(aapl, datalog.NewKeyword(":price/open"), p.open)
		tx.Add(aapl, datalog.NewKeyword(":price/close"), p.close)
		tx.Add(aapl, datalog.NewKeyword(":price/volume"), p.volume)
		tx.Add(aapl, datalog.NewKeyword(":price/date"), p.date.Format("2006-01-02"))
		txID, _ := tx.Commit()
		fmt.Printf("  %s: Open=$%.2f, Close=$%.2f (Tx=%d)\n", 
			p.date.Format("2006-01-02"), p.open, p.close, txID)
	}

	// Query: Get latest price
	fmt.Println("\nLatest price data:")
	exec := executor.NewExecutor(db.Matcher())
	
	// This gets the most recent price by transaction time
	q1, _ := parser.ParseQuery(`
		[:find ?date ?open ?close ?tx
		 :where [?s :security/ticker "AAPL"]
		        [?s :price/date ?date ?tx]
		        [?s :price/open ?open ?tx]
		        [?s :price/close ?close ?tx]]`)
	
	result1, _ := exec.Execute(q1)
	
	// Find the latest by transaction ID
	var latestTx uint64
	var latestIdx int
	for i := 0; i < result1.Size(); i++ {
		tuple := result1.Get(i)
		tx := tuple[3].(uint64)
		if tx > latestTx {
			latestTx = tx
			latestIdx = i
		}
	}
	
	if result1.Size() > 0 {
		tuple := result1.Get(latestIdx)
		fmt.Printf("  Date: %s, Open: $%.2f, Close: $%.2f\n",
			tuple[0], tuple[1], tuple[2])
	}

	// Query: Price history with daily changes
	fmt.Println("\nPrice history with daily changes:")
	q2, _ := parser.ParseQuery(`
		[:find ?date ?close ?tx
		 :where [?s :security/ticker "AAPL"]
		        [?s :price/date ?date ?tx]
		        [?s :price/close ?close ?tx]]`)
	
	result2, _ := exec.Execute(q2)
	
	// Sort by transaction ID (time)
	type pricePoint struct {
		date  string
		close float64
		tx    uint64
	}
	
	var points []pricePoint
	for i := 0; i < result2.Size(); i++ {
		tuple := result2.Get(i)
		points = append(points, pricePoint{
			date:  tuple[0].(string),
			close: tuple[1].(float64),
			tx:    tuple[2].(uint64),
		})
	}
	
	// Sort by tx (time)
	for i := 0; i < len(points)-1; i++ {
		for j := 0; j < len(points)-i-1; j++ {
			if points[j].tx > points[j+1].tx {
				points[j], points[j+1] = points[j+1], points[j]
			}
		}
	}
	
	// Display with changes
	for i, p := range points {
		if i == 0 {
			fmt.Printf("  %s: $%.2f\n", p.date, p.close)
		} else {
			prev := points[i-1]
			change := p.close - prev.close
			pct := (change / prev.close) * 100
			fmt.Printf("  %s: $%.2f (%+.2f, %+.2f%%)\n", 
				p.date, p.close, change, pct)
		}
	}

	// Demonstrate as-of query capability
	// Use end of day to include all data from Jan 3
	asOfDate := time.Date(2024, 1, 3, 23, 59, 59, 999999999, time.UTC)
	fmt.Printf("\nPrices as of %s:\n", asOfDate.Format("2006-01-02"))
	
	// Debug: Show what transaction IDs we're looking for
	fmt.Printf("Looking for transactions <= %d\n", asOfDate.UnixNano())
	
	// Use AsOf matcher
	asOfMatcher := db.AsOf(uint64(asOfDate.UnixNano()))
	asOfExec := executor.NewExecutor(asOfMatcher)
	
	// First check if we can find the security
	qCheck, _ := parser.ParseQuery(`
		[:find ?s ?ticker
		 :where [?s :security/ticker ?ticker]]`)
	
	resultCheck, _ := asOfExec.Execute(qCheck)
	fmt.Printf("Securities found: %d\n", resultCheck.Size())
	
	// Now try the price query with transaction IDs
	q3, _ := parser.ParseQuery(`
		[:find ?date ?close ?tx
		 :where [?s :security/ticker "AAPL"]
		        [?s :price/date ?date ?tx]
		        [?s :price/close ?close ?tx]]`)
	
	result3, _ := asOfExec.Execute(q3)
	
	// Group by date and pick the latest transaction for each date
	dateMap := make(map[string]struct{
		close float64
		tx    uint64
	})
	
	for i := 0; i < result3.Size(); i++ {
		tuple := result3.Get(i)
		date := tuple[0].(string)
		close := tuple[1].(float64)
		tx := tuple[2].(uint64)
		
		if existing, ok := dateMap[date]; !ok || tx > existing.tx {
			dateMap[date] = struct{
				close float64
				tx    uint64
			}{close, tx}
		}
	}
	
	fmt.Printf("Price data as of %s:\n", asOfDate.Format("2006-01-02"))
	dates := []string{"2024-01-02", "2024-01-03"}
	for _, date := range dates {
		if data, ok := dateMap[date]; ok {
			fmt.Printf("  %s: $%.2f\n", date, data.close)
		}
	}
}
