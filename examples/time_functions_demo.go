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
	// Create a database
	dbPath := "/tmp/datalog-timefunc"
	os.RemoveAll(dbPath)

	db, err := storage.NewDatabaseWithTimeTx(dbPath)
	if err != nil {
		log.Fatal("Failed to create database:", err)
	}
	defer db.Close()

	// Add some time-series data
	fmt.Println("Loading time-series data...")
	
	// Add trades for different dates and times
	trades := []struct {
		symbol    string
		timestamp time.Time
		price     float64
		volume    int64
	}{
		{"AAPL", time.Date(2024, 1, 15, 9, 30, 0, 0, time.UTC), 185.50, 1000000},
		{"AAPL", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), 186.25, 1200000},
		{"AAPL", time.Date(2024, 1, 15, 11, 30, 0, 0, time.UTC), 186.00, 800000},
		{"AAPL", time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC), 185.75, 1500000},
		{"AAPL", time.Date(2024, 1, 16, 9, 30, 0, 0, time.UTC), 186.00, 900000},
		{"AAPL", time.Date(2024, 1, 16, 15, 30, 0, 0, time.UTC), 187.50, 2000000},
		{"AAPL", time.Date(2024, 2, 1, 10, 0, 0, 0, time.UTC), 188.00, 1100000},
		{"AAPL", time.Date(2024, 2, 1, 15, 0, 0, 0, time.UTC), 189.25, 1300000},
		{"MSFT", time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC), 380.00, 500000},
		{"MSFT", time.Date(2024, 1, 15, 15, 0, 0, 0, time.UTC), 382.50, 700000},
		{"MSFT", time.Date(2024, 2, 1, 11, 0, 0, 0, time.UTC), 385.00, 600000},
	}

	for _, trade := range trades {
		tx := db.NewTransactionAt(trade.timestamp)
		e := datalog.NewIdentity(fmt.Sprintf("trade:%s:%d", trade.symbol, trade.timestamp.UnixNano()))
		tx.Add(e, datalog.NewKeyword(":trade/symbol"), trade.symbol)
		tx.Add(e, datalog.NewKeyword(":trade/timestamp"), trade.timestamp)
		tx.Add(e, datalog.NewKeyword(":trade/price"), trade.price)
		tx.Add(e, datalog.NewKeyword(":trade/volume"), trade.volume)
		tx.Commit()
	}

	// Create executor
	exec := executor.NewExecutor(db.Matcher())

	// Test 1: Extract date components
	fmt.Println("\nTest 1: Extract date components from trades")
	q1, _ := parser.ParseQuery(`
		[:find ?symbol ?time ?year ?month ?day ?hour
		 :where [?t :trade/symbol ?symbol]
		        [?t :trade/timestamp ?time]
		        [(year ?time) ?year]
		        [(month ?time) ?month]
		        [(day ?time) ?day]
		        [(hour ?time) ?hour]]`)

	result1, _ := exec.Execute(q1)
	fmt.Println("Symbol | Timestamp | Year | Month | Day | Hour")
	fmt.Println("-------|-----------|------|-------|-----|------")
	for i := 0; i < result1.Size() && i < 5; i++ { // Show first 5
		tuple := result1.Get(i)
		t := tuple[1].(time.Time)
		fmt.Printf("%-6s | %s UTC | %4d | %5d | %3d | %4d\n",
			tuple[0], t.UTC().Format("2006-01-02 15:04"), 
			tuple[2], tuple[3], tuple[4], tuple[5])
	}

	// Test 2: Group by month
	fmt.Println("\nTest 2: Volume by month")
	q2, _ := parser.ParseQuery(`
		[:find ?year ?month (sum ?volume)
		 :where [?t :trade/timestamp ?time]
		        [?t :trade/volume ?volume]
		        [(year ?time) ?year]
		        [(month ?time) ?month]]`)

	result2, _ := exec.Execute(q2)
	fmt.Println("Year | Month | Total Volume")
	fmt.Println("-----|-------|-------------")
	for i := 0; i < result2.Size(); i++ {
		tuple := result2.Get(i)
		fmt.Printf("%4d | %5d | %12.0f\n", tuple[0], tuple[1], tuple[2])
	}

	// Test 3: Average price by hour of day
	fmt.Println("\nTest 3: Average price by hour of day for AAPL")
	q3, _ := parser.ParseQuery(`
		[:find ?hour (avg ?price) (count ?t)
		 :where [?t :trade/symbol "AAPL"]
		        [?t :trade/timestamp ?time]
		        [?t :trade/price ?price]
		        [(hour ?time) ?hour]]`)

	result3, _ := exec.Execute(q3)
	fmt.Println("Hour | Avg Price | Count")
	fmt.Println("-----|-----------|-------")
	for i := 0; i < result3.Size(); i++ {
		tuple := result3.Get(i)
		fmt.Printf("%4d | $%8.2f | %5d\n", tuple[0], tuple[1], tuple[2])
	}

	// Test 4: Daily aggregates with date extraction
	fmt.Println("\nTest 4: Daily volume for AAPL")
	q4, _ := parser.ParseQuery(`
		[:find ?year ?month ?day (sum ?volume)
		 :where [?t :trade/symbol "AAPL"]
		        [?t :trade/timestamp ?time]
		        [?t :trade/volume ?volume]
		        [(year ?time) ?year]
		        [(month ?time) ?month]
		        [(day ?time) ?day]]`)

	result4, _ := exec.Execute(q4)
	fmt.Println("Date       | Total Volume")
	fmt.Println("-----------|-------------")
	for i := 0; i < result4.Size(); i++ {
		tuple := result4.Get(i)
		fmt.Printf("%4d-%02d-%02d | %12.0f\n", 
			tuple[0], tuple[1], tuple[2], tuple[3])
	}

	// Test 5: Filter using time components
	fmt.Println("\nTest 5: Morning trades (before noon)")
	q5, _ := parser.ParseQuery(`
		[:find ?symbol ?time ?price
		 :where [?t :trade/symbol ?symbol]
		        [?t :trade/timestamp ?time]
		        [?t :trade/price ?price]
		        [(hour ?time) ?hour]
		        [(< ?hour 12)]]`)

	result5, _ := exec.Execute(q5)
	fmt.Println("Symbol | Time             | Price")
	fmt.Println("-------|------------------|-------")
	for i := 0; i < result5.Size(); i++ {
		tuple := result5.Get(i)
		t := tuple[1].(time.Time)
		fmt.Printf("%-6s | %s UTC | $%.2f\n", 
			tuple[0], t.UTC().Format("2006-01-02 15:04"), tuple[2])
	}
}
