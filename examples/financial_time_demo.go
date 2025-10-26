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
	dbPath := "/tmp/datalog-financial"
	os.RemoveAll(dbPath)

	// Use time-based transaction IDs for financial data
	db, err := storage.NewDatabaseWithTimeTx(dbPath)
	if err != nil {
		log.Fatal("Failed to create database:", err)
	}
	defer db.Close()

	// Security entities
	aapl := datalog.NewIdentity("security:AAPL")
	googl := datalog.NewIdentity("security:GOOGL")

	// Add security metadata (current time is fine for this)
	tx := db.NewTransaction()
	tx.Add(aapl, datalog.NewKeyword(":security/ticker"), "AAPL")
	tx.Add(aapl, datalog.NewKeyword(":security/name"), "Apple Inc.")
	tx.Add(googl, datalog.NewKeyword(":security/ticker"), "GOOGL")
	tx.Add(googl, datalog.NewKeyword(":security/name"), "Alphabet Inc.")
	tx.Commit()

	// Now add historical price data with specific timestamps
	
	// Market close on 2024-01-02
	marketClose1 := time.Date(2024, 1, 2, 16, 0, 0, 0, time.UTC)
	tx1 := db.NewTransaction()
	tx1.SetTime(marketClose1)
	tx1.Add(aapl, datalog.NewKeyword(":price/close"), 185.64)
	tx1.Add(aapl, datalog.NewKeyword(":price/volume"), int64(82488700))
	tx1.Add(googl, datalog.NewKeyword(":price/close"), 138.17)
	tx1.Add(googl, datalog.NewKeyword(":price/volume"), int64(23738400))
	txID1, _ := tx1.Commit()
	fmt.Printf("Added prices for %s (Tx: %d)\n", marketClose1.Format("2006-01-02"), txID1)

	// Market close on 2024-01-03
	marketClose2 := time.Date(2024, 1, 3, 16, 0, 0, 0, time.UTC)
	tx2 := db.NewTransaction()
	tx2.SetTime(marketClose2)
	tx2.Add(aapl, datalog.NewKeyword(":price/close"), 184.25)
	tx2.Add(aapl, datalog.NewKeyword(":price/volume"), int64(58414500))
	tx2.Add(googl, datalog.NewKeyword(":price/close"), 139.52)
	tx2.Add(googl, datalog.NewKeyword(":price/volume"), int64(20813700))
	txID2, _ := tx2.Commit()
	fmt.Printf("Added prices for %s (Tx: %d)\n", marketClose2.Format("2006-01-02"), txID2)

	// Market close on 2024-01-04
	marketClose3 := time.Date(2024, 1, 4, 16, 0, 0, 0, time.UTC)
	tx3 := db.NewTransaction()
	tx3.SetTime(marketClose3)
	tx3.Add(aapl, datalog.NewKeyword(":price/close"), 181.91)
	tx3.Add(aapl, datalog.NewKeyword(":price/volume"), int64(71885400))
	tx3.Add(googl, datalog.NewKeyword(":price/close"), 140.31)
	tx3.Add(googl, datalog.NewKeyword(":price/volume"), int64(19871300))
	txID3, _ := tx3.Commit()
	fmt.Printf("Added prices for %s (Tx: %d)\n", marketClose3.Format("2006-01-02"), txID3)

	fmt.Println("\nNote: Transaction IDs are Unix nanosecond timestamps")
	fmt.Printf("  2024-01-02 16:00 UTC = %d\n", marketClose1.UnixNano())
	fmt.Printf("  2024-01-03 16:00 UTC = %d\n", marketClose2.UnixNano())
	fmt.Printf("  2024-01-04 16:00 UTC = %d\n", marketClose3.UnixNano())

	// Query: Get all price data with timestamps
	fmt.Println("\nAll price data with timestamps:")
	query1 := `[:find ?ticker ?price ?tx
	            :where [?s :security/ticker ?ticker]
	                   [?s :price/close ?price ?tx]]`

	q1, _ := parser.ParseQuery(query1)
	exec := executor.NewExecutor(db.Matcher())
	result1, _ := exec.Execute(q1)

	for i := 0; i < result1.Size(); i++ {
		tuple := result1.Get(i)
		ticker := tuple[0]
		price := tuple[1]
		txNano := tuple[2].(uint64)
		txTime := time.Unix(0, int64(txNano))
		fmt.Printf("  %s: $%.2f at %s\n", ticker, price, txTime.Format("2006-01-02 15:04"))
	}

	// Query: Get prices on a specific date
	targetDate := marketClose2.UnixNano()
	fmt.Printf("\nPrices on 2024-01-03 (tx=%d):\n", targetDate)
	
	query2 := `[:find ?ticker ?price
	            :where [?s :security/ticker ?ticker]
	                   [?s :price/close ?price ?tx]
	                   [(= ?tx %d)]]`
	
	q2, _ := parser.ParseQuery(fmt.Sprintf(query2, targetDate))
	result2, _ := exec.Execute(q2)

	for i := 0; i < result2.Size(); i++ {
		tuple := result2.Get(i)
		fmt.Printf("  %s: $%.2f\n", tuple[0], tuple[1])
	}

	// Query: Find price changes over time
	fmt.Println("\nPrice changes for AAPL:")
	query3 := `[:find ?date1 ?price1 ?date2 ?price2
	            :where [?s :security/ticker "AAPL"]
	                   [?s :price/close ?price1 ?date1]
	                   [?s :price/close ?price2 ?date2]
	                   [(< ?date1 ?date2)]]`

	q3, _ := parser.ParseQuery(query3)
	result3, _ := exec.Execute(q3)

	for i := 0; i < result3.Size(); i++ {
		tuple := result3.Get(i)
		t1 := time.Unix(0, int64(tuple[0].(uint64)))
		p1 := tuple[1].(float64)
		t2 := time.Unix(0, int64(tuple[2].(uint64)))
		p2 := tuple[3].(float64)
		change := p2 - p1
		pctChange := (change / p1) * 100
		fmt.Printf("  %s ($%.2f) -> %s ($%.2f): %+.2f (%.2f%%)\n",
			t1.Format("01/02"), p1, t2.Format("01/02"), p2, change, pctChange)
	}
}
