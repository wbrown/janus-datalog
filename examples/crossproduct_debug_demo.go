//go:build example
// +build example

package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func main() {
	// Create database
	dbPath := "/tmp/crossproduct-debug"
	os.RemoveAll(dbPath)
	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a small dataset with just 2 symbols and 3 days of data
	fmt.Println("Creating minimal test data...")
	tx := db.NewTransaction()

	// Add two symbols
	symA := datalog.NewIdentity("symbol-A")
	symB := datalog.NewIdentity("symbol-B")

	tx.Add(symA, datalog.NewKeyword(":symbol/ticker"), "AAA")
	tx.Add(symB, datalog.NewKeyword(":symbol/ticker"), "BBB")

	// Add 3 prices for each symbol at different times
	// Opening prices (minute 570 = 9:30 AM)
	// Closing prices (minute 960 = 4:00 PM)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	
	for day := 0; day < 3; day++ {
		dayTime := baseTime.AddDate(0, 0, day)
		
		// Symbol A prices
		openPriceA := datalog.NewIdentity(fmt.Sprintf("price-A-open-%d", day))
		tx.Add(openPriceA, datalog.NewKeyword(":price/symbol"), symA)
		tx.Add(openPriceA, datalog.NewKeyword(":price/minute-of-day"), int64(570))
		tx.Add(openPriceA, datalog.NewKeyword(":price/time"), dayTime.Add(9*time.Hour + 30*time.Minute))
		tx.Add(openPriceA, datalog.NewKeyword(":price/open"), float64(100.0 + float64(day)))

		closePriceA := datalog.NewIdentity(fmt.Sprintf("price-A-close-%d", day))
		tx.Add(closePriceA, datalog.NewKeyword(":price/symbol"), symA)
		tx.Add(closePriceA, datalog.NewKeyword(":price/minute-of-day"), int64(960))
		tx.Add(closePriceA, datalog.NewKeyword(":price/time"), dayTime.Add(16 * time.Hour))
		tx.Add(closePriceA, datalog.NewKeyword(":price/close"), float64(105.0 + float64(day)))

		// Symbol B prices
		openPriceB := datalog.NewIdentity(fmt.Sprintf("price-B-open-%d", day))
		tx.Add(openPriceB, datalog.NewKeyword(":price/symbol"), symB)
		tx.Add(openPriceB, datalog.NewKeyword(":price/minute-of-day"), int64(570))
		tx.Add(openPriceB, datalog.NewKeyword(":price/time"), dayTime.Add(9*time.Hour + 30*time.Minute))
		tx.Add(openPriceB, datalog.NewKeyword(":price/open"), float64(200.0 + float64(day)))

		closePriceB := datalog.NewIdentity(fmt.Sprintf("price-B-close-%d", day))
		tx.Add(closePriceB, datalog.NewKeyword(":price/symbol"), symB)
		tx.Add(closePriceB, datalog.NewKeyword(":price/minute-of-day"), int64(960))
		tx.Add(closePriceB, datalog.NewKeyword(":price/time"), dayTime.Add(16 * time.Hour))
		tx.Add(closePriceB, datalog.NewKeyword(":price/close"), float64(205.0 + float64(day)))
	}

	_, err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Created 2 symbols with 3 days of data each (12 price records total)\n")
	fmt.Printf("Symbol A ID: %s (L85: %s)\n", symA, symA.L85())
	fmt.Printf("Symbol B ID: %s (L85: %s)\n\n", symB, symB.L85())

	// Run the problematic query
	// NOTE: This query WILL produce a cross-product because there's no constraint
	// linking ?p-open and ?p-close to the same day. The query finds all combinations
	// of open prices (3) and close prices (3) for the same symbol = 9 results.
	// To get one result per day, you'd need a constraint like a shared day entity.
	queryStr := `[:find ?open ?close ?time-open ?time-close
		  :where [?s :symbol/ticker "AAA"]
		         [?p-open :price/symbol ?s]
		         [?p-open :price/minute-of-day 570]
		         [?p-open :price/open ?open]
		         [?p-open :price/time ?time-open]
		         [?p-close :price/symbol ?s]
		         [?p-close :price/minute-of-day 960]
		         [?p-close :price/close ?close]
		         [?p-close :price/time ?time-close]]`

	fmt.Println("Running query (NOTE: WILL create cross-product due to lack of day constraint):")
	fmt.Println(queryStr)
	fmt.Println()

	// Parse query
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		log.Fatal(err)
	}

	// Create executor with annotations
	matcher := db.Matcher()
	exec := executor.NewExecutor(matcher)
	
	// Create annotation handler
	formatter := annotations.NewOutputFormatter(os.Stdout)
	handler := annotations.Handler(formatter.Handle)
	ctx := executor.NewContext(handler)

	// Execute query
	result, err := exec.ExecuteWithContext(ctx, q)
	if err != nil {
		log.Fatal(err)
	}

	// Print results
	fmt.Printf("\nQuery returned %d results:\n", result.Size())
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		timeOpen := tuple[2].(time.Time)
		timeClose := tuple[3].(time.Time)
		fmt.Printf("  ?open = %.2f, ?close = %.2f (open: %s, close: %s)\n", 
			tuple[0], tuple[1], timeOpen.Format("Jan 2"), timeClose.Format("Jan 2"))
	}

	fmt.Printf("\nExpected: 9 results (3 opens × 3 closes)\n")
	fmt.Printf("Actual: %d results\n", result.Size())
	if result.Size() == 9 {
		fmt.Println("✅ CORRECT! Query produces expected cross-product (3 opens × 3 closes = 9)")
		fmt.Println("   To avoid this, add a constraint linking open/close to same day")
	} else {
		fmt.Printf("❌ UNEXPECTED RESULT COUNT\n")
	}
}
