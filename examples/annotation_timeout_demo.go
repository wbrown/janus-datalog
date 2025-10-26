//go:build example
// +build example

package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func main() {
	// Create a new database
	db, err := storage.NewDatabase(".")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create annotation handler
	handler := annotations.NewOutputFormatter(os.Stdout)
	
	// Create a transaction
	tx := db.NewTransaction()

	// Add some test data
	fmt.Println("Adding financial data...")
	
	// Add symbols
	symbol := datalog.NewIdentity("symbol:CRWV")
	err = tx.Add(symbol, datalog.NewKeyword(":symbol/ticker"), "CRWV")
	if err != nil {
		log.Fatal(err)
	}

	// Add price data with various times and minutes
	baseTime := int64(1640000000) // Some base timestamp
	for day := 0; day < 30; day++ {
		for minute := 570; minute <= 960; minute += 30 { // 9:30 AM to 4:00 PM
			priceEntity := datalog.NewIdentity(fmt.Sprintf("price:%d:%d", day, minute))
			
			// Price entity with all attributes
			tx.Add(priceEntity, datalog.NewKeyword(":price/symbol"), symbol)
			tx.Add(priceEntity, datalog.NewKeyword(":price/time"), baseTime + int64(day*86400 + minute*60))
			tx.Add(priceEntity, datalog.NewKeyword(":price/minute-of-day"), int64(minute))
			tx.Add(priceEntity, datalog.NewKeyword(":price/high"), 100.0 + float64(minute%10))
			tx.Add(priceEntity, datalog.NewKeyword(":price/low"), 95.0 + float64(minute%10))
			
			// Add open/close for specific minutes
			if minute == 570 {
				tx.Add(priceEntity, datalog.NewKeyword(":price/open"), 98.0)
			}
			if minute == 960 {
				tx.Add(priceEntity, datalog.NewKeyword(":price/close"), 102.0)
			}
		}
	}

	// Commit the transaction
	txID, err := tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Committed transaction %d\n", txID)

	// Parse the complex query
	queryStr := `[:find ?date (max ?high) (min ?low) ?open ?close
                 :where
                        [?s :symbol/ticker "CRWV"]
                        [?p :price/symbol ?s]
                        [?p :price/time ?time]
                        [?p :price/minute-of-day ?mod]
                        [(>= ?mod 570)]
                        [(<= ?mod 960)]
                        [(year ?time) ?year]
                        [(month ?time) ?month]
                        [(day ?time) ?day]
                        [?p :price/high ?high]
                        [?p :price/low ?low]
                        [(str ?year "-" ?month "-" ?day) ?date]
                        [(str ?year "-" ?month "-" ?day) ?date-key]
                        
                        [?p-open :price/symbol ?s]
                        [?p-open :price/time ?time-open]
                        [(str (year ?time-open) "-" (month ?time-open) "-" (day ?time-open)) ?date-key]
                        [?p-open :price/minute-of-day 570]
                        [?p-open :price/open ?open]
                        
                        [?p-close :price/symbol ?s]
                        [?p-close :price/time ?time-close]
                        [(str (year ?time-close) "-" (month ?time-close) "-" (day ?time-close)) ?date-key]
                        [?p-close :price/minute-of-day 960]
                        [?p-close :price/close ?close]]`

	// Parse query directly from string
	query, err := parser.ParseQuery(queryStr)
	if err != nil {
		log.Fatal(err)
	}

	// Create matcher with store (not transaction)
	matcher := storage.NewBadgerMatcher(db.Store())

	// Wrap matcher with annotations
	annotatedMatcher := executor.WrapMatcher(matcher, handler.Handle)

	// Execute with annotated matcher
	exec := executor.NewExecutor(annotatedMatcher)
	
	fmt.Println("\nExecuting complex financial query with annotations...")
	fmt.Println("========================================")

	result, err := exec.Execute(query)
	if err != nil {
		log.Fatal(err)
	}

	// Print results
	fmt.Printf("\nFound %d results\n", result.Size())
	iter := result.Iterator()
	count := 0
	for iter.Next() && count < 5 {
		tuple := iter.Tuple()
		fmt.Printf("Date: %v, High: %v, Low: %v, Open: %v, Close: %v\n",
			tuple[0], tuple[1], tuple[2], tuple[3], tuple[4])
		count++
	}
	if result.Size() > 5 {
		fmt.Printf("... and %d more results\n", result.Size()-5)
	}
}
