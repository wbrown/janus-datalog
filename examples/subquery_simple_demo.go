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
	dbPath := "/tmp/datalog-subquery-simple"
	os.RemoveAll(dbPath)

	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		log.Fatal("Failed to create database:", err)
	}
	defer db.Close()

	// Add test data
	fmt.Println("Loading test data...")
	
	// Add symbols
	tx := db.NewTransaction()
	aapl := datalog.NewIdentity("symbol:aapl")
	goog := datalog.NewIdentity("symbol:goog")
	
	tx.Add(aapl, datalog.NewKeyword(":symbol/ticker"), "AAPL")
	tx.Add(goog, datalog.NewKeyword(":symbol/ticker"), "GOOG")
	
	_, err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit symbols:", err)
	}
	
	// Add simple price data - 3 days, 2 prices per day
	baseTime := time.Date(2024, 1, 1, 9, 30, 0, 0, time.UTC)
	
	tx = db.NewTransaction()
	
	// AAPL prices
	// Day 1
	tx.Add(datalog.NewIdentity("price:aapl:1:1"), datalog.NewKeyword(":price/symbol"), aapl)
	tx.Add(datalog.NewIdentity("price:aapl:1:1"), datalog.NewKeyword(":price/time"), baseTime)
	tx.Add(datalog.NewIdentity("price:aapl:1:1"), datalog.NewKeyword(":price/value"), 150.0)
	
	tx.Add(datalog.NewIdentity("price:aapl:1:2"), datalog.NewKeyword(":price/symbol"), aapl)
	tx.Add(datalog.NewIdentity("price:aapl:1:2"), datalog.NewKeyword(":price/time"), baseTime.Add(4*time.Hour))
	tx.Add(datalog.NewIdentity("price:aapl:1:2"), datalog.NewKeyword(":price/value"), 152.0)
	
	// Day 2
	tx.Add(datalog.NewIdentity("price:aapl:2:1"), datalog.NewKeyword(":price/symbol"), aapl)
	tx.Add(datalog.NewIdentity("price:aapl:2:1"), datalog.NewKeyword(":price/time"), baseTime.Add(24*time.Hour))
	tx.Add(datalog.NewIdentity("price:aapl:2:1"), datalog.NewKeyword(":price/value"), 153.0)
	
	tx.Add(datalog.NewIdentity("price:aapl:2:2"), datalog.NewKeyword(":price/symbol"), aapl)
	tx.Add(datalog.NewIdentity("price:aapl:2:2"), datalog.NewKeyword(":price/time"), baseTime.Add(24*time.Hour + 4*time.Hour))
	tx.Add(datalog.NewIdentity("price:aapl:2:2"), datalog.NewKeyword(":price/value"), 155.0)
	
	// Day 3
	tx.Add(datalog.NewIdentity("price:aapl:3:1"), datalog.NewKeyword(":price/symbol"), aapl)
	tx.Add(datalog.NewIdentity("price:aapl:3:1"), datalog.NewKeyword(":price/time"), baseTime.Add(48*time.Hour))
	tx.Add(datalog.NewIdentity("price:aapl:3:1"), datalog.NewKeyword(":price/value"), 154.0)
	
	tx.Add(datalog.NewIdentity("price:aapl:3:2"), datalog.NewKeyword(":price/symbol"), aapl)
	tx.Add(datalog.NewIdentity("price:aapl:3:2"), datalog.NewKeyword(":price/time"), baseTime.Add(48*time.Hour + 4*time.Hour))
	tx.Add(datalog.NewIdentity("price:aapl:3:2"), datalog.NewKeyword(":price/value"), 156.0)
	
	// GOOG prices
	// Day 1
	tx.Add(datalog.NewIdentity("price:goog:1:1"), datalog.NewKeyword(":price/symbol"), goog)
	tx.Add(datalog.NewIdentity("price:goog:1:1"), datalog.NewKeyword(":price/time"), baseTime)
	tx.Add(datalog.NewIdentity("price:goog:1:1"), datalog.NewKeyword(":price/value"), 2800.0)
	
	tx.Add(datalog.NewIdentity("price:goog:1:2"), datalog.NewKeyword(":price/symbol"), goog)
	tx.Add(datalog.NewIdentity("price:goog:1:2"), datalog.NewKeyword(":price/time"), baseTime.Add(4*time.Hour))
	tx.Add(datalog.NewIdentity("price:goog:1:2"), datalog.NewKeyword(":price/value"), 2810.0)
	
	// Day 2
	tx.Add(datalog.NewIdentity("price:goog:2:1"), datalog.NewKeyword(":price/symbol"), goog)
	tx.Add(datalog.NewIdentity("price:goog:2:1"), datalog.NewKeyword(":price/time"), baseTime.Add(24*time.Hour))
	tx.Add(datalog.NewIdentity("price:goog:2:1"), datalog.NewKeyword(":price/value"), 2820.0)
	
	tx.Add(datalog.NewIdentity("price:goog:2:2"), datalog.NewKeyword(":price/symbol"), goog)
	tx.Add(datalog.NewIdentity("price:goog:2:2"), datalog.NewKeyword(":price/time"), baseTime.Add(24*time.Hour + 4*time.Hour))
	tx.Add(datalog.NewIdentity("price:goog:2:2"), datalog.NewKeyword(":price/value"), 2830.0)
	
	// Day 3
	tx.Add(datalog.NewIdentity("price:goog:3:1"), datalog.NewKeyword(":price/symbol"), goog)
	tx.Add(datalog.NewIdentity("price:goog:3:1"), datalog.NewKeyword(":price/time"), baseTime.Add(48*time.Hour))
	tx.Add(datalog.NewIdentity("price:goog:3:1"), datalog.NewKeyword(":price/value"), 2825.0)
	
	tx.Add(datalog.NewIdentity("price:goog:3:2"), datalog.NewKeyword(":price/symbol"), goog)
	tx.Add(datalog.NewIdentity("price:goog:3:2"), datalog.NewKeyword(":price/time"), baseTime.Add(48*time.Hour + 4*time.Hour))
	tx.Add(datalog.NewIdentity("price:goog:3:2"), datalog.NewKeyword(":price/value"), 2840.0)
	
	_, err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit prices:", err)
	}
	
	fmt.Println("Data loaded successfully!")
	fmt.Println()

	exec := executor.NewExecutor(db.Matcher())

	// Demo 1: Find max price per symbol using subqueries
	fmt.Println("Demo 1: Max price per symbol using subqueries")
	fmt.Println("=============================================")
	
	query1 := `[:find ?ticker ?max-price
	           :where
	           [?s :symbol/ticker ?ticker]
	           [(q [:find (max ?v)
	                :in $ ?sym
	                :where [?p :price/symbol ?sym]
	                       [?p :price/value ?v]]
	               ?s) [[?max-price]]]]`

	q1, err := parser.ParseQuery(query1)
	if err != nil {
		log.Fatal("Failed to parse query 1:", err)
	}

	fmt.Println("\nQuery:")
	fmt.Println(parser.FormatQuery(q1))

	result1, err := exec.Execute(q1)
	if err != nil {
		log.Fatal("Failed to execute query 1:", err)
	}

	fmt.Printf("\nResults:\n")
	executor.PrintResult(result1)

	// Demo 2: Find daily max using subqueries with date filtering
	fmt.Println("\n\nDemo 2: Daily max prices using subqueries")
	fmt.Println("=========================================")
	
	// First, let's verify what dates we have
	verifyQuery := `[:find ?ticker ?time
	                 :where
	                 [?s :symbol/ticker ?ticker]
	                 [?p :price/symbol ?s]
	                 [?p :price/time ?time]]`
	
	qv, _ := parser.ParseQuery(verifyQuery)
	resultv, _ := exec.Execute(qv)
	
	// Get unique dates
	dateSet := make(map[string]map[string]bool)
	for i := 0; i < resultv.Size(); i++ {
		tuple := resultv.Get(i)
		ticker := tuple[0].(string)
		t := tuple[1].(time.Time)
		dateStr := fmt.Sprintf("%d-%02d-%02d", t.Year(), t.Month(), t.Day())
		
		if dateSet[ticker] == nil {
			dateSet[ticker] = make(map[string]bool)
		}
		dateSet[ticker][dateStr] = true
	}
	
	fmt.Println("\nData verification - unique dates per symbol:")
	for ticker, dates := range dateSet {
		fmt.Printf("%s: ", ticker)
		first := true
		for date := range dates {
			if !first {
				fmt.Print(", ")
			}
			fmt.Print(date)
			first = false
		}
		fmt.Println()
	}
	
	// Now run the daily max query for a specific date
	query2 := `[:find ?ticker ?max-price
	           :where
	           [?s :symbol/ticker ?ticker]
	           [(q [:find (max ?v)
	                :in $ ?sym
	                :where [?p :price/symbol ?sym]
	                       [?p :price/time ?t]
	                       [(year ?t) ?y]
	                       [(month ?t) ?m]
	                       [(day ?t) ?d]
	                       [(= ?y 2024)]
	                       [(= ?m 1)]
	                       [(= ?d 2)]
	                       [?p :price/value ?v]]
	               ?s) [[?max-price]]]]`

	q2, err := parser.ParseQuery(query2)
	if err != nil {
		log.Fatal("Failed to parse query 2:", err)
	}

	fmt.Println("\nQuery:")
	fmt.Println(parser.FormatQuery(q2))

	result2, err := exec.Execute(q2)
	if err != nil {
		log.Fatal("Failed to execute query 2:", err)
	}

	fmt.Printf("\nMax prices for 2024-01-02:\n")
	executor.PrintResult(result2)

	// Demo 3: Relation binding - get all prices for a symbol
	fmt.Println("\n\nDemo 3: All prices using relation binding")
	fmt.Println("=========================================")
	
	query3 := `[:find ?ticker ?time ?price
	           :where
	           [?s :symbol/ticker ?ticker]
	           [(= ?ticker "AAPL")]
	           [(q [:find ?t ?v
	                :in $ ?sym
	                :where [?p :price/symbol ?sym]
	                       [?p :price/time ?t]
	                       [?p :price/value ?v]]
	               ?s) [[?time ?price] ...]]]`

	q3, err := parser.ParseQuery(query3)
	if err != nil {
		log.Fatal("Failed to parse query 3:", err)
	}

	fmt.Println("\nQuery:")
	fmt.Println(parser.FormatQuery(q3))

	result3, err := exec.Execute(q3)
	if err != nil {
		log.Fatal("Failed to execute query 3:", err)
	}

	fmt.Printf("\nAPPL prices:\n")
	executor.PrintResult(result3)
}
