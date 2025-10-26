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
	dbPath := "/tmp/datalog-subquery-proper"
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
	msft := datalog.NewIdentity("symbol:msft")
	
	tx.Add(aapl, datalog.NewKeyword(":symbol/ticker"), "AAPL")
	tx.Add(goog, datalog.NewKeyword(":symbol/ticker"), "GOOG")
	tx.Add(msft, datalog.NewKeyword(":symbol/ticker"), "MSFT")
	
	_, err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit symbols:", err)
	}
	
	// Add OHLC price data - proper daily OHLC bars
	baseTime := time.Date(2024, 1, 1, 16, 0, 0, 0, time.UTC) // Market close
	
	tx = db.NewTransaction()
	
	// Daily bars for 3 days
	dailyData := []struct {
		symbol string
		day    int
		open   float64
		high   float64
		low    float64
		close  float64
		volume int64
	}{
		// AAPL
		{"AAPL", 0, 150.0, 152.5, 149.5, 151.0, 10000000},
		{"AAPL", 1, 151.5, 155.0, 151.0, 154.5, 12000000},
		{"AAPL", 2, 154.0, 156.0, 153.5, 155.5, 11000000},
		// GOOG
		{"GOOG", 0, 2800.0, 2820.0, 2795.0, 2810.0, 5000000},
		{"GOOG", 1, 2815.0, 2850.0, 2810.0, 2845.0, 5500000},
		{"GOOG", 2, 2840.0, 2875.0, 2835.0, 2870.0, 6000000},
		// MSFT
		{"MSFT", 0, 380.0, 384.0, 379.0, 383.0, 8000000},
		{"MSFT", 1, 383.5, 388.0, 383.0, 387.0, 8500000},
		{"MSFT", 2, 387.5, 392.0, 387.0, 391.0, 9000000},
	}
	
	for _, bar := range dailyData {
		barId := datalog.NewIdentity(fmt.Sprintf("bar:%s:%d", bar.symbol, bar.day))
		barTime := baseTime.Add(time.Duration(bar.day) * 24 * time.Hour)
		
		var symbolId datalog.Identity
		switch bar.symbol {
		case "AAPL":
			symbolId = aapl
		case "GOOG":
			symbolId = goog
		case "MSFT":
			symbolId = msft
		}
		
		tx.Add(barId, datalog.NewKeyword(":bar/symbol"), symbolId)
		tx.Add(barId, datalog.NewKeyword(":bar/time"), barTime)
		tx.Add(barId, datalog.NewKeyword(":bar/open"), bar.open)
		tx.Add(barId, datalog.NewKeyword(":bar/high"), bar.high)
		tx.Add(barId, datalog.NewKeyword(":bar/low"), bar.low)
		tx.Add(barId, datalog.NewKeyword(":bar/close"), bar.close)
		tx.Add(barId, datalog.NewKeyword(":bar/volume"), bar.volume)
	}
	
	_, err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit bars:", err)
	}
	
	fmt.Println("Data loaded successfully!")
	fmt.Println()

	exec := executor.NewExecutor(db.Matcher())

	// Demo 1: Find the highest closing price for each symbol
	fmt.Println("Demo 1: Highest closing price per symbol")
	fmt.Println("========================================")
	
	query1 := `[:find ?ticker ?max-close
	           :where
	           [?s :symbol/ticker ?ticker]
	           [(q [:find (max ?close)
	                :in $ ?sym
	                :where [?bar :bar/symbol ?sym]
	                       [?bar :bar/close ?close]]
	               ?s) [[?max-close]]]]`

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

	// Demo 2: Find average daily volume for each symbol
	fmt.Println("\n\nDemo 2: Average daily volume per symbol")
	fmt.Println("=======================================")
	
	query2 := `[:find ?ticker ?avg-volume
	           :where
	           [?s :symbol/ticker ?ticker]
	           [(q [:find (avg ?vol)
	                :in $ ?sym
	                :where [?bar :bar/symbol ?sym]
	                       [?bar :bar/volume ?vol]]
	               ?s) [[?avg-volume]]]]`

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

	fmt.Printf("\nResults:\n")
	executor.PrintResult(result2)

	// Demo 3: Find trading days where close > open (bullish days) per symbol
	fmt.Println("\n\nDemo 3: Count of bullish days per symbol")
	fmt.Println("=========================================")
	
	query3 := `[:find ?ticker ?bullish-days
	           :where
	           [?s :symbol/ticker ?ticker]
	           [(q [:find (count ?bar)
	                :in $ ?sym
	                :where [?bar :bar/symbol ?sym]
	                       [?bar :bar/open ?open]
	                       [?bar :bar/close ?close]
	                       [(> ?close ?open)]]
	               ?s) [[?bullish-days]]]]`

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

	fmt.Printf("\nResults:\n")
	executor.PrintResult(result3)

	// Demo 4: Get all bars for AAPL using relation binding
	fmt.Println("\n\nDemo 4: All AAPL bars (using relation binding)")
	fmt.Println("===============================================")
	
	query4 := `[:find ?ticker ?date ?high ?low
	           :where
	           [?s :symbol/ticker ?ticker]
	           [(= ?ticker "AAPL")]
	           [(q [:find ?t ?h ?l
	                :in $ ?sym
	                :where [?bar :bar/symbol ?sym]
	                       [?bar :bar/time ?t]
	                       [?bar :bar/high ?h]
	                       [?bar :bar/low ?l]]
	               ?s) [[?date ?high ?low] ...]]]`

	q4, err := parser.ParseQuery(query4)
	if err != nil {
		log.Fatal("Failed to parse query 4:", err)
	}

	fmt.Println("\nQuery:")
	fmt.Println(parser.FormatQuery(q4))

	result4, err := exec.Execute(q4)
	if err != nil {
		log.Fatal("Failed to execute query 4:", err)
	}

	fmt.Printf("\nResults:\n")
	executor.PrintResult(result4)

	// Demo 5: Count high-volume bars
	fmt.Println("\n\nDemo 5: High-volume bars per symbol")
	fmt.Println("===================================")
	
	query5 := `[:find ?ticker (count ?bar)
	           :where
	           [?s :symbol/ticker ?ticker]
	           [?bar :bar/symbol ?s]
	           [?bar :bar/volume ?v]
	           [(> ?v 8000000)]]`

	q5, err := parser.ParseQuery(query5)
	if err != nil {
		log.Fatal("Failed to parse query 5:", err)
	}

	fmt.Println("\nQuery:")
	fmt.Println(parser.FormatQuery(q5))

	result5, err := exec.Execute(q5)
	if err != nil {
		log.Fatal("Failed to execute query 5:", err)
	}

	fmt.Printf("\nResults (bars with volume > 8M):\n")
	executor.PrintResult(result5)

	// Demo 6: Combining aggregations with non-aggregated values
	// This demonstrates the fix for the bug in datalog_aggregation_bug_report.md
	fmt.Println("\n\nDemo 6: Daily OHLC with proper aggregation (bug fix demonstration)")
	fmt.Println("==================================================================")
	fmt.Println("This query demonstrates how subqueries solve the Cartesian product")
	fmt.Println("problem when combining aggregated and non-aggregated values.")
	
	// First add some intraday data to make it more realistic
	tx = db.NewTransaction()
	
	// Add minute-of-day attribute to the first and last bars of each day
	// 9:30 AM = 570 minutes from midnight, 4:00 PM = 960 minutes
	for _, bar := range dailyData {
		if bar.symbol == "AAPL" { // Add intraday data for all AAPL days
			// Morning bar (open)
			morningBar := datalog.NewIdentity(fmt.Sprintf("bar:%s:%d:morning", bar.symbol, bar.day))
			tx.Add(morningBar, datalog.NewKeyword(":bar/symbol"), aapl)
			tx.Add(morningBar, datalog.NewKeyword(":bar/time"), baseTime.Add(time.Duration(bar.day) * 24 * time.Hour))
			tx.Add(morningBar, datalog.NewKeyword(":bar/minute-of-day"), int64(570)) // 9:30 AM
			tx.Add(morningBar, datalog.NewKeyword(":bar/open"), bar.open)
			tx.Add(morningBar, datalog.NewKeyword(":bar/high"), bar.open + 1.0)
			tx.Add(morningBar, datalog.NewKeyword(":bar/low"), bar.open - 0.5)
			tx.Add(morningBar, datalog.NewKeyword(":bar/close"), bar.open + 0.8)
			
			// Midday bar
			middayBar := datalog.NewIdentity(fmt.Sprintf("bar:%s:%d:midday", bar.symbol, bar.day))
			tx.Add(middayBar, datalog.NewKeyword(":bar/symbol"), aapl)
			tx.Add(middayBar, datalog.NewKeyword(":bar/time"), baseTime.Add(time.Duration(bar.day) * 24 * time.Hour))
			tx.Add(middayBar, datalog.NewKeyword(":bar/minute-of-day"), int64(720)) // 12:00 PM
			tx.Add(middayBar, datalog.NewKeyword(":bar/open"), bar.low + 2.0)
			tx.Add(middayBar, datalog.NewKeyword(":bar/high"), bar.high) // Daily high happens midday
			tx.Add(middayBar, datalog.NewKeyword(":bar/low"), bar.low)   // Daily low also happens midday
			tx.Add(middayBar, datalog.NewKeyword(":bar/close"), bar.high - 0.5)
			
			// Afternoon bar (close)
			afternoonBar := datalog.NewIdentity(fmt.Sprintf("bar:%s:%d:afternoon", bar.symbol, bar.day))
			tx.Add(afternoonBar, datalog.NewKeyword(":bar/symbol"), aapl)
			tx.Add(afternoonBar, datalog.NewKeyword(":bar/time"), baseTime.Add(time.Duration(bar.day) * 24 * time.Hour))
			tx.Add(afternoonBar, datalog.NewKeyword(":bar/minute-of-day"), int64(960)) // 4:00 PM
			tx.Add(afternoonBar, datalog.NewKeyword(":bar/open"), bar.close - 0.5)
			tx.Add(afternoonBar, datalog.NewKeyword(":bar/high"), bar.close + 0.2)
			tx.Add(afternoonBar, datalog.NewKeyword(":bar/low"), bar.close - 0.3)
			tx.Add(afternoonBar, datalog.NewKeyword(":bar/close"), bar.close)
		}
	}
	
	_, err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit intraday data:", err)
	}
	
	query6 := `[:find ?date ?daily-high ?daily-low ?open-price ?close-price
	           :where
	           [?s :symbol/ticker "AAPL"]
	           
	           ; Get distinct dates by using the morning bar as our anchor
	           ; This avoids duplicates from multiple bars per day
	           [?morning-bar :bar/symbol ?s]
	           [?morning-bar :bar/minute-of-day 570]
	           [?morning-bar :bar/time ?t]
	           [(year ?t) ?year]
	           [(month ?t) ?month]
	           [(day ?t) ?day]
	           [(str ?year "-" ?month "-" ?day) ?date]
	           
	           ; Daily high/low using subquery (aggregation)
	           [(q [:find (max ?h) (min ?l)
	                :in $ ?sym ?y ?m ?d
	                :where [?b :bar/symbol ?sym]
	                       [?b :bar/time ?time]
	                       [(year ?time) ?py]
	                       [(month ?time) ?pm]  
	                       [(day ?time) ?pd]
	                       [(= ?py ?y)]
	                       [(= ?pm ?m)]
	                       [(= ?pd ?d)]
	                       [?b :bar/high ?h]
	                       [?b :bar/low ?l]]
	               ?s ?year ?month ?day) [[?daily-high ?daily-low]]]
	           
	           ; Open price from 9:30 AM bar using subquery
	           [(q [:find ?o
	                :in $ ?sym ?y ?m ?d
	                :where [?b :bar/symbol ?sym]
	                       [?b :bar/time ?time]
	                       [(year ?time) ?py]
	                       [(month ?time) ?pm]
	                       [(day ?time) ?pd]
	                       [(= ?py ?y)]
	                       [(= ?pm ?m)]
	                       [(= ?pd ?d)]
	                       [?b :bar/minute-of-day 570]
	                       [?b :bar/open ?o]]
	               ?s ?year ?month ?day) [[?open-price]]]
	           
	           ; Close price from 4:00 PM bar using subquery
	           [(q [:find ?c
	                :in $ ?sym ?y ?m ?d
	                :where [?b :bar/symbol ?sym]
	                       [?b :bar/time ?time]
	                       [(year ?time) ?py]
	                       [(month ?time) ?pm]
	                       [(day ?time) ?pd]
	                       [(= ?py ?y)]
	                       [(= ?pm ?m)]
	                       [(= ?pd ?d)]
	                       [?b :bar/minute-of-day 960]
	                       [?b :bar/close ?c]]
	               ?s ?year ?month ?day) [[?close-price]]]
	           :order-by [?date]]`

	q6, err := parser.ParseQuery(query6)
	if err != nil {
		log.Fatal("Failed to parse query 6:", err)
	}

	fmt.Println("\nQuery:")
	fmt.Println(parser.FormatQuery(q6))

	result6, err := exec.Execute(q6)
	if err != nil {
		log.Fatal("Failed to execute query 6:", err)
	}

	fmt.Printf("\nResults (3 rows, one per day with correct OHLC data, sorted by date):\n")
	executor.PrintResult(result6)

	fmt.Println("\nWithout subqueries, this query would produce 3 days × multiple bars per day")
	fmt.Println("= many incorrect rows due to Cartesian product!")

	fmt.Println("\nNote: Without subqueries, combining aggregated values (max/min) with")
	fmt.Println("non-aggregated values (specific open/close) in a single query would")
	fmt.Println("produce a Cartesian product with many incorrect rows, as described in")
	fmt.Println("the bug report. Subqueries properly scope each aggregation and lookup!")
	fmt.Println("\nThis solves the issue reported in ../gopher-street/datalog_aggregation_bug_report.md")
}
