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
	// Create a database
	dbPath := "/tmp/datalog-subquery-demo"
	os.RemoveAll(dbPath)

	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		log.Fatal("Failed to create database:", err)
	}
	defer db.Close()

	// Create annotation output handler that prints events as they happen
	formatter := annotations.NewOutputFormatter(os.Stdout)
	handler := annotations.Handler(formatter.Handle)
	
	// Add some test data
	fmt.Println("Loading test data...")
	
	// Add multiple symbols
	symbols := []string{"AAPL", "GOOGL", "MSFT"}
	
	tx := db.NewTransaction()
	for _, ticker := range symbols {
		symbol := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
		tx.Add(symbol, datalog.NewKeyword(":symbol/ticker"), ticker)
		tx.Add(symbol, datalog.NewKeyword(":symbol/name"), fmt.Sprintf("%s Inc.", ticker))
	}
	
	// Commit symbols first
	_, err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit symbols:", err)
	}
	
	// Add OHLC price data
	baseTime := time.Date(2024, 1, 1, 9, 30, 0, 0, time.UTC)
	
	// Price data for 5 days with intraday data
	priceData := map[string][]struct {
		day    int
		hour   int
		minute int
		open   float64
		high   float64
		low    float64
		close  float64
		volume int64
	}{
		"AAPL": {
			// Day 1
			{0, 9, 30, 150.0, 151.5, 149.8, 151.0, 1000000},
			{0, 10, 0, 151.0, 152.0, 150.5, 151.8, 800000},
			{0, 14, 0, 151.8, 153.2, 151.5, 152.5, 600000},
			{0, 15, 30, 152.5, 153.0, 152.0, 152.8, 900000},
			// Day 2
			{1, 9, 30, 153.0, 154.0, 152.5, 153.5, 1100000},
			{1, 10, 0, 153.5, 155.0, 153.0, 154.8, 900000},
			{1, 14, 0, 154.8, 155.5, 154.5, 155.2, 700000},
			{1, 15, 30, 155.2, 155.8, 155.0, 155.5, 850000},
			// Day 3
			{2, 9, 30, 155.0, 156.0, 154.5, 155.5, 1200000},
			{2, 10, 0, 155.5, 157.0, 155.0, 156.8, 950000},
			{2, 14, 0, 156.8, 157.5, 156.5, 157.2, 800000},
			{2, 15, 30, 157.2, 157.8, 157.0, 157.5, 920000},
		},
		"GOOGL": {
			// Day 1
			{0, 9, 30, 2800.0, 2815.0, 2795.0, 2810.0, 500000},
			{0, 10, 0, 2810.0, 2820.0, 2805.0, 2815.0, 400000},
			{0, 14, 0, 2815.0, 2825.0, 2810.0, 2820.0, 300000},
			{0, 15, 30, 2820.0, 2830.0, 2815.0, 2825.0, 450000},
			// Day 2
			{1, 9, 30, 2830.0, 2840.0, 2825.0, 2835.0, 550000},
			{1, 10, 0, 2835.0, 2845.0, 2830.0, 2840.0, 420000},
			{1, 14, 0, 2840.0, 2850.0, 2835.0, 2845.0, 350000},
			{1, 15, 30, 2845.0, 2855.0, 2840.0, 2850.0, 480000},
			// Day 3
			{2, 9, 30, 2850.0, 2860.0, 2845.0, 2855.0, 600000},
			{2, 10, 0, 2855.0, 2865.0, 2850.0, 2860.0, 450000},
			{2, 14, 0, 2860.0, 2870.0, 2855.0, 2865.0, 380000},
			{2, 15, 30, 2865.0, 2875.0, 2860.0, 2870.0, 520000},
		},
		"MSFT": {
			// Day 1
			{0, 9, 30, 380.0, 382.0, 379.5, 381.5, 800000},
			{0, 10, 0, 381.5, 383.0, 381.0, 382.5, 700000},
			{0, 14, 0, 382.5, 384.0, 382.0, 383.5, 600000},
			{0, 15, 30, 383.5, 384.5, 383.0, 384.0, 750000},
			// Day 2
			{1, 9, 30, 384.5, 386.0, 384.0, 385.5, 850000},
			{1, 10, 0, 385.5, 387.0, 385.0, 386.5, 720000},
			{1, 14, 0, 386.5, 388.0, 386.0, 387.5, 650000},
			{1, 15, 30, 387.5, 388.5, 387.0, 388.0, 780000},
			// Day 3
			{2, 9, 30, 388.0, 390.0, 387.5, 389.5, 900000},
			{2, 10, 0, 389.5, 391.0, 389.0, 390.5, 750000},
			{2, 14, 0, 390.5, 392.0, 390.0, 391.5, 680000},
			{2, 15, 30, 391.5, 392.5, 391.0, 392.0, 820000},
		},
	}
	
	tx = db.NewTransaction()
	for ticker, prices := range priceData {
		symbol := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
		
		for _, price := range prices {
			priceEntity := datalog.NewIdentity(fmt.Sprintf("price:%s:%d:%d:%d", ticker, price.day, price.hour, price.minute))
			priceTime := baseTime.Add(time.Duration(price.day)*24*time.Hour + 
				time.Duration(price.hour-9)*time.Hour + 
				time.Duration(price.minute)*time.Minute)
			
			tx.Add(priceEntity, datalog.NewKeyword(":price/symbol"), symbol)
			tx.Add(priceEntity, datalog.NewKeyword(":price/time"), priceTime)
			tx.Add(priceEntity, datalog.NewKeyword(":price/open"), price.open)
			tx.Add(priceEntity, datalog.NewKeyword(":price/high"), price.high)
			tx.Add(priceEntity, datalog.NewKeyword(":price/low"), price.low)
			tx.Add(priceEntity, datalog.NewKeyword(":price/close"), price.close)
			tx.Add(priceEntity, datalog.NewKeyword(":price/volume"), price.volume)
		}
	}
	
	// Commit price data
	_, err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit prices:", err)
	}
	
	fmt.Println("Data loaded successfully!")
	fmt.Println()

	// Demo 1: Find daily OHLC using subqueries
	fmt.Println("Demo 1: Daily OHLC using subqueries")
	fmt.Println("====================================")
	
	query1 := `[:find ?ticker ?year ?month ?day ?daily-high ?daily-low ?daily-volume
	           :where
	           [?s :symbol/ticker ?ticker]
	           [?p :price/symbol ?s]
	           [?p :price/time ?time]
	           [(year ?time) ?year]
	           [(month ?time) ?month]
	           [(day ?time) ?day]
	           
	           ; Find daily high using subquery
	           [(q [:find (max ?h)
	                :in $ ?sym ?y ?m ?d
	                :where [?p :price/symbol ?sym]
	                       [?p :price/time ?t]
	                       [(year ?t) ?py]
	                       [(month ?t) ?pm]
	                       [(day ?t) ?pd]
	                       [(= ?py ?y)]
	                       [(= ?pm ?m)]
	                       [(= ?pd ?d)]
	                       [?p :price/high ?h]]
	               ?s ?year ?month ?day) [[?daily-high]]]
	           
	           ; Find daily low using subquery
	           [(q [:find (min ?l)
	                :in $ ?sym ?y ?m ?d
	                :where [?p :price/symbol ?sym]
	                       [?p :price/time ?t]
	                       [(year ?t) ?py]
	                       [(month ?t) ?pm]
	                       [(day ?t) ?pd]
	                       [(= ?py ?y)]
	                       [(= ?pm ?m)]
	                       [(= ?pd ?d)]
	                       [?p :price/low ?l]]
	               ?s ?year ?month ?day) [[?daily-low]]]
	               
	           ; Find daily volume using subquery
	           [(q [:find (sum ?v)
	                :in $ ?sym ?y ?m ?d
	                :where [?p :price/symbol ?sym]
	                       [?p :price/time ?t]
	                       [(year ?t) ?py]
	                       [(month ?t) ?pm]
	                       [(day ?t) ?pd]
	                       [(= ?py ?y)]
	                       [(= ?pm ?m)]
	                       [(= ?pd ?d)]
	                       [?p :price/volume ?v]]
	               ?s ?year ?month ?day) [[?daily-volume]]]]`

	q1, err := parser.ParseQuery(query1)
	if err != nil {
		log.Fatal("Failed to parse query 1:", err)
	}

	fmt.Println("\nQuery:")
	fmt.Println(parser.FormatQuery(q1))

	exec := executor.NewExecutor(db.Matcher())
	ctx := executor.NewContext(handler)

	start := time.Now()
	result1, err := exec.ExecuteWithContext(ctx, q1)
	elapsed := time.Since(start)

	if err != nil {
		log.Fatal("Failed to execute query 1:", err)
	}

	fmt.Printf("\nQuery completed in %v\n", elapsed)
	fmt.Printf("\nResults:\n")
	executor.PrintResult(result1)

	// Demo 2: Find stocks with highest intraday volatility using subqueries
	fmt.Println("\n\nDemo 2: Intraday volatility analysis using subqueries")
	fmt.Println("=====================================================")
	
	query2 := `[:find ?ticker ?date-str ?volatility
	           :where
	           [?s :symbol/ticker ?ticker]
	           
	           ; Get all unique dates for this symbol
	           [?p :price/symbol ?s]
	           [?p :price/time ?time]
	           [(year ?time) ?year]
	           [(month ?time) ?month]
	           [(day ?time) ?day]
	           [(str ?year "-" ?month "-" ?day) ?date-str]
	           
	           ; Calculate intraday high
	           [(q [:find (max ?h)
	                :in $ ?sym ?y ?m ?d
	                :where [?p :price/symbol ?sym]
	                       [?p :price/time ?t]
	                       [(year ?t) ?py]
	                       [(month ?t) ?pm]
	                       [(day ?t) ?pd]
	                       [(= ?py ?y)]
	                       [(= ?pm ?m)]
	                       [(= ?pd ?d)]
	                       [?p :price/high ?h]]
	               ?s ?year ?month ?day) [[?daily-high]]]
	           
	           ; Calculate intraday low
	           [(q [:find (min ?l)
	                :in $ ?sym ?y ?m ?d
	                :where [?p :price/symbol ?sym]
	                       [?p :price/time ?t]
	                       [(year ?t) ?py]
	                       [(month ?t) ?pm]
	                       [(day ?t) ?pd]
	                       [(= ?py ?y)]
	                       [(= ?pm ?m)]
	                       [(= ?pd ?d)]
	                       [?p :price/low ?l]]
	               ?s ?year ?month ?day) [[?daily-low]]]
	               
	           ; Calculate volatility as percentage
	           [(- ?daily-high ?daily-low) ?range]
	           [(/ ?range ?daily-low) ?volatility-ratio]
	           [(* ?volatility-ratio 100.0) ?volatility]]`

	q2, err := parser.ParseQuery(query2)
	if err != nil {
		log.Fatal("Failed to parse query 2:", err)
	}

	fmt.Println("\nQuery:")
	fmt.Println(parser.FormatQuery(q2))

	start = time.Now()
	result2, err := exec.ExecuteWithContext(ctx, q2)
	elapsed = time.Since(start)

	if err != nil {
		log.Fatal("Failed to execute query 2:", err)
	}

	fmt.Printf("\nQuery completed in %v\n", elapsed)
	fmt.Printf("\nResults:\n")
	executor.PrintResult(result2)

	// Demo 3: Find all high prices for AAPL using relation binding
	fmt.Println("\n\nDemo 3: All AAPL high prices using relation binding")
	fmt.Println("===================================================")
	
	query3 := `[:find ?ticker ?time ?high
	           :where
	           [?s :symbol/ticker ?ticker]
	           [(= ?ticker "AAPL")]
	           [(q [:find ?t ?h
	                :in $ ?sym
	                :where [?p :price/symbol ?sym]
	                       [?p :price/time ?t]
	                       [?p :price/high ?h]]
	               ?s) [[?time ?high] ...]]]`

	q3, err := parser.ParseQuery(query3)
	if err != nil {
		log.Fatal("Failed to parse query 3:", err)
	}

	fmt.Println("\nQuery:")
	fmt.Println(parser.FormatQuery(q3))

	start = time.Now()
	result3, err := exec.ExecuteWithContext(ctx, q3)
	elapsed = time.Since(start)

	if err != nil {
		log.Fatal("Failed to execute query 3:", err)
	}

	fmt.Printf("\nQuery completed in %v\n", elapsed)
	fmt.Printf("\nResults:\n")
	executor.PrintResult(result3)
}
