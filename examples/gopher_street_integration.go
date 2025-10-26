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

// This example demonstrates how to use janus-datalog for financial market analysis
// with features specifically designed for the gopher-street trading system

func main() {
	// Create a time-based database for financial data
	dbPath := "/tmp/gopher-street-db"
	os.RemoveAll(dbPath)

	db, err := storage.NewDatabaseWithTimeTx(dbPath)
	if err != nil {
		log.Fatal("Failed to create database:", err)
	}
	defer db.Close()

	// Initialize reference data
	initReferenceData(db)
	
	// Load market data
	loadMarketData(db)
	
	// Create query executor
	exec := executor.NewExecutor(db.Matcher())

	// Example 1: Real-time position tracking
	fmt.Println("=== Example 1: Real-time Position Tracking ===")
	positionQuery, _ := parser.ParseQuery(`
		[:find ?symbol ?position ?avgPrice
		 :where [?p :position/symbol ?symbol]
		        [?p :position/quantity ?position]
		        [?p :position/avg-price ?avgPrice]]`)
	
	result, _ := exec.Execute(positionQuery)
	fmt.Println("Symbol | Position | Avg Price")
	fmt.Println("-------|----------|----------")
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		fmt.Printf("%-6s | %8.0f | $%8.2f\n", tuple[0], tuple[1], tuple[2])
	}

	// Example 2: P&L calculation with aggregations
	fmt.Println("\n=== Example 2: P&L Calculation ===")
	pnlQuery, err := parser.ParseQuery(`
		[:find ?symbol ?position ?avgPrice ?lastPrice ?pnl
		 :where [?p :position/symbol ?symbol]
		        [?p :position/quantity ?position]
		        [?p :position/avg-price ?avgPrice]
		        [?s :security/ticker ?symbol]
		        [?s :price/last ?lastPrice]
		        [(- ?lastPrice ?avgPrice) ?priceDiff]
		        [(* ?position ?priceDiff) ?pnl]]`)
	
	if err != nil {
		fmt.Printf("Error parsing P&L query: %v\n", err)
		return
	}
	
	result, err = exec.Execute(pnlQuery)
	if err != nil {
		fmt.Printf("Error executing P&L query: %v\n", err)
		return
	}
	fmt.Println("Symbol | Position | Avg Price | Last Price | P&L")
	fmt.Println("-------|----------|-----------|------------|----------")
	totalPnL := 0.0
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		pnl := tuple[4].(float64)
		totalPnL += pnl
		fmt.Printf("%-6s | %8.0f | $%8.2f | $%9.2f | $%8.2f\n", 
			tuple[0], tuple[1], tuple[2], tuple[3], pnl)
	}
	fmt.Printf("\nTotal P&L: $%.2f\n", totalPnL)

	// Example 3: Sector exposure analysis
	fmt.Println("\n=== Example 3: Sector Exposure Analysis ===")
	sectorQuery, _ := parser.ParseQuery(`
		[:find ?sector (sum ?exposure)
		 :where [?p :position/symbol ?symbol]
		        [?p :position/quantity ?qty]
		        [?s :security/ticker ?symbol]
		        [?s :security/sector ?sector]
		        [?s :price/last ?price]
		        [(* ?qty ?price) ?exposure]]`)
	
	result, _ = exec.Execute(sectorQuery)
	fmt.Println("Sector     | Exposure")
	fmt.Println("-----------|------------")
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		fmt.Printf("%-10s | $%10.2f\n", tuple[0], tuple[1])
	}

	// Example 4: Historical price analysis with time functions
	fmt.Println("\n=== Example 4: Daily Trading Volume by Hour ===")
	volumeQuery, _ := parser.ParseQuery(`
		[:find ?hour (avg ?volume) (count ?t)
		 :where [?t :trade/timestamp ?time]
		        [?t :trade/volume ?volume]
		        [(hour ?time) ?hour]]`)
	
	result, _ = exec.Execute(volumeQuery)
	fmt.Println("Hour | Avg Volume | Trade Count")
	fmt.Println("-----|------------|------------")
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		hour := tuple[0].(int64)
		avgVol := tuple[1].(float64)
		// Count might be returned as different numeric type
		var count int64
		switch v := tuple[2].(type) {
		case int64:
			count = v
		case int:
			count = int64(v)
		case float64:
			count = int64(v)
		default:
			fmt.Printf("Unexpected count type: %T value: %v\n", tuple[2], tuple[2])
			count = 0
		}
		fmt.Printf("%4d | %10.0f | %11d\n", hour, avgVol, count)
	}

	// Example 5: Risk metrics - concentration analysis
	fmt.Println("\n=== Example 5: Position Concentration ===")
	concentrationQuery, err := parser.ParseQuery(`
		[:find ?symbol ?exposure ?pctx100
		 :where [?p :position/symbol ?symbol]
		        [?p :position/quantity ?qty]
		        [?s :security/ticker ?symbol]
		        [?s :price/last ?price]
		        [(* ?qty ?price) ?exposure]
		        [(* ?exposure 100) ?exposurex100]
		        [(/ ?exposurex100 1000000) ?pctx100]]`)
	
	if err != nil {
		fmt.Printf("Error parsing concentration query: %v\n", err)
	} else {
		result, _ = exec.Execute(concentrationQuery)
		fmt.Println("Symbol | Exposure    | % of Capital")
		fmt.Println("-------|-------------|-------------")
		for i := 0; i < result.Size(); i++ {
			tuple := result.Get(i)
			fmt.Printf("%-6s | $%10.2f | %11.2f%%\n", tuple[0], tuple[1], tuple[2])
		}
	}

	// Example 6: As-of queries for historical analysis
	fmt.Println("\n=== Example 6: Historical Position Analysis ===")
	// Get positions as of yesterday
	yesterday := time.Now().Add(-24 * time.Hour)
	asOfMatcher := db.AsOf(uint64(yesterday.UnixNano()))
	asOfExec := executor.NewExecutor(asOfMatcher)
	
	fmt.Printf("Positions as of %s:\n", yesterday.Format("2006-01-02"))
	result, _ = asOfExec.Execute(positionQuery)
	fmt.Println("Symbol | Position | Avg Price")
	fmt.Println("-------|----------|----------")
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		fmt.Printf("%-6s | %8.0f | $%8.2f\n", tuple[0], tuple[1], tuple[2])
	}

	// Example 7: Complex join - find correlated securities
	fmt.Println("\n=== Example 7: Correlated Securities ===")
	correlationQuery, _ := parser.ParseQuery(`
		[:find ?symbol1 ?symbol2 ?correlation
		 :where [?c :correlation/symbol1 ?symbol1]
		        [?c :correlation/symbol2 ?symbol2]
		        [?c :correlation/value ?correlation]
		        [(> ?correlation 0.7)]]`)
	
	result, _ = exec.Execute(correlationQuery)
	fmt.Println("Symbol 1 | Symbol 2 | Correlation")
	fmt.Println("---------|----------|------------")
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		fmt.Printf("%-8s | %-8s | %11.3f\n", tuple[0], tuple[1], tuple[2])
	}

	// Example 8: Intraday VWAP calculation
	fmt.Println("\n=== Example 8: VWAP Calculation ===")
	// VWAP = Sum(Price * Volume) / Sum(Volume)
	// We can approximate this with our current aggregation support
	vwapQuery, _ := parser.ParseQuery(`
		[:find ?symbol (sum ?dollarVolume) (sum ?volume)
		 :where [?t :trade/symbol ?symbol]
		        [?t :trade/price ?price]
		        [?t :trade/volume ?volume]
		        [(* ?price ?volume) ?dollarVolume]]`)
	
	result, _ = exec.Execute(vwapQuery)
	fmt.Println("Symbol | VWAP")
	fmt.Println("-------|--------")
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		dollarVol := tuple[1].(float64)
		vol := tuple[2].(float64)
		vwap := dollarVol / vol
		fmt.Printf("%-6s | $%.2f\n", tuple[0], vwap)
	}
	
	// Example 9: Alert generation based on rules
	fmt.Println("\n=== Example 9: Risk Alerts ===")
	alertQuery, _ := parser.ParseQuery(`
		[:find ?symbol ?exposure ?message
		 :where [?p :position/symbol ?symbol]
		        [?p :position/quantity ?qty]
		        [?s :security/ticker ?symbol]
		        [?s :price/last ?price]
		        [(* ?qty ?price) ?exposure]
		        [(> ?exposure 100000)]
		        [(str "High exposure warning: " ?symbol) ?message]]`)
	
	result, _ = exec.Execute(alertQuery)
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		fmt.Printf("⚠️  %s - Exposure: $%.2f\n", tuple[2], tuple[1])
	}
}

func initReferenceData(db *storage.Database) {
	// Add securities
	securities := []struct {
		ticker string
		name   string
		sector string
	}{
		{"AAPL", "Apple Inc.", "Technology"},
		{"GOOGL", "Alphabet Inc.", "Technology"},
		{"JPM", "JPMorgan Chase", "Financials"},
		{"TSLA", "Tesla Inc.", "Automotive"},
		{"MSFT", "Microsoft Corp.", "Technology"},
	}

	tx := db.NewTransaction()
	for _, sec := range securities {
		e := datalog.NewIdentity("security:" + sec.ticker)
		tx.Add(e, datalog.NewKeyword(":security/ticker"), sec.ticker)
		tx.Add(e, datalog.NewKeyword(":security/name"), sec.name)
		tx.Add(e, datalog.NewKeyword(":security/sector"), sec.sector)
	}
	tx.Commit()

	// Add correlations
	correlations := []struct {
		symbol1     string
		symbol2     string
		correlation float64
	}{
		{"AAPL", "MSFT", 0.85},
		{"GOOGL", "MSFT", 0.78},
		{"AAPL", "GOOGL", 0.82},
		{"JPM", "C", 0.91},
	}

	tx = db.NewTransaction()
	for _, corr := range correlations {
		e := datalog.NewIdentity(fmt.Sprintf("correlation:%s-%s", corr.symbol1, corr.symbol2))
		tx.Add(e, datalog.NewKeyword(":correlation/symbol1"), corr.symbol1)
		tx.Add(e, datalog.NewKeyword(":correlation/symbol2"), corr.symbol2)
		tx.Add(e, datalog.NewKeyword(":correlation/value"), corr.correlation)
	}
	tx.Commit()
}

func loadMarketData(db *storage.Database) {
	// Current prices
	prices := map[string]float64{
		"AAPL":  185.50,
		"GOOGL": 142.65,
		"JPM":   172.25,
		"TSLA":  238.45,
		"MSFT":  384.25,
	}

	tx := db.NewTransactionAt(time.Now())
	for symbol, price := range prices {
		e := datalog.NewIdentity("security:" + symbol)
		tx.Add(e, datalog.NewKeyword(":price/last"), price)
	}
	tx.Commit()

	// Historical positions (from yesterday)
	yesterday := time.Now().Add(-24 * time.Hour)
	historicalPositions := []struct {
		symbol   string
		quantity float64
		avgPrice float64
	}{
		{"AAPL", 800, 175.50},
		{"GOOGL", 400, 142.00},
		{"JPM", 600, 168.50},
		{"MSFT", 250, 375.00},
	}

	tx = db.NewTransactionAt(yesterday)
	for _, pos := range historicalPositions {
		// Use unique entity IDs by adding a timestamp suffix
		e := datalog.NewIdentity(fmt.Sprintf("position:%s:%d", pos.symbol, yesterday.Unix()))
		tx.Add(e, datalog.NewKeyword(":position/symbol"), pos.symbol)
		tx.Add(e, datalog.NewKeyword(":position/quantity"), pos.quantity)
		tx.Add(e, datalog.NewKeyword(":position/avg-price"), pos.avgPrice)
	}
	tx.Commit()

	// Current positions (from today)
	positions := []struct {
		symbol   string
		quantity float64
		avgPrice float64
	}{
		{"AAPL", 1000, 180.25},
		{"GOOGL", 500, 145.50},
		{"JPM", 800, 170.00},
		{"TSLA", 200, 245.00},
		{"MSFT", 300, 380.00},
	}

	now := time.Now()
	tx = db.NewTransactionAt(now)
	for _, pos := range positions {
		// Use unique entity IDs by adding a timestamp suffix
		e := datalog.NewIdentity(fmt.Sprintf("position:%s:%d", pos.symbol, now.Unix()))
		tx.Add(e, datalog.NewKeyword(":position/symbol"), pos.symbol)
		tx.Add(e, datalog.NewKeyword(":position/quantity"), pos.quantity)
		tx.Add(e, datalog.NewKeyword(":position/avg-price"), pos.avgPrice)
	}
	tx.Commit()

	// Historical trades (for volume analysis)
	trades := []struct {
		symbol    string
		timestamp time.Time
		price     float64
		volume    int64
	}{
		{"AAPL", now.Add(-6 * time.Hour), 184.50, 1000000},
		{"AAPL", now.Add(-5 * time.Hour), 185.00, 1200000},
		{"AAPL", now.Add(-4 * time.Hour), 185.25, 800000},
		{"AAPL", now.Add(-3 * time.Hour), 185.50, 1500000},
		{"GOOGL", now.Add(-5 * time.Hour), 142.00, 500000},
		{"GOOGL", now.Add(-3 * time.Hour), 142.65, 700000},
		{"MSFT", now.Add(-4 * time.Hour), 383.50, 600000},
		{"MSFT", now.Add(-2 * time.Hour), 384.25, 900000},
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
}
