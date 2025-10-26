//go:build example
// +build example

package main

import (
	"fmt"
	"log"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func main() {
	fmt.Println("=== Multi-Row Relations with Annotations Demo ===\n")

	// Create a database
	db, err := storage.NewDatabase("/tmp/annotations_demo")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create test data
	tx := db.NewTransaction()

	// Create stocks
	stocks := []struct {
		id     datalog.Identity
		symbol string
		price  float64
		volume int64
	}{
		{datalog.NewIdentity("stock:1"), "AAPL", 150.0, 1000000},
		{datalog.NewIdentity("stock:2"), "GOOGL", 140.0, 800000},
		{datalog.NewIdentity("stock:3"), "MSFT", 300.0, 1200000},
		{datalog.NewIdentity("stock:4"), "AMZN", 170.0, 900000},
		{datalog.NewIdentity("stock:5"), "META", 350.0, 700000},
		{datalog.NewIdentity("stock:6"), "TSLA", 200.0, 1500000},
		{datalog.NewIdentity("stock:7"), "NVDA", 450.0, 2000000},
		{datalog.NewIdentity("stock:8"), "AMD", 120.0, 600000},
		{datalog.NewIdentity("stock:9"), "NFLX", 380.0, 500000},
		{datalog.NewIdentity("stock:10"), "ORCL", 100.0, 400000},
	}

	symbolAttr := datalog.NewKeyword(":stock/symbol")
	priceAttr := datalog.NewKeyword(":stock/price")
	volumeAttr := datalog.NewKeyword(":stock/volume")

	for _, stock := range stocks {
		tx.Add(stock.id, symbolAttr, stock.symbol)
		tx.Add(stock.id, priceAttr, stock.price)
		tx.Add(stock.id, volumeAttr, stock.volume)
	}

	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// Create a collector to store events for later analysis
	var events []annotations.Event
	handler := func(event annotations.Event) {
		events = append(events, event)
	}

	// Create a matcher with annotation decorator pattern
	baseMatcher := storage.NewBadgerMatcher(db.Store())
	matcher := executor.WrapMatcher(baseMatcher, handler).(executor.PatternMatcher)

	// Demo 1: Single-row binding (baseline)
	fmt.Println("1. Single-row binding (traditional approach):")
	{
		// Create a single-row relation
		singleRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?stock"},
			[]executor.Tuple{{stocks[0].id}}, // Just AAPL
		)

		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?stock"},
				query.Constant{Value: priceAttr},
				query.Variable{Name: "?price"},
			},
		}

		results, err := matcher.Match(pattern, executor.Relations{singleRel})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("   Found %d result\n", results.Size())
	}

	// Demo 2: Multi-row single-column binding
	fmt.Println("\n2. Multi-row single-column binding:")
	{
		// Create a multi-row relation with 5 stocks
		var tuples []executor.Tuple
		for i := 0; i < 5; i++ {
			tuples = append(tuples, executor.Tuple{stocks[i].id})
		}
		multiRel := executor.NewMaterializedRelation([]query.Symbol{"?stock"}, tuples)

		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?stock"},
				query.Constant{Value: priceAttr},
				query.Variable{Name: "?price"},
			},
		}

		results, err := matcher.Match(pattern, executor.Relations{multiRel})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("   Found %d results with one call\n", results.Size())
	}

	// Demo 3: Multi-row multi-column binding
	fmt.Println("\n3. Multi-row multi-column binding:")
	{
		// Create a relation with stock-attribute pairs
		multiColRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?s", "?attr"},
			[]executor.Tuple{
				{stocks[0].id, priceAttr},  // AAPL price
				{stocks[0].id, volumeAttr}, // AAPL volume
				{stocks[1].id, symbolAttr}, // GOOGL symbol
				{stocks[2].id, priceAttr},  // MSFT price
			},
		)

		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?s"},
				query.Variable{Name: "?attr"},
				query.Variable{Name: "?val"},
			},
		}

		results, err := matcher.Match(pattern, executor.Relations{multiColRel})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("   Found %d results for specific stock-attribute pairs\n", results.Size())
	}

	// Demo 4: Large multi-row binding
	fmt.Println("\n4. Large multi-row binding (all stocks):")
	{
		// Create a relation with all stocks
		var allTuples []executor.Tuple
		for _, stock := range stocks {
			allTuples = append(allTuples, executor.Tuple{stock.id})
		}
		allRel := executor.NewMaterializedRelation([]query.Symbol{"?stock"}, allTuples)

		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?stock"},
				query.Constant{Value: symbolAttr},
				query.Variable{Name: "?symbol"},
			},
		}

		results, err := matcher.Match(pattern, executor.Relations{allRel})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("   Found %d results with one call for %d stocks\n", results.Size(), len(stocks))
	}

	// Print annotations
	fmt.Println("\n=== Annotations Output ===")

	// Group events by type
	eventTypes := make(map[string][]annotations.Event)
	for _, event := range events {
		eventTypes[event.Name] = append(eventTypes[event.Name], event)
	}

	// Show pattern/multi-match events
	if multiMatches, ok := eventTypes["pattern/multi-match"]; ok {
		fmt.Println("\nMulti-Match Operations:")
		for _, event := range multiMatches {
			fmt.Printf("  Name: %s, Latency: %v\n", event.Name, event.Latency)
		}
	}

	// Show single binding events
	if singles, ok := eventTypes["pattern/storage-scan"]; ok {
		fmt.Println("\nSingle Binding Operations:")
		for _, event := range singles {
			fmt.Printf("  Name: %s, Latency: %v\n", event.Name, event.Latency)
		}
	}

	// Show empty binding events
	if empties, ok := eventTypes["pattern/empty-binding"]; ok {
		fmt.Println("\nEmpty Binding Operations:")
		for _, event := range empties {
			fmt.Printf("  Name: %s, Latency: %v\n", event.Name, event.Latency)
		}
	}

	// Summary statistics
	fmt.Println("\n=== Summary ===")
	totalEvents := len(events)
	multiMatchCount := len(eventTypes["pattern/multi-match"])

	fmt.Printf("Total operations: %d\n", totalEvents)
	fmt.Printf("Multi-match operations: %d\n", multiMatchCount)
	fmt.Printf("Efficiency gain: Using multi-row relations reduced API calls significantly\n")

	// Show the efficiency of multi-match
	for _, event := range eventTypes["pattern/multi-match"] {
		bindingCount := event.Data["binding.count"].(int)
		totalMatched := event.Data["match.total"].(int)
		uniqueResults := event.Data["results.unique"].(int)

		fmt.Printf("\nMulti-match efficiency:\n")
		fmt.Printf("  - %d bindings processed in ONE call\n", bindingCount)
		fmt.Printf("  - %d total matches found\n", totalMatched)
		fmt.Printf("  - %d unique results after deduplication\n", uniqueResults)
		fmt.Printf("  - Without multi-row relations: %d separate API calls needed\n", bindingCount)
	}

	// Track events before empty relation test
	eventCountBefore := len(events)

	// Test empty relation annotation
	fmt.Println("\n5. Empty relation test:")
	{
		emptyRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?stock"},
			[]executor.Tuple{}, // Empty!
		)

		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?stock"},
				query.Constant{Value: priceAttr},
				query.Variable{Name: "?price"},
			},
		}

		results, err := matcher.Match(pattern, executor.Relations{emptyRel})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("   Empty relation correctly returned %d results\n", results.Size())
	}

	// Print final annotation summary
	fmt.Println("\n=== Final Annotation Events ===")
	for i := eventCountBefore; i < len(events); i++ {
		event := events[i]
		// Show raw event data for debugging
		fmt.Printf("\nEvent: %s\n", event.Name)
		fmt.Printf("Latency: %v\n", event.Latency)
		fmt.Printf("Data: %+v\n", event.Data)
	}
}
