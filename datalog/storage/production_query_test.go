package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestProductionQueryPattern reproduces the exact pattern from gopher-street
// that led to fetching 7088 bars when only ~200 were needed
func TestProductionQueryPattern(t *testing.T) {
	// This test is about query patterns, not streaming optimization
	// No need to manipulate global state - just test the functionality

	// Create test database
	dbPath := "/tmp/test-production-query"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := db.store

	// Create a symbol
	symbolEntity := datalog.NewIdentity("symbol:CRWV")

	// Load data: simulate trading days of minute bars
	// We pass calendar days but weekends are skipped
	// 20 calendar days from Monday = 15 trading days
	t.Log("Loading price bar data (20 calendar days = ~15 trading days)...")
	loadPriceBars(t, db, symbolEntity, 20)

	// The production query pattern:
	// 1. First, find all trading days by looking for morning bars
	// 2. Then for each day, fetch all bars for that day to compute OHLC

	t.Run("Step1_FindTradingDays", func(t *testing.T) {
		// Pattern: [?bar :price/minute-of-day 570] to find 9:30 AM bars
		// This identifies which days have data

		matcher := NewBadgerMatcher(store)

		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?bar"},
				query.Constant{Value: datalog.NewKeyword(":price/minute-of-day")},
				query.Constant{Value: int64(570)}, // 9:30 AM
			},
		}

		// No bindings - just find all morning bars
		start := time.Now()
		result, err := matcher.Match(pattern, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Iterate and count results
		it := result.Iterator()
		barCount := 0
		for it.Next() {
			barCount++
		}
		it.Close()
		duration := time.Since(start)

		t.Logf("Finding trading days (morning bars):")
		t.Logf("  Time: %v", duration)
		t.Logf("  Morning bars found: %d", barCount)
		t.Logf("  This identifies %d trading days", barCount)

		// Expected: 15 morning bars (one per trading day, excluding weekends)
		if barCount != 15 {
			t.Errorf("Expected 15 morning bars, got %d", barCount)
		}
	})

	t.Run("Step2_FetchBarsForEachDay_Problem", func(t *testing.T) {
		// This simulates the subquery that runs for each day
		// Pattern: [?b :price/symbol ?s] with ?s bound to CRWV
		// Then filter by date in memory

		matcher := NewBadgerMatcher(store)

		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?b"},
				query.Constant{Value: datalog.NewKeyword(":price/symbol")},
				query.Variable{Name: "?s"},
			},
		}

		// Bind to the symbol
		symbolRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?s"},
			[]executor.Tuple{{symbolEntity}},
		)

		start := time.Now()
		result, err := matcher.Match(pattern, executor.Relations{symbolRel})
		if err != nil {
			t.Fatal(err)
		}

		// Iterate and count results
		it := result.Iterator()
		barCount := 0
		for it.Next() {
			barCount++
		}
		it.Close()
		duration := time.Since(start)

		totalBars := 20 * 390 // 20 days * 390 minutes
		t.Logf("PROBLEM - Fetching bars for symbol without date filter:")
		t.Logf("  Time: %v", duration)
		t.Logf("  Bars fetched: %d", barCount)
		t.Logf("  Expected for one day: 390")
		t.Logf("  Overhead: %.1fx", float64(barCount)/390.0)

		if barCount == totalBars {
			t.Logf("  ❌ FETCHING ALL %d BARS instead of 390 for one day!", barCount)
		}

		// Note: Without annotations, we can't see the actual scan count
		// but we know it's fetching all bars for the symbol
	})

	t.Run("Step3_MultiDayQuery_Multiplies_Problem", func(t *testing.T) {
		// When querying multiple days, the problem multiplies
		// Each day subquery fetches ALL bars, not just that day's bars

		numDays := 5
		totalFetched := 0
		totalTime := time.Duration(0)

		for day := 0; day < numDays; day++ {
			matcher := NewBadgerMatcher(store)

			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?b"},
					query.Constant{Value: datalog.NewKeyword(":price/symbol")},
					query.Variable{Name: "?s"},
				},
			}

			symbolRel := executor.NewMaterializedRelation(
				[]query.Symbol{"?s"},
				[]executor.Tuple{{symbolEntity}},
			)

			start := time.Now()
			result, _ := matcher.Match(pattern, executor.Relations{symbolRel})

			// Iterate and count results
			it := result.Iterator()
			count := 0
			for it.Next() {
				count++
			}
			it.Close()

			totalTime += time.Since(start)
			totalFetched += count
		}

		t.Logf("Multi-day query simulation (%d days):", numDays)
		t.Logf("  Total time: %v", totalTime)
		t.Logf("  Total bars fetched: %d", totalFetched)
		t.Logf("  Expected (390 * %d): %d", numDays, 390*numDays)
		t.Logf("  Overhead: %.1fx", float64(totalFetched)/float64(390*numDays))
		t.Logf("  ❌ Each day fetches ALL bars, not just that day's data!")
	})

	t.Run("Step4_CheckIteratorReuse", func(t *testing.T) {
		// Check if iterator reuse would help
		// The pattern is: [?bar :price/minute-of-day ?mod] with ?mod bound to different values

		// Test with matcher_v2 which has iterator reuse logic
		// matcher := NewBadgerMatcher(store) // Not used in this test

		// Pattern with minute-of-day that will be bound
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?bar"},
				query.Constant{Value: datalog.NewKeyword(":price/minute-of-day")},
				query.Variable{Name: "?mod"},
			},
		}

		// Bind to multiple minute values
		minutes := []int64{570, 571, 572, 573, 574} // First 5 minutes
		var tuples []executor.Tuple
		for _, min := range minutes {
			tuples = append(tuples, executor.Tuple{min})
		}
		minuteRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?mod"},
			tuples,
		)

		// Check what strategy would be chosen
		strategy := analyzeReuseStrategy(pattern, minuteRel)

		t.Logf("Iterator reuse analysis for minute-of-day pattern:")
		t.Logf("  Pattern: [?bar :price/minute-of-day ?mod]")
		t.Logf("  Strategy type: %s", strategy.Type.String())
		t.Logf("  Position: %d", strategy.Position)

		if strategy.Type == NoReuse {
			t.Log("  ❌ Iterator reuse WON'T HELP - Value is secondary sort key in AVET index")
			t.Log("  Explanation: AVET sorts by Attribute first, then Value")
			t.Log("  All :price/minute-of-day datoms are together, but values are scattered")
		} else {
			t.Log("  ✓ Iterator reuse would help this pattern")
		}
	})

	t.Run("Step5_WhatActuallyHelps", func(t *testing.T) {
		// The real solution: better query planning
		// Instead of fetching all bars then filtering by date,
		// the planner should push date predicates into the storage layer

		t.Log("What would actually help:")
		t.Log("1. Push date predicates into storage queries")
		t.Log("2. Use compound indices like (symbol, date, minute)")
		t.Log("3. Partition data by date at storage level")
		t.Log("4. Use bitmap indices for minute-of-day values")
		t.Log("")
		t.Log("Iterator reuse helps when:")
		t.Log("- Binding on primary sort key (Entity in EAVT, Attribute in AEVT)")
		t.Log("- Multiple sequential lookups with sorted values")
		t.Log("")
		t.Log("Iterator reuse DOESN'T help when:")
		t.Log("- Binding on secondary sort key (Value in AVET when Attribute is constant)")
		t.Log("- Values are scattered throughout the index")
	})
}

func loadPriceBars(t *testing.T, db *Database, symbolEntity datalog.Identity, days int) {
	minutesPerDay := 390
	loc, _ := time.LoadLocation("America/New_York")
	baseTime := time.Date(2024, 6, 3, 9, 30, 0, 0, loc) // Start on a Monday

	barsLoaded := 0
	for day := 0; day < days; day++ {
		// Skip weekends
		dayTime := baseTime.AddDate(0, 0, day)
		if dayTime.Weekday() == time.Saturday || dayTime.Weekday() == time.Sunday {
			continue
		}

		tx := db.NewTransaction()

		// Create bars for this day
		for minute := 0; minute < minutesPerDay; minute++ {
			barTime := dayTime.Add(time.Duration(minute) * time.Minute)
			barEntity := datalog.NewIdentity(fmt.Sprintf("bar:%d:%d", day, minute))

			// Add symbol reference
			tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)

			// Add minute-of-day
			minuteOfDay := int64(570 + minute) // 570 = 9:30 AM
			tx.Add(barEntity, datalog.NewKeyword(":price/minute-of-day"), minuteOfDay)

			// Add time
			tx.Add(barEntity, datalog.NewKeyword(":price/time"), barTime)

			// Add OHLC values
			tx.Add(barEntity, datalog.NewKeyword(":price/open"), 100.0+float64(minute%10))
			tx.Add(barEntity, datalog.NewKeyword(":price/high"), 100.5+float64(minute%10))
			tx.Add(barEntity, datalog.NewKeyword(":price/low"), 99.5+float64(minute%10))
			tx.Add(barEntity, datalog.NewKeyword(":price/close"), 100.25+float64(minute%10))
			tx.Add(barEntity, datalog.NewKeyword(":price/volume"), int64(100000+minute*100))

			barsLoaded++
		}

		if _, err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	}

	t.Logf("Loaded %d price bars", barsLoaded)
}

// TestPlannerPredicatePushdownIntegration tests the full integration of predicate pushdown
// from planner through executor to storage
func TestPlannerPredicatePushdownIntegration(t *testing.T) {
	// Create test database
	dbPath := "/tmp/test-predicate-pushdown"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create test data: symbol with price bars for multiple days
	// Create symbol first
	{
		tx := db.NewTransaction()
		crwv := datalog.NewIdentity("symbol:CRWV")
		tx.Add(crwv, datalog.NewKeyword(":symbol/ticker"), "CRWV")
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Create price bars in daily batches
	// Use fewer bars for testing (10 per day instead of 390)
	crwv := datalog.NewIdentity("symbol:CRWV")
	barsPerDay := 10
	for day := 1; day <= 20; day++ {
		tx := db.NewTransaction()
		for minute := 0; minute < barsPerDay; minute++ {
			barTime := time.Date(2025, 6, day, 9, 30+minute/60, minute%60, 0, time.UTC)
			bar := datalog.NewIdentity(fmt.Sprintf("bar:%d:%d", day, minute))

			tx.Add(bar, datalog.NewKeyword(":price/symbol"), crwv)
			tx.Add(bar, datalog.NewKeyword(":price/time"), barTime)
			tx.Add(bar, datalog.NewKeyword(":price/high"), 100.0+float64(minute%10))
			tx.Add(bar, datalog.NewKeyword(":price/low"), 95.0+float64(minute%5))
			tx.Add(bar, datalog.NewKeyword(":price/close"), 98.0+float64(minute%7))
			tx.Add(bar, datalog.NewKeyword(":price/volume"), int64(1000*(minute+1)))
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit day %d: %v", day, err)
		}
	}

	// The OHLC query with day filter
	queryStr := `[:find ?b ?t ?h ?l ?c ?v
	              :where 
	              [?s :symbol/ticker "CRWV"]
	              [?b :price/symbol ?s]
	              [?b :price/time ?t]
	              [?b :price/high ?h]
	              [?b :price/low ?l]
	              [?b :price/close ?c]
	              [?b :price/volume ?v]
	              [(day ?t) ?d]
	              [(= ?d 20)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("WithoutPredicatePushdown", func(t *testing.T) {
		// Create executor with predicate pushdown disabled
		matcher := NewBadgerMatcher(db.store)
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnablePredicatePushdown: false,
			EnableFineGrainedPhases: true,
		})

		result, err := exec.Execute(q)
		if err != nil {
			t.Fatal(err)
		}

		// Iterate and count results
		it := result.Iterator()
		barCount := 0
		for it.Next() {
			barCount++
		}
		it.Close()

		// Without pushdown, we fetch all days then filter
		// Result should still be correct (390 bars for day 20)
		if barCount != barsPerDay {
			t.Errorf("Expected %d bars, got %d", barsPerDay, barCount)
		}

		t.Logf("Without pushdown: %d results (filtered from all %d days)",
			barCount, 20)
	})

	t.Run("WithPredicatePushdown", func(t *testing.T) {
		// Create executor with predicate pushdown enabled (default)
		matcher := NewBadgerMatcher(db.store)
		exec := executor.NewExecutor(matcher) // Has pushdown enabled by default

		result, err := exec.Execute(q)
		if err != nil {
			t.Fatal(err)
		}

		// Iterate and count results
		it := result.Iterator()
		barCount := 0
		for it.Next() {
			barCount++
		}
		it.Close()

		// With pushdown, storage only fetches day 20 data
		if barCount != barsPerDay {
			t.Errorf("Expected %d bars, got %d", barsPerDay, barCount)
		}

		t.Logf("With pushdown: %d results (filtered at storage layer)", barCount)
	})
}
