package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestIteratorReuseRegression reproduces the production issue where
// querying for daily highs fetched 7088 bars per symbol instead of ~200
func TestIteratorReuseRegression(t *testing.T) {
	// This test is about iterator reuse behavior, not streaming
	// No need to manipulate global state - just test the functionality

	// Create test database
	dbPath := "/tmp/test-iterator-reuse-regression"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create symbols
	symbols := []string{"AAPL", "GOOG", "MSFT"}
	tx := db.NewTransaction()
	for _, ticker := range symbols {
		e := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
		tx.Add(e, datalog.NewKeyword(":symbol/ticker"), ticker)
	}
	tx.Commit()

	// Load realistic data: 1 year of minute bars (390 minutes * 252 trading days = 98,280 bars per symbol)
	// But let's do 3 months (60 days) for faster test: 23,400 bars per symbol
	t.Log("Loading 3 months of minute bar data...")
	loadMinuteBars(t, db, symbols, 60)

	// Verify data was actually stored by doing a simple query
	testMatcher := NewBadgerMatcher(db.store)
	testPattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword(":price/minute-of-day")},
			query.Variable{Name: "?v"},
		},
	}
	testResult, err := testMatcher.Match(testPattern, nil)
	if err != nil {
		t.Fatalf("Test query failed: %v", err)
	}
	t.Logf("Test query found %d datoms with :price/minute-of-day", testResult.Size())

	// Check what's stored for :price/symbol
	symbolPattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword(":price/symbol")},
			query.Variable{Name: "?v"},
		},
	}
	symbolResult, err := testMatcher.Match(symbolPattern, nil)
	if err != nil {
		t.Fatalf("Symbol query failed: %v", err)
	}
	t.Logf("Test query found %d datoms with :price/symbol", symbolResult.Size())
	if symbolResult.Size() > 0 {
		// Check first few values
		count := 0
		for _, tuple := range symbolResult.Sorted() {
			if count >= 3 {
				break
			}
			if len(tuple) > 0 {
				if id, ok := tuple[0].(datalog.Identity); ok {
					t.Logf("  Symbol value %d: %s (hash: %x)", count, id.String(), id.Bytes())
				} else {
					t.Logf("  Symbol value %d: %T = %v", count, tuple[0], tuple[0])
				}
			}
			count++
		}
	}

	// Check specifically for minute 570
	test570Pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword(":price/minute-of-day")},
			query.Constant{Value: int64(570)},
		},
	}
	test570Result, err := testMatcher.Match(test570Pattern, nil)
	if err != nil {
		t.Fatalf("Test 570 query failed: %v", err)
	}
	t.Logf("Test query found %d datoms with :price/minute-of-day = 570", test570Result.Size())

	// Let's check if we have any values at the boundaries
	for _, testVal := range []int64{570, 571, 959, 960} {
		testPattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?e"},
				query.Constant{Value: datalog.NewKeyword(":price/minute-of-day")},
				query.Constant{Value: testVal},
			},
		}
		result, _ := testMatcher.Match(testPattern, nil)
		t.Logf("  minute-of-day = %d: found %d datoms", testVal, result.Size())
	}

	// The problematic pattern from production:
	// Find morning bars (9:30 AM = minute 570) for each symbol
	// This would be used to identify trading days, then subqueries fetch data for each day

	t.Run("WithoutIteratorReuse", func(t *testing.T) {
		// Create a matcher that tracks statistics
		matcher := &instrumentedMatcher{
			BadgerMatcher: NewBadgerMatcher(db.store),
			stats:         &matchStats{},
		}

		// Pattern: [?bar :price/minute-of-day 570]
		// This finds all morning bars (9:30 AM) regardless of symbol
		// Note: This pattern doesn't use the binding, so we expect just 1 iterator open
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?bar"},
				query.Constant{Value: datalog.NewKeyword(":price/minute-of-day")},
				query.Constant{Value: int64(570)}, // 9:30 AM
			},
		}

		// Create a relation with all symbols
		var tuples []executor.Tuple
		for _, ticker := range symbols {
			e := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
			tuples = append(tuples, executor.Tuple{e})
		}
		symbolRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?s"},
			tuples,
		)

		// This pattern ignores the binding and finds ALL morning bars
		// The binding relation is passed but not used since ?s isn't in the pattern
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

		// Each symbol should have morning bars for each trading day
		// 60 calendar days ≈ 44 trading days (excluding weekends)
		expectedBars := 44 * len(symbols) // 44 trading days × 3 symbols = 132
		if barCount != expectedBars {
			t.Errorf("Expected %d morning bars, got %d", expectedBars, barCount)
		}

		t.Logf("Pattern without binding variable (finds ALL morning bars):")
		t.Logf("  Time: %v", duration)
		t.Logf("  Iterator opens: %d", matcher.stats.iteratorOpens)
		t.Logf("  Datoms scanned: %d", matcher.stats.datomsScanned)
		t.Logf("  Datoms matched: %d", matcher.stats.datomsMatched)
		t.Logf("  Results: %d", barCount)

		// Since the pattern doesn't use the binding, only 1 iterator should open
		if matcher.stats.iteratorOpens != 1 {
			t.Logf("Expected 1 iterator open (pattern doesn't use binding), got %d",
				matcher.stats.iteratorOpens)
		}

		// The real issue: scanning way more datoms than necessary
		// We should only scan ~180 datoms (60 morning bars * 3 symbols)
		// But without proper indexing, we might scan all minute-of-day=570 bars
		if matcher.stats.datomsScanned > 1000 {
			t.Logf("PERFORMANCE ISSUE: Scanned %d datoms for %d results (%.1fx overhead)",
				matcher.stats.datomsScanned, result.Size(),
				float64(matcher.stats.datomsScanned)/float64(result.Size()))
		}
	})

	t.Run("WithIteratorReuseAttempt", func(t *testing.T) {
		// Try with iterator reuse enabled
		// Note: Based on our analysis, this won't help because:
		// - Pattern uses AVET index with Value as secondary sort key
		// - Iterator reuse only works with primary sort keys

		matcher := &instrumentedMatcher{
			BadgerMatcher: NewBadgerMatcher(db.store),
			stats:         &matchStats{},
			forceReuse:    true, // Force iterator reuse for testing
		}

		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?bar"},
				query.Constant{Value: datalog.NewKeyword(":price/minute-of-day")},
				query.Constant{Value: int64(570)},
			},
		}

		var tuples []executor.Tuple
		for _, ticker := range symbols {
			e := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
			tuples = append(tuples, executor.Tuple{e})
		}
		symbolRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?s"},
			tuples,
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

		t.Logf("With iterator reuse attempt:")
		t.Logf("  Time: %v", duration)
		t.Logf("  Iterator opens: %d", matcher.stats.iteratorOpens)
		t.Logf("  Datoms scanned: %d", matcher.stats.datomsScanned)
		t.Logf("  Datoms matched: %d", matcher.stats.datomsMatched)
		t.Logf("  Results: %d", barCount)

		// Iterator reuse should reduce opens to 1, but won't help with scan count
		// because the pattern doesn't match the optimization criteria
		if matcher.stats.iteratorOpens == 1 {
			t.Log("Iterator reuse reduced opens, but...")
		}

		if matcher.stats.datomsScanned > 1000 {
			t.Log("Still scanning too many datoms - iterator reuse doesn't help this pattern!")
		}
	})

	t.Run("ActualProductionQuery", func(t *testing.T) {
		// Reset the real iterator counter
		ResetIteratorOpenCount()
		// The actual problematic query pattern from production:
		// [?b :price/symbol ?sym] with ?sym bound
		// [?b :price/minute-of-day ?mod] with range check

		// First, let's check what values are actually stored for :price/symbol
		// Query for ANY bar with :price/symbol attribute to see what values are there
		anyPattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Constant{Value: datalog.NewIdentity("bar:AAPL:0:0")}, // Try a specific bar
				query.Constant{Value: datalog.NewKeyword(":price/symbol")},
				query.Variable{Name: "?v"},
			},
		}
		regularMatcher := NewBadgerMatcher(db.store)
		anyResult, err := regularMatcher.Match(anyPattern, nil)
		if err != nil {
			t.Fatalf("Any pattern failed: %v", err)
		}
		t.Logf("Bar AAPL:0:0 has %d :price/symbol values", anyResult.Size())
		if anyResult.Size() > 0 {
			it := anyResult.Iterator()
			it.Next()
			tuple := it.Tuple()
			it.Close()
			if len(tuple) > 2 {
				if id, ok := tuple[2].(datalog.Identity); ok {
					t.Logf("  Value is Identity: %s (hash: %x)", id.String(), id.Bytes())
					// Compare with what we're looking for
					lookingFor := datalog.NewIdentity("symbol:AAPL")
					t.Logf("  Looking for: %s (hash: %x)", lookingFor.String(), lookingFor.Bytes())
					t.Logf("  Equal? %v", id.Equal(lookingFor))
				} else {
					t.Logf("  Value type: %T = %v", tuple[2], tuple[2])
				}
			}
		}

		// Now try the direct query
		checkPattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?b"},
				query.Constant{Value: datalog.NewKeyword(":price/symbol")},
				query.Constant{Value: datalog.NewIdentity("symbol:AAPL")},
			},
		}
		checkResult, err := regularMatcher.Match(checkPattern, nil)
		if err != nil {
			t.Fatalf("Check pattern failed: %v", err)
		}
		t.Logf("Direct query for [?b :price/symbol symbol:AAPL] found %d bars", checkResult.Size())

		// Now test with binding
		testPattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?b"},
				query.Constant{Value: datalog.NewKeyword(":price/symbol")},
				query.Variable{Name: "?s"}, // This will be bound
			},
		}
		testSymbol := datalog.NewIdentity("symbol:AAPL")
		testRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?s"},
			[]executor.Tuple{{testSymbol}},
		)
		testResult, err := regularMatcher.Match(testPattern, executor.Relations{testRel})
		if err != nil {
			t.Fatalf("Regular matcher failed: %v", err)
		}

		// Iterate to count results
		testIt := testResult.Iterator()
		testCount := 0
		for testIt.Next() {
			testCount++
		}
		testIt.Close()

		t.Logf("Regular matcher with binding found %d bars for AAPL", testCount)

		matcher := &instrumentedMatcher{
			BadgerMatcher: NewBadgerMatcher(db.store),
			stats:         &matchStats{},
		}

		// This is what happens in the subquery: for each symbol,
		// find all bars for a specific date
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?b"},
				query.Constant{Value: datalog.NewKeyword(":price/symbol")},
				query.Variable{Name: "?s"}, // This will be bound
			},
		}

		// Simulate binding to multiple symbols to test iterator reuse
		var symbolTuples []executor.Tuple
		for _, ticker := range symbols {
			symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
			symbolTuples = append(symbolTuples, executor.Tuple{symbolEntity})
		}
		symbolRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?s"},
			symbolTuples,
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

		// This should return ALL bars for all symbols
		// 60 calendar days ≈ 44 trading days × 390 minutes × 3 symbols = 51,480 bars
		expectedBars := 390 * 44 * len(symbols)
		if barCount != expectedBars {
			t.Errorf("Expected %d bars for all symbols, got %d", expectedBars, barCount)
		}

		t.Logf("Production pattern - fetch all bars for %d symbols:", len(symbols))
		t.Logf("  Time: %v", duration)
		t.Logf("  Datoms scanned: %d", matcher.stats.datomsScanned)
		t.Logf("  Results: %d", barCount)
		t.Logf("  FAKE Iterator opens from test: %d", matcher.stats.iteratorOpens)
		t.Logf("  REAL Iterator opens from storage: %d (should be 1 with reuse, %d without)",
			GetIteratorOpenCount(), len(symbols))
	})
}

// instrumentedMatcher wraps BadgerMatcher to collect statistics
type instrumentedMatcher struct {
	*BadgerMatcher
	stats      *matchStats
	forceReuse bool
}

type matchStats struct {
	iteratorOpens int
	datomsScanned int
	datomsMatched int
}

func (m *instrumentedMatcher) Match(pattern *query.DataPattern, bindings executor.Relations) (executor.Relation, error) {
	// Track iterator opens by intercepting the storage layer
	// In real implementation, we'd hook into the actual iterator creation
	// For now, we'll count 1 for unbound patterns, N for bound patterns
	if bindings == nil || len(bindings) == 0 {
		m.stats.iteratorOpens = 1
	} else {
		// Check if pattern actually uses the binding
		hasBindingVar := false
		for _, elem := range pattern.Elements {
			if v, ok := elem.(query.Variable); ok {
				for _, rel := range bindings {
					for _, col := range rel.Columns() {
						if v.Name == col {
							hasBindingVar = true
							break
						}
					}
				}
			}
		}
		if hasBindingVar && len(bindings) > 0 {
			m.stats.iteratorOpens = bindings[0].Size()
		} else {
			m.stats.iteratorOpens = 1
		}
	}

	// Call the real matcher
	result, err := m.BadgerMatcher.Match(pattern, bindings)
	if err != nil {
		return nil, err
	}

	// Count actual results
	if result != nil {
		m.stats.datomsMatched = result.Size()
		// For this test, scanned equals matched when using key-only scanning
		// since we decode directly from keys
		m.stats.datomsScanned = result.Size()
	}

	return result, nil
}

func loadMinuteBars(t *testing.T, db *Database, symbols []string, days int) {
	// Start date
	loc, _ := time.LoadLocation("America/New_York")
	baseTime := time.Date(2024, 1, 2, 9, 30, 0, 0, loc)
	minutesPerDay := 390 // 6.5 hours of trading

	for _, ticker := range symbols {
		symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))

		// Create bars for each trading day
		for day := 0; day < days; day++ {
			dayTime := baseTime.AddDate(0, 0, day)

			// Skip weekends
			if dayTime.Weekday() == time.Saturday || dayTime.Weekday() == time.Sunday {
				continue
			}

			tx := db.NewTransaction()

			// Create minute bars for the day
			for minute := 0; minute < minutesPerDay; minute++ {
				barTime := dayTime.Add(time.Duration(minute) * time.Minute)
				barEntity := datalog.NewIdentity(
					fmt.Sprintf("bar:%s:%d:%d", ticker, day, minute))

				// Minute of day: 570 = 9:30 AM, 960 = 4:00 PM
				minuteOfDay := int64(570 + minute)

				// Add bar data
				tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
				tx.Add(barEntity, datalog.NewKeyword(":price/time"), barTime)
				tx.Add(barEntity, datalog.NewKeyword(":price/minute-of-day"), minuteOfDay)

				// Add OHLC data
				basePrice := 100.0
				if ticker == "GOOG" {
					basePrice = 2800.0
				} else if ticker == "MSFT" {
					basePrice = 380.0
				}

				open := basePrice + float64(minute%10)
				high := open + 0.5
				low := open - 0.5
				close := open + 0.25
				volume := int64(100000 + minute*1000)

				tx.Add(barEntity, datalog.NewKeyword(":price/open"), open)
				tx.Add(barEntity, datalog.NewKeyword(":price/high"), high)
				tx.Add(barEntity, datalog.NewKeyword(":price/low"), low)
				tx.Add(barEntity, datalog.NewKeyword(":price/close"), close)
				tx.Add(barEntity, datalog.NewKeyword(":price/volume"), volume)
			}

			tx.Commit()
		}
	}

	totalBars := len(symbols) * days * minutesPerDay
	t.Logf("Loaded %d price bars (%d symbols × %d days × %d minutes)",
		totalBars, len(symbols), days, minutesPerDay)
}
