package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestHashJoinScanRangeBug reproduces the production hang where HashJoinScan
// scans the entire attribute range instead of using bound values to narrow the scan.
//
// Bug scenario:
// - Query: [?s :symbol/ticker "CRWV"] [?e :price/symbol ?s] [?e :price/open ?open]
// - ?s is bound to a single symbol entity
// - Pattern [?e :price/symbol ?s] should scan only datoms for THAT symbol
// - But calculatePatternScanRange() only looks at constants, ignoring bound variables
// - Result: Scans ALL :price/symbol datoms (10,000+) instead of just 78
//
// With production dataset (28,040 datoms), this causes a 10+ second hang.
// With IndexNestedLoop disabled (threshold=0), this becomes the default path.
func TestHashJoinScanRangeBug(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	assert.NoError(t, err)
	defer db.Close()

	symbolKw := datalog.NewKeyword(":symbol/ticker")
	priceSymbol := datalog.NewKeyword(":price/symbol")
	priceTime := datalog.NewKeyword(":price/time")
	priceMinuteOfDay := datalog.NewKeyword(":price/minute-of-day")
	priceOpen := datalog.NewKeyword(":price/open")
	priceHigh := datalog.NewKeyword(":price/high")
	priceLow := datalog.NewKeyword(":price/low")
	priceClose := datalog.NewKeyword(":price/close")
	priceVolume := datalog.NewKeyword(":price/volume")

	loc, _ := time.LoadLocation("America/New_York")

	// Create 10 symbols with 1,000 bars each = 10,000 price entities total
	// We query for 1 symbol's bars (1,000) but scan all 10,000 without range narrowing
	// Production has ~3,500 bars × 8 attributes = 28,000 datoms
	symbols := []string{"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "AMD", "INTC", "NFLX"}
	symbolEntities := make(map[string]datalog.Identity)

	tx := db.NewTransaction()
	for _, sym := range symbols {
		symbolEntity := datalog.NewIdentity(sym)
		symbolEntities[sym] = symbolEntity
		err = tx.Add(symbolEntity, symbolKw, sym)
		assert.NoError(t, err)
	}
	_, err = tx.Commit()
	assert.NoError(t, err)

	// Add 1,000 price bars per symbol (10,000 total, 80,000 datoms with 8 attributes each)
	// Match production schema exactly
	batchSize := 50
	baseTime := time.Date(2025, 8, 1, 9, 30, 0, 0, loc)
	for _, sym := range symbols {
		for batch := 0; batch < 1000; batch += batchSize {
			tx := db.NewTransaction()
			for i := batch; i < batch+batchSize && i < 1000; i++ {
				priceEntity := datalog.NewIdentity(fmt.Sprintf("%s-bar-%d", sym, i))
				barTime := baseTime.Add(time.Duration(i) * time.Minute)
				minuteOfDay := int64(barTime.Hour()*60 + barTime.Minute())

				tx.Add(priceEntity, priceSymbol, symbolEntities[sym])
				tx.Add(priceEntity, priceTime, barTime)
				tx.Add(priceEntity, priceMinuteOfDay, minuteOfDay)
				tx.Add(priceEntity, priceOpen, float64(100+i%100))
				tx.Add(priceEntity, priceHigh, float64(105+i%100))
				tx.Add(priceEntity, priceLow, float64(95+i%100))
				tx.Add(priceEntity, priceClose, float64(102+i%100))
				tx.Add(priceEntity, priceVolume, int64(1000+i))
			}
			_, err := tx.Commit()
			assert.NoError(t, err)
		}
	}

	// Production query that hangs:
	// 1. [?s :symbol/ticker "AAPL"] returns 1 entity
	// 2. [?e :price/symbol ?s] should scan only ~1,000 AAPL price bars
	//    BUT: calculatePatternScanRange() ignores bound ?s, scans ALL 10,000 price bars (all symbols)
	// 3. Then 8 more attribute patterns + time extractions + predicates + aggregations
	// 4. With 80,000 datoms and scanning 10× more than needed, this becomes 10+ second hang
	queryStr := `[:find ?year ?month ?day (min ?open) (max ?high) (min ?low) (max ?close) (sum ?volume)
	              :where [?s :symbol/ticker "AAPL"]
	                     [?e :price/symbol ?s]
	                     [?e :price/time ?time]
	                     [(year ?time) ?year]
	                     [(month ?time) ?month]
	                     [(day ?time) ?day]
	                     [?e :price/minute-of-day ?mod]
	                     [(>= ?mod 570)]
	                     [(<= ?mod 960)]
	                     [?e :price/open ?open]
	                     [?e :price/high ?high]
	                     [?e :price/low ?low]
	                     [?e :price/close ?close]
	                     [?e :price/volume ?volume]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	// Force HashJoinScan (our default now) and use same options as datalog-cli
	// NOTE: db.NewExecutorWithOptions creates matcher with IndexNestedLoopThreshold: 0 by default
	exec := db.NewExecutorWithOptions(DefaultPlannerOptions())

	// Time the query - should be <100ms but currently can be 10+ seconds
	start := time.Now()
	result, err := exec.Execute(q)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.False(t, result.IsEmpty(), "Should have aggregation results")

	t.Logf("Query took %v for aggregation over %d AAPL bars (but scanned all %d bars)",
		elapsed, 1000, 10000)

	// This test will FAIL with current implementation (takes multiple seconds)
	// After fix (using bound values in scan range), should be <500ms
	if elapsed > 2*time.Second {
		t.Errorf("Query took %v - HashJoinScan is scanning ALL :price/symbol datoms (%d) instead of just AAPL's (%d)",
			elapsed, 10000, 1000)
		t.Logf("Bug: calculatePatternScanRange() only looks at constants, ignores bound variables")
		t.Logf("Fix: Check if pattern variables have bound values in binding relation and use them for scan range")
		t.Logf("With 80,000 total datoms and 10× unnecessary scanning, this causes production hang")
	}

	// Verify we got aggregated results (should be grouped by year/month/day)
	it := result.Iterator()
	defer it.Close()
	count := 0
	for it.Next() {
		tuple := it.Tuple()
		t.Logf("Aggregation result: year=%v month=%v day=%v", tuple[0], tuple[1], tuple[2])
		count++
	}
	assert.Greater(t, count, 0, "Should have at least one aggregated day")
}

// BenchmarkHashJoinScanRangeComparison benchmarks the performance difference
// between scanning entire attribute range vs using bound values
func BenchmarkHashJoinScanRangeComparison(b *testing.B) {
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	symbolKw := datalog.NewKeyword(":symbol/ticker")
	priceSymbol := datalog.NewKeyword(":price/symbol")
	priceOpen := datalog.NewKeyword(":price/open")

	// Create 10 symbols with 1000 bars each = 10,000 price entities
	symbols := make([]string, 10)
	symbolEntities := make(map[string]datalog.Identity)
	for i := 0; i < 10; i++ {
		sym := fmt.Sprintf("SYM%d", i)
		symbols[i] = sym
		symbolEntities[sym] = datalog.NewIdentity(sym)
	}

	tx := db.NewTransaction()
	for _, sym := range symbols {
		tx.Add(symbolEntities[sym], symbolKw, sym)
	}
	_, err = tx.Commit()
	if err != nil {
		b.Fatal(err)
	}

	batchSize := 100
	for _, sym := range symbols {
		for batch := 0; batch < 1000; batch += batchSize {
			tx := db.NewTransaction()
			for i := batch; i < batch+batchSize && i < 1000; i++ {
				priceEntity := datalog.NewIdentity(fmt.Sprintf("%s-bar-%d", sym, i))
				tx.Add(priceEntity, priceSymbol, symbolEntities[sym])
				tx.Add(priceEntity, priceOpen, float64(100+i))
			}
			_, err := tx.Commit()
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	queryStr := `[:find ?e ?open
	              :where [?s :symbol/ticker "SYM0"]
	                     [?e :price/symbol ?s]
	                     [?e :price/open ?open]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("HashJoinScan_Current", func(b *testing.B) {
		matcher := NewBadgerMatcherWithOptions(db.Store(), executor.ExecutorOptions{
			IndexNestedLoopThreshold: 0, // Force HashJoinScan
		})
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatal(err)
			}
			if result.Size() != 1000 {
				b.Fatalf("Expected 1000 results, got %d", result.Size())
			}
		}
	})

	b.Run("IndexNestedLoop_Baseline", func(b *testing.B) {
		matcher := NewBadgerMatcherWithOptions(db.Store(), executor.ExecutorOptions{
			IndexNestedLoopThreshold: 999999, // Force IndexNestedLoop
		})
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatal(err)
			}
			if result.Size() != 1000 {
				b.Fatalf("Expected 1000 results, got %d", result.Size())
			}
		}
	})
}
