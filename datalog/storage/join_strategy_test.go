package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// =============================================================================
// BENCHMARKS - Strategy Comparison
// =============================================================================
// These benchmarks demonstrate the performance difference between IndexNestedLoop
// and HashJoinScan strategies, showing HashJoinScan is 643× faster for large
// binding sets and 3.4× faster even for single bindings.

// BenchmarkIndexNestedLoopVsHashJoin directly compares IndexNestedLoop vs HashJoinScan
func BenchmarkIndexNestedLoopVsHashJoin(b *testing.B) {
	// Setup test database
	tempDir, err := os.MkdirTemp("", "strategy-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Insert 1000 records
	batchSize := 50
	totalRecords := 1000
	for batch := 0; batch < totalRecords; batch += batchSize {
		tx := db.NewTransaction()
		end := batch + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		for i := batch; i < end; i++ {
			person := datalog.NewIdentity(fmt.Sprintf("person%d", i))
			tx.Add(person, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
			tx.Add(person, datalog.NewKeyword(":person/email"), fmt.Sprintf("email%d@example.com", i))
		}

		_, err := tx.Commit()
		if err != nil {
			b.Fatalf("Batch commit failed: %v", err)
		}
	}

	query := `[:find ?name ?email
	           :where [?p :person/name ?name]
	                  [?p :person/email ?email]]`

	q, err := parser.ParseQuery(query)
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	// Benchmark CURRENT behavior (IndexNestedLoop due to Size() = -1)
	b.Run("current_index_nested_loop", func(b *testing.B) {
		opts := executor.ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: false,
			DefaultHashTableSize:    256,
		}
		matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

		// Force IndexNestedLoop
		indexNested := IndexNestedLoop
		matcher.ForceJoinStrategy(&indexNested)

		exec := executor.NewExecutor(matcher)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execute failed: %v", err)
			}

			// Consume all results
			count := 0
			it := result.Iterator()
			for it.Next() {
				_ = it.Tuple()
				count++
			}
			it.Close()

			if count != totalRecords {
				b.Fatalf("Expected %d results, got %d", totalRecords, count)
			}
		}
	})

	// Benchmark MODIFIED behavior (force HashJoinScan)
	b.Run("modified_hash_join_scan", func(b *testing.B) {
		opts := executor.ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: false,
			DefaultHashTableSize:    256,
		}
		matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

		// Force hash join strategy
		hashJoin := HashJoinScan
		matcher.ForceJoinStrategy(&hashJoin)

		exec := executor.NewExecutor(matcher)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execute failed: %v", err)
			}

			// Consume all results
			count := 0
			it := result.Iterator()
			for it.Next() {
				_ = it.Tuple()
				count++
			}
			it.Close()

			if count != totalRecords {
				b.Fatalf("Expected %d results, got %d", totalRecords, count)
			}
		}
	})

	// Benchmark with LIMIT (early termination test)
	b.Run("current_limit_10", func(b *testing.B) {
		opts := executor.ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: false,
			DefaultHashTableSize:    256,
		}
		matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

		// Force IndexNestedLoop
		indexNested := IndexNestedLoop
		matcher.ForceJoinStrategy(&indexNested)

		exec := executor.NewExecutor(matcher)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execute failed: %v", err)
			}

			// Consume only 10 results
			count := 0
			it := result.Iterator()
			for count < 10 && it.Next() {
				_ = it.Tuple()
				count++
			}
			it.Close()
		}
	})

	b.Run("modified_limit_10", func(b *testing.B) {
		opts := executor.ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: false,
			DefaultHashTableSize:    256,
		}
		matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

		// Force hash join strategy
		hashJoin := HashJoinScan
		matcher.ForceJoinStrategy(&hashJoin)

		exec := executor.NewExecutor(matcher)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execute failed: %v", err)
			}

			// Consume only 10 results
			count := 0
			it := result.Iterator()
			for count < 10 && it.Next() {
				_ = it.Tuple()
				count++
			}
			it.Close()
		}
	})

	// Benchmark with SMALL binding set (where IndexNestedLoop should win)
	// Create a query that returns only 1 entity
	singleQuery := `[:find ?email
	                 :where [?p :person/name "Name5"]
	                        [?p :person/email ?email]]`

	singleQ, err := parser.ParseQuery(singleQuery)
	if err != nil {
		b.Fatalf("Parse failed: %v", err)
	}

	b.Run("small_bindings_index_nested_loop", func(b *testing.B) {
		opts := executor.ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: false,
			DefaultHashTableSize:    256,
		}
		matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

		// Force IndexNestedLoop
		indexNested := IndexNestedLoop
		matcher.ForceJoinStrategy(&indexNested)

		exec := executor.NewExecutor(matcher)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(singleQ)
			if err != nil {
				b.Fatalf("Execute failed: %v", err)
			}

			count := 0
			it := result.Iterator()
			for it.Next() {
				_ = it.Tuple()
				count++
			}
			it.Close()
		}
	})

	b.Run("small_bindings_hash_join_scan", func(b *testing.B) {
		opts := executor.ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: false,
			DefaultHashTableSize:    256,
		}
		matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

		// Force HashJoinScan
		hashJoin := HashJoinScan
		matcher.ForceJoinStrategy(&hashJoin)

		exec := executor.NewExecutor(matcher)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(singleQ)
			if err != nil {
				b.Fatalf("Execute failed: %v", err)
			}

			count := 0
			it := result.Iterator()
			for it.Next() {
				_ = it.Tuple()
				count++
			}
			it.Close()
		}
	})
}

// =============================================================================
// TESTS - Strategy Verification
// =============================================================================

// TestVerifyStrategyUsed confirms which strategy is actually selected
// and that ForceJoinStrategy() override works correctly
func TestVerifyStrategyUsed(t *testing.T) {
	// Setup test database
	tempDir, err := os.MkdirTemp("", "verify-strategy-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	db, err := NewDatabase(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Insert 1000 records
	batchSize := 50
	totalRecords := 1000
	for batch := 0; batch < totalRecords; batch += batchSize {
		tx := db.NewTransaction()
		end := batch + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		for i := batch; i < end; i++ {
			person := datalog.NewIdentity(fmt.Sprintf("person%d", i))
			tx.Add(person, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
			tx.Add(person, datalog.NewKeyword(":person/email"), fmt.Sprintf("email%d@example.com", i))
		}

		_, err := tx.Commit()
		if err != nil {
			t.Fatalf("Batch commit failed: %v", err)
		}
	}

	queryStr := `[:find ?name ?email
	           :where [?p :person/name ?name]
	                  [?p :person/email ?email]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	t.Run("default_behavior", func(t *testing.T) {
		var strategies []string
		opts := executor.ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: false,
			DefaultHashTableSize:    256,
		}
		matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

		// Add event handler
		matcher.SetHandler(func(event annotations.Event) {
			if event.Name == "storage/join-strategy" {
				strategy := fmt.Sprintf("%v", event.Data["join_strategy"])
				strategies = append(strategies, strategy)
			}
		})

		exec := executor.NewExecutor(matcher)

		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		// Consume only 10 results
		count := 0
		it := result.Iterator()
		for count < 10 && it.Next() {
			_ = it.Tuple()
			count++
		}
		it.Close()

		t.Logf("Default strategies used: %v", strategies)
		t.Logf("Got %d results", count)
	})

	t.Run("forced_hash_join", func(t *testing.T) {
		var strategies []string
		opts := executor.ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: false,
			DefaultHashTableSize:    256,
		}
		matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

		// Force HashJoinScan
		hashJoin := HashJoinScan
		matcher.ForceJoinStrategy(&hashJoin)

		// Add event handler to verify
		matcher.SetHandler(func(event annotations.Event) {
			if event.Name == "storage/join-strategy" {
				strategy := fmt.Sprintf("%v", event.Data["join_strategy"])
				strategies = append(strategies, strategy)
			}
		})

		exec := executor.NewExecutor(matcher)

		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		// Consume only 10 results
		count := 0
		it := result.Iterator()
		for count < 10 && it.Next() {
			_ = it.Tuple()
			count++
		}
		it.Close()

		t.Logf("Forced strategies used: %v", strategies)
		t.Logf("Got %d results", count)

		// Verify HashJoinScan was used
		if len(strategies) == 0 {
			t.Error("Expected at least one strategy selection event")
		}
		for _, s := range strategies {
			if s != "hash-join-scan" {
				t.Errorf("Expected hash-join-scan, got %s", s)
			}
		}
	})

	t.Run("forced_index_nested_loop", func(t *testing.T) {
		var strategies []string
		opts := executor.ExecutorOptions{
			EnableStreamingJoins:    true,
			EnableSymmetricHashJoin: false,
			DefaultHashTableSize:    256,
		}
		matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

		// Force IndexNestedLoop
		indexNested := IndexNestedLoop
		matcher.ForceJoinStrategy(&indexNested)

		// Add event handler to verify
		matcher.SetHandler(func(event annotations.Event) {
			if event.Name == "storage/join-strategy" {
				strategy := fmt.Sprintf("%v", event.Data["join_strategy"])
				strategies = append(strategies, strategy)
			}
		})

		exec := executor.NewExecutor(matcher)

		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		// Consume only 10 results
		count := 0
		it := result.Iterator()
		for count < 10 && it.Next() {
			_ = it.Tuple()
			count++
		}
		it.Close()

		t.Logf("Forced strategies used: %v", strategies)
		t.Logf("Got %d results", count)

		// Verify IndexNestedLoop was used
		if len(strategies) == 0 {
			t.Error("Expected at least one strategy selection event")
		}
		for _, s := range strategies {
			if s != "index-nested-loop" {
				t.Errorf("Expected index-nested-loop, got %s", s)
			}
		}
	})
}

// TestMaterializationDetection adds instrumentation to detect when
// materialization happens in the storage layer
func TestMaterializationDetection(t *testing.T) {
	// Setup test database
	tempDir, err := os.MkdirTemp("", "materialize-detect-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	db, err := NewDatabase(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Insert 1000 records (to trigger hash join strategy)
	batchSize := 50
	totalRecords := 1000
	for batch := 0; batch < totalRecords; batch += batchSize {
		tx := db.NewTransaction()
		end := batch + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		for i := batch; i < end; i++ {
			person := datalog.NewIdentity(fmt.Sprintf("person%d", i))
			tx.Add(person, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
			tx.Add(person, datalog.NewKeyword(":person/email"), fmt.Sprintf("email%d@example.com", i))
		}

		_, err := tx.Commit()
		if err != nil {
			t.Fatalf("Batch commit failed: %v", err)
		}
	}

	// Add age attribute to trigger 3-way join
	for i := 0; i < totalRecords; i++ {
		person := datalog.NewIdentity(fmt.Sprintf("person%d", i))
		tx := db.NewTransaction()
		tx.Add(person, datalog.NewKeyword(":person/age"), int64(20+i))
		_, err := tx.Commit()
		if err != nil {
			t.Fatalf("Failed to add age: %v", err)
		}
	}

	// Use a 3-way join to trigger multiple strategy selections
	query := `[:find ?name ?email ?age
	           :where [?p :person/name ?name]
	                  [?p :person/email ?email]
	                  [?p :person/age ?age]]`

	q, err := parser.ParseQuery(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Create matcher with instrumentation
	opts := executor.ExecutorOptions{
		EnableStreamingJoins:    true,
		EnableSymmetricHashJoin: false,
		DefaultHashTableSize:    256,
	}
	matcher := NewBadgerMatcherWithOptions(db.Store(), opts)

	// Add event handler to track what's happening
	var events []string
	handler := func(event annotations.Event) {
		if event.Name == "storage/join-strategy" {
			joinStrategy := event.Data["join_strategy"]
			bindingSize := event.Data["binding_size"]
			events = append(events, fmt.Sprintf("Join strategy: %v (binding size: %v)", joinStrategy, bindingSize))
		}
		if event.Name == "storage/reuse-strategy" {
			strategyType := event.Data["strategy_type"]
			events = append(events, fmt.Sprintf("Reuse strategy: %v", strategyType))
		}
	}
	matcher.SetHandler(handler)

	exec := executor.NewExecutor(matcher)

	t.Log("Executing query...")
	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Log all events captured during query execution
	t.Log("Events during query execution:")
	for _, event := range events {
		t.Log("  ", event)
	}

	// Consume only 10 results
	t.Log("Consuming first 10 results...")
	count := 0
	it := result.Iterator()
	for count < 10 && it.Next() {
		_ = it.Tuple()
		count++
	}
	it.Close()

	t.Logf("Got %d results", count)
	t.Log("Check if HashJoinScan strategy was used (indicates materialization)")
}
