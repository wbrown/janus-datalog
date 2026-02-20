package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestTupleBuilderCacheConcurrency tests that the tuple builder cache is thread-safe
// This reproduces the concurrent map access bug reported by gopher-street team
func TestTupleBuilderCacheConcurrency(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	matcher := NewBadgerMatcher(db.Store())

	// Create test pattern and columns
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":test/attr")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
	columns := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}

	// Spawn 1000 goroutines accessing cache concurrently
	// This should trigger the concurrent map access bug if not fixed
	var wg sync.WaitGroup
	errorChan := make(chan error, 1000)

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				builder := matcher.getTupleBuilder(pattern, columns)
				if builder == nil {
					select {
					case errorChan <- nil:
					default:
					}
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errorChan)

	// Check if any goroutine reported an error
	if err := <-errorChan; err != nil {
		t.Errorf("getTupleBuilder returned nil")
	}
}

// TestTupleBuilderCacheSharing tests that AsOf matchers share the cache correctly
func TestTupleBuilderCacheSharing(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	baseMatcher := NewBadgerMatcher(db.Store())
	asOfMatcher := baseMatcher.AsOf(datalog.ElementID{Lamport: 100})

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":test/attr")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
	columns := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}

	// Get builder from base matcher
	builder1 := baseMatcher.getTupleBuilder(pattern, columns)

	// Get builder from AsOf matcher - should be the same instance (shared cache)
	builder2 := asOfMatcher.getTupleBuilder(pattern, columns)

	if builder1 != builder2 {
		t.Error("AsOf matcher should share cache with base matcher")
	}
}

func createTestDB(t *testing.T) *Database {
	t.Helper()
	db, err := NewDatabase(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	return db
}

// TestIteratorWorkspaceIsolation verifies that two iterators on the same
// pattern have independent tuple storage (workspace reuse doesn't cross iterators).
// This test ensures that after Phase 2 (workspace reuse), each iterator
// maintains its own workspace and doesn't corrupt other iterators' tuples.
func TestIteratorWorkspaceIsolation(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	// Add test data
	tx := db.NewTransaction()
	for i := 0; i < 10; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		tx.Add(e, datalog.NewKeyword(":test/value"), int64(i))
	}
	_, err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	matcher := NewBadgerMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":test/value")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	// Create two iterators on the same pattern
	rel1, err := matcher.Match(pattern, nil)
	if err != nil {
		t.Fatal(err)
	}
	rel2, err := matcher.Match(pattern, nil)
	if err != nil {
		t.Fatal(err)
	}

	it1 := rel1.Iterator()
	it2 := rel2.Iterator()
	defer it1.Close()
	defer it2.Close()

	// Advance it1 once and capture the tuple
	if !it1.Next() {
		t.Fatal("it1 should have at least one tuple")
	}
	originalTuple := it1.Tuple()
	tuple1Copy := make(executor.Tuple, len(originalTuple))
	copy(tuple1Copy, originalTuple)

	// Advance it2 through all its tuples - should not affect it1's current tuple
	count2 := 0
	for it2.Next() {
		_ = it2.Tuple()
		count2++
	}
	if count2 != 10 {
		t.Errorf("it2 expected 10 tuples, got %d", count2)
	}

	// Verify it1's tuple is unchanged after it2 iterated
	currentTuple := it1.Tuple()
	for i := range tuple1Copy {
		if tuple1Copy[i] != currentTuple[i] {
			t.Errorf("Iterator 1 tuple was corrupted by iterator 2 at index %d: expected %v, got %v",
				i, tuple1Copy[i], currentTuple[i])
		}
	}

	// Continue iterating it1 and count total
	count1 := 1 // Already advanced once
	for it1.Next() {
		count1++
	}
	if count1 != 10 {
		t.Errorf("it1 expected 10 tuples, got %d", count1)
	}
}

// TestIteratorWorkspaceConcurrency verifies no race conditions with
// concurrent iterator creation and iteration. This test should be run
// with -race flag to detect any data races introduced by workspace reuse.
func TestIteratorWorkspaceConcurrency(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	// Add test data - 100 entities
	tx := db.NewTransaction()
	for i := 0; i < 100; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		tx.Add(e, datalog.NewKeyword(":test/value"), int64(i))
	}
	_, err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	matcher := NewBadgerMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":test/value")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Spawn 100 goroutines, each creating its own iterator and iterating
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			rel, err := matcher.Match(pattern, nil)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: Match failed: %v", id, err)
				return
			}

			count := 0
			it := rel.Iterator()
			defer it.Close()

			for it.Next() {
				tuple := it.Tuple()
				// Verify tuple has expected structure
				if len(tuple) != 2 {
					errors <- fmt.Errorf("goroutine %d: tuple length %d, expected 2", id, len(tuple))
					return
				}
				count++
			}

			if count != 100 {
				errors <- fmt.Errorf("goroutine %d: expected 100 tuples, got %d", id, count)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Collect all errors
	var allErrors []error
	for err := range errors {
		allErrors = append(allErrors, err)
	}

	for _, err := range allErrors {
		t.Error(err)
	}
}
