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
// This reproduces a concurrent map access bug.
func TestTupleBuilderCacheConcurrency(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	matcher := NewPatternMatcher(db.Store())

	// Create test pattern and symbols
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":test/attr")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
	symbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}

	// Spawn 1000 goroutines accessing cache concurrently
	// This should trigger the concurrent map access bug if not fixed
	var wg sync.WaitGroup
	errorChan := make(chan error, 1000)

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				builder := matcher.getTupleBuilder(pattern, symbols)
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

	baseMatcher := NewPatternMatcher(db.Store())
	asOfMatcher := baseMatcher.AsOf(datalog.ElementID{Lamport: 100})

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":test/attr")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
	symbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}

	// Get builder from base matcher
	builder1 := baseMatcher.getTupleBuilder(pattern, symbols)

	// Get builder from AsOf matcher - should be the same instance (shared cache)
	builder2 := asOfMatcher.getTupleBuilder(pattern, symbols)

	if builder1 != builder2 {
		t.Error("AsOf matcher should share cache with base matcher")
	}
}

// TestDatabaseMatchersShareTupleBuilders pins that every matcher a Database
// mints shares one structurally-keyed builder population: builders warmed by
// one matcher serve the next, instead of every Matcher() call starting with
// an empty cache. Temporal handles inherit the parent's population.
func TestDatabaseMatchersShareTupleBuilders(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":test/attr")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
	symbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}

	first := db.Matcher().(*PatternMatcher)
	second := db.Matcher().(*PatternMatcher)
	builder1 := first.getTupleBuilder(pattern, symbols)
	builder2 := second.getTupleBuilder(pattern, symbols)
	if builder1 != builder2 {
		t.Error("matchers minted by one Database must share tuple builders")
	}

	histBuilder := db.History().Matcher().(*PatternMatcher).getTupleBuilder(pattern, symbols)
	if histBuilder != builder1 {
		t.Error("temporal-handle matchers must share the parent Database's tuple builders")
	}
}

// TestTupleBuilderCacheKeysOnStructure pins that the cache key is the
// builder's structural identity — position-variable placement and output
// symbols — never the pattern's rendered text. Constants contribute nothing
// to a builder, so patterns differing only in a constant share one cache
// entry; keying on rendered constants grew the cache per distinct entity
// and paid an L85 render per Match (the per-entity resolve allocation
// regression in docs/perf/storage_correctness_campaign_benchstat_2026-07-20.txt).
func TestTupleBuilderCacheKeysOnStructure(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	matcher := NewPatternMatcher(db.Store())
	symbols := []query.Symbol{datalog.NewSymbol("?v")}

	patternBoundTo := func(seed string) *query.DataPattern {
		return &query.DataPattern{
			Elements: []query.PatternElement{
				query.Constant{Value: datalog.NewIdentity(seed)},
				query.Constant{Value: datalog.NewKeyword(":test/attr")},
				query.Variable{Name: datalog.NewSymbol("?v")},
			},
		}
	}

	builderAlice := matcher.getTupleBuilder(patternBoundTo("alice"), symbols)
	builderBob := matcher.getTupleBuilder(patternBoundTo("bob"), symbols)
	if builderAlice != builderBob {
		t.Error("patterns differing only in a constant must share one builder")
	}
}

// TestTupleBuilderCacheLookupDoesNotAllocate pins the warm-cache lookup at
// zero allocations: the key is a stack-built struct over a typed map, so a
// cache whose purpose is avoiding work never allocates to ask for it.
func TestTupleBuilderCacheLookupDoesNotAllocate(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	matcher := NewPatternMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: datalog.NewIdentity("alice")},
			query.Constant{Value: datalog.NewKeyword(":test/attr")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
	symbols := []query.Symbol{datalog.NewSymbol("?v")}
	matcher.getTupleBuilder(pattern, symbols)

	allocs := testing.AllocsPerRun(100, func() {
		matcher.getTupleBuilder(pattern, symbols)
	})
	if allocs != 0 {
		t.Errorf("warm tuple-builder lookup allocated %v times per call, want 0", allocs)
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

	matcher := NewPatternMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":test/value")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	// Create two iterators on the same pattern
	rel1, err := matcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatal(err)
	}
	rel2, err := matcher.Match(query.PatternQuery(pattern), nil)
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

	matcher := NewPatternMatcher(db.Store())
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

			rel, err := matcher.Match(query.PatternQuery(pattern), nil)
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
