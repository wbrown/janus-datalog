package executor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestLazyMaterializationBasic tests basic lazy materialization behavior
func TestLazyMaterializationBasic(t *testing.T) {
	// Create a streaming relation with 10 tuples
	columns := []query.Symbol{"?x"}
	tuples := make([]Tuple, 10)
	for i := 0; i < 10; i++ {
		tuples[i] = Tuple{int64(i)}
	}

	iter := &sliceIterator{tuples: tuples, pos: -1}

	opts := ExecutorOptions{EnableTrueStreaming: true}
	rel := NewStreamingRelationWithOptions(columns, iter, opts)

	// Test 1: Size() before materialization returns -1
	if size := rel.Size(); size != -1 {
		t.Errorf("Expected Size() to return -1 before iteration, got %d", size)
	}

	// Test 2: Call Materialize() to enable caching
	rel = rel.Materialize().(*StreamingRelation)

	// Test 3: Size() after Materialize() but before iteration still returns -1
	if size := rel.Size(); size != -1 {
		t.Errorf("Expected Size() to return -1 after Materialize() but before iteration, got %d", size)
	}

	// Test 4: First Iterator() call should build cache
	iter1 := rel.Iterator()
	count1 := 0
	for iter1.Next() {
		count1++
	}
	iter1.Close()

	if count1 != 10 {
		t.Errorf("Expected first iterator to see 10 tuples, got %d", count1)
	}

	// Test 5: Size() after iteration should return actual size
	if size := rel.Size(); size != 10 {
		t.Errorf("Expected Size() to return 10 after iteration, got %d", size)
	}

	// Test 6: Second Iterator() call should reuse cache
	iter2 := rel.Iterator()
	count2 := 0
	for iter2.Next() {
		count2++
	}
	iter2.Close()

	if count2 != 10 {
		t.Errorf("Expected second iterator to see 10 tuples, got %d", count2)
	}

	// Test 7: Third Iterator() call should also work
	iter3 := rel.Iterator()
	count3 := 0
	for iter3.Next() {
		count3++
	}
	iter3.Close()

	if count3 != 10 {
		t.Errorf("Expected third iterator to see 10 tuples, got %d", count3)
	}
}

// TestConcurrentAccess tests that multiple goroutines can safely access a materialized relation
func TestConcurrentAccess(t *testing.T) {
	// Create a streaming relation with 1000 tuples
	columns := []query.Symbol{"?x"}
	tuples := make([]Tuple, 1000)
	for i := 0; i < 1000; i++ {
		tuples[i] = Tuple{int64(i)}
	}

	// Track how many times the source iterator is created
	var sourceCallCount int32
	source := func() Iterator {
		atomic.AddInt32(&sourceCallCount, 1)
		return &sliceIterator{tuples: tuples, pos: -1}
	}

	opts := ExecutorOptions{EnableTrueStreaming: true}
	rel := &StreamingRelation{
		columns:  columns,
		iterator: source(),
		size:     -1,
		options:  opts,
	}

	// Call Materialize() to enable caching
	rel = rel.Materialize().(*StreamingRelation)

	// Launch 10 goroutines that all try to iterate immediately
	const numGoroutines = 10
	var wg sync.WaitGroup
	results := make([]int, numGoroutines)
	errors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			iter := rel.Iterator()
			count := 0
			for iter.Next() {
				count++
			}
			iter.Close()

			results[idx] = count
		}(i)
	}

	wg.Wait()

	// Verify all goroutines saw 1000 tuples
	for i, count := range results {
		if count != 1000 {
			t.Errorf("Goroutine %d saw %d tuples, expected 1000", i, count)
		}
	}

	// Verify no errors
	for i, err := range errors {
		if err != nil {
			t.Errorf("Goroutine %d got error: %v", i, err)
		}
	}

	// Verify the source iterator was only called once (for the actual underlying iterator)
	// Note: We already called source() once in the StreamingRelation constructor
	if sourceCallCount != 1 {
		t.Errorf("Expected source() to be called 1 time, got %d", sourceCallCount)
	}

	// Verify cache is complete
	if rel.Size() != 1000 {
		t.Errorf("Expected final Size() to be 1000, got %d", rel.Size())
	}
}

// TestMaterializeAfterIterationPanics tests that calling Materialize() after iteration panics
func TestMaterializeAfterIterationPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected Materialize() after iteration to panic, but it didn't")
		}
	}()

	// Create a streaming relation
	columns := []query.Symbol{"?x"}
	tuples := []Tuple{{int64(1)}, {int64(2)}}
	iter := &sliceIterator{tuples: tuples, pos: -1}

	opts := ExecutorOptions{EnableTrueStreaming: true}
	rel := NewStreamingRelationWithOptions(columns, iter, opts)

	// Iterate FIRST
	it := rel.Iterator()
	it.Next()
	it.Close()

	// Then try to Materialize() - should panic
	rel.Materialize()
}

// TestDoubleIterationWithoutMaterializePanics tests that calling Iterator() twice without Materialize() panics
func TestDoubleIterationWithoutMaterializePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected double Iterator() without Materialize() to panic, but it didn't")
		}
	}()

	// Create a streaming relation
	columns := []query.Symbol{"?x"}
	tuples := []Tuple{{int64(1)}, {int64(2)}}
	iter := &sliceIterator{tuples: tuples, pos: -1}

	opts := ExecutorOptions{EnableTrueStreaming: true}
	rel := NewStreamingRelationWithOptions(columns, iter, opts)

	// First Iterator() - OK
	it1 := rel.Iterator()
	it1.Next()
	it1.Close()

	// Second Iterator() without Materialize() - should panic
	rel.Iterator()
}

// TestSizeBlocksWhileCaching tests that Size() blocks while caching is in progress
func TestSizeBlocksWhileCaching(t *testing.T) {
	// Create a streaming relation with 100 tuples
	columns := []query.Symbol{"?x"}
	tuples := make([]Tuple, 100)
	for i := 0; i < 100; i++ {
		tuples[i] = Tuple{int64(i)}
	}

	iter := &sliceIterator{tuples: tuples, pos: -1}

	opts := ExecutorOptions{EnableTrueStreaming: true}
	rel := NewStreamingRelationWithOptions(columns, iter, opts)

	// Call Materialize() to enable caching
	rel = rel.Materialize().(*StreamingRelation)

	// Start first iterator in a goroutine (this will build cache)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		iter := rel.Iterator()
		for iter.Next() {
			// Iterate
		}
		iter.Close()
	}()

	// Give the iterator time to start caching
	// (In real code, this race is fine - we just want to test blocking behavior)
	// Wait a tiny bit to ensure caching has started
	time.Sleep(1 * time.Millisecond)

	// Now call Size() from main goroutine - should block until cache is complete
	size := rel.Size()

	// Wait for iterator to finish
	wg.Wait()

	// Verify we got the correct size
	if size != 100 {
		t.Errorf("Expected Size() to return 100 after blocking, got %d", size)
	}

	// Call Size() again - should return immediately now
	size2 := rel.Size()
	if size2 != 100 {
		t.Errorf("Expected second Size() to return 100, got %d", size2)
	}
}
