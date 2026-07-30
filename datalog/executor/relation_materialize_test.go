package executor

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestLazyMaterializationBasic tests basic lazy materialization behavior
func TestLazyMaterializationBasic(t *testing.T) {
	// Create a streaming relation with 10 tuples
	symbols := []query.Symbol{datalog.NewSymbol("?x")}
	tuples := make([]Tuple, 10)
	for i := 0; i < 10; i++ {
		tuples[i] = Tuple{int64(i)}
	}

	iter := &sliceIterator{tuples: tuples, pos: -1}

	opts := ExecutorOptions{EnableTrueStreaming: true}
	rel := NewStreamingRelationWithOptions(symbols, iter, opts)

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
	symbols := []query.Symbol{datalog.NewSymbol("?x")}
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
		symbols:  symbols,
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
	symbols := []query.Symbol{datalog.NewSymbol("?x")}
	tuples := []Tuple{{int64(1)}, {int64(2)}}
	iter := &sliceIterator{tuples: tuples, pos: -1}

	opts := ExecutorOptions{EnableTrueStreaming: true}
	rel := NewStreamingRelationWithOptions(symbols, iter, opts)

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
	symbols := []query.Symbol{datalog.NewSymbol("?x")}
	tuples := []Tuple{{int64(1)}, {int64(2)}}
	iter := &sliceIterator{tuples: tuples, pos: -1}

	opts := ExecutorOptions{EnableTrueStreaming: true}
	rel := NewStreamingRelationWithOptions(symbols, iter, opts)

	// First Iterator() - OK
	it1 := rel.Iterator()
	it1.Next()
	it1.Close()

	// Second Iterator() without Materialize() - should panic
	rel.Iterator()
}

// gatedSliceIterator yields its tuples but blocks on `gate` before the first
// one, closing `started` first. It lets a test pin a StreamingRelation in the
// caching-in-progress state with no timing assumptions: once `started` is
// closed, StreamingRelation.Iterator() has already set cachingInProgress (it
// does so before pulling the source), and the cache build is parked here, so it
// cannot complete until the test closes `gate`.
type gatedSliceIterator struct {
	tuples  []Tuple
	pos     int
	started chan struct{}
	gate    chan struct{}
	blocked bool
}

func (it *gatedSliceIterator) Next() bool {
	if !it.blocked {
		it.blocked = true
		close(it.started)
		<-it.gate
	}
	it.pos++
	return it.pos < len(it.tuples)
}

func (it *gatedSliceIterator) Tuple() Tuple { return it.tuples[it.pos] }
func (it *gatedSliceIterator) Close() error { return nil }
func (it *gatedSliceIterator) Error() error { return nil }

// TestSizeBlocksWhileCaching verifies that Size() called while the cache is
// being built returns the correct count (never -1 or a partial count). The
// gated source iterator makes the caching-in-progress state deterministic via
// channel handshakes, so the test has no timing assumptions and cannot flake.
func TestSizeBlocksWhileCaching(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?x")}
	tuples := make([]Tuple, 100)
	for i := 0; i < 100; i++ {
		tuples[i] = Tuple{int64(i)}
	}

	src := &gatedSliceIterator{
		tuples:  tuples,
		pos:     -1,
		started: make(chan struct{}),
		gate:    make(chan struct{}),
	}

	opts := ExecutorOptions{EnableTrueStreaming: true}
	rel := NewStreamingRelationWithOptions(symbols, src, opts).Materialize().(*StreamingRelation)

	// Build the cache in a goroutine; it parks inside the gated source.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		it := rel.Iterator()
		for it.Next() {
		}
		it.Close()
	}()

	// Handshake: once the source has been entered, Iterator() has set
	// cachingInProgress and the build is parked, so it cannot complete. From
	// here Size() can never observe the not-started state.
	<-src.started

	// Call Size() while caching is held open. cacheReady is false (the source
	// is parked, so the build cannot have finished), so Size() must take the
	// caching-in-progress branch and block on completion rather than fast-path.
	sizeCalled := make(chan struct{})
	sizeCh := make(chan int, 1)
	go func() {
		close(sizeCalled)
		sizeCh <- rel.Size()
	}()
	<-sizeCalled

	// Releasing the gate lets the build finish; only then can Size() return.
	close(src.gate)

	if size := <-sizeCh; size != 100 {
		t.Errorf("Size() during caching = %d, want 100", size)
	}
	wg.Wait()

	// After caching completes, Size() returns immediately.
	if size := rel.Size(); size != 100 {
		t.Errorf("Size() after caching = %d, want 100", size)
	}
}
