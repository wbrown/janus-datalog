package executor

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLazySeq_ThunkCalledOnce verifies that the thunk executes exactly once
// regardless of how many times First()/Rest()/Empty() are called.
func TestLazySeq_ThunkCalledOnce(t *testing.T) {
	var callCount int32
	seq := &LazySeq{}
	seq.thunk = func() {
		atomic.AddInt32(&callCount, 1)
		seq.hasElems = true
		seq.first = 42
	}

	// Multiple accesses should all return 42, thunk called once
	v1, err := seq.First()
	require.NoError(t, err)
	assert.Equal(t, 42, v1)

	v2, err := seq.First()
	require.NoError(t, err)
	assert.Equal(t, 42, v2)

	_ = seq.Empty()
	_, _ = seq.Rest()

	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount), "thunk should be called exactly once")
}

// TestLazySeq_ThreadSafe verifies that 100 goroutines calling First()
// concurrently all get the same value and the thunk runs once.
func TestLazySeq_ThreadSafe(t *testing.T) {
	var callCount int32
	seq := &LazySeq{}
	seq.thunk = func() {
		atomic.AddInt32(&callCount, 1)
		seq.hasElems = true
		seq.first = "hello"
	}

	var wg sync.WaitGroup
	results := make([]any, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			v, _ := seq.First()
			results[idx] = v
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&callCount))
	for i, v := range results {
		assert.Equal(t, "hello", v, "goroutine %d got wrong value", i)
	}
}

// TestLazySeq_EmptySeq verifies that a nil-returning thunk produces an empty seq.
func TestLazySeq_EmptySeq(t *testing.T) {
	seq := &LazySeq{}
	seq.thunk = func() {} // does nothing — hasElems stays false

	assert.True(t, seq.Empty())
	v, err := seq.First()
	require.NoError(t, err)
	assert.Nil(t, v)
	r, err := seq.Rest()
	require.NoError(t, err)
	assert.Nil(t, r)
}

// TestLazySeq_ErrorPropagation verifies that thunk errors propagate through First().
func TestLazySeq_ErrorPropagation(t *testing.T) {
	seq := &LazySeq{}
	seq.thunk = func() {
		seq.err = assert.AnError
	}

	// Empty returns false so caller proceeds to First() which surfaces the error
	assert.False(t, seq.Empty())
	_, err := seq.First()
	assert.Error(t, err)
}

// TestLazySeq_ChainedCells verifies the cons-cell structure: 3 elements in a chain.
func TestLazySeq_ChainedCells(t *testing.T) {
	// Build chain: 1 → 2 → 3 → nil using thunks that set fields directly
	cell3 := &LazySeq{}
	cell3.thunk = func() { cell3.hasElems = true; cell3.first = 3 }

	cell2 := &LazySeq{}
	cell2.thunk = func() { cell2.hasElems = true; cell2.first = 2; cell2.rest = cell3 }

	cell1 := &LazySeq{}
	cell1.thunk = func() { cell1.hasElems = true; cell1.first = 1; cell1.rest = cell2 }

	v, _ := cell1.First()
	assert.Equal(t, 1, v)

	r, _ := cell1.Rest()
	c2 := r.(*LazySeq)
	v, _ = c2.First()
	assert.Equal(t, 2, v)

	r, _ = c2.Rest()
	c3 := r.(*LazySeq)
	v, _ = c3.First()
	assert.Equal(t, 3, v)

	r, _ = c3.Rest()
	assert.Nil(t, r)

	// Zero-value LazySeq with no thunk is empty
	assert.True(t, (&LazySeq{}).Empty())
}

// --- Tuple-specific tests using Iterator ---

// lazySeqTestIterator provides a controllable iterator for testing.
type lazySeqTestIterator struct {
	tuples  []Tuple
	pos     int
	closed  bool
	closeMu sync.Mutex
	err     error
}

func newLazySeqTestIterator(tuples []Tuple) *lazySeqTestIterator {
	return &lazySeqTestIterator{tuples: tuples}
}

func (m *lazySeqTestIterator) Next() bool {
	if m.pos >= len(m.tuples) {
		return false
	}
	m.pos++
	return true
}

func (m *lazySeqTestIterator) Tuple() Tuple {
	return m.tuples[m.pos-1]
}

func (m *lazySeqTestIterator) Close() error {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	m.closed = true
	return nil
}

func (m *lazySeqTestIterator) Error() error { return m.err }

func (m *lazySeqTestIterator) IsClosed() bool {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	return m.closed
}

// TestTupleSeq_LazyConsumption verifies that wrapping a 1000-tuple iterator
// in NewTupleSeq and consuming 2 elements only advances the iterator 2 steps.
func TestTupleSeq_LazyConsumption(t *testing.T) {
	tuples := make([]Tuple, 1000)
	for i := range tuples {
		tuples[i] = Tuple{i}
	}
	it := newLazySeqTestIterator(tuples)

	seq := NewTupleSeq(it, false)

	// Consume first element
	v, err := seq.First()
	require.NoError(t, err)
	assert.Equal(t, Tuple{0}, v)

	// Same element again (cached)
	v2, _ := seq.First()
	assert.Equal(t, v, v2)

	// Consume second element
	rest, _ := seq.Rest()
	require.NotNil(t, rest)
	v3, _ := rest.(*LazySeq).First()
	assert.Equal(t, Tuple{1}, v3)

	// Iterator should have advanced exactly 2 steps
	assert.Equal(t, 2, it.pos, "iterator should have advanced exactly 2 steps, not %d", it.pos)
}

// TestTupleSeq_MultipleConsumers verifies that two consumers share the same
// LazySeq cells. Consumer A realizes cells 0-49, Consumer B reads those from
// cache then realizes cells 50-99.
func TestTupleSeq_MultipleConsumers(t *testing.T) {
	tuples := make([]Tuple, 100)
	for i := range tuples {
		tuples[i] = Tuple{i}
	}
	it := newLazySeqTestIterator(tuples)

	seq := NewTupleSeq(it, false)

	// Consumer A: realize first 50 cells
	cur := seq
	for i := 0; i < 50; i++ {
		v, err := cur.First()
		require.NoError(t, err)
		assert.Equal(t, Tuple{i}, v)
		r, _ := cur.Rest()
		if r == nil {
			break
		}
		cur = r.(*LazySeq)
	}
	assert.Equal(t, 50, it.pos, "consumer A should have advanced iterator to 50")

	// Consumer B: start from beginning, read cached cells
	cur = seq
	for i := 0; i < 100; i++ {
		v, err := cur.First()
		require.NoError(t, err)
		assert.Equal(t, Tuple{i}, v)
		r, _ := cur.Rest()
		if r == nil {
			break
		}
		cur = r.(*LazySeq)
	}
	// Iterator should be at 100 (consumer B drove it from 50 to 100)
	assert.Equal(t, 100, it.pos)
}

// TestTupleSeq_IteratorCloseOnExhaustion verifies that the iterator is
// closed automatically when all elements are consumed.
func TestTupleSeq_IteratorCloseOnExhaustion(t *testing.T) {
	tuples := []Tuple{{1}, {2}, {3}, {4}, {5}}
	it := newLazySeqTestIterator(tuples)

	seq := NewTupleSeq(it, false)

	// Consume all 5 elements
	cur := seq
	for i := 0; i < 5; i++ {
		_, err := cur.First()
		require.NoError(t, err)
		r, _ := cur.Rest()
		if r == nil {
			break
		}
		cur = r.(*LazySeq)
	}

	// Try to get one more (should be empty, triggering close)
	r, _ := cur.Rest()
	if r != nil {
		ls := r.(*LazySeq)
		_ = ls.Empty() // Force realization of the terminating cell
	}

	assert.True(t, it.IsClosed(), "iterator should be closed after exhaustion")
}

// TestTupleSeq_IterGuardFinalizer verifies that dropping all references
// to the LazySeq closes the iterator via the GC finalizer.
func TestTupleSeq_IterGuardFinalizer(t *testing.T) {
	tuples := make([]Tuple, 100)
	for i := range tuples {
		tuples[i] = Tuple{i}
	}
	it := newLazySeqTestIterator(tuples)

	func() {
		seq := NewTupleSeq(it, false)
		// Consume 2 elements to partially realize
		_, _ = seq.First()
		r, _ := seq.Rest()
		_, _ = r.(*LazySeq).First()
		// seq goes out of scope here
	}()

	// Force GC to run finalizers
	runtime.GC()
	runtime.GC() // Run twice to ensure finalizer fires

	assert.True(t, it.IsClosed(), "iterator should be closed by GC finalizer")
}

// TestTupleSeq_NeedsCopy verifies that when needsCopy=true, tuples are
// copied before caching so mutations to the original don't affect cached cells.
func TestTupleSeq_NeedsCopy(t *testing.T) {
	// Use a shared buffer that gets overwritten (simulating RequiresCopy behavior)
	shared := Tuple{0, "original"}
	tuples := []Tuple{shared}
	it := newLazySeqTestIterator(tuples)

	seq := NewTupleSeq(it, true)

	v, err := seq.First()
	require.NoError(t, err)
	tuple := v.(Tuple)
	assert.Equal(t, "original", tuple[1])

	// Mutate the source tuple
	tuples[0][1] = "mutated"

	// Cached value should be unaffected
	v2, _ := seq.First()
	tuple2 := v2.(Tuple)
	assert.Equal(t, "original", tuple2[1], "cached tuple should not be affected by source mutation")
}
