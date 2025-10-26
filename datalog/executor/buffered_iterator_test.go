package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestBufferedIterator(t *testing.T) {
	t.Run("BasicIteration", func(t *testing.T) {
		// Create test data
		tuples := []Tuple{
			{1, "alice"},
			{2, "bob"},
			{3, "charlie"},
		}
		source := newMockIterator(tuples)
		buffered := NewBufferedIterator(source)

		// First iteration
		var results []Tuple
		for buffered.Next() {
			tuple := buffered.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			results = append(results, tupleCopy)
		}

		assert.Len(t, results, 3)
		assert.Equal(t, tuples[0], results[0])
		assert.Equal(t, tuples[1], results[1])
		assert.Equal(t, tuples[2], results[2])
	})

	t.Run("Reset", func(t *testing.T) {
		tuples := []Tuple{
			{1, "alice"},
			{2, "bob"},
		}
		source := newMockIterator(tuples)
		buffered := NewBufferedIterator(source)

		// First iteration - consume partially
		assert.True(t, buffered.Next())
		assert.Equal(t, tuples[0], buffered.Tuple())

		// Reset and iterate again
		buffered.Reset()
		var results []Tuple
		for buffered.Next() {
			tuple := buffered.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			results = append(results, tupleCopy)
		}

		assert.Len(t, results, 2)
		assert.Equal(t, tuples[0], results[0])
		assert.Equal(t, tuples[1], results[1])
	})

	t.Run("MultipleIterations", func(t *testing.T) {
		tuples := []Tuple{
			{1, "alice"},
			{2, "bob"},
			{3, "charlie"},
		}
		source := newMockIterator(tuples)
		buffered := NewBufferedIterator(source)

		// First full iteration
		count1 := 0
		for buffered.Next() {
			count1++
		}
		assert.Equal(t, 3, count1)

		// Reset and iterate again
		buffered.Reset()
		count2 := 0
		for buffered.Next() {
			count2++
		}
		assert.Equal(t, 3, count2)

		// Reset and iterate once more
		buffered.Reset()
		count3 := 0
		for buffered.Next() {
			count3++
		}
		assert.Equal(t, 3, count3)
	})

	t.Run("Size", func(t *testing.T) {
		tuples := []Tuple{
			{1, "alice"},
			{2, "bob"},
			{3, "charlie"},
			{4, "diana"},
		}
		source := newMockIterator(tuples)
		buffered := NewBufferedIterator(source)

		// Size should consume the source if needed
		assert.Equal(t, 4, buffered.Size())

		// Size should be consistent on subsequent calls
		assert.Equal(t, 4, buffered.Size())

		// Can still iterate after Size()
		buffered.Reset()
		count := 0
		for buffered.Next() {
			count++
		}
		assert.Equal(t, 4, count)
	})

	t.Run("IsEmpty", func(t *testing.T) {
		// Test with non-empty source
		tuples := []Tuple{{1, "alice"}}
		source := newMockIterator(tuples)
		buffered := NewBufferedIterator(source)

		assert.False(t, buffered.IsEmpty())

		// Can still iterate after IsEmpty()
		assert.True(t, buffered.Next())
		assert.Equal(t, tuples[0], buffered.Tuple())

		// Test with empty source
		emptySource := newMockIterator([]Tuple{})
		emptyBuffered := NewBufferedIterator(emptySource)
		assert.True(t, emptyBuffered.IsEmpty())
	})

	t.Run("Clone", func(t *testing.T) {
		tuples := []Tuple{
			{1, "alice"},
			{2, "bob"},
			{3, "charlie"},
		}
		source := newMockIterator(tuples)
		buffered := NewBufferedIterator(source)

		// Consume some from original
		assert.True(t, buffered.Next())
		assert.Equal(t, tuples[0], buffered.Tuple())

		// Clone should start from beginning
		cloned := buffered.Clone()
		var clonedResults []Tuple
		for cloned.Next() {
			tuple := cloned.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			clonedResults = append(clonedResults, tupleCopy)
		}

		assert.Len(t, clonedResults, 3)
		assert.Equal(t, tuples[0], clonedResults[0])
		assert.Equal(t, tuples[1], clonedResults[1])
		assert.Equal(t, tuples[2], clonedResults[2])

		// Original can continue where it left off
		assert.True(t, buffered.Next())
		assert.Equal(t, tuples[1], buffered.Tuple())
	})

	t.Run("EmptySource", func(t *testing.T) {
		source := newMockIterator([]Tuple{})
		buffered := NewBufferedIterator(source)

		assert.True(t, buffered.IsEmpty())
		assert.Equal(t, 0, buffered.Size())
		assert.False(t, buffered.Next())

		// Clone of empty should also be empty
		cloned := buffered.Clone()
		assert.False(t, cloned.Next())
	})

	t.Run("PartialConsumptionThenSize", func(t *testing.T) {
		tuples := []Tuple{
			{1, "alice"},
			{2, "bob"},
			{3, "charlie"},
		}
		source := newMockIterator(tuples)
		buffered := NewBufferedIterator(source)

		// Consume one tuple
		assert.True(t, buffered.Next())
		assert.Equal(t, tuples[0], buffered.Tuple())

		// Size should still return full size
		assert.Equal(t, 3, buffered.Size())

		// Can reset and iterate all
		buffered.Reset()
		count := 0
		for buffered.Next() {
			count++
		}
		assert.Equal(t, 3, count)
	})

	t.Run("IsEmptyDoesNotConsumeAll", func(t *testing.T) {
		// IsEmpty should only consume the first tuple, not all
		callCount := 0
		countingIterator := &countingMockIterator{
			tuples: []Tuple{
				{1, "alice"},
				{2, "bob"},
				{3, "charlie"},
			},
			nextCallCount: &callCount,
		}

		buffered := NewBufferedIterator(countingIterator)
		assert.False(t, buffered.IsEmpty())

		// Should have only called Next() once
		assert.Equal(t, 1, callCount, "IsEmpty should only consume first tuple")

		// Can still iterate all tuples
		count := 0
		for buffered.Next() {
			count++
		}
		assert.Equal(t, 3, count)
	})
}

// countingMockIterator tracks how many times Next() is called
type countingMockIterator struct {
	tuples        []Tuple
	pos           int
	nextCallCount *int
}

func (it *countingMockIterator) Next() bool {
	*it.nextCallCount++
	it.pos++
	return it.pos <= len(it.tuples)
}

func (it *countingMockIterator) Tuple() Tuple {
	if it.pos > 0 && it.pos <= len(it.tuples) {
		return it.tuples[it.pos-1]
	}
	return nil
}

func (it *countingMockIterator) Close() error {
	return nil
}

func TestStreamingRelationWithBuffering(t *testing.T) {
	// Test that StreamingRelation uses auto-materialization for multiple iterations
	// EnableTrueStreaming=false allows multiple Iterator() calls via materialization
	opts := ExecutorOptions{EnableTrueStreaming: false}

	t.Run("MultipleIterations", func(t *testing.T) {
		tuples := []Tuple{
			{1, "alice", 100},
			{2, "bob", 200},
		}
		columns := []query.Symbol{"?id", "?name", "?score"}

		source := newMockIterator(tuples)
		rel := NewStreamingRelationWithOptions(columns, source, opts)

		// Call Materialize() to enable multiple iterations
		_ = rel.Materialize()

		// First iteration
		it1 := rel.Iterator()
		var results1 []Tuple
		for it1.Next() {
			tuple := it1.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			results1 = append(results1, tupleCopy)
		}
		it1.Close()

		assert.Len(t, results1, 2)

		// Second iteration should work
		it2 := rel.Iterator()
		var results2 []Tuple
		for it2.Next() {
			tuple := it2.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			results2 = append(results2, tupleCopy)
		}
		it2.Close()

		assert.Len(t, results2, 2)
		assert.Equal(t, results1, results2)
	})

	t.Run("EfficientIsEmpty", func(t *testing.T) {
		tuples := []Tuple{{1, "alice"}}
		columns := []query.Symbol{"?id", "?name"}

		source := newMockIterator(tuples)
		// Use EnableTrueStreaming=true for this test since we're testing that IsEmpty() is efficient
		// With true streaming, IsEmpty() returns false without consuming the iterator
		streamingOpts := ExecutorOptions{EnableTrueStreaming: true}
		rel := NewStreamingRelationWithOptions(columns, source, streamingOpts)

		// IsEmpty should not trigger full materialization
		assert.False(t, rel.IsEmpty())

		// Should still be able to iterate
		it := rel.Iterator()
		assert.True(t, it.Next())
		assert.Equal(t, tuples[0], it.Tuple())
		it.Close()
	})
}
