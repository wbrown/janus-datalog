package executor

import (
	"sync"
)

// BufferedIterator wraps a streaming iterator and buffers results for re-iteration
// This allows a streaming iterator to be consumed multiple times
type BufferedIterator struct {
	source   Iterator
	buffer   []Tuple
	position int
	mu       sync.Mutex
	consumed bool // true after source has been fully consumed
}

// NewBufferedIterator creates a new buffered iterator
func NewBufferedIterator(source Iterator) *BufferedIterator {
	return &BufferedIterator{
		source:   source,
		buffer:   make([]Tuple, 0),
		position: -1,
	}
}

// Next advances to the next tuple
func (it *BufferedIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.position++

	// If we're still within the buffer, return true
	if it.position < len(it.buffer) {
		return true
	}

	// If source is already consumed, we're done
	if it.consumed {
		return false
	}

	// Try to get next from source
	if it.source.Next() {
		tuple := it.source.Tuple()
		// Make a copy to avoid mutation issues
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		it.buffer = append(it.buffer, tupleCopy)
		return true
	}

	// Source is exhausted
	it.consumed = true
	it.source.Close()
	return false
}

// Tuple returns the current tuple
func (it *BufferedIterator) Tuple() Tuple {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.position >= 0 && it.position < len(it.buffer) {
		return it.buffer[it.position]
	}
	return nil
}

// Reset allows re-iteration from the beginning
func (it *BufferedIterator) Reset() {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.position = -1
}

// Close releases resources
func (it *BufferedIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if !it.consumed && it.source != nil {
		return it.source.Close()
	}
	return nil
}

// Size returns the number of tuples (requires full consumption)
func (it *BufferedIterator) Size() int {
	it.mu.Lock()
	defer it.mu.Unlock()

	// If not fully consumed, consume everything
	if !it.consumed {
		oldPos := it.position
		it.position = len(it.buffer) - 1 // Start from last buffered position

		for it.source.Next() {
			tuple := it.source.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			it.buffer = append(it.buffer, tupleCopy)
		}
		it.consumed = true
		it.source.Close()
		it.position = oldPos // Restore position
	}

	return len(it.buffer)
}

// IsEmpty checks if the iterator has any tuples without full consumption
// This only consumes the first tuple if needed
func (it *BufferedIterator) IsEmpty() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	// If we have buffered data, we're not empty
	if len(it.buffer) > 0 {
		return false
	}

	// If source is consumed and buffer is empty, we're empty
	if it.consumed {
		return true
	}

	// Try to get one tuple from source
	if it.source.Next() {
		tuple := it.source.Tuple()
		tupleCopy := make(Tuple, len(tuple))
		copy(tupleCopy, tuple)
		it.buffer = append(it.buffer, tupleCopy)
		return false
	}

	// Source is empty
	it.consumed = true
	it.source.Close()
	return true
}

// Clone creates a new independent iterator over the same buffered data
// The clone shares the buffer but has independent iteration position
func (it *BufferedIterator) Clone() Iterator {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Ensure all data is buffered
	if !it.consumed {
		for it.source.Next() {
			tuple := it.source.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			it.buffer = append(it.buffer, tupleCopy)
		}
		it.consumed = true
		it.source.Close()
	}

	// Create a simple iterator over the buffered data
	return &bufferedSliceIterator{
		tuples:   it.buffer,
		position: -1,
	}
}

// bufferedSliceIterator is a simple iterator over a slice of tuples
type bufferedSliceIterator struct {
	tuples   []Tuple
	position int
}

func (it *bufferedSliceIterator) Next() bool {
	it.position++
	return it.position < len(it.tuples)
}

func (it *bufferedSliceIterator) Tuple() Tuple {
	if it.position >= 0 && it.position < len(it.tuples) {
		return it.tuples[it.position]
	}
	return nil
}

func (it *bufferedSliceIterator) Close() error {
	return nil
}
