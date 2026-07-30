// LazySeq provides a lazy sequence abstraction for streaming data
// through multiple consumers without materialization. Adapted from the
// a Clojure runtime's lazy-seq implementation.
//
// A LazySeq is a cons cell whose value is computed on first access (via
// a thunk) and cached for subsequent consumers. Chaining cells creates
// a shared stream: the first consumer drives the underlying iterator
// forward, and later consumers read from already-realized cells.
package executor

import (
	"runtime"
	"sync"
)

// iterGuard keeps an iterator alive via runtime.SetFinalizer.
// Every cell in the chain holds a reference to the guard, so the
// iterator stays open as long as any cell is reachable by the GC.
type iterGuard struct {
	closer func() error
}

type tupleSeqState struct {
	it        Iterator
	closeOnce sync.Once
	errMu     sync.Mutex
	err       error
}

func (s *tupleSeqState) close() error {
	s.closeOnce.Do(func() {
		err := s.it.Error()
		if closeErr := s.it.Close(); err == nil {
			err = closeErr
		}
		s.errMu.Lock()
		s.err = err
		s.errMu.Unlock()
	})
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// LazySeq is a cons cell with deferred realization. The thunk is called
// at most once; subsequent accesses return cached first/rest values.
// Thread-safe via sync.Once.
type LazySeq struct {
	once     sync.Once
	hasElems bool         // true when the seq has at least one element
	first    any          // cached first element
	rest     any          // nil, or *LazySeq
	err      error        // cached error from thunk
	thunk    func()       // sets hasElems/first/rest/err directly as side effects
	closer   func() error // optional: closes underlying resource (iterator)
	guard    *iterGuard   // prevents GC of iterator while cells are reachable
}

// realize calls the thunk and caches first/rest. Thread-safe via sync.Once:
// first call executes the thunk, subsequent calls are a single atomic load.
// The thunk sets hasElems/first/rest/err directly on the LazySeq as side effects.
func (ls *LazySeq) realize() {
	ls.once.Do(func() {
		if ls.thunk == nil {
			return
		}
		ls.thunk()
		ls.thunk = nil // allow GC of closure
	})
}

// Empty returns true if this lazy seq has no elements. Realizes if necessary.
// Returns false when the thunk errored, so callers proceed to First() which
// surfaces the error.
func (ls *LazySeq) Empty() bool {
	ls.realize()
	if ls.err != nil {
		return false
	}
	return !ls.hasElems
}

// First returns the first element, realizing if necessary.
func (ls *LazySeq) First() (any, error) {
	ls.realize()
	return ls.first, ls.err
}

// Rest returns the remaining sequence, realizing if necessary.
// Returns nil if there are no more elements.
func (ls *LazySeq) Rest() (any, error) {
	ls.realize()
	return ls.rest, ls.err
}

// Close releases the underlying resource (e.g., iterator) if one exists.
// Safe to call multiple times.
func (ls *LazySeq) Close() error {
	if ls.closer != nil {
		return ls.closer()
	}
	return ls.err
}

// NewTupleSeq wraps an Iterator in a chain of LazySeqs.
// Each realization advances the shared iterator by one step, producing
// a Tuple value. The iterator is closed automatically when exhausted
// or when all LazySeq cells become unreachable (via finalizer).
func NewTupleSeq(it Iterator, needsCopy bool) *LazySeq {
	state := &tupleSeqState{it: it}
	guard := &iterGuard{closer: state.close}
	runtime.SetFinalizer(guard, func(g *iterGuard) { _ = g.closer() })
	ls := newTupleCell(state, needsCopy, guard)
	ls.closer = state.close
	return ls
}

// newTupleCell creates a single LazySeq cell that advances the iterator.
// The thunk sets first/rest directly on the cell — no inner cell allocation.
func newTupleCell(state *tupleSeqState, needsCopy bool, guard *iterGuard) *LazySeq {
	ls := &LazySeq{guard: guard}
	ls.thunk = func() {
		if !state.it.Next() {
			ls.err = state.close()
			return // hasElems stays false
		}
		tuple := state.it.Tuple()
		if needsCopy {
			cp := make(Tuple, len(tuple))
			copy(cp, tuple)
			tuple = cp
		}
		ls.hasElems = true
		ls.first = tuple
		ls.rest = newTupleCell(state, needsCopy, guard)
		ls.rest.(*LazySeq).closer = state.close
	}
	return ls
}

// LazySeqToSlice realizes the entire lazy seq into a slice.
func LazySeqToSlice(seq *LazySeq) ([]any, error) {
	var result []any
	cur := seq
	for cur != nil && !cur.Empty() {
		v, err := cur.First()
		if err != nil {
			return nil, err
		}
		result = append(result, v)
		r, err := cur.Rest()
		if err != nil {
			return nil, err
		}
		if r == nil {
			break
		}
		cur = r.(*LazySeq)
	}
	return result, nil
}
