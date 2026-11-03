package executor

import (
	"runtime"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// ---------------------------------------------------------------------------
// A streaming input reusing its workspace
// ---------------------------------------------------------------------------

// reusingWorkspaceIterator yields tuples by mutating a single shared
// workspace slice — the standard storage-iterator pattern used in
// matcher_iterator_nonreusing.go and hash_join_matcher.go (search for
// `BuildTupleInternedInto(datom, it.workspace)`).
// Callers that retain the result of Tuple() across Next() calls without
// copying will see whatever values the most recent Next() wrote.
//
// Wrapped in a StreamingRelation (RequiresCopy() returns true), this
// reproduces the workspace-reuse race that's invisible when tests use
// MaterializedRelation inputs (which return stable tuples). Callers
// passing materialized inputs don't trip this race in practice — but
// the function must handle streaming inputs that do.
type reusingWorkspaceIterator struct {
	source    []Tuple // logical tuples to yield
	idx       int
	workspace Tuple // single shared backing slice, mutated on each Next
}

func newReusingWorkspaceStream(symbols []query.Symbol, tuples []Tuple) *StreamingRelation {
	width := 0
	for _, t := range tuples {
		if len(t) > width {
			width = len(t)
		}
	}
	return NewStreamingRelation(symbols, &reusingWorkspaceIterator{
		source:    tuples,
		idx:       -1,
		workspace: make(Tuple, width),
	})
}

func (it *reusingWorkspaceIterator) Next() bool {
	it.idx++
	if it.idx >= len(it.source) {
		return false
	}
	src := it.source[it.idx]
	// Mutate the shared workspace, not the source slice. This is the key
	// behavior we're testing: subsequent Next() calls clobber the
	// previously-returned tuple's contents.
	//
	// Yield between iterations so a worker that's mid-Build on the
	// previous tuple has a chance to be scheduled before this mutation
	// completes. Without the yield the producer often runs to completion
	// before any worker starts reading, masking the corruption (the race
	// detector still catches it, but the test result accidentally passes).
	runtime.Gosched()
	for i := 0; i < len(it.workspace); i++ {
		if i < len(src) {
			it.workspace[i] = src[i]
		}
	}
	return true
}

func (it *reusingWorkspaceIterator) Tuple() Tuple { return it.workspace }
func (it *reusingWorkspaceIterator) Close() error { return nil }
func (it *reusingWorkspaceIterator) Error() error { return nil }

// RequiresCopy is true: Tuple hands back the one workspace the next pull
// overwrites.
func (it *reusingWorkspaceIterator) RequiresCopy() bool { return true }
