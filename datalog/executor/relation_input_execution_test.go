package executor

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ExecuteWithRelations over a relation input: the input enters as the bound
// group BindQueryInputs builds (executor_utils.go), and the plan runs once
// over it. relation_input_test.go pins the answers themselves; this file pins
// what the run reports, what it reads, and what it leaves behind:
//
//  1. Over an input holding duplicate tuples the answer is the exact set, each
//     matching entity once, compared as a sorted tuple list so an extra or a
//     dropped tuple shows as a length difference.
//  2. A matcher error, on the first pattern or a later one, is the query's
//     error.
//  3. An error a pattern's result defers to Iterator().Error() is the query's
//     error.
//  4. Repeated queries leave no goroutine behind.
//  5. A streaming input whose iterator reuses one workspace answers as a
//     materialized input does.
//  6. Concurrent queries over one matcher and one input each answer the same
//     tuples.

// ---------------------------------------------------------------------------
// Error-injection matchers
// ---------------------------------------------------------------------------

// errInjectedMatcher is returned by failingMatcher when its hook fires.
var errInjectedMatcher = errors.New("injected matcher failure")

// failingMatcher wraps a delegate PatternMatcher and lets a test inject
// failures based on a hook. Match-call counting is mutex-protected, so the
// matcher is safe under concurrent Match calls.
type failingMatcher struct {
	delegate   PatternMatcher
	mu         sync.Mutex
	callCount  int
	shouldFail func(callCount int, p *query.DataPattern) error
}

func (m *failingMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	p, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.callCount++
	n := m.callCount
	m.mu.Unlock()
	if m.shouldFail != nil {
		if err := m.shouldFail(n, p); err != nil {
			return nil, err
		}
	}
	return m.delegate.Match(q, bindings)
}

// deferredFailMatcher wraps a delegate and on the failOnCall-th Match() it
// returns a Relation whose iterator yields nothing and then reports an error
// via Error() — mimicking a Tier-3 blob decode failure or any other deferred
// storage error that surfaces only after Next() returns false. Used to verify
// the deferred-error propagation contract.
type deferredFailMatcher struct {
	delegate   PatternMatcher
	mu         sync.Mutex
	callCount  int
	failOnCall int // 1-indexed; 0 = never
}

func (m *deferredFailMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	rel, err := m.delegate.Match(q, bindings)
	if err != nil || rel == nil {
		return rel, err
	}
	m.mu.Lock()
	m.callCount++
	n := m.callCount
	m.mu.Unlock()
	if m.failOnCall > 0 && n == m.failOnCall {
		// Wrap with a deferred-failure iterator. failingRelation lives in
		// iterator_error_boundary_test.go; it embeds a Relation and replaces
		// Iterator() with a failingIterator that errors after failAfter tuples.
		return failingRelation{Relation: rel, failAfter: 0}, nil
	}
	return rel, nil
}

// ---------------------------------------------------------------------------
// Common test fixtures
// ---------------------------------------------------------------------------

// buildPeopleDatoms creates a small but non-trivial dataset for relation-input
// iteration tests. Three names × two years × one matching person per (name,
// year) keeps the expected results small enough to enumerate by hand while
// still exercising the join.
func buildPeopleDatoms() []datalog.Datom {
	nameAttr := datalog.NewKeyword(":name")
	yearAttr := datalog.NewKeyword(":year")
	ageAttr := datalog.NewKeyword(":age")
	d := func(eid string, attr datalog.Keyword, v interface{}, tx uint64) datalog.Datom {
		return datalog.Datom{E: datalog.NewIdentity(eid), A: attr, V: v, Tx: datalog.ElementID{Lamport: tx, ReplicaID: 1}}
	}
	return []datalog.Datom{
		d("a1", nameAttr, "Alice", 1), d("a1", yearAttr, int64(2020), 1), d("a1", ageAttr, int64(25), 1),
		d("a2", nameAttr, "Alice", 2), d("a2", yearAttr, int64(2021), 2), d("a2", ageAttr, int64(26), 2),
		d("b1", nameAttr, "Bob", 3), d("b1", yearAttr, int64(2020), 3), d("b1", ageAttr, int64(30), 3),
		d("b2", nameAttr, "Bob", 4), d("b2", yearAttr, int64(2021), 4), d("b2", ageAttr, int64(31), 4),
		d("c1", nameAttr, "Carol", 5), d("c1", yearAttr, int64(2020), 5), d("c1", ageAttr, int64(40), 5),
	}
}

// peopleQueryNoAgg is `[:find ?n ?y ?age :in $ [[?n ?y] ...] :where ...]`.
// Returns one tuple per matching entity.
const peopleQueryNoAgg = `[:find ?n ?y ?age
 :in $ [[?n ?y] ...]
 :where [?e :name ?n]
        [?e :year ?y]
        [?e :age ?age]]`

// sortedTupleStrings renders a relation as a sorted []string for multiset
// comparison. Two relations are multiset-equal iff their sortedTupleStrings
// are reflect.DeepEqual.
func sortedTupleStrings(t *testing.T, rel Relation) []string {
	t.Helper()
	var out []string
	it := rel.Iterator()
	defer it.Close()
	for it.Next() {
		out = append(out, fmt.Sprintf("%v", it.Tuple()))
	}
	require.NoError(t, it.Error(), "iterator error during multiset collection")
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------------
// The exact set over an input holding duplicate tuples
// ---------------------------------------------------------------------------

// TestRelationInput_DuplicateInputTuplesAnswerTheExactSet pins that the
// answer over an input relation holding duplicate tuples is the exact set of
// matching entities, each once, compared as the sorted list of its tuples.
func TestRelationInput_DuplicateInputTuplesAnswerTheExactSet(t *testing.T) {
	matcher := NewMemoryPatternMatcher(buildPeopleDatoms())
	q, err := parser.ParseQuery(peopleQueryNoAgg)
	require.NoError(t, err)

	// Mix of distinct tuples and duplicates.
	inputRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?y")},
		[]Tuple{
			{"Alice", int64(2020)},
			{"Alice", int64(2020)}, // dup
			{"Alice", int64(2021)},
			{"Bob", int64(2020)},
			{"Bob", int64(2021)},
			{"Bob", int64(2021)}, // dup
			{"Carol", int64(2020)},
		},
	)
	expected := []string{
		"[Alice 2020 25]",
		"[Alice 2021 26]",
		"[Bob 2020 30]",
		"[Bob 2021 31]",
		"[Carol 2020 40]",
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			result, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
			require.NoError(t, err)

			require.Equal(t, expected, sortedTupleStrings(t, result),
				"the answer over duplicate input tuples is the exact set, each matching entity once")
		})
	}
}

// ---------------------------------------------------------------------------
// A matcher error is the query's error
// ---------------------------------------------------------------------------

// TestRelationInput_PropagatesMatcherError pins that an error the
// PatternMatcher returns while matching a pattern over the bound group
// surfaces as a non-nil error from ExecuteWithRelations — not as a partial
// successful result that quietly drops the failed pattern.
//
// The phase that ran the pattern wraps the failure, so errors.Is must unwrap
// to the injected sentinel.
func TestRelationInput_PropagatesMatcherError(t *testing.T) {
	// Shared read-only delegate; the stateful failingMatcher wrapper (call
	// counter) is rebuilt fresh per mode below so each mode's execution sees
	// its own "fail on call N" state.
	delegate := NewMemoryPatternMatcher(buildPeopleDatoms())

	q, err := parser.ParseQuery(peopleQueryNoAgg)
	require.NoError(t, err)

	inputRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?y")},
		[]Tuple{
			{"Alice", int64(2020)},
			{"Bob", int64(2020)},
			{"Carol", int64(2020)},
		},
	)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// Fail on the very first Match() call.
			fm := &failingMatcher{
				delegate:   delegate,
				shouldFail: func(n int, _ *query.DataPattern) error { return errInjectedMatcher },
			}

			exec := NewExecutorWithOptions(fm, nil, mode.plannerOptions())

			_, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
			require.Error(t, err, "matcher error must not be silently dropped")
			require.True(t, errors.Is(err, errInjectedMatcher),
				"error must unwrap to the injected sentinel; got %v", err)
		})
	}
}

// TestRelationInput_PropagatesMatcherErrorAfterAnEarlierPattern covers the
// case where an earlier pattern matched and a later one fails. The contract
// is that the error propagates whatever its position.
func TestRelationInput_PropagatesMatcherErrorAfterAnEarlierPattern(t *testing.T) {
	// Shared read-only delegate; the stateful failingMatcher wrapper (call
	// counter) is rebuilt fresh per mode below so each mode's execution sees
	// its own "fail on call N" state.
	delegate := NewMemoryPatternMatcher(buildPeopleDatoms())

	q, err := parser.ParseQuery(peopleQueryNoAgg)
	require.NoError(t, err)

	inputRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?y")},
		[]Tuple{
			{"Alice", int64(2020)},
			{"Alice", int64(2021)},
			{"Bob", int64(2020)},
			{"Bob", int64(2021)},
			{"Carol", int64(2020)},
		},
	)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// Fail on the second Match() call: the query's three patterns are
			// matched once each over the bound group, so the second call is
			// the "after an earlier pattern succeeded" case.
			const failAfter = 2
			fm := &failingMatcher{
				delegate: delegate,
				shouldFail: func(n int, _ *query.DataPattern) error {
					if n >= failAfter {
						return errInjectedMatcher
					}
					return nil
				},
			}

			exec := NewExecutorWithOptions(fm, nil, mode.plannerOptions())

			// The later pattern runs over the join result through the window,
			// whose scan failure surfaces when the result is driven.
			result, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
			require.NoError(t, err, "the matcher error surfaces at the drive, not the call")
			_, err = CollectTuples(result, nil)
			require.Error(t, err, "matcher error after partial success must not be silently dropped")
			require.True(t, errors.Is(err, errInjectedMatcher),
				"error must unwrap to the injected sentinel; got %v", err)
		})
	}
}

// ---------------------------------------------------------------------------
// A deferred iterator error is the query's error
// ---------------------------------------------------------------------------

// TestRelationInput_PropagatesDeferredIteratorError pins the iterator-error
// contract: a pattern whose result relation defers an error to
// Iterator().Error() (the way a failed Tier-3 blob decode would) must surface
// as a non-nil error from ExecuteWithRelations.
func TestRelationInput_PropagatesDeferredIteratorError(t *testing.T) {
	// Shared read-only delegate; the stateful deferredFailMatcher wrapper
	// (call counter) is rebuilt fresh per mode below so each mode's
	// execution sees its own "fail on call N" state.
	delegate := NewMemoryPatternMatcher(buildPeopleDatoms())

	q, err := parser.ParseQuery(peopleQueryNoAgg)
	require.NoError(t, err)

	inputRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?y")},
		[]Tuple{
			{"Alice", int64(2020)},
			{"Bob", int64(2020)},
		},
	)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			dm := &deferredFailMatcher{
				delegate:   delegate,
				failOnCall: 1, // first Match() returns a deferred-failure relation
			}

			exec := NewExecutorWithOptions(dm, nil, mode.plannerOptions())

			_, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
			require.Error(t, err, "deferred iterator error must propagate, not be laundered into a clean result")
			require.True(t, errors.Is(err, errInjectedIterator),
				"error must unwrap to errInjectedIterator (from failingIterator); got %v", err)
		})
	}
}

// ---------------------------------------------------------------------------
// No goroutine left behind
// ---------------------------------------------------------------------------

// TestRelationInput_NoGoroutineLeak verifies that repeated queries over a
// relation input leave no goroutine behind: the goroutine count after fifty
// queries is the count before them. A pool running the query's jobs spawns
// its goroutines per call, and a forgotten wg.Wait(), an abandoned worker or
// an unclosed channel leaves them running past the call's return.
func TestRelationInput_NoGoroutineLeak(t *testing.T) {
	matcher := NewMemoryPatternMatcher(buildPeopleDatoms())
	q, err := parser.ParseQuery(peopleQueryNoAgg)
	require.NoError(t, err)

	inputRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?y")},
		[]Tuple{
			{"Alice", int64(2020)},
			{"Alice", int64(2021)},
			{"Bob", int64(2020)},
			{"Bob", int64(2021)},
			{"Carol", int64(2020)},
		},
	)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			// Warm up: first run amortizes one-time initializations that may park
			// goroutines (intern caches, etc.). Without this, "before" can be lower
			// than "after" purely because of first-call setup, not a leak.
			_, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
			require.NoError(t, err)

			// Let any test-framework or first-call goroutines settle.
			runtime.GC()
			time.Sleep(50 * time.Millisecond)
			runtime.GC()
			before := runtime.NumGoroutine()

			const iterations = 50
			for i := 0; i < iterations; i++ {
				_, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
				require.NoError(t, err)
			}

			// Give the runtime time to reap finished goroutines. A query returns
			// only after every goroutine it started signals done, so there should
			// be no in-flight workers; what we're guarding against is workers that
			// linger past the function return.
			runtime.GC()
			time.Sleep(100 * time.Millisecond)
			runtime.GC()
			after := runtime.NumGoroutine()

			// Generous bound: if any goroutine leaks per call, after-before would
			// be at least iterations (50). A bound of 10 swallows test-framework
			// noise without missing a real leak.
			require.LessOrEqualf(t, after-before, 10,
				"goroutine leak suspected: started with %d, ended with %d after %d iterations",
				before, after, iterations)
		})
	}
}

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

// TestRelationInput_AStreamingInputReusingItsWorkspaceAnswersAsAMaterializedOne:
// a streaming input relation whose iterator reuses a single workspace slice
// across Next() calls (the production-default storage-iterator pattern, used by
// matcher_iterator_nonreusing.go and hash_join_matcher.go) must produce the
// same multiset of results as a materialized input. Without a producer-side
// copy, workers race against the producer's workspace overwrites — and
// `go test -race` reports it as a data race on the workspace's backing
// array.
//
// The tests above use MaterializedRelation as the iteration input
// (RequiresCopy() == false, stable tuples), which does not trigger this
// race; this test uses a streaming input instead so the race surfaces.
//
// Design notes:
//   - Each input tuple matches exactly ONE entity in the dataset, so each
//     iteration produces exactly one unique result tuple. Any workspace
//     corruption that causes a worker to see a different input's values
//     will silently drop an expected tuple (the wrong entity is found, or
//     no match at all).
//   - We use 100 distinct inputs so even a small per-tuple corruption rate
//     produces visible misses. With set-semantic output, the test asserts
//     that all 100 expected tuples are present — any miss fails the test.
//   - runtime.Gosched in the iterator's Next() widens the race window so
//     workers reliably interleave with producer writes; without it the
//     producer can finish all sends before any worker reads its tuple,
//     and the test accidentally passes (the race detector still catches
//     it under -race, but the result happens to come out correct).
func TestRelationInput_AStreamingInputReusingItsWorkspaceAnswersAsAMaterializedOne(t *testing.T) {
	nameAttr := datalog.NewKeyword(":name")
	yearAttr := datalog.NewKeyword(":year")
	ageAttr := datalog.NewKeyword(":age")

	const entityCount = 100

	// Build entityCount distinct entities — each input tuple matches one,
	// so corruption to ANY tuple drops exactly one expected output tuple.
	var datoms []datalog.Datom
	var inputTuples []Tuple
	expectedTuples := make(map[string]bool, entityCount)
	for i := 0; i < entityCount; i++ {
		name := fmt.Sprintf("P%03d", i)
		year := int64(2000 + i)
		age := int64(20 + i)
		eid := fmt.Sprintf("e%d", i)
		datoms = append(datoms,
			datalog.Datom{E: datalog.NewIdentity(eid), A: nameAttr, V: name, Tx: datalog.ElementID{Lamport: uint64(i*3 + 1), ReplicaID: 1}},
			datalog.Datom{E: datalog.NewIdentity(eid), A: yearAttr, V: year, Tx: datalog.ElementID{Lamport: uint64(i*3 + 2), ReplicaID: 1}},
			datalog.Datom{E: datalog.NewIdentity(eid), A: ageAttr, V: age, Tx: datalog.ElementID{Lamport: uint64(i*3 + 3), ReplicaID: 1}},
		)
		inputTuples = append(inputTuples, Tuple{name, year})
		expectedTuples[fmt.Sprintf("[%s %d %d]", name, year, age)] = false
	}

	matcher := NewMemoryPatternMatcher(datoms)
	q, err := parser.ParseQuery(peopleQueryNoAgg)
	require.NoError(t, err)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// newReusingWorkspaceStream wraps a single-use streaming iterator, so
			// it must be rebuilt fresh per mode rather than shared like the
			// read-only in-memory matchers above.
			inputRel := newReusingWorkspaceStream(
				[]query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?y")},
				inputTuples,
			)

			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			result, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
			require.NoError(t, err)

			seen := make(map[string]bool, len(expectedTuples))
			for key := range expectedTuples {
				seen[key] = false
			}

			it := result.Iterator()
			defer it.Close()
			for it.Next() {
				key := fmt.Sprintf("%v", it.Tuple())
				if _, ok := seen[key]; ok {
					seen[key] = true
				}
			}
			require.NoError(t, it.Error())

			var missing []string
			for key, found := range seen {
				if !found {
					missing = append(missing, key)
				}
			}
			sort.Strings(missing)
			require.Emptyf(t, missing,
				"%d of %d expected tuples missing from result; the workspace-reuse "+
					"race let workers read stale workspace values. Run with -race "+
					"to see the data race directly. Missing (first few): %v",
				len(missing), entityCount, missing[:min(len(missing), 5)])
		})
	}
}

// ---------------------------------------------------------------------------
// Concurrent queries
// ---------------------------------------------------------------------------

// TestRelationInput_ConcurrentQueriesAnswerAlike pins that concurrent queries
// over one matcher and one input relation each answer the tuples one query
// alone answers. Sixteen goroutines, each with its own executor, run the
// query twenty times each, and every run's tuples are compared with the
// expected list, never only its error, which is what the concurrent subtest
// of TestRelationInput_ThreeHundredTuples checks.
func TestRelationInput_ConcurrentQueriesAnswerAlike(t *testing.T) {
	matcher := NewMemoryPatternMatcher(buildPeopleDatoms())
	q, err := parser.ParseQuery(peopleQueryNoAgg)
	require.NoError(t, err)

	inputRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?y")},
		[]Tuple{
			{"Alice", int64(2020)},
			{"Alice", int64(2021)},
			{"Bob", int64(2020)},
			{"Bob", int64(2021)},
			{"Carol", int64(2020)},
		},
	)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// The expected tuples, from one query run alone.
			expected, err := NewExecutorWithOptions(matcher, nil, mode.plannerOptions()).
				ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
			require.NoError(t, err)
			expectedTuples := sortedTupleStrings(t, expected)

			const concurrency = 16
			const itersPerGoroutine = 20

			type runResult struct {
				tuples []string
				err    error
			}
			results := make(chan runResult, concurrency*itersPerGoroutine)

			var wg sync.WaitGroup
			wg.Add(concurrency)
			for g := 0; g < concurrency; g++ {
				go func() {
					defer wg.Done()
					// Each outer goroutine gets its own executor, mirroring the
					// production pattern (per-task or per-request executor).
					exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
					for i := 0; i < itersPerGoroutine; i++ {
						rel, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
						if err != nil {
							results <- runResult{err: err}
							continue
						}
						results <- runResult{tuples: sortedTupleStrings(t, rel)}
					}
				}()
			}
			wg.Wait()
			close(results)

			count := 0
			for r := range results {
				count++
				require.NoError(t, r.err, "concurrent invocation %d failed", count)
				require.Equal(t, expectedTuples, r.tuples,
					"concurrent invocation %d produced wrong multiset", count)
			}
			require.Equal(t, concurrency*itersPerGoroutine, count,
				"expected %d results, got %d", concurrency*itersPerGoroutine, count)
		})
	}
}
