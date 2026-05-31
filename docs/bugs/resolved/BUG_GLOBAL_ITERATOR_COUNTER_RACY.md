# BUG: Global Iterator Counter Is Unsynchronized Debug State

**Date**: 2026-05-30
**Severity**: Low — test/debug state is race-prone and violates local state discipline
**Status**: ✅ RESOLVED (2026-05-31)
**Affected**: `storage.globalIteratorOpens`, `ResetIteratorOpenCount`, `GetIteratorOpenCount`

## Resolution

Removed. The counter was vestigial: `globalIteratorOpens` was declared, reset to
0, and read, but **never incremented anywhere** — so `GetIteratorOpenCount()`
always returned 0 and no data race was actually reachable (nothing wrote it
except `Reset`). The only test reading it (`iterator_reuse_regression_test.go`)
just `t.Logf`-ed the value; its real iterator-open instrumentation comes from the
separate `instrumentedMatcher.stats.iteratorOpens` wrapper, which is untouched.

The package-level `var` and the two exported functions were deleted from
`matcher_relations.go`, and the dead `ResetIteratorOpenCount()` call and
`GetIteratorOpenCount()` log line were removed from the test. No behavior change;
the wrapper-based counting the test relied on still proves iterator reuse without
package-global state.

## Summary

`matcher_relations.go` defines package-level mutable debug state for counting
iterator opens:

```go
// DEBUG: Global counter for iterator opens
var globalIteratorOpens int

func ResetIteratorOpenCount() {
	globalIteratorOpens = 0
}

func GetIteratorOpenCount() int {
	return globalIteratorOpens
}
```

This counter is package-global, non-atomic, and exposed through reset/get
functions. It appears to be test-oriented instrumentation, but it is not safe
under concurrent query execution or concurrent tests.

## Why This Matters

The repository otherwise goes out of its way to avoid global configuration
state, thread options through constructors, and make concurrent query execution
safe. This counter cuts across those conventions:

1. It is shared by all databases and matchers in the process.
2. It has no synchronization.
3. `ResetIteratorOpenCount()` can affect another query or test running at the
   same time.
4. If incremented from storage iterator creation, it will race under parallel
   queries or `t.Parallel()` tests.

Even if the counter is only used by tests today, package-level mutable debug
state tends to survive and later become production-observable by accident.

## Code Evidence

The counter is declared in production code, not a `_test.go` file:

```go
// DEBUG: Global counter for iterator opens
var globalIteratorOpens int

// ResetIteratorOpenCount resets the global iterator open counter for testing
func ResetIteratorOpenCount() {
	globalIteratorOpens = 0
}

// GetIteratorOpenCount returns the current iterator open count for testing
func GetIteratorOpenCount() int {
	return globalIteratorOpens
}
```

It is used by tests such as `iterator_reuse_regression_test.go` to inspect
storage behavior.

## Expected Behavior

Instrumentation should be scoped to a matcher, context, annotation collector, or
test-only wrapper. If it must be global, it should at least be atomic and clearly
limited to tests.

## Actual Behavior

The counter is a package-level `int` in production code with unsynchronized
read/write access.

## Suggested Fix

Prefer removing this global from production code:

1. Move iterator-open instrumentation into a test-only matcher/store wrapper.
2. Or emit an annotation event when a storage iterator opens and count events in
   tests.
3. Or add a small per-matcher/per-store counter object passed explicitly where
   needed by tests.

If a quick tactical fix is needed before removing it, change the counter to
`atomic.Int64` and make reset/get use atomic operations. That removes the data
race but still leaves global test coupling.

## Tests Needed

If the counter remains:

1. Run the iterator-reuse tests under `go test -race`.
2. Add a concurrent query test that opens storage iterators from multiple
   goroutines while reading the counter.
3. Verify no tests rely on global reset semantics that can interfere with
   parallel execution.

If the counter is removed:

1. Port `iterator_reuse_regression_test.go` to annotation-based or wrapper-based
   counting.
2. Confirm the tests still prove iterator reuse without package-global state.
