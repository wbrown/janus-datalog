# Testing Strategy - REQUIRED READING

**This file is PART OF CLAUDE.md and must be read before writing any code.**

## CRITICAL: Tests Are Not Optional

The implementation is NOT complete until tests pass. This is non-negotiable because:
- **Bugs hide in untested code** - The merge join algorithm looked correct but had a critical bug found only by testing
- **"It compiles" ≠ "It works"** - Compilation proves syntax, tests prove correctness
- **Manual testing lies** - Small manual tests miss edge cases that comprehensive tests catch

---

## Testing Workflow (Mandatory)

### 1. Write the implementation

### 2. Write comprehensive tests covering:
- Happy path (expected inputs)
- Edge cases (empty, single, large)
- Error cases (invalid, missing, conflicting)
- Performance validation (if relevant)

### 3. Run the tests
```bash
go test -v ./package -run TestName
```

**If tests timeout**:
- Run smaller subsets OR use longer timeouts on tool calls - don't give up
- **Wait for completion**: Be patient, don't assume success

### 4. Verify tests PASS
- Not just "no errors", but actual PASS
- Test failure = implementation has bugs
- "No race detected" ≠ test passed
- Read the full output, don't stop at first sign of success

### 5. Only when tests PASS: Then commit

---

## NEVER

- ❌ Declare work "done" before writing tests
- ❌ Write tests but not run them
- ❌ Assume test failures are "test problems" - they reveal real bugs
- ❌ Create test_*.go files in the root directory
- ❌ Commit because "tests are taking too long"
- ❌ Commit on first failure with unrelated error
- ❌ Skip verification because of timeouts
- ❌ Assume a fix works based on theory alone

---

## ALWAYS

- ✅ Write tests in *_test.go files in the appropriate package
- ✅ Run tests immediately after writing them
- ✅ **Wait for tests to complete** - be patient
- ✅ Investigate every test failure thoroughly
- ✅ Use `go test` not standalone programs
- ✅ Verify PASS status, not just absence of specific errors

---

## Test Timeouts

- Timeouts mean WAIT or TEST DIFFERENTLY, not COMMIT ANYWAY
- Use longer timeout values on tool calls (e.g., 600000ms for slow tests)
- Run smaller test subsets if full suite times out
- If you get impatient: ASK the user, don't decide unilaterally

---

## Test Coverage Guidelines

### 1. Property-based tests for relational algebra laws

Example:
```go
func TestJoinCommutative(t *testing.T) {
    // R ⋈ S = S ⋈ R
    result1 := r.Join(s)
    result2 := s.Join(r)
    assert.Equal(t, result1, result2)
}
```

### 2. Differential testing against reference implementations

Compare results with known-good implementations when possible.

### 3. Correctness tests with known inputs/outputs

```go
func TestAggregationCorrectness(t *testing.T) {
    // Known input → Known output
    input := createTestData()
    expected := []Tuple{...}
    actual := executeQuery(query, input)
    assert.Equal(t, expected, actual)
}
```

### 4. Performance benchmarks tracking query execution

```go
func BenchmarkComplexQuery(b *testing.B) {
    for i := 0; i < b.N; i++ {
        executor.Execute(complexQuery)
    }
}
```

### 5. Fuzz testing with random queries and data

Generate random valid queries and verify no panics or crashes.

---

## Testing Query Optimizations

**CRITICAL**: Query optimizations are NOT tested the same way as regular features.

When implementing or modifying query optimizations (CSE, decorrelation, predicate pushdown, etc.), you MUST test:

### 1. Semantic Preservation
Optimization doesn't change query meaning:

```go
// Test that optimized query returns same results as unoptimized
resultOptimized := execWithOptimization.Execute(query)
resultUnoptimized := execWithoutOptimization.Execute(query)
assert.Equal(resultOptimized, resultUnoptimized)
```

### 2. Structural Invariants
Query structure is preserved correctly:

```go
// Test internal structure using annotations
event := captureAnnotation("aggregation/executed")
assert.Equal(0, event.Data["groupby_count"])  // Pure agg stays pure
```

### 3. Category Distinctions
Different types are treated differently:

```go
// Pure aggregations should NOT be optimized the same as grouped
if isPureAggregation(query) {
    assert.False(wasDecorrelated(plan))
}
```

### 4. Realistic Data Sizes
Test with production-scale data:

```go
// Use 100s-1000s of tuples, not just 2-5
datoms := generateLargeDataset(1000)
// Complex queries that stress optimization logic
```

### 5. Failure Modes
Test that optimization doesn't break edge cases:

```go
// Test with nil values, empty relations, single tuples
// Verify no panics, no data corruption
```

---

## Why This Matters

**The decorrelation bug** existed because tests only verified:
- ✅ "Does it return the right answer?" (outcome)
- ❌ "Did optimization preserve query semantics?" (structure)
- ❌ "Are internal transformations correct?" (invariants)

**Lesson**: Test transformations at the structural level, not just outcomes.

---

## Use Annotations for Testing Optimizations

The annotation system is essential for testing optimizations:

```go
// Capture what the optimizer actually did
handler := func(event annotations.Event) {
    if event.Name == "aggregation/executed" {
        // Verify groupby_count, find_elements, etc.
    }
}
```

---

## Property-Based Testing for Optimizations

```go
// For ANY query Q with property P:
// Optimized(Q) must preserve P
func TestOptimizationPreservesPureAggregations(t *testing.T) {
    for _, query := range generateQueriesWithPureAggregations() {
        plan := planner.Plan(query)
        // Pure aggregations should never be modified
        for _, subq := range plan.Subqueries {
            if isPure(subq.OriginalQuery) {
                assert.True(isPure(subq.OptimizedQuery))
            }
        }
    }
}
```

---

## Example: The Merge Join Bug

The merge join implementation appeared correct but had a binding advancement bug. Only comprehensive testing revealed it.

**Tests found the bug, not "careful review".**

This proves: Implementation without tests = incomplete implementation.

---

## Testing Checklist

Before declaring any work complete:

- [ ] Tests written in appropriate package (*_test.go)
- [ ] Happy path tested
- [ ] Edge cases tested (empty, single, large inputs)
- [ ] Error cases tested
- [ ] Tests actually RUN (not just written)
- [ ] Tests actually PASS (verified in output)
- [ ] For optimizations: semantic preservation tested
- [ ] For optimizations: structural invariants tested
- [ ] For optimizations: realistic data sizes used

**Only when ALL checkboxes are checked: The work is done.**