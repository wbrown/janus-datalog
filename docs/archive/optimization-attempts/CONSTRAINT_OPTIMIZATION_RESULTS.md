# Constraint Optimization Performance Results

## Summary
Our optimization of constraint evaluation shows significant performance improvements:

### Individual Constraint Evaluations
| Type | Optimized | Unoptimized | Speedup |
|------|-----------|-------------|---------|
| int64 | 1.56 ns | 3.49 ns | **2.24x faster** |
| string | 3.24 ns | 4.49 ns | **1.38x faster** |
| bool | 2.24 ns | N/A | (not measured) |
| time.Time | 4.98 ns | N/A | (uses ValuesEqual fallback) |

### Real-World Scenario: 1000 Datoms
| Version | Time | Operations/sec | Speedup |
|---------|------|----------------|---------|
| Optimized | 1,513 ns | 660,937 ops/sec | **5.78x faster** |
| Unoptimized | 8,742 ns | 114,389 ops/sec | baseline |

## Key Insights

1. **Fast paths eliminate overhead**: By checking common types (int64, string, bool) directly, we avoid the expensive `ValuesEqual` function which does 6 type assertions for pointers.

2. **Massive improvement for int64**: The most common case (comparing ages, IDs, counts) is now 2.24x faster per comparison.

3. **Cumulative effect is dramatic**: When evaluating constraints on 1000 datoms (typical storage scan), the optimization provides **5.78x speedup**.

4. **Zero allocations**: Both versions allocate no memory during evaluation, maintaining good GC characteristics.

## Why Predicate Pushdown is Still Slower

Despite these optimizations, predicate pushdown remains slower than no pushdown for our test case:

- **1000 constraint evaluations at 1.5ns each = 1,500ns overhead**
- This is still more expensive than the saved work of returning fewer tuples
- The fundamental issue: we must decode ALL datoms before evaluating constraints

## Architectural Implications

To make predicate pushdown truly beneficial, we would need:

1. **Index-level filtering**: Skip keys entirely without decoding
2. **Composite indices**: E.g., (attribute, value) indices for equality constraints  
3. **Pushdown to storage engine**: Let BadgerDB filter before returning keys

Our current architecture makes these impossible without major changes. The optimization helps, but doesn't overcome the fundamental limitation that we decode everything.

## Conclusion

The constraint optimization provides substantial performance improvements (5.78x for realistic workloads), but it's not enough to make predicate pushdown faster than application-level filtering for simple equality constraints. The test correctly documents this architectural tradeoff.