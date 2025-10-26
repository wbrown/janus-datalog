# Datalog Implementation - Historical Insights

**Original**: This document was extracted from the original 2,100-line design exploration document (`DATALOG_GO_NOTES.md`). Most implementation details have been superseded by current architecture (see `ARCHITECTURE.md` and `CLAUDE.md`).

**Preserved here**: Key insights and lessons learned that remain relevant.

---

## Critical Algorithm: Relation Collapsing

### The Problem

Naive join ordering can cause memory exhaustion on complex queries. Consider:

```
[?x :follows ?y] ⋈ [?y :follows ?z] ⋈ [?z :name "Alice"]
```

If executed left-to-right with 1M follows relationships, the first join produces billions of tuples before filtering by name.

### The Solution

**Dynamic join ordering with early termination**:

1. Group relations by shared columns (connectivity)
2. Join progressively as relations become available
3. Terminate immediately on empty results
4. Keep disjoint relation groups separate

**Why this matters**: This is NOT just an optimization - it's the difference between a system that demos well on toy data and one that handles production workloads. Without proper join ordering:
- Memory exhaustion on multi-way joins
- Minutes/hours for queries that should be sub-second
- Inability to handle real-world data volumes

**Implementation notes**:
- This is a greedy algorithm (simpler than cost-based optimizers)
- Requires good initial ordering from the query planner
- Phase-based planning helps provide that ordering
- Early termination is critical for failing fast

---

## Storage Design Insights

### Fixed vs Variable-Size Keys

**Decision**: Use fixed 72-byte keys (E:20 + A:32 + Tx:20)

**Why fixed size**:
- Enables efficient range scans without parsing
- Predictable memory layout for cache efficiency
- Simpler iterator implementation
- Sort order preserved lexicographically

**Why these specific sizes**:
- **E (Entity): 20 bytes** - SHA1 hash provides good distribution
- **A (Attribute): 32 bytes** - Extended from 20 to support longer attribute names
- **Tx (Transaction): 20 bytes** - Transaction IDs as hashes
- **V (Value): Variable** - Values can be unbounded (long strings, etc.)

### L85 Encoding

**Custom Base85 encoding** that preserves sort order:

**Properties**:
- Space efficient: 25% overhead (vs Base64's 33%)
- Sort-preserving: Lexicographic sort of encoded strings matches byte order
- Terminal-safe: All printable ASCII, no quotes/spaces/backslashes
- Fixed output: 20 bytes → 25 characters

**Why this matters**:
- Keys can be logged and debugged without binary issues
- Range scans work without decoding
- URLs and JSON safe without escaping
- Slightly more compact than Base64

**Implementation**: See `datalog/codec/l85.go`

---

## Type System Philosophy

### The Variant Problem

**Some approaches**: Complex variant type systems with visitor patterns

**Our approach**: Direct Go types without wrappers

```go
// NOT this (Java/C++ style):
type Value struct {
    kind ValueKind
    data interface{}
}

// THIS (Go idiom):
type Value = interface{}  // Direct Go types
```

**Why**:
- Go's interface{} + type switches are idiomatic
- No boxing/unboxing overhead
- Direct compatibility with Go standard library
- Simpler code, easier debugging
- Type assertions are fast in Go

**Trade-off**: Less type safety at compile time, but:
- Tests catch type errors
- Runtime type assertions are cheap
- Code is much more readable

---

## Query Planning Lessons

### Phase-Based Execution

**Insight**: Group patterns into phases based on symbol dependencies

**Why**:
- Prevents Cartesian products from unrelated patterns
- Provides natural ordering for join execution
- Enables early predicate evaluation
- Simplifies join condition detection

**Example**:
```
Phase 1: [?e :person/name "Alice"] → produces ?e
Phase 2: [?e :person/age ?age] → uses ?e from phase 1
Phase 3: [(> ?age 21)] → filters using ?age
```

**Critical**: Each phase can only reference symbols from previous phases. This creates a natural topological ordering.

### Expression Scheduling

**Problem**: Expressions can bridge disjoint relation groups

**Insight**: Schedule expressions at the earliest phase where all input symbols are available

**Why**: An expression like `[(+ ?x ?y) ?z]` can:
- Connect two disjoint patterns (one providing ?x, another ?y)
- Enable downstream joins via ?z
- Must execute before predicates that reference ?z

**Implementation**: See `planner/planner.go` expression phase assignment

---

## Performance Pitfalls Discovered

### 1. Unique Value Counting in Annotations

**Mistake**: Added expensive unique value counting to annotation system
**Impact**: 150× slowdown on complex queries
**Lesson**: Performance annotations must have negligible overhead
**Solution**: Removed unique counting, kept only fast metrics

### 2. Premature Optimization is Real

**Attempts that failed**:
- Key mask iterator (overhead > benefit)
- Complex iterator reuse strategies (simpler was faster)
- Aggressive CSE with parallelization (removed parallelism opportunity)

**Lesson**: Profile first, optimize second. "Fast enough" simple code beats complex "optimized" code.

### 3. Memory Pre-allocation

**Win**: Pre-sizing hash maps for joins
**Impact**: 24-32% faster, 24-30% less memory
**Lesson**: When you know the size, pre-allocate

**Example**:
```go
// Before
m := make(map[TupleKey][]Tuple)  // grows dynamically

// After
m := make(map[TupleKey][]Tuple, leftSize)  // pre-sized
```

---

## Scale-Up vs Scale-Out

### The Hybrid Approach

**Single-node approach**: Fixed-size keys, direct memory operations
**Distributed approach**: Lazy evaluation, sophisticated planning
**Our Go implementation**: Pragmatic middle ground

**Decisions**:
- Fixed keys enable efficient scanning
- Phase-based planning prevents bad joins
- Streaming iterators reduce memory
- Single-node focus keeps it simple

**Scale targets**:
- 100K-1M datoms: Excellent performance
- 1M-10M datoms: Good performance
- 10M-100M datoms: Reasonable with query optimization
- 100M+: Consider distributed approach

---

## Testing Philosophy

### Property-Based Testing

**Relational algebra laws**:
- Commutativity: R ⋈ S = S ⋈ R
- Associativity: (R ⋈ S) ⋈ T = R ⋈ (S ⋈ T)
- Distributivity: σ(R ⋈ S) = σ(R) ⋈ S (when applicable)

**Why**: If these laws don't hold, the implementation is wrong

### Differential Testing

**Approach**: Test against reference implementations
- Small queries: Compare with in-memory Datalog
- Known results: OHLC queries with expected outputs
- Edge cases: Empty relations, single tuples, etc.

**Critical**: Don't just test that queries run - test that they produce correct results

---

## What We Got Right

1. **Simplicity over complexity** - Direct Go types, not complex variant systems
2. **Algorithms over micro-optimizations** - Join ordering matters more than cache tricks
3. **Testing is mandatory** - Tests found bugs that code review missed
4. **Honest documentation** - Document failures and limitations
5. **Profile before optimizing** - Measured performance guided decisions

---

## What We'd Do Differently

1. **Start with streaming** - We refactored to streaming iterators after initial materialization
2. **Cost-based planning earlier** - Greedy join ordering works but could be smarter
3. **Statistics from day one** - Cardinality estimates would help query planning
4. **More aggressive testing** - Property-based tests earlier would have caught bugs sooner

---

## For Future Implementers

If you're building a Datalog engine:

1. **Get join ordering right** - This is 90% of performance
2. **Use streaming** - Don't materialize unless necessary
3. **Phase-based planning** - Prevents Cartesian products
4. **Test thoroughly** - Relational algebra is subtle
5. **Profile first** - Don't guess where time is spent
6. **Keep it simple** - Complexity doesn't equal performance

And most importantly: **Study existing implementations**. Understanding different implementation patterns taught us more than any paper could.

---

## References

**Current Architecture**: See `ARCHITECTURE.md` in repository root
**Implementation Guide**: See `CLAUDE.md` for current implementation details

**Classic Papers**:
- Selinger et al. "Access Path Selection" (1979) - Join ordering
- C.J. Date "Database in Depth" - Relational theory
- Datalog papers from Stanford, MIT, etc.
