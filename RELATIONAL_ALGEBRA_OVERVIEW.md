# Relational Algebra in Janus Datalog

## Executive Summary

Janus Datalog implements a complete relational algebra system as the foundation for query execution. Unlike toy Datalog implementations that use naive nested loops, this system employs sophisticated join algorithms, streaming iterators, and dynamic optimization to handle production-scale data efficiently.

**Key Insight**: The relation collapsing algorithm with dynamic join ordering is what separates this from toy implementations - it prevents memory exhaustion on complex queries by intelligently ordering joins based on intermediate result sizes.

For the detailed set/replayability laws and materialization decision record, see
[Relation Materialization and Set Invariants](docs/ideas/RELATION_MATERIALIZATION_AND_SET_INVARIANTS.md).

## Core Architecture

### Logical Optimization, Then Physical Planning

The optimizer contract is pure Datalog:

```
query.Query → algebra tree → optimized algebra tree → query.Query
```

Algebra is an internal logical representation. Every optimized operator lowers
back into Datalog before physical planning; scoped operators use nested queries
and relation bindings. `Project`, for example, lowers to a subquery with an exact
`:find`. `ClauseBasedPlanner` alone turns the optimized query into
`RealizedPlan` phases.

This separation keeps algebraic equivalence independent from execution topology:
logical rewrites cannot create physical phases, and physical planning cannot
silently erase logical scope.

### ρ-Renaming Through Relation Bindings

A relation-binding subquery positionally maps inner find symbols to outer
binding symbols. This is the relational rename operator, ρ—not projection.

Execution applies the same positional ρ-renaming to proven ordering and
candidate keys. For example, an inner grouped aggregate keyed by `?scenario`
remains keyed after binding that position as `?outer-scenario`. Downstream hash
joins can then consume the proof for unique-build specialization and
deduplication elision.

### The Relation Interface

The `Relation` interface (in `datalog/executor/relation.go`) is the fundamental abstraction:

```go
type Relation interface {
    // Core Operations (Relational Algebra)
    Project(symbols []Symbol) (Relation, error)      // π (projection)
    Filter(filter Filter) Relation                   // σ (selection)
    Join(other Relation) Relation                    // ⋈ (natural join)
    HashJoin(other Relation, joinSymbols []Symbol) Relation // ⋈ (equi-join)
    SemiJoin(other Relation, joinSymbols []Symbol) Relation // ⋉ (semi-join)
    AntiJoin(other Relation, joinSymbols []Symbol) Relation // ▷ (anti-join)
    Aggregate(elements []FindElement) Relation       // γ (grouping/aggregation)
    Sort(orderBy []OrderByClause) Relation          // τ (sort)

    // Datalog-Specific Extensions
    EvaluateFunction(fn Function, output Symbol) Relation
    FilterWithPredicate(pred Predicate) Relation

    // Metadata & Access
    Symbols() []Symbol                    // Schema
    Properties() RelationProperties       // Proven ordering and candidate keys
    Iterator() Iterator                   // Streaming access
    Size() int                            // Cardinality (-1 when unknown; never consumes)
    Materialize() Relation                // Force materialization
}
```

**Design Principles**:
- **Immutable**: All operations return NEW relations
- **Set semantics**: Every Relation contains each complete tuple at most once
- **Streaming-First**: Iterator-based to avoid full materialization
- **Type-Safe**: Strong typing through Go's type system

Physical tuple streams may contain repeated tuples while an operator is still
constructing its result. Before that result is exposed as a `Relation`, the
operator must establish set semantics. Operators that preserve a source set
(selection, deterministic extension, sorting, limiting, semi/anti join) do not
deduplicate again. Projection and union restore set semantics unless a retained
key proves injectivity.

`Materialize()` means ensure replayability, not force eager conversion.
Materialized relations return themselves; streaming relations enable lazy
caching; `LazySeqRelation` is already replayable and also returns itself.

### Two Implementation Strategies

#### 1. MaterializedRelation
- Holds all tuples in memory
- Fast random access via `Get(index)`
- Efficient for small-medium result sets
- Used after operations that require full materialization (sorting, aggregation)

#### 2. StreamingRelation
- Wraps an iterator without materializing
- Lazy evaluation of operations
- Memory efficient for large datasets
- Chains operations without intermediate storage

```go
// Example: Chaining operations without materialization
result := storageRelation.
    Filter(predicate).      // Applied during iteration
    Project(symbols).       // Applied during iteration
    HashJoin(other, joinSymbols) // Only materializes hash table
```

## Progressive Join Execution

### The Problem
Naive join ordering can create massive intermediate results:
```
[?x :follows ?y] ⋈ [?y :follows ?z] ⋈ [?z :name "Alice"]
```
If executed left-to-right with 1M follows relationships, the first join could produce billions of tuples before filtering by name.

### The Solution: Greedy Join Ordering

The `Relations.Collapse()` method implements a greedy join algorithm:

```go
func (rs Relations) Collapse(ctx Context) Relations {
    // Groups relations by shared symbols
    // Joins them progressively as encountered
    // Short-circuits on empty results
    // Returns independent groups if disjoint
}
```

**Algorithm Steps**:
1. **Group by connectivity**: Relations sharing symbols can join
2. **Progressive joining**: Add relations in the order received
3. **Early termination**: Stop if any join produces empty result
4. **Disjoint handling**: Keep independent groups separate

**Note**: This is a **simplified version** of Selinger's join ordering (1979). It uses a greedy approach without:
- Cost-based optimization
- Cardinality statistics
- Dynamic reordering based on intermediate sizes

The query planner is responsible for providing relations in a sensible order.

**Example Execution**:
```
Query planner provides: R2(100 tuples), R3(10K tuples), R1(1M tuples)
Shared symbols: R1↔R3, R2↔R3

Step 1: Start with R2
Step 2: Join R2⋈R3 → 50 tuples (if shared symbols)
Step 3: Join (R2⋈R3)⋈R1 → 25 tuples
Result: Single relation with 25 tuples
```

**What prevents bad performance**: The query planner's selectivity scoring and phase grouping provides good initial ordering. The `Collapse()` method just executes the plan safely.

## Join Algorithms

### 1. Natural Join
Joins on ALL shared symbols:
```go
func (r *MaterializedRelation) Join(other Relation) Relation {
    sharedSymbols := findSharedSymbols(r, other)
    if len(sharedSymbols) == 0 {
        return crossProduct(r, other) // No shared symbols
    }
    return r.HashJoin(other, sharedSymbols)
}
```

### 2. Hash Join
Most common join algorithm (O(n+m) time, O(min(n,m)) space):
```go
func HashJoin(left, right Relation, joinSymbols []Symbol) Relation {
    // Build phase: Hash smaller relation
    hashTable := buildHashTable(smaller, joinSymbols)

    // Probe phase: Stream through larger relation
    for tuple := range larger {
        key := extractKey(tuple, joinSymbols)
        if matches := hashTable[key]; matches != nil {
            output combineTuples(tuple, matches)
        }
    }
}
```

**Optimizations**:
- Pre-sized hash tables (24-30% memory reduction)
- Smaller relation always used as build side
- Streaming probe phase

### 3. Semi-Join (Existence Check)
Returns tuples from left that have matches in right:
```go
func SemiJoin(left, right Relation, joinSymbols []Symbol) Relation {
    // Build set of keys from right
    rightKeys := buildKeySet(right, joinSymbols)

    // Filter left by existence in right
    return left.Filter(func(tuple) bool {
        key := extractKey(tuple, joinSymbols)
        return rightKeys.Contains(key)
    })
}
```

### 4. Anti-Join (Non-Existence Check)
Returns tuples from left with NO matches in right:
```go
func AntiJoin(left, right Relation, joinSymbols []Symbol) Relation {
    // Inverse of semi-join
    rightKeys := buildKeySet(right, joinSymbols)
    return left.Filter(func(tuple) bool {
        return !rightKeys.Contains(extractKey(tuple, joinSymbols))
    })
}
```

### Same-Entity Constraint Fusion

When a relation already binds `?e`, a subsequent proven CardinalityOne pattern
such as `[?e :task/status :status/complete]` is a selection over that relation.
The executor implements it directly:

1. Resolve the attribute through the cache-backed entity lookup.
2. Compare with `datalog.ValuesEqual`.
3. Retain matching tuples in the same traversal.

This is equivalent to matching a second relation and joining on `?e`, but avoids
constructing and hashing that relation. The same pass can attach fresh bindings
from `[?e :attr ?value]`; contiguous fetches and constraints share one traversal.
History, as-of, unknown-schema, CardinalityMany, and CardinalityVector patterns
retain ordinary match semantics.

On 1K/10K entities, constant constraint fusion is 21.9–23.2% faster with
35.3–38.8% less memory and 42.3–43.4% fewer allocations. The production-shaped
complex checkpoint improves 11.1% time, 21.8% memory, and 23.2% allocations.

### Correlated OR/Fallback Outer Replacement

Correlated OR and fallback execution consumes an outer relation and emits tuples
containing that outer binding plus branch results. The emitted relation is
therefore a replacement for the consumed outer relation, not an independent
relation to join back against it.

`QueryExecutor` records which relation groups formed the OR outer input and
replaces exactly those groups with the result. Unrelated groups remain and still
collapse normally. Uncorrelated union continues to append an independent
relation.

This removes five redundant natural joins from the production-shaped complex
query, improving 11.3% time, 8.3% memory, and 10.6% allocations. Replacement is
observable through `or/outer-replaced`; iterator, cache-build, and close errors
propagate rather than being interpreted as a missing branch.

Outer selection also leaves a single relation streaming until a branch shape
requires re-iteration for join-key narrowing. This changes when materialization
occurs but is neutral for full-result performance; it is a streaming contract,
not an optimization claim. Required materialization is observable through
`or-fallback/outer.materialized`.

### Replayable Sources, Single-Use Products

Correlated subqueries may need to reuse their source relations after extracting
input combinations. Those source relations use lazy replay caches. Their
combined product, however, is consumed exactly once by typed combination
deduplication and should not itself be materialized.

This distinction avoids a full product tuple slice and an initial full-tuple
dedup pass immediately before projected-symbol deduplication. On valid 10K/100K
set products, direct product streaming improves 39.6–44.2% time and 37.0–38.7%
memory.

Materialization and iterator composition retain strict failure semantics:
incomplete early-close caches, predicate evaluation failures, iterator errors,
and close errors propagate rather than producing partial successful relations.

## Aggregation System

### Grouped Aggregation
Non-aggregated find symbols define the grouping key:

```go
func Aggregate(rel Relation, groupBy []Symbol, aggs []Aggregate) Relation {
    groups := make(map[TupleKey][]Tuple)

    // Group phase
    for tuple := range rel {
        key := extractKey(tuple, groupBy)
        groups[key] = append(groups[key], tuple)
    }

    // Aggregate phase
    for key, group := range groups {
        tuple := append(key.Values(), computeAggregates(group, aggs)...)
        output(tuple)
    }
}
```

### Streaming Aggregation
For memory efficiency with large groups:

```go
type StreamingAggregate struct {
    groups map[TupleKey]*AggregateState
}

func (s *StreamingAggregate) Process(tuple Tuple) {
    key := extractGroupKey(tuple)
    state := s.groups[key]
    state.Update(tuple) // Incremental update
}
```

### Conditional Aggregation
Filters during aggregation (optimization for correlated subqueries):

```go
func ConditionalAggregate(rel Relation, predicate Predicate, agg Aggregate) {
    for tuple := range rel {
        if predicate.Eval(tuple) {
            agg.Update(extractValue(tuple))
        }
    }
}
```

## Query Execution Pipeline

### Phase-Based Execution
Queries are organized into phases based on variable dependencies:

```
Phase 1: [?s :symbol/ticker "AAPL"]          // Provides ?s
Phase 2: [?b :price/symbol ?s]               // Uses ?s, provides ?b
         [?b :price/time ?t]                  // Uses ?b, provides ?t
Phase 3: [(year ?t) ?y]                      // Uses ?t, provides ?y
         [(= ?y 2024)]                        // Uses ?y (predicate)
```

### Execution Flow

1. **Pattern Matching**: Storage scans produce initial relations
2. **Progressive Joining**: Relations collapse within each phase
3. **Expression Evaluation**: Computed symbols added
4. **Predicate Application**: Filtering based on conditions
5. **Cross-Phase Joins**: Results flow between phases
6. **Final Operations**: Sorting, aggregation, projection

```go
func ExecutePhase(phase *planner.RealizedPhase, inputs []executor.Relation) ([]executor.Relation, error) {
    // phase.Query is an executable Datalog fragment. One-pattern fragments
    // preserve safe OrderBy/Limit requirements at the matcher boundary.
    groups, err := queryExecutor.Execute(ctx, phase.Query, inputs)
    if err != nil {
        return nil, err
    }
    return executor.Relations(groups).Collapse(ctx), nil
}
```

## Performance Characteristics

### Time Complexity

| Operation | Best Case | Average | Worst Case | Space |
|-----------|-----------|---------|------------|-------|
| Hash Join | O(n+m) | O(n+m) | O(n×m)* | O(min(n,m)) |
| Semi-Join | O(n+m) | O(n+m) | O(n+m) | O(m) |
| Anti-Join | O(n+m) | O(n+m) | O(n+m) | O(m) |
| Project | O(n) | O(n) | O(n) | O(n) |
| Filter | O(n) | O(n) | O(n) | O(k) |
| Sort | O(n log n) | O(n log n) | O(n log n) | O(n) |
| Aggregate | O(n) | O(n) | O(n) | O(g) |

*Worst case for hash join occurs with many hash collisions

### Memory Usage

**Streaming Operations** (constant memory):
- Filter
- Project (symbol subset)
- Scan from storage

**Materializing Operations** (O(n) memory):
- Sort
- Hash Join (build side)
- Aggregate (group states)
- Deduplication

**Optimization**: The system automatically chooses streaming vs materialized based on operation requirements.

## Real-World Performance

### Example: OHLC Query
Computing daily Open-High-Low-Close for 260 trading days:

**Without Optimization**:
- 260 subqueries × 4 aggregates = 1,040 storage scans
- Time: 41 seconds

**With Relation Collapsing**:
- Single scan with grouped aggregation
- Time: 10.2 seconds (4× faster)

### Example: Complex Join
Finding friends-of-friends in social network:

**Naive Ordering**:
```
[?a :follows ?b] ⋈ [?b :follows ?c] ⋈ [?c :name "Alice"]
1M tuples × 1M tuples = 1 trillion intermediate tuples
```

**With Collapsing**:
```
[?c :name "Alice"] ⋈ [?b :follows ?c] ⋈ [?a :follows ?b]
1 tuple × 1000 tuples × 1000 tuples = 1M tuples processed
```

## Theoretical Foundation

### Relational Algebra Laws

The implementation respects fundamental laws:

1. **Commutativity**: `R ⋈ S = S ⋈ R`
2. **Associativity**: `(R ⋈ S) ⋈ T = R ⋈ (S ⋈ T)`
3. **Distributivity**: `σ(R ⋈ S) = σ(R) ⋈ S` (when predicate only references R)
4. **Idempotence**: `π(π(R)) = π(R)` (with proper symbol subset)

### Set Semantics

Following Datalog tradition:
- Relations are sets (no duplicate tuples)
- Operations preserve set semantics
- Empty input → empty output (no NULL values)

### Optimization Rules

The query planner applies standard optimizations:
- **Predicate Pushdown**: Apply filters early
- **Join Reordering**: Start with most selective
- **Early Projection**: Remove unnecessary symbols
- **Join Elimination**: Remove redundant joins

## Comparison with Other Systems

### vs. Traditional SQL Databases
- **No NULL values**: Follows pure relational theory
- **Set semantics**: Automatic deduplication
- **Pattern-based**: Optimized for graph-like queries
- **Immutable**: No UPDATE/DELETE, only INSERT

### vs. Graph Databases
- **Relational foundation**: Predictable performance
- **No path explosions**: Bounded by relation sizes
- **Join-based**: Standard algorithms, not traversals
- **Set operations**: Natural aggregation support

### vs. Other Datalog Engines
- **Production-ready**: Handles large datasets
- **Streaming-first**: Not purely in-memory
- **Dynamic optimization**: Not fixed execution plans
- **Type-safe**: Go's type system prevents errors

## Best Practices

### For Query Authors
1. **Provide selective patterns first** (most constraints)
2. **Use entity IDs when known** (uses EAVT index)
3. **Aggregate in subqueries** (reduces data flow)
4. **Avoid Cartesian products** (ensure relations share variables)

### For Developers
1. **Prefer streaming operations** when possible
2. **Materialize only when necessary** (sorting, random access)
3. **Pre-size collections** based on statistics
4. **Reuse tuple builders** for allocation efficiency
5. **Profile join ordering** for complex queries

## Future Enhancements

### Near-term
- Statistics-based join ordering (cardinality estimation)
- Merge join for pre-sorted relations
- Parallel hash join building
- Adaptive query execution

### Long-term
- Distributed execution across nodes
- Incremental view maintenance
- Cost-based optimization
- Query result caching

## Conclusion

The relational algebra implementation in Janus Datalog demonstrates that:

1. **Theory matters**: Following relational algebra principles ensures correctness
2. **Algorithms matter**: Relation collapsing prevents exponential blowups
3. **Engineering matters**: Streaming, pre-sizing, and careful memory management enable production use

This is not a toy - it's a production-ready query engine that happens to implement Datalog semantics on top of a solid relational algebra foundation.