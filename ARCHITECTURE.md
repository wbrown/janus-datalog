# Janus Datalog Architecture

**Last Updated**: February 2026

## Overview

Janus Datalog is a Datomic-style Datalog query engine implemented in Go, inspired by memories of both single-node optimized and distributed scale-out implementations.

## Core Components

### 1. Storage Layer (`datalog/storage/`)

**EAVT Model**: Entity-Attribute-Value-Transaction with CRDT semantics
- **Fixed-size keys**: 69 bytes (E:20 + A:32 + Tx:16 + Op:1), plus optional 16-byte AfterRef for RGA operations
- **Variable-size values**: 2-byte size prefix + 1-byte type tag + data
- **L85 encoding**: Custom Base85 that preserves sort order for range scans
- **Seven indices**: EAVT, EATV, AEVT, AETV, AVET, VAET, TAEV

**Index Semantics**:
| Index | Use Case |
|-------|----------|
| EAVT  | Entity lookups with add-wins resolution (cardinality-many) |
| EATV  | Entity lookups with LWW resolution (cardinality-one/vector) |
| AEVT  | Attribute scans across entities |
| AETV  | Attribute scans with temporal ordering |
| AVET  | Value-based lookups (uniqueness, reverse lookup) |
| VAET  | Reverse reference traversal |
| TAEV  | Transaction-based queries, history |

**BadgerDB Backend**:
- Persistent LSM-tree storage
- Iterator reuse optimizations (SinglePositionReuse, MultiPositionReuse)
- Batch scanning for large binding sets (>100 tuples threshold)
- Key-only scanning for existence checks
- Cardinality-aware early termination (cardinality-one returns first match)

**CRDT Semantics**:
- **Last-Writer-Wins (LWW)**: For cardinality-one attributes; EATV index orders by Tx descending so first result is current value
- **Add-Wins Sets**: For cardinality-many attributes; tracks add/remove operations with tombstones
- **RGA (Replicated Growable Array)**: For cardinality-vector; positional inserts with AfterRef and tombstones
- **ElementID as Tx**: 16-byte transaction ID (8-byte Lamport clock + 8-byte ReplicaID) enables causal ordering across replicas

### 2. Type System (`datalog/`)

**User-Facing Types** (defined in top-level `datalog/` package):
- `Datom`: Core unit with (E, A, V, Tx, Op, AfterRef)
  - `E: Identity` — Entity identifier with SHA1 hash and L85 encoding
  - `A: Keyword` — Interned attribute pointer (pointer equality for O(1) comparison)
  - `V: Value` — Direct Go types: `string`, `int64`, `float64`, `bool`, `time.Time`, `[]byte`, `Identity`, `Keyword`
  - `Tx: ElementID` — 16-byte Lamport+ReplicaID (not uint64)
  - `Op: CRDTOp` — Operation type: None(0), Add(1), Remove(2), RGAInsert(3), RGATombstone(4)
  - `AfterRef: ElementID` — RGA position reference (only when Op is RGA)
- `Identity`: Entity identifier with SHA1 hash, L85 cache, and original string
- `Keyword`: Interned pointer type (`*keyword`); pointer equality implies value equality
- `Symbol`: Interned pointer type (`*symbol`); used only in query ASTs, not stored
- `ElementID`: 16-byte struct (Lamport uint64 + ReplicaID uint64) for CRDT ordering
- `Value`: Just `interface{}` — direct Go types, no wrappers

**Storage Types** (internal, `datalog/storage/`):
- `StorageDatom`: Fixed byte arrays ([20]byte E, [32]byte A, [16]byte Tx) for efficient indexing
- Automatic conversion between user/storage types
- L85 encoding for sortable keys

### 3. Query Engine (`datalog/executor/`)

#### QueryExecutor

Query execution via RealizedPlan:

```go
type QueryExecutor interface {
    Execute(ctx Context, q *query.Query, inputs []Relation) ([]Relation, error)
}
```

**Key Design**:
- Phases ARE Datalog query fragments (not operation type collections)
- Universal interface: `Query + Relations → Relations`
- Multi-relation semantics with progressive collapse
- Clause-by-clause execution with early termination

**Execution Flow**:
```
Phase 1 Query → Execute(inputs=[]) → []Relation
Phase 2 Query → Execute(inputs=Phase1 Keep) → []Relation
Phase 3 Query → Execute(inputs=Phase2 Keep) → Relation
```

Each phase executes its `:where` clauses progressively:
1. Execute clause → produce new relation
2. Append to relation groups
3. Collapse groups (join shared symbols)
4. Early termination on empty
5. Repeat for next clause

#### Streaming Architecture

**Iterator Composition**: Zero-copy lazy evaluation
- `FilterIterator`, `ProjectIterator`, `TransformIterator`
- `PredicateFilterIterator`, `FunctionEvaluatorIterator`
- `DedupIterator`, `ConcatIterator`

**BufferedIterator**: Solves single-consumption problem
- Buffers on first iteration for re-use
- Efficient `IsEmpty()` checks (peek at first tuple)
- `Clone()` creates independent iterators
- `Reset()` and `Size()` for re-iteration
- Multiple concurrent iterations supported

**Symmetric Hash Join** (optional):
- Stream-to-stream joins without materialization
- Dual hash table with incremental processing
- Trade-off: Slightly slower but enables full pipeline streaming

**Configuration**: Options-based (no global state)
```go
type ExecutorOptions struct {
    // Annotations
    Collector *annotations.Collector   // nil = zero overhead

    // Streaming
    EnableIteratorComposition  bool    // Lazy evaluation (default: true)
    EnableTrueStreaming        bool    // No auto-materialization (default: true)
    EnableSymmetricHashJoin    bool    // Stream-to-stream joins (default: false)

    // Parallel execution
    EnableParallelSubqueries   bool
    MaxSubqueryWorkers         int

    // Subquery optimization
    EnableSubqueryDecorrelation bool   // Batch identical subqueries
    UseStreamingSubqueryUnion   bool   // Streaming union for results (default: true)
    UseComponentizedSubquery    bool   // Component-based execution (strategy, batcher, pool)

    // Join tuning
    EnableStreamingJoins       bool
    DefaultHashTableSize       int    // Default 256 for streaming relations
    IndexNestedLoopThreshold   int    // 0 = always use HashJoinScan

    // Aggregation
    EnableStreamingAggregation      bool
    EnableStreamingAggregationDebug bool

    EnableDebugLogging bool
}
```

> For comprehensive streaming architecture history, see [docs/archive/2025-10/STREAMING_ARCHITECTURE_COMPLETE.md](docs/archive/2025-10/STREAMING_ARCHITECTURE_COMPLETE.md)

#### Relation Collapsing

**Critical Algorithm** — prevents memory exhaustion on complex queries:
```
Given N relations from clause execution:
1. Take first ungrouped relation as seed
2. Scan remaining for any with shared columns
3. Join shared relations into seed group
4. Repeat until no more can join
5. If ungrouped remain, start new seed group
6. Error if disjoint groups persist after all expressions
```

This is O(n²) where n = relations per phase (typically 2-5), so effectively constant.

> For a comprehensive guide to the relational algebra system, see [RELATIONAL_ALGEBRA_OVERVIEW.md](RELATIONAL_ALGEBRA_OVERVIEW.md)

#### Multi-Source Query Execution

- **SourceRouter** (`executor/source_router.go`): Routes patterns by `pattern.Source` field via `map[Symbol]PatternMatcher`
  - Also implements `PredicateAwareMatcher` (delegates pushdown) and `EntityLookupMatcher` (delegates to `$` for `get-else`, `missing?`, `get-some`)
- **SliceSource[T]** (`executor/slice_source.go`): Wraps Go slices as queryable data sources with `AttributeSchema`
- **MemoryPatternMatcher** (`executor/pattern_match.go`): In-memory pattern matching over `[]Datom`

#### NOT/OR Clause Execution

- **NOT** (`not`, `not-join`): For each unique combination of join variables, executes inner clauses; filters tuples where inner query produces results
- **OR** (`or`, `or-join`): Two modes:
  - **Union mode** (pattern-only branches): Execute each branch independently, union results
  - **Fallback mode** (expression-aware): Per-tuple evaluation, try branches in order, short-circuit on first match

### 4. Query Planning (`datalog/planner/`)

**Single Planner**: `ClauseBasedPlanner` (the only planner; old phase-based planner has been removed)

The planner operates directly on `[]query.Clause` using a greedy phasing algorithm:

```
Parse → Clauses → Greedy Phase Once → RealizedPlan
```

**Key Design Principles**:
- **Single phasing pass**: No re-phasing after optimizations
- **Context-dependent scoring**: Clauses scored based on available symbols
- **Unified clause interface**: No separation into pattern types
- **Greedy selection**: Pick best executable clause at each step

**Greedy Algorithm** (`clause_phasing.go`):
1. Start with input symbols as available
2. Score all executable clauses (requirements satisfied)
3. Select highest-scoring clause and add to phase
4. Update available symbols with clause outputs
5. Repeat until no clauses can execute
6. Start new phase with remaining clauses

> Implementation: `datalog/planner/clause_phasing.go` (greedy algorithm)
> Symbol analysis: `datalog/planner/clause_utils.go` (extraction, scoring)
> Entry point: `datalog/planner/planner_clause_based.go` (ClauseBasedPlanner)

**Output**: `RealizedPlan` with query fragments
```go
type RealizedPhase struct {
    Query     *query.Query           // Datalog query fragment for this phase
    Available []query.Symbol         // Symbols from inputs + previous phases
    Provides  []query.Symbol         // Symbols this phase produces
    Keep      []query.Symbol         // Symbols to pass to next phase
    Metadata  map[string]interface{}

    // Explain fields (for plan inspection)
    Patterns    []PatternPlan
    Expressions []ExpressionPlan
    Predicates  []PredicatePlan
    Subqueries  []SubqueryPlan
}
```

**Symbol Semantics**: The distinction between `Available` and `Provides` is critical:
- **Available**: Environment symbols (input parameters + previous outputs) usable for filtering/correlation
- **Provides**: Relation columns actually IN the phase's output data
- **Invariant**: `Keep ⊆ Provides ∩ Available` (can only keep columns that exist in the relation)

Input parameters from `:in` clause are in ALL phases' `Available` but typically NOT in `Provides` (they filter data but don't appear as result columns, analogous to SQL prepared statement parameters).

See [docs/INPUT_PARAMETER_SEMANTICS.md](docs/INPUT_PARAMETER_SEMANTICS.md) for detailed explanation.

**Optimizations**:
- Index selection based on bound values (O(1) decision tree)
- Equality predicates pushed into joins
- Query plan caching (3x speedup)
- Early predicate filtering in executor
- Phase reordering by symbol connectivity (prevents cross-products)

**Design History — Constraint-Driven Innovation**:

The phase abstraction originated in distributed Datalog implementations using Elasticsearch, where the parent-child document model imposes a critical constraint: **queries can only traverse one level of relationships per request**.

This forced decomposition into phases — each performing one entity-fact join — which revealed deeper truths about query composition:
- Each phase is an **independent relational algebra expression**
- Phases compose via natural join with **provable correctness**: `Result = Phase₀ ⋈ Phase₁ ⋈ ... ⋈ Phaseₙ`
- Explicit metadata makes dependencies clear and **checkable**: `Keep ⊆ Provides ∩ Available`

Today, Janus works with any storage backend that supports arbitrary join depths. The phase abstraction is retained for debuggability, provable correctness, optimization opportunities, and explicit metadata flow.

> See CLAUDE.md "Design History" section for the full narrative with related work (MapReduce, Unix pipes, React).

### 5. Schema (`datalog/schema/`)

Optional, additive schema enforcement:
- **Type validation**: Attribute value type checking at write time
- **Cardinality**: `:db.cardinality/one` (LWW) and `:db.cardinality/many` (add-wins sets) and `:db.cardinality/vector` (RGA)
- **Uniqueness constraints**: `:db.unique/value` and `:db.unique/identity`
- **Schema resolution**: Merges schema definitions from transactions
- Integrated with Pull API for reference traversal and cardinality-aware results

> See [docs/reference/SCHEMA.md](docs/reference/SCHEMA.md) for full documentation.

### 6. Pull API (`datalog/executor/pull.go`)

Declarative entity attribute retrieval:
- Attribute specs, wildcards (`[*]`), nested reference traversal
- Cycle detection via visited set
- Cardinality-aware: returns single value for card-one, vectors for card-many
- **9x faster** than equivalent Datalog queries for entity attribute retrieval

### 7. Parser (`datalog/parser/`)

**EDN Support**:
- Clojure-style syntax parsing
- Query transformation to internal representation
- Pull pattern parsing

**Query Features**:
- Pattern matching: `[?e :attr ?v]`
- Predicates: `[(< ?x 100)]`, variadic chained: `[(< 0 ?x 100)]`
- Expressions: `[(+ ?x ?y) ?z]`, `[(str ?first " " ?last) ?full]`
- Aggregations: `(sum ?amount)`, `(count ?e)`, `(avg ?x)`, `(min ?x)`, `(max ?x)`
- Subqueries: `[(q [...] $) [[?result]]]`
- Order-by: `:order-by [?x :desc]`
- NOT clauses: `(not ...)`, `(not-join [?x] ...)`
- OR clauses: `(or ...)`, `(or-join [?x] ...)`
- Pull expressions: `(pull ?e [:name :age])`
- Time functions: `year`, `month`, `day`, `hour`, `minute`, `second`
- History predicates: `[(history)]`, `[(as-of ?tx N)]`

### 8. Reflect API (`datalog/reflect/`)

Go struct reflection for Datalog:
- **Writer**: Convert Go structs to datoms via struct tags
- **Reader**: Query results back into Go structs
- **QueryReader**: Typed query results with struct tag mapping
- Schema inference from struct tags

> See [docs/reference/REFLECT.md](docs/reference/REFLECT.md) for documentation.

### 9. Query Builder (`datalog/qb/`)

Type-safe query construction in Go:
- Fluent API for building queries programmatically
- Source management (`Source()`, `PatFrom()` for multi-source)
- Pattern, predicate, expression, subquery, aggregation, pull builders
- Compile-time safety vs. raw EDN strings

### 10. Annotations (`datalog/annotations/`)

Performance monitoring via decorator pattern:
- `WrapMatcher()` wraps any `PatternMatcher` transparently
- Handler injection: storage layer receives handler for detailed events
- Zero overhead when disabled (nil handler)
- Event types: pattern matching, join operations, expression evaluation, phase execution

## Key Design Decisions

### Relations Over Bindings
Replaced simple `map[Symbol]Value` with full `Relation` abstraction:
- Supports multi-value variables
- Enables sorted iteration for optimizer
- Cleaner join semantics

### Iterator Reuse Strategy
Storage layer keeps iterators open across multiple seeks:
- SinglePositionReuse: One varying position
- MultiPositionReuse: Multiple positions vary
- Batch scanning: Collect multiple values per seek (>100 tuple threshold)

### Semantic Correctness First
RelationInput iteration ensures correct aggregation scoping:
- `:in $ [[?x ?y] ...]` iterates over tuples
- Each tuple processed independently
- Performance optimization comes after correctness

### Interned Types for O(1) Equality
Keywords and Symbols are interned pointers:
- Pointer comparison replaces string comparison
- Panics on interning violation (same value, different pointers)
- Critical for hash join and deduplication performance

## Complexity Analysis

### Core Algorithm Complexities

| Algorithm | Time | Space | Notes |
|-----------|------|-------|-------|
| **Hash Join** | O(\|B\| + \|P\| × \|M\|) | O(\|B\|) | B=build, P=probe, M=avg matches per probe |
| **Semi-Join** | O(\|L\| + \|R\|) | O(\|R\|) | Existence check only |
| **Anti-Join** | O(\|L\| + \|R\|) | O(\|R\|) | NOT EXISTS |
| **Relation Collapse** | O(n²) | O(1) | n=relations per phase, typically 2-5 |
| **Query Planning** | O(\|c\|² × \|s\|) | O(\|c\|) | c=clauses, s=symbols |
| **Index Selection** | O(1) | O(1) | Fixed-depth decision tree |
| **Pattern Scan** | O(\|results\|) | O(1) streaming | B-tree range scan; O(1) for card-one |
| **Batch Aggregation** | O(\|rel\|) | O(\|groups\|) | Single-pass accumulation |
| **Streaming Aggregation** | O(\|rel\|) | O(\|groups\|) | Lazy with StreamingAggregateRelation |
| **L85 Encode/Decode** | O(n) | O(n) | n=16-32 bytes typically; 5 mod ops per 4-byte group |
| **Keyword/Symbol Comparison** | O(1) | O(1) | Pointer equality via interning |

### Join Strategy Details

**Hash Join** (`executor/join.go`):
- Build phase: Iterates smaller relation, inserts into `TupleKeyMap` using FNV-1a hash
- Probe phase: Iterates larger relation, looks up matching tuples
- Streaming mode: Returns `StreamingRelation` for lazy evaluation (hash table materialized, probe side streams)
- Transaction deduplication: Optional map tracking latest Tx per entity-attribute pair

**Semi-Join / Anti-Join**: Build right-side key set O(\|R\|), filter left side O(\|L\|) with O(1) lookups.

### Query Execution Complexity

**Single phase** with p patterns, e expressions, and f predicates:
```
T(phase) = Σᵢ T(patternᵢ) + Σⱼ T(expressionⱼ) + Σₖ T(predicateₖ) + (p+e+f) × T(collapse)
```

Where:
- `T(pattern)` = O(\|scan results\|) — dominated by storage I/O
- `T(expression)` = O(\|input relation\|) — per-tuple evaluation
- `T(predicate)` = O(\|input relation\|) — per-tuple filtering
- `T(collapse)` = O(n²) where n ≤ 5 — effectively O(1)

**Multi-phase query**: T(query) = Σ T(phaseᵢ), phases execute sequentially.

### Subquery Execution

| Mode | Time | When Used |
|------|------|-----------|
| Sequential | O(\|combos\| × T(sub)) | Default |
| Batched (componentized) | O(T(sub)) | When `UseComponentizedSubquery=true` |
| Parallel | O(T(sub) / workers + overhead) | When `EnableParallelSubqueries=true` |

Where \|combos\| = unique input variable combinations from outer relation.

### NOT/OR Complexity

| Clause | Time | Space |
|--------|------|-------|
| NOT | O(\|combos\| × T(inner) + \|input\|) | O(\|combos\|) |
| NOT-JOIN | Same as NOT, restricted join vars | O(\|combos\|) |
| OR (union) | O(Σ T(branchᵢ)) | O(\|result\|) |
| OR (fallback) | O(\|outer\| × T(branch)) avg | O(\|outer\| × \|branch result\|) |

OR fallback short-circuits after first successful branch per tuple.

### Pull API Complexity

- **Flat pull** (no refs): O(\|specs\| × log\|storage\|) — one B-tree lookup per attribute
- **Nested pull** (following refs): O(d × \|specs\| × log\|storage\|) — d = reference depth
- **Wildcard pull**: O(\|entity attributes\|) — scans all attributes for entity
- Cycle detection: O(d) space via visited set

### Storage Key Operations

- **Key encoding**: O(69) bytes = O(1) — fixed-size encoding per datom
- **Range scan prefix**: O(prefix length) — typically 20-52 bytes depending on bound components
- **Index choice**: O(1) — decision tree with ≤5 branches based on which of (E, A, V, Tx) are bound

## Datalog Feature Support

### Implemented (~85%)
- Basic patterns and joins
- Predicates and expressions (arithmetic, string, variadic comparisons)
- Aggregations with grouping (`sum`, `count`, `avg`, `min`, `max`)
- Subqueries with proper scoping (TupleBinding, RelationBinding)
- Order-by clause (multi-column, directional)
- Time extraction functions (`year`, `month`, `day`, `hour`, `minute`, `second`)
- As-of queries and history predicates
- Pull API (attributes, wildcards, nested refs, cycle detection)
- NOT/OR clauses (`not`, `not-join`, `or`, `or-join`)
- Schema support (types, cardinality, uniqueness)
- CRDT storage (LWW, add-wins, RGA)
- Multi-source queries (cross-database joins)
- Database export/import (EDN format)
- QueryInto API (typed results into Go structs)
- Reflect API (Go structs to/from datoms)
- Query builder (type-safe Go API)

### Not Implemented
- Rules system
- Recursive queries
- Window functions
- Distinct aggregation modifier
- CollectionBinding for subqueries

## Datomic Compatibility (~70-75%)

### Compatible Features
- EAVT data model
- Transaction time with as-of queries
- Core query syntax (patterns, predicates, expressions)
- Aggregations with grouping
- Subqueries with scoped bindings
- Pull API (attributes, wildcards, nested refs)
- Schema (types, cardinality, uniqueness)
- History queries (`[(history)]`)
- NOT/OR clause semantics

### Differences
- No transaction functions (`:db/fn`)
- No database functions beyond built-ins
- ElementID (Lamport+ReplicaID) instead of monotonic long for Tx
- CRDT semantics instead of MVCC for conflict resolution
- No lazy entity API
- No log API

## Code Organization

```
datalog/
├── storage/      # BadgerDB backend, CRDT resolution, index management
├── executor/     # Query execution, joins, Pull API, multi-source routing
├── planner/      # Clause-based query planning
├── parser/       # EDN and query parsing, pull pattern parsing
├── query/        # Query type definitions, clause types
├── codec/        # L85 encoding (sort-preserving Base85)
├── edn/          # EDN lexer/parser
├── schema/       # Schema types, validation, cardinality, uniqueness
├── reflect/      # Go struct ↔ datom reflection API
├── qb/           # Type-safe query builder
├── annotations/  # Performance monitoring (decorator pattern)
├── constraints/  # Time range constraints
└── experimental/ # Disabled optimization experiments
```

Top-level `datalog/` package contains core types: `Datom`, `Identity`, `Keyword`, `Symbol`, `ElementID`, `Value`, interning, and comparison.

## Performance Characteristics

### Achieved Optimizations (Verified 2025-10-25)
- **2.22x faster**: Streaming execution on low-selectivity filters
- **52% memory reduction**: Up to 91.5% on large datasets with predicate pushdown
- **4.06x speedup**: Iterator composition vs materialized operations
- **2.06x speedup**: Parallel subquery execution (8 workers)
- **1.58-2.78x faster**: Predicate pushdown (scales with dataset size)
- **3x faster**: Query plan caching
- **6.26x speedup**: Parallel intern cache optimization
- **9x faster**: Pull API vs equivalent queries

**Streaming Performance by Selectivity** (10K tuples):
| Selectivity | Speedup | Memory Reduction |
|-------------|---------|------------------|
| High (1%)   | 1.07x   | 2% |
| Medium (10%)| 1.44x   | 19% |
| Low (50%)   | 2.22x   | 52% |
| Iterator Composition | 4.06x | 89% |

### Known Bottlenecks
- No parallel execution within phases
- Symmetric hash join slightly slower than standard (trade-off for streaming)
- Streaming aggregation opt-in (not default)

## Future Optimizations

1. **Streaming Aggregations** — Reduce memory for large groups (default)
2. **Distinct Aggregation** — Add `distinct` support to existing aggregations
3. **Statistics-Based Planning** — Cardinality estimation for cost-based optimization
4. **WASM Build** — Browser deployment support
