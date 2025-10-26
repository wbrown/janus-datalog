# Janus Datalog Architecture

**Last Updated**: October 2025

## Recent Major Updates (October 2025)

### QueryExecutor & RealizedPlan (Stage B)
- Phases are now Datalog query fragments, not operation type collections
- Universal `Query + Relations → Relations` interface
- Simplified execution model with multi-relation semantics
- Foundation for future AST-oriented planner (Stage C proposal)

### True Streaming Architecture
- **2.22× faster** with **52% memory reduction** (up to 91.5% on large datasets)
- **4.06× speedup** from iterator composition alone
- BufferedIterator for re-iteration support
- Symmetric hash join option for stream-to-stream joins
- **Enabled by default** with options-based configuration

## Overview

Janus Datalog is a Datomic-style Datalog query engine implemented in Go, inspired by memories of both single-node optimized and distributed scale-out implementations.

## Core Components

### 1. Storage Layer (`datalog/storage/`)

**EAVT Model**: Entity-Attribute-Value-Transaction
- **Fixed-size keys**: 72 bytes (E:20 + A:32 + Tx:20)
- **Variable-size values**: Type prefix + data
- **L85 encoding**: Custom Base85 that preserves sort order
- **Multiple indices**: EAVT, AEVT, AVET, VAET, TAEV

**BadgerDB Backend**:
- Persistent LSM-tree storage
- Iterator reuse optimizations
- Batch scanning (5x performance improvement)
- Key-only scanning for existence checks

### 2. Type System (`datalog/`)

**User-Facing Types**:
- `Datom`: Core unit with (E, A, V, Tx)
- `Identity`: Entity identifier with SHA1 hash
- `Keyword`: Interned attribute names
- `Value`: Direct Go types (no wrappers)

**Storage Types** (internal):
- `StorageDatom`: Fixed byte arrays for indexing
- Automatic conversion between user/storage types
- L85 encoding for sortable keys

### 3. Query Engine (`datalog/executor/`)

**Two-Tier Execution Architecture**:

#### QueryExecutor (Stage B - October 2025)

Modern query execution via RealizedPlan:

```go
type QueryExecutor interface {
    // Execute a Datalog query with input relations
    Execute(ctx Context, query *query.Query, inputs []Relation) ([]Relation, error)
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

#### True Streaming Architecture (October 2025)

**Iterator Composition**: Zero-copy lazy evaluation
- `FilterIterator`, `ProjectIterator`, `TransformIterator`
- `PredicateFilterIterator`, `FunctionEvaluatorIterator`
- `DedupIterator`, `ConcatIterator`

**BufferedIterator**: Solves single-consumption problem
- Buffers on first iteration for re-use
- Efficient `IsEmpty()` checks (peek at first tuple)
- `Clone()` creates independent iterators
- Multiple concurrent iterations supported

**Symmetric Hash Join** (optional):
- Stream-to-stream joins without materialization
- Dual hash table with incremental processing
- Trade-off: Slightly slower but enables full pipeline streaming

**Performance**: 2.22× faster with 52% memory reduction (up to 91.5% on large datasets with predicate pushdown)

**Configuration**: Options-based (no global state)
```go
type ExecutorOptions struct {
    EnableIteratorComposition  bool  // Lazy evaluation (default: true)
    EnableTrueStreaming       bool  // No auto-materialization (default: true)
    EnableSymmetricHashJoin   bool  // Stream-to-stream joins (default: false)
}
```

> For comprehensive streaming architecture history, see [docs/archive/2025-10/STREAMING_ARCHITECTURE_COMPLETE.md](docs/archive/2025-10/STREAMING_ARCHITECTURE_COMPLETE.md)

#### Legacy Sequential Executor

Original phase-based execution with operation type collections:
- Still available for compatibility
- Not used with QueryExecutor/RealizedPlan
- Maintained for reference

**Critical Algorithm - Relation Collapsing**:
```
1. Start with most selective relations
2. Join progressively, checking sizes
3. Early termination on empty results
4. Defer problematic joins when results grow
```

This prevents memory exhaustion on complex queries.

> For a comprehensive guide to the relational algebra system, see [RELATIONAL_ALGEBRA_OVERVIEW.md](RELATIONAL_ALGEBRA_OVERVIEW.md)

### 4. Query Planning (`datalog/planner/`)

**Evolution to RealizedPlan Architecture**:

#### Current Implementation (Phase-Based)

**Phase-Based Execution**:
- Patterns grouped by symbol dependencies
- Each phase uses symbols from previous phases
- Expressions assigned to earliest possible phase
- Predicates classified as intra/inter-phase

**Output**: `RealizedPlan` with query fragments
```go
type RealizedPhase struct {
    Query     *query.Query      // Datalog query fragment for this phase
    Available []query.Symbol    // Symbols from inputs + previous phases
    Provides  []query.Symbol    // Symbols this phase produces
    Keep      []query.Symbol    // Symbols to pass to next phase
    Metadata  map[string]interface{}
}
```

**Realize() Method**: Converts internal Phase structures to clean query fragments:
```go
func (qp *QueryPlan) Realize() *RealizedPlan {
    // Transforms Phase{Patterns, Expressions, Predicates, ...}
    // Into Phase{Query} where Query.Where contains all clauses
}
```

#### Current Architecture: Clause-Based Planning (October 2025)

**Implementation Status**: COMPLETE

The planner now operates directly on `[]query.Clause` using a greedy phasing algorithm:

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

**Architecture Comparison**:
- **Old**: Separate patterns → phase by dependency → optimize → re-phase
- **New**: Clauses → greedy phase once → RealizedPlan

This represents a fundamentally different architectural approach compared to dependency-based phasing with post-optimization re-phasing.

> Implementation: `datalog/planner/clause_phasing.go` (greedy algorithm)
> Symbol analysis: `datalog/planner/clause_utils.go` (extraction, scoring)
> Entry point: `datalog/planner/planner_clause_based.go` (ClauseBasedPlanner)

**Performance Comparison: Old vs New Planner**:

Both planners produce the same output format (`RealizedPlan` with phases), but differ in how those phases are created:

| Characteristic | Old Planner (Phase-Based) | New Planner (Clause-Based) |
|---------------|---------------------------|----------------------------|
| **Approach** | Group by type → phase → optimize | Optimize → greedy phase once |
| **Phase creation** | Pre-defined type groupings | Adaptive greedy selection |
| **Planning speed** | 3-12 μs | 1-7 μs (37-88% faster) |
| **Plan quality** | Baseline | Equivalent (within noise) |
| **Combined with new executor** | ~4-8s (OHLC) | **~2-4s (2× faster)** |
| **Executor compatibility** | Old executor only (without flag) | Both executors |

**Key Finding**: The 2× performance improvement comes from QueryExecutor's clause-by-clause streaming execution, not from plan quality differences. Both planners produce equivalent-quality plans when using the same executor.

**Note**: Planning overhead is negligible (1-15 microseconds vs milliseconds/seconds of execution). The new planner is faster at planning (37-88% speedup), but both planners produce equivalent-quality plans. The 2× speedup comes from QueryExecutor's streaming execution model.

> **Detailed Comparison**: See [docs/reference/PLANNER_COMPARISON.md](docs/reference/PLANNER_COMPARISON.md) for architectural details, benchmark suite explanation, and migration guide.

**Design History - Constraint-Driven Innovation**:

The phase abstraction has a fascinating origin story. In distributed Datalog implementations using Elasticsearch as the storage layer, the parent-child document model imposes a critical constraint: **queries can only traverse one level of relationships per request** (parent → child, not parent → child → grandchild).

This architectural limitation forced the query planner to decompose Datalog queries into phases:

```clojure
;; Original query intention:
[?s :symbol/ticker "AAPL"]     ; Get symbol entity
[?p :price/symbol ?s]           ; Get price facts for symbol
[?p :price/time ?t]             ; Get time from prices

;; Must execute as phases due to ES constraint:
Phase 0: [?s :symbol/ticker "AAPL"]       → ES parent query
Phase 1: [?p :price/symbol ?s]            → ES child query (using ?s from Phase 0)
Phase 2: [?p :price/time ?t]              → ES child query (using ?p from Phase 1)
```

Each phase performs one entity-fact join, with subsequent phases using entity IDs from previous phases as input bindings.

**From Necessity to Abstraction**:

Distributed implementations developed sophisticated machinery to handle this constraint:

1. **Pattern Grouping**: Group patterns by entity symbol (`e` component)
2. **Symbol Tracking**: Explicit `Available`, `Provides` metadata
3. **Phase Reordering**: Greedy algorithm based on symbol intersections
4. **Symbol Lifetime**: Calculate which symbols to `Keep` for future phases

This constraint-driven design had elegant theoretical properties worth preserving and generalizing:

**Formalized as First-Class Abstraction**:
```go
type Phase struct {
    Available  []Symbol  // Symbols visible from inputs + previous phases
    Provides   []Symbol  // Symbols this phase produces
    Keep       []Symbol  // Symbols to carry forward
    Metadata   map[string]interface{}  // Optimization metadata
}
```

**Symbol Semantics**: The distinction between `Available` and `Provides` is critical:
- **Available**: Environment symbols (input parameters + previous outputs) usable for filtering/correlation
- **Provides**: Relation columns actually IN the phase's output data
- **Invariant**: `Keep ⊆ Provides ∩ Available` (can only keep columns that exist in the relation)

Input parameters from `:in` clause are in ALL phases' `Available` but typically NOT in `Provides` (they filter data but don't appear as result columns, analogous to SQL prepared statement parameters).

See [docs/INPUT_PARAMETER_SEMANTICS.md](docs/INPUT_PARAMETER_SEMANTICS.md) for detailed explanation.

**Key Insight**: What started as a workaround for Elasticsearch's limitations revealed deeper truths about query composition:

- Each phase is an **independent relational algebra expression**
- Phases compose via natural join with **provable correctness**: `Result = Phase₀ ⋈ Phase₁ ⋈ ... ⋈ Phaseₙ`
- Explicit metadata makes dependencies clear and **checkable**: `Keep ⊆ Provides ∩ Available`
- Dependency-based reordering is **sound by construction**

**Why This Matters**:

Today, Janus works with any storage backend (BadgerDB, in-memory, etc.) that supports arbitrary join depths. We kept the phase abstraction not because we need it for storage constraints, but because it provides:

1. **Debuggability**: Inspect each phase independently during query execution
2. **Provable Correctness**: Composition follows relational algebra laws
3. **Optimization Opportunities**: Reordering phases based on dependencies
4. **Transitive Metadata**: Information flows explicitly through phases

This is a case study in **recognizing emergent abstractions**: a solution forced by practical constraints that reveals deeper theoretical elegance. The Elasticsearch limitation didn't just force a workaround—it led to discovering an abstraction that makes complex query planning tractable.

**Related Work**:
- Similar pattern in MapReduce: GFS constraints → Map/Reduce phases → general computation model
- Unix pipes: PDP-7 memory limits → stream processing → compositional shell programming
- React: DOM update costs → Virtual DOM → declarative UI model

**Optimizations**:
- Index selection based on bound values
- Equality predicates pushed into joins
- Query plan caching (3x speedup)
- Early predicate filtering in executor
- Phase reordering by symbol connectivity (prevents cross-products)

### 5. Parser (`datalog/parser/`)

**EDN Support**:
- Clojure-style syntax parsing
- Query transformation to internal representation
- Support for all Datalog clauses

**Query Features**:
- Pattern matching: `[?e :attr ?v]`
- Predicates: `[(< ?x 100)]`
- Expressions: `[(+ ?x ?y) ?z]`
- Aggregations: `(sum ?amount)`
- Subqueries: `[(q [...] $) [[?result]]]`
- Order-by: `:order-by [?x :desc]`

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
- Batch scanning: Collect multiple values per seek

### Semantic Correctness First
RelationInput iteration ensures correct aggregation scoping:
- `:in $ [[?x ?y] ...]` iterates over tuples
- Each tuple processed independently
- Performance optimization comes after correctness

## Performance Characteristics

### Achieved Optimizations (Verified 2025-10-25)
- **2.22× faster**: Streaming execution on low-selectivity filters
- **52% memory reduction**: Up to 91.5% on large datasets with predicate pushdown
- **4.06× speedup**: Iterator composition vs materialized operations
- **2.06× speedup**: Parallel subquery execution (8 workers)
- **1.58-2.78× faster**: Predicate pushdown (scales with dataset size)
- **3× faster**: Query plan caching
- **6.26× speedup**: Parallel intern cache optimization

**Streaming Performance by Selectivity** (10K tuples):
| Selectivity | Speedup | Memory Reduction |
|-------------|---------|------------------|
| High (1%)   | 1.07×   | 2% |
| Medium (10%)| 1.44×   | 19% |
| Low (50%)   | 2.22×   | 52% |
| Iterator Composition | 4.06× | 89% |

### Known Bottlenecks
- Subquery iteration still sequential (870 executions for OHLC)
- No parallel execution within phases (planned)
- Symmetric hash join slightly slower than standard (trade-off for streaming)

## Datalog Feature Support

### Implemented (~70% Complete)
✅ Basic patterns and joins
✅ Predicates and expressions  
✅ Aggregations with grouping
✅ Subqueries with proper scoping
✅ Order-by clause
✅ Time extraction functions
✅ As-of queries

### Not Implemented
❌ Rules system
❌ Pull syntax
❌ NOT/OR clauses
❌ Recursive queries
❌ Window functions

## Datomic Compatibility (~40-50%)

### Compatible Features
- EAVT data model
- Transaction time
- Core query syntax
- Aggregations
- Subqueries

### Differences
- No schema enforcement
- No transaction functions
- No history API
- No pull syntax
- Simpler type system

## Code Organization

```
datalog/
├── storage/      # BadgerDB backend
├── executor/     # Query execution engine
├── planner/      # Query planning
├── parser/       # EDN and query parsing
├── query/        # Query type definitions
├── codec/        # L85 encoding
├── edn/          # EDN lexer/parser
└── annotations/  # Performance monitoring
```

## Future Optimizations

1. **Parallel Execution**: Run independent patterns/iterations concurrently
2. **Smart Predicate Pushdown**: Cross-pattern predicate analysis
3. **Statistics-Based Planning**: Cardinality estimation
4. **Materialized Views**: Pre-computed aggregations
5. **WASM Build**: Browser deployment