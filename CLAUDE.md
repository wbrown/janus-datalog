# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## REQUIRED READING - Read These Files FIRST

**The following files are PART OF THIS DOCUMENT and MUST be read before making any code changes:**

1. **[CLAUDE_TESTING.md](CLAUDE_TESTING.md)** - Testing requirements and strategy
2. **[CLAUDE_BUGS.md](CLAUDE_BUGS.md)** - Historical bugs and patterns to avoid
3. **[CLAUDE_DEBUGGING.md](CLAUDE_DEBUGGING.md)** - Systematic debugging methodology

These files contain critical implementation patterns, common pitfalls, and testing requirements. Not reading them WILL result in bugs.

---

## Architectural Authority

**The user owns all architectural decisions. Claude implements them.**

Before making ANY of these decisions, ASK:
- Introducing new patterns (globals, managers, abstractions)
- Changing existing patterns (options → globals, Relations → Bindings)
- Adding new cross-cutting concerns (configuration, logging, caching)
- Deviating from established conventions for any reason

**If you're unsure whether something is an "architectural decision":**
- Would it affect multiple files/packages?
- Would it change how components interact?
- Would it require other code to change to accommodate it?
- Are you thinking "I'll ask forgiveness later"?

**Then ASK first.**

**Red flags that indicate you're overstepping:**
- "This is just temporary/experimental"
- "I'll refactor this later"
- "It's faster to do it this way"
- Making a choice between multiple valid approaches without consulting

**The user's job**: Set direction, make architectural choices, review designs
**Your job**: Implement, follow patterns, propose options (not make choices)

## Project Overview

This repository contains a Datomic-style Datalog engine implementation in Go, inspired by memories of previous single-node and distributed implementations.

The Go implementation takes a pragmatic middle ground: production-ready with features like aggregations, annotations, time functions, and persistent storage.

## Architecture Summary

For a complete architecture overview, see [ARCHITECTURE.md](ARCHITECTURE.md).

The Datalog engine consists of these core components:
1. **EDN Parser**: Parses Clojure-style EDN syntax for queries
2. **Query Parser**: Transforms EDN into internal query representation
3. **Query Engine**: Executes queries using relational algebra operations
4. **EAVT Storage**: Entity-Attribute-Value-Transaction storage with multiple indices
5. **Type System**: Direct Go types without complex wrappers

## Key Architectural Insights

### Relation-Based Query Execution
The engine uses a **greedy join ordering approach** with several important safeguards:
- Progressive joining: Relations are joined as they become available
- Early termination: Stops immediately on empty results
- Disjoint detection: Catches queries that would create Cartesian products
- Streaming iterators: Avoids materializing large intermediate results

This is **standard database query optimization** (similar to Selinger's algorithm from 1979), but without cost-based optimization or statistics. It's not novel, but it's correctly implemented and crucial for preventing OOM failures on complex queries.

**What makes this codebase production-ready** is the combination of multiple techniques working together:
- **Phase-based planning**: Groups patterns intelligently to avoid bad join orders
- **Early predicate filtering**: Applies filters as soon as required symbols are available
- **Streaming architecture**: Iterator-based processing throughout
- **Explicit error handling**: Returns errors for Cartesian products instead of silently creating billions of tuples

### Relation-Centric Query Execution
**Relations are the fundamental abstraction** for query execution:
- All data sources (storage, intermediate results) implement the `Relation` interface
- Relations provide `Iterator` access for streaming without full materialization
- Storage iterators are wrapped as `StreamingRelation` to participate in joins
- Hash joins use the smaller relation as the build side for efficiency
- Predicates are applied as soon as their required symbols are available

### Storage Design
- **Fixed 72-byte keys**: E(20) + A(32) + Tx(20) for efficient indexing
- **Unbounded values**: Stored last with 2-byte size prefix and 1-byte type
- **L85 encoding**: Custom Base85 variant preserving sort order (see below)
- **Multiple indices**: EAVT, AEVT, AVET, VAET, TAEV for different access patterns
- **Keyword interning**: Keywords hashed once and reused
- **RefValues**: 20-byte entity references are L85-encoded like E/Tx components
- **Attribute size**: Increased from 20 to 32 bytes to support longer attribute names (e.g., `:option/open-interest`)

### L85 Encoding Details

L85 is a custom Base85 encoding that is critical to the storage layer's performance:

**Key Properties**:
- **Space Efficient**: 25% overhead (better than Base64's 33%)
- **Sort Order Preserving**: Lexicographic sort of encoded strings matches byte order
- **Terminal Safe**: All printable ASCII, no quotes/spaces/backslashes
- **Fixed Output**: 20 bytes → 25 characters (perfect for SHA1 hashes)
- **Extended Support**: 32 bytes → 40 characters (for longer attributes)

**The Alphabet**:
```
!$%&()+,-./0123456789:;<=>@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]_`abcdefghijklmnopqrstuvwxyz{}
```

**Why This Matters**:
- Enables efficient range scans without decoding
- Keys can be debugged, logged, and copied without binary issues
- URLs and JSON safe without escaping
- 3 fewer characters per key than Base64 (scales to millions of keys)

**Implementation Notes**:
- Located in `datalog/codec/l85.go`
- Inspired by Base85 encoding patterns with sort-preservation
- Decode table uses i+1 (0 = invalid) for cleaner validation
- Big-endian encoding preserves numeric sort order
- RefValues (entity references) are L85-encoded in storage keys
- Other value types remain as raw bytes for flexibility
- Added `EncodeFixed32` and `DecodeFixed32` functions for 32-byte attributes

### Scale-Up vs Scale-Out
This implementation supports a hybrid approach:
- Single-node optimization with fixed-size keys and efficient storage
- Sophisticated query planning with phase-based execution
- Iterator-based streaming for memory efficiency
- Can be extended for distributed processing in the future

## Package Structure

When implementing, organize code as:
```
datalog/
├── parser/      # EDN and Datalog parsers
├── types/       # Core type definitions
├── query/       # Query structures and types
├── symbolic/    # User-facing types (EntityID, Keyword, Datom)
├── executor/    # Query execution with Relations
├── planner/     # Query planning and optimization
├── store/       # Storage abstraction and backends
├── index/       # Index implementations
├── codec/       # L85 and value encoding
├── edn/         # EDN lexer and parser
└── storage/     # Storage implementations (BadgerDB)
```

## Type System Architecture

The codebase maintains a clean separation between user-facing types and storage representations:

### Core Types (`datalog/`)
- **Datom**: The fundamental unit with proper types (not strings!)
  - `E: Identity` - Entity identifier with SHA1 hash and L85 encoding
  - `A: Keyword` - Attribute keyword (interned string)
  - `V: Value` - Any value (interface{} containing Go types directly)
  - `Tx: uint64` - Transaction ID
- **Identity**: Like C++ Reference and Clojure Identity - contains hash, L85, and original string
- **Value**: Just `interface{}` - no wrapper types, direct Go types:
  - Scalars: `string`, `int64`, `float64`, `bool`, `time.Time`, `[]byte`
  - References: `Identity` (aliased as `Reference` when used as a value)
  - Keywords: `Keyword` (can be used as values, e.g., `:status/active`)
- **Join Keys**: Use L85 encoding for efficient comparison

### Storage Layer (`datalog/storage/`)
- **Purpose**: Internal storage representation only
- **StorageDatom**: Uses fixed byte arrays for efficient indexing ([20]byte for E/Tx, [32]byte for A)
- **Conversion**: Storage layer converts between user types and storage types internally
- **L85 Encoding**: Used for sortable storage keys
- **Invisible to Query Engine**: The query engine never sees these types

This separation ensures the query engine remains simple and focused on logic, while the storage layer handles all encoding/decoding complexity.

### Storage Integration
The storage layer connects the query engine to BadgerDB:
- **Database API**: High-level interface for creating transactions and querying
- **Transaction API**: Write datoms with automatic indexing across all 5 indices
- **BadgerMatcher**: Implements PatternMatcher interface for the executor
  - Chooses optimal index based on bound values in patterns
  - Converts between user types (Identity, Keyword) and storage types ([20]byte for E/Tx, [32]byte for A)
  - Handles L85 encoding for index keys with proper size handling
- **Value Encoding**: Serializes all value types with proper type tags
  - Special handling for Identity references to preserve join semantics
  - Fixed bugs where entity IDs were decoded incorrectly

## Implementation Status

### ✅ Recent Updates (October 2025)
1. **QueryExecutor & RealizedPlan Architecture (Stage B)** - Major architectural improvement
   - Phases are now Datalog query fragments, not operation type collections
   - Universal `Query + Relations → Relations` interface
   - Multi-relation semantics with progressive collapse
   - Foundation for future AST-oriented planner (Stage C)
   - See [docs/wip/PHASE_AS_QUERY_ARCHITECTURE.md](docs/wip/PHASE_AS_QUERY_ARCHITECTURE.md) for detailed proposal

2. **True Streaming Architecture** - Performance breakthrough
   - **2.22× faster** with **52% memory reduction** (up to 91.5% on large datasets)
   - **4.06× speedup** from iterator composition alone
   - BufferedIterator for re-iteration support
   - Symmetric hash join option for stream-to-stream joins
   - Options-based configuration (no global state)
   - **Enabled by default** as of October 2025
   - See [docs/archive/2025-10/STREAMING_ARCHITECTURE_COMPLETE.md](docs/archive/2025-10/STREAMING_ARCHITECTURE_COMPLETE.md) for complete history

### ✅ Earlier Updates (August 2025)
1. **Performance Analysis & Consolidation** - Reality check complete
   - Benchmarked all optimization attempts
   - Documented what's actually active vs. experimental
   - Created `PERFORMANCE_STATUS.md` as single source of truth
2. **Batch Scanning Implementation** - Code clarity improvement
   - SimpleBatchScanner for large binding sets (>100 tuples)
   - Threshold-based activation in matcher_relations.go
   - Modest performance impact, cleaner code structure
3. **Predicate Infrastructure** - Classification and constraints
   - PredicateClassifier for analyzing pushdown candidates
   - StorageConstraint infrastructure in place
   - JoinCondition detection for equality predicates

### ✅ Core Features Complete
1. **Core types and storage interface** - EAVT storage with BadgerDB backend
2. **EDN parser** - Complete implementation for query syntax
3. **Query parser** - Transforms EDN to typed query structures with comparator support
4. **Query planner** - Index selection and phase-based execution planning
5. **Relation abstraction** - Iterator-based streaming with join operations
6. **Relation collapsing algorithm** - Critical algorithm preventing memory exhaustion
7. **Expression clauses** - Arithmetic (`+`, `-`, `*`, `/`), string operations (`str`), ground values, identity binding
8. **Variadic comparators** - Clojure-style chained comparisons (e.g., `[(< 0 ?x 100)]`)
9. **Query executor** - Full pattern matching and query execution with joins
10. **Storage integration** - Database and Transaction API, BadgerMatcher for pattern matching
11. **Value encoding** - Proper serialization for all value types including entity references
12. **Aggregation functions** - `sum`, `count`, `avg`, `min`, `max` with grouping support (with proper time.Time support)
13. **Time-based queries** - Time-based transaction IDs and as-of queries
14. **Time extraction functions** - `year`, `month`, `day`, `hour`, `minute`, `second` for temporal analysis
15. **Subqueries** - Full implementation with TupleBinding and RelationBinding support
16. **Result/Relation unification** - Eliminated redundant Result type, unified API
17. **Table formatter** - Markdown table formatting using tablewriter library
18. **Order-by clause** - Full implementation with multi-column sorting and direction control
19. **Time comparison fix** - Proper time.Time comparison in aggregations and predicates
20. **Datomic compatibility** - ~40-50% feature parity (see DATOMIC_COMPATIBILITY.md)
21. **Relations migration** - Multi-value variable support throughout codebase

### 📋 TODO (Priority Order)

See `TODO.md` and `PERFORMANCE_STATUS.md` for detailed roadmap.

**High Priority**:
1. **Streaming aggregations** - Reduce memory for large groups

**Medium Priority**:
4. **Distinct aggregation** - Add `distinct` support to existing aggregations
5. **CollectionBinding** - Implement `?coll` binding for subqueries
6. **NOT/OR clauses** - Negation and disjunction support

**Long Term**:
7. **Schema management** - Attribute definitions and constraints
8. **Statistics-based optimization** - Query planning with cardinality estimates
9. **WASM build** - Browser deployment support

## Go Implementation Guidelines

### Write Idiomatic Go, Not Java-in-Go
This codebase should follow Go idioms, NOT Java/Enterprise patterns:

### CRITICAL: No Global Configuration State
**NEVER use package-level variables for configuration**. Instead:
- Add flags to existing options structs (ExecutorOptions, PlannerOptions)
- Thread options through constructors and ensure propagation
- Configuration flows through the call graph, not global state
- **Why**: Breaks concurrent usage, creates hidden dependencies, makes testing hard
- **Example violation**: Adding `var EnableStreamingAggregation = false` at package level
- **Correct approach**: Add to ExecutorOptions, ensure Options() propagates through relations/joins

### CRITICAL: Stop Creating V2 Versions
**NEVER create V2 versions of functions/interfaces**. Instead:
- Fix the original implementation
- If you need different behavior, add a parameter/option
- Creating parallel implementations is Java-style abstraction madness

**DO (Go idioms):**
- Simple functions for stateless operations
- Methods on types that operate on that type's data
- Interfaces only when you need polymorphism
- Small, focused packages
- Return errors explicitly
- Use composition over inheritance

**DON'T (Java patterns to avoid):**
- Manager/Service/Controller/Factory classes
- Unnecessary abstraction layers
- Deep inheritance hierarchies
- Getter/setter methods for every field
- "One class to rule them all" patterns
- Dependency injection frameworks

**Example - Good Go vs Bad Java-style:**
```go
// BAD: Java-style with manager class
type PredicatePropagator struct {
    phases []Phase
}
func (pp *PredicatePropagator) Propagate() { ... }

// GOOD: Methods on the types themselves
func (p *Phase) PushPredicates() { ... }
func (pp *PatternPlan) ApplyConstraints(predicates []PredicatePlan) { ... }
```

## Performance Considerations

- **Memory optimization**: Use datom interning, compressed storage, and lazy sequences
- **Parallel processing**: Leverage Go's concurrency for pattern matching and joins
- **Caching hierarchy**: Pattern compilation, tuple deduplication, query plans
- **Early termination**: Stop execution immediately on empty intermediate results

## Query Planning and Execution Architecture

### Modern Architecture (October 2025)

#### RealizedPlan & QueryExecutor (Stage B)

The query engine now uses a **two-tier execution architecture**:

**RealizedPlan Output**:
```go
type RealizedPhase struct {
    Query     *query.Query      // Datalog query fragment for this phase
    Available []query.Symbol    // Symbols from inputs + previous phases
    Provides  []query.Symbol    // Symbols this phase produces
    Keep      []query.Symbol    // Symbols to pass to next phase
    Metadata  map[string]interface{}
}
```

**Universal Interface**:
```go
type QueryExecutor interface {
    Execute(ctx Context, query *query.Query, inputs []Relation) ([]Relation, error)
}
```

**Key Design**:
- **Phases ARE Datalog query fragments** (not operation type collections)
- **Query + Relations → Relations** universal interface
- Multi-relation semantics with progressive collapse
- Clause-by-clause execution with early termination
- Foundation for future AST-oriented planner (Stage C)

> See [ARCHITECTURE.md](ARCHITECTURE.md) for complete architecture overview

### Phase-Based Execution (Current Implementation)

The query planner organizes patterns into phases based on symbol dependencies:
- **Phase Creation**: Patterns are grouped into phases where each phase can only use symbols from previous phases
- **Symbol Tracking**: Each phase tracks what symbols it provides and what it needs to keep for later phases
- **Expression Planning**: Expressions are assigned to the earliest phase that has all required input symbols
- **Realize() Method**: Converts internal Phase structures to clean RealizedPlan query fragments

### Join Optimization Strategies

#### 1. Progressive Join Execution
Within each phase, multiple relations are combined using a greedy algorithm:
- Joins relations that share columns, keeps disjoint relations separate
- Early termination on empty joins to avoid wasted work
- Uses hash joins for shared columns, prevents cross products otherwise
- Note: This is a simple greedy approach without cost-based optimization

#### 2. Predicate Pushdown
Predicates are classified and optimized based on their scope:
- **Intra-phase predicates**: Applied immediately within the phase as filters
- **Inter-phase predicates**: Deferred until all required symbols are available
- **Expression predicates**: Can reference expression output symbols

#### 3. Equi-Join Detection
The planner detects equality predicates that can be pushed into joins:
- Identifies `[(= ?x ?y)]` patterns where symbols come from different phases
- Converts these into join conditions rather than post-join filters
- Dramatically reduces intermediate result sizes (e.g., 540,000 → 600 tuples)

### Performance Monitoring

The annotation system uses a decorator pattern for zero-overhead observability:

**Usage Pattern**:
```go
// Create an event handler
handler := func(event annotations.Event) {
    // Process event (log, store, analyze, etc.)
}

// Wrap the matcher with annotation decorator
baseMatcher := storage.NewBadgerMatcher(db.Store())
matcher := executor.WrapMatcher(baseMatcher, handler)

// Use matcher normally - annotations are transparent
executor := executor.NewExecutor(matcher)
```

**Key Design Principles**:
- **Decorator pattern**: `WrapMatcher()` wraps any `PatternMatcher` with annotation support
- **Handler injection**: Storage layer receives handler via `SetHandler()` for detailed events
- **Zero overhead when disabled**: Pass `nil` handler for production deployments
- **Type transparency**: Wrapped matcher implements same interface as base matcher

**Event Types**:
- **Pattern Matching**: Index selection, storage scan, filtering, and relation conversion
- **Join Operations**: Type (hash/nested/merge), sizes, and reduction ratios
- **Expression Evaluation**: Input/output sizes and computation time
- **Phase Execution**: Overall timing and tuple counts

### Critical Performance Insights
1. **Avoid Intermediate Materialization**: Use streaming iterators wherever possible
2. **Early Filtering**: Apply predicates as soon as their symbols are available
3. **Join Order Matters**: The relation collapser dynamically optimizes join order
4. **Index Selection**: The storage layer chooses optimal indices based on bound values
5. **Memory Pre-allocation**: Pre-allocate slices with exact capacity to avoid reallocation

### Architectural Philosophy
The implementation follows a pragmatic approach:
- **Simplicity over complexity**: Direct Go types instead of complex variant systems
- **Clear separation of concerns**: Storage layer handles encoding, query engine handles logic
- **Performance through algorithms**: Focus on relation collapsing and join ordering
- **Explicit over implicit**: Verbose but debuggable code with clear phase boundaries

### Handling Disjoint Relations in Query Execution

The query executor has sophisticated handling for disjoint relation groups that may arise during phase execution:

1. **What are Disjoint Relations?**
   - Relations that share no common columns (symbols)
   - Cannot be joined without creating a Cartesian product
   - Example: `[?person :person/name ?name]` and `[?product :product/price ?price]` share no variables

2. **When Do They Occur?**
   - During pattern matching within a phase
   - Before expressions add connecting symbols
   - Due to query planning decisions

3. **How They're Handled:**
   ```go
   // In executePhaseSequentialV2:
   // Progressive collapsing after each pattern
   independentGroups = append(independentGroups, newRel)
   independentGroups = independentGroups.Collapse(ctx)
   // Returns []Relation - multiple groups if disjoint
   ```
   
   - Relations are collapsed after each pattern execution
   - Enables early termination on empty joins
   - More memory efficient than accumulating all relations

4. **Expression-Based Joining:**
   - Expressions can add symbols that bridge disjoint groups
   - After each expression, relations are re-collapsed
   - Example:
     ```
     Group 1: [?x, ?y]
     Group 2: [?a, ?b]
     Expression: [(+ ?y 10) ?z] on Group 1
     Expression: [(* ?b 2) ?z] on Group 2
     Result: Groups can now join on ?z
     ```

5. **Error Handling:**
   - If disjoint groups remain after all expressions/predicates
   - Returns error: "phase resulted in N disjoint relation groups - Cartesian products not supported"
   - This prevents accidental Cartesian products that explode result sizes

6. **Design Rationale:**
   - Avoids memory explosion from Cartesian products
   - Allows expressions to intelligently connect data
   - Makes implicit cross-products explicit errors
   - Forces better query design

Note: While our planner is explicit and feature-complete, the **information flow** approach used in some distributed Datalog implementations offers algorithmic insights that could improve our query optimization (see `planner-improvements.md`).

## Important Documentation

### Core Documentation
- **[DATOMIC_COMPATIBILITY.md](DATOMIC_COMPATIBILITY.md)** - Comprehensive compatibility guide for Datomic users (~40-50% feature parity)
- **[PERFORMANCE_STATUS.md](PERFORMANCE_STATUS.md)** - Current performance status, active optimizations, and benchmarks
- **[TODO.md](TODO.md)** - Active task tracking with completed and pending features
- **[DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md)** - Complete documentation navigation

### Implementation Guides
- **[docs/INPUT_PARAMETER_SEMANTICS.md](docs/INPUT_PARAMETER_SEMANTICS.md)** - Comprehensive guide to input parameter handling
- **[docs/reference/PLANNER_OPTIONS.md](docs/reference/PLANNER_OPTIONS.md)** - Complete planner options reference with performance guidance

### Historical Context
- **[docs/archive/early-design/DATALOG_GO_NOTES_HISTORICAL_INSIGHTS.md](docs/archive/early-design/DATALOG_GO_NOTES_HISTORICAL_INSIGHTS.md)** - Architectural insights and lessons learned
- **[docs/ideas/planner-improvements.md](docs/ideas/planner-improvements.md)** - Proposed query planner enhancements using information flow approaches
- **[docs/archive/completed/subquery-implementation-plan.md](docs/archive/completed/subquery-implementation-plan.md)** - Detailed plan for implementing subqueries (COMPLETED)
- **[docs/archive/completed/order-by-implementation-plan.md](docs/archive/completed/order-by-implementation-plan.md)** - Implementation plan for :order-by clause (COMPLETED)