# Janus Datalog Architecture

**Last Updated**: February 2026

## Overview

Janus Datalog is a Datomic-style Datalog query engine implemented in Go. It combines EAVT storage with CRDT conflict resolution, a streaming query executor, and multiple query entry points (EDN strings, type-safe builder, struct reflection).

This document describes how the system works — how data flows through it, where the architectural seams are, and what each component is responsible for.

## How Queries Work

All query paths converge on the same planner and executor. The difference is how the query gets constructed and how the results get delivered.

### Query Path Overview

```
                                 ┌─────────────┐
                                 │  EDN String  │
                                 └──────┬───────┘
                                        │ parser.ParseQuery()
                                        ▼
┌──────────────┐                ┌───────────────┐
│ Query Builder │──MustBuild()──▶  *query.Query  │
└──────────────┘                └───────┬───────┘
                                        │ planner.PlanQuery()
                                        ▼
                                ┌───────────────┐
                                │ *RealizedPlan │
                                │  ([]Phase)    │
                                └───────┬───────┘
                                        │ executor.ExecuteRealized()
                                        ▼
                                ┌───────────────┐
                                │  []Relation   │
                                └───────┬───────┘
                                        │
                        ┌───────────────┼───────────────┐
                        ▼               ▼               ▼
                  [][]interface{}   QueryInto(T)    Pull (separate path)
                  (raw tuples)     (struct mapping)  (no planner)
```

### Entry Point 1: EDN String

The most direct path. An EDN string is parsed, planned, and executed:

```go
results, err := db.ExecuteQuery(`[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`)
```

**Call chain**: `ExecuteQuery` → `resolveQuery` (detects string → calls `parser.ParseQuery`) → `ExecuteQueryWithInputs` → planner → executor → `[][]interface{}`

### Entry Point 2: Query Builder (`qb/`)

The builder produces a `*query.Query` directly, skipping EDN parsing:

```go
q := qb.Query().
    Find(qb.Var("?name"), qb.Var("?age")).
    Where(
        qb.Pat("?e", ":person/name", "?name"),
        qb.Pat("?e", ":person/age", "?age"),
    ).MustBuild()

results, err := db.ExecuteQuery(q)  // *query.Query passed directly
```

**Call chain**: `ExecuteQuery` → `resolveQuery` (detects `*query.Query` → returns it as-is) → same path from there. No parsing overhead.

### Entry Point 3: QueryInto

Wraps any query path with struct-mapped result delivery:

```go
var people []Person
err := db.QueryInto(&people, query, inputs...)
```

**Call chain**: Normal query execution → `[][]interface{}` → `reflect.QueryResultMapper.MapAll()` → populates struct slice via field tags.

For scalar types (single-column queries), extracts the first column directly without struct mapping.

### Entry Point 4: Pull API (Separate Path)

Pull does **not** go through the query planner. It's direct pattern matching against storage:

```go
result, err := db.Pull(entityID, "[:person/name :person/age {:person/friends [:person/name]}]")
```

**Call chain**: `parser.ParsePullPattern` → `PullExecutor.Pull` → recursive `pullWithVisited`:
- For each attribute spec: `matcher.Match()` directly against storage
- For reference attributes: recursive pull with cycle detection via visited set
- For wildcards: scan all entity attributes

**PullInto** adds struct reflection: `reflect.GeneratePullPattern(structType)` → auto-generates pull pattern from struct tags → pulls → `reflect.ReadStructWithID()` maps result to struct.

### Multi-Source Queries

Any query path supports multiple data sources via `WithSources`:

```go
results, err := db.ExecuteQueryWithInputs(query,
    WithSources(map[Symbol]PatternMatcher{
        "$":      mainDB.Matcher(),
        "$users": userSliceSource,
    }),
)
```

**How it works**: `buildSourceMap` creates a `SourceRouter` that wraps all sources. The router becomes the executor's `PatternMatcher`. During execution, each `DataPattern` clause is routed to its declared source (`$` by default). Cross-source joins happen naturally through the relation collapsing algorithm.

### Phase Execution Detail

Within the executor, each phase is a mini-query:

```
For each phase in plan:
  For each clause in phase.Query.Where:
    ├── DataPattern  → matcher.Match(pattern, bindings) → new Relation
    ├── Expression   → evaluate over existing relations → add column
    ├── Predicate    → filter existing relations
    ├── Subquery     → recursive ExecuteQuery
    ├── NOT clause   → anti-join (filter where inner query matches)
    └── OR clause    → union branches or per-tuple fallback
  After each clause: collapse relation groups (join on shared symbols)
  Early terminate if any group is empty
```

## How Writes Work

### Write Path Overview

```
                    ┌─────────────┐     ┌──────────────┐
                    │  tx.Add()   │     │ reflect.Write │
                    │  tx.Set()   │     │   SaveStruct  │
                    │  tx.Remove()│     └───────┬───────┘
                    └──────┬──────┘             │
                           │      ┌─────────────┘
                           ▼      ▼
                    ┌──────────────┐
                    │  Transaction │
                    │  .datoms[]   │
                    └──────┬───────┘
                           │ schema.ValidateDatom()  ← type check
                           │ clock.Next()            ← ElementID
                           │ cardinality → CRDT op   ← Op assignment
                           │
                           │ tx.Commit()
                           ▼
                    ┌──────────────┐
                    │  Uniqueness  │
                    │  Validation  │
                    └──────┬───────┘
                           │ store.Assert() / store.Retract()
                           ▼
                    ┌──────────────┐
                    │   BadgerDB   │
                    │  7 indices   │
                    │  per datom   │
                    └──────────────┘
```

### Transaction Lifecycle

1. **`db.NewTransaction()`** — Creates transaction, holds `[]Datom` buffer
2. **`tx.Add(entity, attr, value)`** — Per-datom:
   - Schema validation (type check against declared attribute type, skipped if no schema)
   - Lamport clock increment → `ElementID{Lamport, ReplicaID}`
   - Cardinality determines CRDT operation:
     - **Cardinality-one**: `Op=None` (LWW — latest Tx wins)
     - **Cardinality-many**: `Op=CRDTAdd` (add-wins set semantics)
     - **Cardinality-vector**: `Op=RGAInsert` with `AfterRef` pointing to previous element
   - Datom appended to transaction buffer
3. **`tx.Commit()`** — Atomic:
   - Uniqueness validation (for `:db.unique/value` and `:db.unique/identity` attributes)
   - `store.Retract(retracts)` — write tombstones
   - `store.Assert(datoms)` — write to all 7 indices per datom
   - Transaction metadata datom (`tx:N :db/txInstant time`)
   - EA cache invalidation for touched (entity, attribute) pairs

### Reflect Write Path

The reflect API converts Go structs to transactions:

```go
tx := db.NewTransaction()
id, err := tx.SaveStruct(&person)  // Person{Name: "Alice", Age: 30}
tx.Commit()
```

**How it works**: `StructWriter` inspects struct tags → iterates fields → calls `tx.Add()` for each field. Slice fields become multiple datoms (cardinality-many). Nested structs become reference values. `UpdateMode.Replace` diffs existing values to minimize writes.

ID generation: If the struct's ID field is zero, `generateUniqueID()` creates a SHA1 hash from timestamp + random bytes.

### Export/Import

Export scans the EAVT index and writes EDN-formatted datoms. Import parses them back and calls `store.Assert()` directly — **bypassing the transaction layer** (no schema validation, no uniqueness checks, no clock advancement). This is intentional: import restores exact storage state including original ElementIDs.

## Architectural Seams

Five interfaces define the boundaries between components. To extend the system, you implement one of these.

### PatternMatcher — The Core Seam

```go
type PatternMatcher interface {
    Match(pattern *query.DataPattern, bindings Relations) (Relation, error)
}
```

**This is the most important interface in the system.** Every data source implements it. The executor only knows about `PatternMatcher` — it doesn't know if data comes from BadgerDB, an in-memory slice, or another database.

**Implementors**: `BadgerMatcher` (storage), `SourceRouter` (multi-source routing), `SliceSource[T]` (Go slices), `MemoryPatternMatcher` (in-memory datoms), `IndexedMemoryMatcher` (optimized in-memory)

**Extended variants**:
- `PredicateAwareMatcher` adds `MatchWithConstraints()` — predicate pushdown into storage
- `EntityLookupMatcher` adds `LookupAttribute()` — single-value lookups for database functions like `get-else`

### Relation — Tuple Set Abstraction

```go
type Relation interface {
    Columns() []query.Symbol
    Iterator() Iterator
    Size() int
    IsEmpty() bool
    Project(columns []query.Symbol) (Relation, error)
    Join(other Relation) Relation
    HashJoin(other Relation, joinCols []query.Symbol) Relation
    // ... filtering, aggregation, materialization
}
```

Everything in the executor works with Relations. Pattern matches produce them, joins combine them, expressions add columns to them, predicates filter them.

**Key implementations**:
- `MaterializedRelation` — tuples in memory, supports random access and re-iteration
- `StreamingRelation` — iterator-based, lazy evaluation, single-pass
- `BufferedIterator` — wraps any iterator to allow re-iteration (buffers on first pass)

### Iterator — Streaming Pipeline

```go
type Iterator interface {
    Next() bool
    Tuple() Tuple
    Close() error
}
```

Composable lazy evaluation. Iterator implementations chain: `StorageScan → FilterIterator → ProjectIterator → DedupIterator`. Nothing materializes until `Next()` is called.

### QueryExecutor — Phase Execution

```go
type QueryExecutor interface {
    Execute(ctx Context, q *query.Query, inputs []Relation) ([]Relation, error)
}
```

Universal interface: query fragment + input relations → output relations. Used for phase execution, subquery execution, and top-level execution. The single implementation (`DefaultQueryExecutor`) handles all clause types.

### Store — Storage Backend

```go
type Store interface {
    Assert(datoms []datalog.Datom) error
    Retract(datoms []datalog.Datom) error
    Scan(index IndexType, start, end []byte) (Iterator, error)
    Get(index IndexType, key []byte) (*datalog.Datom, error)
    Close() error
}
```

Raw datom storage with 7 indices. Currently only `BadgerStore` (BadgerDB). The `BadgerMatcher` bridges this to the executor by implementing `PatternMatcher` — it chooses the optimal index based on which pattern components are bound, scans BadgerDB, and wraps results as `StreamingRelation`.

### EntityResolver — CRDT Resolution

```go
type EntityResolver interface {
    ResolveAllAttributes(entity datalog.Identity) (map[datalog.Keyword]interface{}, error)
}
```

Returns all attributes for an entity with CRDT conflict resolution applied. Used by the Pull API for wildcard pulls. Implemented by `Database`.

## Component Reference

### Storage (`datalog/storage/`)

**EAVT Model** with CRDT semantics:
- **Fixed-size keys**: 69 bytes (E:20 + A:32 + Tx:16 + Op:1), plus optional 16-byte AfterRef for RGA
- **Variable-size values**: 2-byte size prefix + 1-byte type tag + data
- **L85 encoding**: Custom sort-preserving Base85 for range scans
- **Seven indices**: Each serves different access patterns:

| Index | Use Case |
|-------|----------|
| EAVT  | Entity lookups, add-wins resolution (cardinality-many) |
| EATV  | Entity lookups, LWW resolution (cardinality-one/vector) |
| AEVT  | Attribute scans across entities |
| AETV  | Attribute scans with temporal ordering |
| AVET  | Value-based lookups (uniqueness, reverse lookup) |
| VAET  | Reverse reference traversal |
| TAEV  | Transaction-based queries, history |

**CRDT Semantics**:
- **LWW** (cardinality-one): EATV index orders Tx descending; first result is current value
- **Add-Wins Sets** (cardinality-many): Tracks add/remove operations; adds win over concurrent removes
- **RGA** (cardinality-vector): Positional inserts with AfterRef chain; tombstones for deletes
- **ElementID**: 16-byte transaction ID (8-byte Lamport clock + 8-byte ReplicaID) for causal ordering

### Type System (`datalog/`)

Core types in the top-level package:
- **Datom**: `{E: Identity, A: Keyword, V: Value, Tx: ElementID, Op: CRDTOp, AfterRef: ElementID}`
- **Identity**: Entity identifier — SHA1 hash + L85 cache + original string
- **Keyword**: Interned pointer type (`*keyword`); pointer equality = value equality, O(1) comparison
- **Symbol**: Interned pointer type (`*symbol`); query variables only, not stored
- **ElementID**: `{Lamport: uint64, ReplicaID: uint64}` — CRDT causality
- **Value**: `interface{}` — direct Go types: `string`, `int64`, `float64`, `bool`, `time.Time`, `[]byte`, `Identity`, `Keyword`

### Query Planning (`datalog/planner/`)

Single planner: `ClauseBasedPlanner`. Greedy phasing algorithm:

1. Start with input symbols as available
2. Score all executable clauses (requirements satisfied by available symbols)
3. Select highest-scoring clause, add to current phase
4. Update available symbols
5. Repeat until no clauses can execute; start new phase with remaining

Output: `RealizedPlan` — sequence of phases, each containing a `*query.Query` fragment with `Available`, `Provides`, and `Keep` symbol sets.

**Key invariant**: `Keep ⊆ Provides ∩ Available` — can only carry forward symbols that exist in the relation and are needed later.

### Query Execution (`datalog/executor/`)

`DefaultQueryExecutor` processes clauses sequentially within each phase. After each clause, relation groups are collapsed — groups sharing columns are joined, disjoint groups are kept separate. If disjoint groups remain after all clauses, it's an error (Cartesian product prevention).

**Streaming**: Iterator composition is the default. `FilterIterator`, `ProjectIterator`, `TransformIterator` etc. chain without materialization. `BufferedIterator` wraps any iterator for re-iteration.

**Hash joins**: Build on smaller side, probe on larger. FNV-1a hashing. `StreamingRelation` for lazy evaluation (build side materialized, probe side streams).

### Schema (`datalog/schema/`)

Optional, additive. Type validation at write time, cardinality enforcement (determines CRDT strategy), uniqueness constraints. Integrated with Pull API for cardinality-aware results. Schema can be defined manually or inferred from Go struct tags via the reflect package.

### Pull API (`datalog/executor/pull.go`)

Direct entity attribute retrieval, bypassing the query planner. Attribute specs, wildcards (`[*]`), nested reference traversal with cycle detection. 9x faster than equivalent queries for entity access patterns.

### Reflect API (`datalog/reflect/`)

Go struct ↔ datom bridging:
- **Writer**: Struct tags → datoms via `tx.Add()`. Handles cardinality-many (slices), references (nested structs), update-with-diff semantics
- **Reader**: Query results / Pull results → struct population
- **Schema inference**: Struct tags → schema definitions (types, cardinality, uniqueness)

### Query Builder (`datalog/qb/`)

Type-safe query construction. Fluent API producing `*query.Query` directly (no EDN string intermediate). Supports patterns, predicates, expressions, subqueries, aggregations, pull expressions, NOT/OR clauses, multi-source (`Source()`, `PatFrom()`).

### Annotations (`datalog/annotations/`)

Performance monitoring via decorator pattern. `WrapMatcher()` wraps any `PatternMatcher` transparently. Zero overhead when handler is nil. Events for: pattern matching (index selection, scan size), join operations (type, sizes, reduction), expression evaluation, phase timing.

### Parser (`datalog/parser/`)

EDN parsing (Clojure-style syntax) and query transformation. Supports: patterns, predicates (variadic chained), expressions (arithmetic, string), aggregations, subqueries, order-by, NOT/OR clauses, pull expressions, time functions, history predicates.

### Other Packages

- **`datalog/constraints/`** — Time range constraints for predicate pushdown
- **`datalog/codec/`** — L85 encoding (sort-preserving Base85)
- **`datalog/edn/`** — EDN lexer/parser
- **`datalog/experimental/`** — Disabled optimization experiments (decorrelation, subquery rewriting)

## Complexity Analysis

### Core Operations

| Operation | Time | Space | Notes |
|-----------|------|-------|-------|
| **Hash Join** | O(\|B\| + \|P\| x \|M\|) | O(\|B\|) | B=build, P=probe, M=avg matches per probe |
| **Semi-Join** | O(\|L\| + \|R\|) | O(\|R\|) | Existence check only (NOT clause) |
| **Anti-Join** | O(\|L\| + \|R\|) | O(\|R\|) | NOT EXISTS |
| **Relation Collapse** | O(n^2) | O(1) | n=relations per phase, typically 2-5 |
| **Query Planning** | O(\|c\|^2 x \|s\|) | O(\|c\|) | c=clauses, s=symbols |
| **Index Selection** | O(1) | O(1) | Decision tree on bound components |
| **Pattern Scan** | O(\|results\|) | O(1) streaming | B-tree range scan; O(1) for cardinality-one |
| **Aggregation** | O(\|rel\|) | O(\|groups\|) | Single-pass accumulation |
| **L85 Encode/Decode** | O(1) | O(1) | Fixed 16-32 byte inputs |
| **Keyword Comparison** | O(1) | O(1) | Pointer equality via interning |

### Query Execution

Single phase with p patterns, e expressions, f predicates:
```
T(phase) = Σ T(pattern_i) + Σ T(expr_j) + Σ T(pred_k) + (p+e+f) x T(collapse)
```
- `T(pattern)` = O(\|scan results\|) — dominated by storage I/O
- `T(expression)` = O(\|input relation\|) — per-tuple evaluation
- `T(predicate)` = O(\|input relation\|) — per-tuple filtering
- `T(collapse)` = O(n^2), n <= 5 — effectively O(1)

Multi-phase: T(query) = Σ T(phase_i), executed sequentially.

### Subquery Modes

| Mode | Time | When |
|------|------|------|
| Sequential | O(\|combos\| x T(sub)) | Default |
| Batched | O(T(sub)) | `UseComponentizedSubquery=true` |
| Parallel | O(T(sub) / workers) | `EnableParallelSubqueries=true` |

### Pull API

- **Flat** (no refs): O(\|specs\| x log\|storage\|) — one B-tree lookup per attribute
- **Nested** (refs): O(d x \|specs\| x log\|storage\|) — d = reference depth
- **Wildcard**: O(\|entity attributes\|) — full entity scan

## Performance

### Verified Benchmarks (October 2025)
- **2.22x faster**: Streaming execution on low-selectivity filters
- **52% memory reduction**: Up to 91.5% on large datasets with predicate pushdown
- **4.06x speedup**: Iterator composition vs materialized operations
- **9x faster**: Pull API vs equivalent queries
- **3x faster**: Query plan caching
- **2.06x speedup**: Parallel subquery execution (8 workers)

### Known Bottlenecks
- No parallel execution within phases
- Symmetric hash join slightly slower than standard (trade-off for streaming)
- Streaming aggregation opt-in (not default)

## Feature Status

### Implemented (~85% Datalog, ~70-75% Datomic)
- Patterns, joins, predicates, expressions, aggregations
- Subqueries (TupleBinding, RelationBinding)
- Order-by (multi-column, directional)
- NOT/OR clauses (`not`, `not-join`, `or`, `or-join`)
- Pull API (attributes, wildcards, nested refs, cycle detection)
- Schema (types, cardinality, uniqueness)
- CRDT storage (LWW, add-wins, RGA)
- Time functions and history predicates
- Multi-source queries
- Database export/import (EDN)
- QueryInto, Reflect API, Query Builder

### Not Implemented
- Rules system
- Recursive queries
- Window functions
- Distinct aggregation modifier
- Transaction functions (`:db/fn`)
- Log API
- Lazy entity API
