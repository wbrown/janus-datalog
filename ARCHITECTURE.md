# Janus Datalog Architecture

**Last Updated**: May 2026 (v0.12.0)

## Overview

Janus Datalog is a Datalog query engine in Go that makes three bets most Datalog implementations don't:

**CRDT conflict resolution instead of MVCC.** Where Datomic uses a single transactor with monotonic transaction IDs, Janus uses CRDTs — last-writer-wins for scalar attributes, add-wins sets for multi-valued attributes, and RGA for ordered vectors. Transaction IDs are ElementIDs (Lamport clock + ReplicaID), not sequential integers. This means the storage layer is designed from the ground up for a world where multiple writers exist and conflicts are resolved by data structure semantics, not coordination. The choice of cardinality *is* the choice of conflict resolution strategy.

**Go structs as first-class participants in the data model.** Most Datalog engines treat the host language as a string-passing client — you construct an EDN query string, ship it to the engine, and get untyped tuples back. Janus provides that path, but also: a type-safe query builder that produces query ASTs directly (no parsing), struct reflection that bridges Go types to datoms and back (struct tags define the schema, `SaveStruct` writes, `PullInto` reads), and `QueryInto` that maps query results into typed structs. The impedance mismatch between Go and Datalog is treated as an engineering problem to solve, not an inherent cost to accept.

These two bets reinforce each other. In traditional Datomic-style Datalog, "updating" an entity means manually retracting the old value and asserting the new one — an awkward fit for struct-level operations. CRDT semantics absorb that complexity: `SaveStruct` writes new datoms, and the storage layer resolves what's current. Cardinality-one attributes use LWW (latest Tx wins, no retraction needed). Cardinality-many attributes use add-wins sets (the reflect layer diffs and emits Add/Remove ops). Cardinality-vector attributes use RGA (positional inserts with AfterRef chains). The result is that struct-level "updates" are append-only immutable writes underneath, with conflict resolution handled by data structure semantics rather than application logic. The choice of Go type — scalar, slice, or `OrderedSet` — maps directly to the CRDT strategy.

**Phase-based query composition with formal symbol contracts.** The query planner decomposes queries into phases — each an independent relational algebra expression — connected by explicit symbol flow (`Available`, `Provides`, `Keep`). This abstraction originated from Elasticsearch's parent-child document constraint (one relationship level per request), but it turned out to be a correct compositional model: phases compose via natural join with provable correctness, dependencies are checkable by construction, and the explicit metadata makes query execution debuggable at every boundary. It's kept not because the storage needs it, but because it's the right abstraction.

The result is a production-grade engine (~80% Datomic feature compatibility) that takes a pragmatic middle ground: single-node performance with the data model foundations for multi-writer replication. It implements the full relational query algebra with Datalog syntax — patterns, joins, negation, aggregation, subqueries — but not the fixed-point semantics (rules, recursive queries) that distinguish Datalog-the-formalism from relational algebra. This is the same trade-off Datomic makes: Datomic has rules that can call themselves, but no automatic fixed-point evaluation with termination guarantees. For most workloads the distinction is invisible; for program analysis or data-dependent recursive graph traversals, it's the whole ballgame.

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
results, err := d.Query(`[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`)
```

**Call chain**: `Query` → `resolveQuery` (detects string → calls `parser.ParseQuery`) → planner → executor → `executor.Relation` (streaming)

### Entry Point 2: Query Builder (`qb/`)

The builder produces a `*query.Query` directly, skipping EDN parsing:

```go
q := qb.Query().
    Find(qb.Var("?name"), qb.Var("?age")).
    Where(
        qb.Pat("?e", ":person/name", "?name"),
        qb.Pat("?e", ":person/age", "?age"),
    ).MustBuild()

results, err := d.Query(q)  // *query.Query passed directly
```

**Call chain**: `Query` → `resolveQuery` (detects `*query.Query` → returns it as-is) → same path from there. No parsing overhead.

### Entry Point 3: QueryInto

Wraps any query path with struct-mapped result delivery:

```go
var people []Person
err := d.QueryInto(&people, query, inputs...)
```

**Call chain**: Normal query execution → `[][]interface{}` → `reflect.QueryResultMapper.MapAll()` → populates struct slice via field tags.

For scalar types (single-symbol queries), extracts the first symbol directly without struct mapping.

### Entry Point 4: Pull API (Separate Path)

Pull does **not** go through the query planner. It's direct pattern matching against storage:

```go
result, err := d.Pull(entityID, "[:person/name :person/age {:person/friends [:person/name]}]")
```

**Call chain**: `parser.ParsePullPattern` → `PullExecutor.Pull` → recursive `pullWithVisited`:
- For each attribute spec: `matcher.Match()` directly against storage
- For reference attributes: recursive pull with cycle detection via visited set
- For wildcards: scan all entity attributes

**PullInto** adds struct reflection: `reflect.GeneratePullPattern(structType)` → auto-generates pull pattern from struct tags → pulls → `reflect.ReadStructWithID()` maps result to struct.

### Multi-Source Queries

Any query path supports multiple data sources via `WithSources`:

```go
results, err := d.Query(query,
    WithSources(map[Symbol]PatternMatcher{
        "$":      mainDB.Matcher(),
        "$users": userSliceSource,
    }),
)
```

**How it works**: `buildSourceMap` creates a `SourceRouter` that wraps all sources. The router becomes the executor's `PatternMatcher`. During execution, each `DataPattern` clause is routed to its declared source (`$` by default). Cross-source joins happen naturally through the relation collapsing algorithm.

### How the Planner Shapes Execution

The planner is the bridge between a declarative query (an unordered set of clauses) and an executable plan (an ordered sequence of phases). It operates in two stages: **rewrite**, then **phase**.

**Algebra-preserving logical optimization.** Before physical planning, the optimizer builds and validates a relational-algebra tree:

- **Algebra IR optimization** (`EnableAlgebraOptimizer`, default-active): clauses are compiled to a relational-algebra tree and passed through transform passes (subquery decorrelation, predicate pushdown, conditional aggregate rewriting, scan rewrites for `get-else` with vector defaults). A schema-aware immutable rebuild refreshes derived outputs after every pass.
- **Nested Datalog lowering**: the optimized tree is lowered back into a cloned `query.Query`. `Project` becomes a relation-binding subquery; aggregate/OR/NOT/lateral scope remains explicit in Datalog. The optimizer contract is therefore `Query → Query`.

- **Constant-bindable scalar detection**: Scalar inputs that only appear in predicates/expressions (not data patterns) are flagged in phase metadata. The executor resolves these as constants rather than creating separate relation groups, avoiding unnecessary joins.

Only after logical lowering does `ClauseBasedPlanner` create physical
`RealizedPlan` phases. When algebra optimization is disabled, the same planner
consumes the original Datalog query for differential verification.

**Clause ordering via selectivity scoring.** The planner scores each clause based on how much it will filter data. The key heuristic: constants get 10x the weight of available variables, because a constant like `:person/name` in a pattern actually filters storage, while an available variable like `?e` only enables a join.

```
Score = 100 (base) + (constants × 100) + (available_vars × 10)

[?e :name "Alice"]    → 300  (2 constants: :name, "Alice")
[?e :skills ?s]       → 200  (1 constant: :skills)
[?e ?a ?v]            → 100  (no constants)
```

This means the most selective patterns execute first, producing smaller intermediate results for everything downstream.

**Phase boundaries as Cartesian product barriers.** A new phase starts when a clause needs symbols that aren't available yet. This forces data to flow through explicit symbol channels (`Keep` sets) rather than allowing unconstrained cross-products between unrelated patterns.

```
Phase 1: [?e :person/name ?name]     ← binds ?e, ?name
         [?e :person/age ?age]       ← ?e available, joins naturally

Phase 2: [?dept :dept/member ?e]     ← needs ?e from Phase 1
         [?dept :dept/name ?dname]   ← ?dept available from above
```

If two patterns share no symbols and no expression bridges them, the executor errors with "Cartesian products not supported" rather than silently producing billions of tuples.

**Operation deferral.** Different clause types get different base scores, which controls when they execute:

| Clause Type | Base Score | Rationale |
|-------------|-----------|-----------|
| DataPattern | +100 | Produces data, highest priority |
| OR clause | +80 | Data source (union of branches) |
| Expression | +10 | Needs inputs bound first |
| Predicate | +5 | Filter, needs inputs bound |
| NOT clause | +2 | Anti-join, needs all inner symbols |
| Subquery | -50 | Expensive, defer as long as possible |

This ensures cheap filtering happens early, and expensive subqueries only execute after the result set has been narrowed.

**Symbol lifetime management.** The planner computes three symbol sets per phase that form the contract with the executor:

- **Available**: What's bound when this phase starts (from `:in` + previous `Keep`)
- **Provides**: What this phase's clauses produce
- **Keep**: What the next phase needs (computed by scanning all remaining clauses + `:find`)

The invariant `Keep ⊆ Provides` ensures the executor never tries to pass forward a symbol that doesn't exist in the relation. Symbols not in `Keep` are projected away between phases, keeping intermediate results narrow.

### Streaming Execution: Relation-Centric Volcano

The executor uses a variant of the Volcano iterator model (Graefe 1994), but with a key difference: it's **relation-centric** instead of operator-centric.

In classic Volcano, each operator (filter, project, join) is an iterator that pulls from child operators. The problem is that Volcano iterators are single-pass — once consumed, the data is gone. This breaks down when you need to re-iterate (hash join build sides, aggregation grouping, subquery correlation). Classic databases solve this by materializing at strategic points, but the decision of *when* to materialize is baked into the operator tree.

Janus separates the decision. Every intermediate result is a `Relation`, which can be either:
- **StreamingRelation** — wraps an `Iterator`, single-pass, lazy. Nothing materializes until `Next()` is called.
- **MaterializedRelation** — tuples in memory, supports random access and re-iteration.

The transition between them is explicit and lazy:
- `relation.Materialize()` doesn't materialize immediately — it sets a flag
- First `Iterator()` call on a marked relation wraps the base iterator with `CachingIterator`, which copies tuples to a buffer as a side effect of iteration
- Subsequent `Iterator()` calls return a slice iterator over the cached buffer
- `BufferedIterator` wraps any iterator for re-iteration without requiring the full `Relation` interface

This means a typical query pipeline looks like:

```
StorageScan → FilterIterator → ProjectIterator → DedupIterator
  → hashJoinIterator (build side materialized, probe side streams)
  → ProjectIterator → FilterIterator → collect results
```

Only the hash join build side materializes. Everything else streams. The 4x speedup and 89% memory reduction from iterator composition comes from this: most of the pipeline never allocates intermediate tuple slices.

**Symmetric hash join** (optional, `EnableSymmetricHashJoin`) extends this to stream-to-stream joins: both sides materialize incrementally into hash tables, processing in interleaved batches. Neither side needs to be fully consumed before results start flowing. Trade-off: slightly slower than standard hash join (~37% overhead) but enables full pipeline streaming when both inputs are themselves streaming.

**Forced materialization** happens only when semantically required: aggregation (must see all groups), order-by (must sort), and `Size()` calls on streaming relations (though `EnableTrueStreaming=true` returns -1 instead of forcing consumption).

### Phase Execution Detail

Within the executor, each phase is a mini-query. The planner decided the ordering; the executor just follows it:

```
For each phase in plan:
  Input relations from previous phase's Keep symbols
  For each clause in phase.Query.Where (planner-ordered):
    ├── DataPattern  → matcher.Match(onePatternQuery, bindings) → new StreamingRelation
    ├── Expression   → evaluate over existing relations → add symbol (lazy)
    ├── Predicate    → filter existing relations (lazy)
    ├── Subquery     → recursive Query
    ├── NOT clause   → anti-join (filter where inner query matches)
    └── OR clause    → union branches or per-tuple fallback
  After each clause: collapse relation groups (join on shared symbols)
  Early terminate if any group is empty
  Project to Keep symbols for next phase (lazy)
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
                    │  8 indices   │
                    │  per datom   │
                    └──────────────┘
```

### Transaction Lifecycle

1. **`d.NewTransaction()`** — Creates transaction, holds `[]Datom` buffer
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
   - `store.Assert(datoms)` — write to all 8 indices per datom
   - Transaction metadata datom (`tx:N :db/txInstant time`)
   - EA cache invalidation for touched (entity, attribute) pairs

### Reflect Write Path

The reflect API converts Go structs to transactions:

```go
tx := d.NewTransaction()
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
    Match(q *query.Query, bindings Relations) (Relation, error)
}
```

**This is the most important interface in the system.** Every data source implements it. The executor only knows about `PatternMatcher` — it doesn't know if data comes from BadgerDB, an in-memory slice, or another database.

The query fragment contains exactly one `DataPattern`. Its `OrderBy` and
`Limit` fields are populated only when the planner proves physical pushdown is
structurally safe. This keeps the matcher contract Datalog-in/Datalog-out:
storage may use those requirements to choose an order-satisfying index, while
custom sources may ignore them and return no ordering guarantee.

**Implementors**: `storage.PatternMatcher` (storage), `SourceRouter` (multi-source routing), `SliceSource[T]` (Go slices), `MemoryPatternMatcher` (in-memory datoms), `IndexedMemoryMatcher` (optimized in-memory)

**Extended variants**:
- `PredicateAwareMatcher` adds `MatchWithConstraints()` — predicate pushdown into storage
- `EntityLookupMatcher` adds error-returning `LookupAttribute()` — single-value
  lookups distinguish absence from storage/decode failure

### Relation — Tuple Set Abstraction

```go
type Relation interface {
    Symbols() []query.Symbol
    Properties() RelationProperties
    Iterator() Iterator
    Size() int
    Project(symbols []query.Symbol) (Relation, error)
    Join(other Relation) Relation
    HashJoin(other Relation, joinCols []query.Symbol) Relation
    // ... filtering, aggregation, materialization
}
```

Everything in the executor works with Relations. Pattern matches produce them, joins combine them, expressions add symbols to them, predicates filter them.

`RelationProperties` is the typed physical contract paired with Datalog's
logical requirements. It carries guaranteed ordering as
`[]query.OrderByClause` and candidate keys as `[][]query.Symbol`. Storage
establishes only properties proven by the selected index and CRDT mode;
operators preserve, transform, or conservatively clear them. The planner and
algebra optimizer still accept and emit Datalog—properties do not travel
through metadata maps or synthetic clauses.

Current propagation rules:
- Filters, limits, materialization, lazy wrapping, and functional attribute
  attachment preserve properties
- Projection preserves the valid ordering prefix and fully retained keys
- Sort establishes ordering; grouped aggregation establishes its group key
- Fresh expression outputs preserve properties
- Joins, unions, products, and fallback relations clear properties until a
  proof rule exists
- A retained candidate key lets streaming projection skip redundant
  deduplication

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

    // Error returns any error encountered during iteration. Callers MUST
    // check Error() after Next() returns false to distinguish normal
    // exhaustion from execution failure (e.g. Tier-3 blob decode failure
    // surfacing only after the iterator has yielded a prefix of tuples).
    Error() error
}
```

Composable lazy evaluation. Iterator implementations chain: `StorageScan → FilterIterator → ProjectIterator → DedupIterator`. Nothing materializes until `Next()` is called.

**Iterator-error contract.** Storage errors can be deferred — a scan may
yield several clean tuples before the underlying decode fails. Every
intermediate operator (filter, project, dedup, hash-join probe side,
union, anti-join, sort, materialization, projection-after-pattern) is
required to propagate that deferred error: either through its own
`Error()` after `Next()` returns false, or by attaching it to the result
relation so the next public boundary observes it. A static guard test
fails the build if any `collectTuplesInto` call site drops its error,
and `Relation.Sorted()` returns `([]Tuple, error)` instead of just
`[]Tuple` so that pre-sort materialization can surface deferred errors.
The contract exists because partial tuples from a failed stream must
never be reported as clean success — that pattern is how Tier-3 blob
decode failures used to silently look like "predicate filtered all the
rows out."

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
    Encoder() *BinaryKeyEncoder

    // Write operations
    Assert(datoms []datalog.Datom) error
    Retract(datoms []datalog.Datom) error
    DeleteDatoms(datoms []datalog.Datom) (int, error)

    // Read operations. There is no point lookup: a complete index key names
    // one (E, A, V, Tx), but Tx is what CRDT resolution determines, so a
    // reader that already knew it would have nothing left to ask. Every read
    // is a prefix scan.
    Scan(bound ScanBound) (Iterator, error)
    ScanKeysOnly(bound ScanBound) (Iterator, error)
    DatomsAfter(eid datalog.ElementID) ([]datalog.Datom, error)
    MaxTxForEntity(e datalog.Identity) (datalog.ElementID, bool, error)
    GetMetadataUint64(key string) (uint64, bool, error)
    SetMetadataUint64(key string, value uint64) error

    // High-water mark, derived from index ordering
    MaxElementID() (datalog.ElementID, error)

    // Consistent read view
    NewReadSession() (ReadSession, error)

    BeginTx() (StoreTx, error)
    Close() error
}
```

A scan names a **`ScanBound`**: an index, plus the leading components of that index's component order bound to datalog values. The k-th element binds the k-th component, so `ScanBound{Index: AVET, Prefix: []datalog.Value{attr, value}}` is "every datom with this attribute and this value." It is typed, not a byte range — a backend that keys on bytes projects the bound at its own boundary, and one that compares typed components directly never encodes at all.

Raw datom storage with 8 indices. Two implementations: `BadgerStore` (BadgerDB, on-disk) and `MemoryStore` (in-process, persisting the same binary keys and all eight indices). Backend selection is by build tag — `openDefaultStore` returns a `BadgerStore` on native targets and a `MemoryStore` under `js && wasm`, where `MemoryStore` is the only backend.

`storage.PatternMatcher` bridges the store to the executor by implementing `executor.PatternMatcher` — it chooses the optimal index based on which pattern components are bound, scans the store, and wraps results as `StreamingRelation`.

`NewReadSession` opens a consistent read view: every read through the session observes one snapshot regardless of writes committed after it opened, so a query can never straddle two database states mid-execution. `StoreReader` is the read subset shared by a `Store` (each call opens its own storage transaction) and a `ReadSession` (all calls share one snapshot), so read paths are written once against `StoreReader` and run identically in either mode. A query executes all its storage reads through one session, released when its result relation is exhausted or closed.

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
- **Eight indices**: Each serves different access patterns:

| Index | Use Case |
|-------|----------|
| EAVT  | Entity lookups, add-wins resolution (cardinality-many) |
| EATV  | Entity lookups, LWW resolution (cardinality-one/vector) |
| AEVT  | Attribute scans across entities |
| AETV  | Attribute scans with temporal ordering |
| ATEV  | AsOf-by-attribute access (A-bound + Tx-bound + V-unbound) |
| AVET  | Value-based lookups (uniqueness, reverse lookup) |
| VAET  | Reverse reference traversal |
| TAEV  | Transaction-based queries, history |

**CRDT Semantics**:
- **LWW** (cardinality-one): EATV index orders Tx descending; first result is current value
- **Add-Wins Sets** (cardinality-many): Tracks add/remove operations; adds win over concurrent removes
- **RGA** (cardinality-vector): Positional inserts with AfterRef chain; tombstones for deletes
- **ElementID**: 16-byte transaction ID (8-byte Lamport clock + 8-byte ReplicaID) for causal ordering

**LZ77+FSE compression with Tier-3 blob store.** Values above a size threshold
are compressed with a custom LZ77+FSE codec (`datalog/codec/{compress,lz77,fse,sequences}.go`)
and stored out-of-line in a content-addressed blob store
(`datalog/storage/blob_store.go`); the index keys reference them by hash, so
identical large values deduplicate automatically. The codec is deterministic
(a hard correctness requirement when blobs are content-keyed), achieves
3.6× compression on English prose and 10–13× on structured / repetitive data,
and decompresses at 2.1–2.4 GB/s on Apple M5 Max. The whole pipeline is
transparent at the storage layer — query execution sees the decompressed
value as if it had been stored inline — and the `#lzj` EDN tagged literal
preserves compressed form through export / import.

### Type System (`datalog/`)

Core types in the top-level package:
- **Datom**: `{E: Identity, A: Keyword, V: Value, Tx: ElementID, Op: CRDTOp, AfterRef: ElementID}`
- **Identity**: Entity identifier — the SHA1 content-address hash; renders as L85. The seed string is consumed by `NewIdentity` and discarded
- **Keyword**: Interned pointer type (`*keyword`); pointer equality = value equality, O(1) comparison
- **Symbol**: Interned pointer type (`*symbol`); query variables only, not stored
- **ElementID**: `{Lamport: uint64, ReplicaID: uint64}` — CRDT causality
- **Value**: `interface{}` — direct Go types: `string`, `int64`, `float64`, `bool`, `time.Time`, `[]byte`, `Identity`, `Keyword`

### Query Planning (`datalog/planner/`)

Single planner: `ClauseBasedPlanner`. Converts a declarative `*query.Query` into a `RealizedPlan` (ordered phases with physical symbol-flow contracts). The default logical path compiles and optimizes algebra, validates exact schemas and free requirements, then lowers back to nested Datalog before the planner constructs phases.

**Algebra lowering**: `EnableAlgebraOptimizer` (on by default) runs Query → algebra IR → transform passes → schema refresh/validation → Query. Non-linear children remain nested Datalog (`q`, OR/fallback, and NOT forms); `Project` lowers to a relation-binding subquery. Algebra never constructs `RealizedPlan`.

**Join projection experiment**: `EnableJoinProjectInsertion` (off by default)
uses backward liveness to insert `Project` on narrowed inner-join children before
nested Datalog lowering. It is correctness-proven for no-input queries, but
relation-subquery execution regresses the focused workload, so it remains an
inactive logical-rewrite experiment.

**Disabled-optimizer algorithm** (`clause_phasing.go`): Greedy clause selection within phases:
1. Start with input symbols as available
2. Score all executable clauses (all required symbols are available)
3. Select highest-scoring clause, add to current phase, mark its output symbols as available
4. Repeat until no more clauses can execute in this phase
5. Start new phase with remaining clauses (new phase inherits `Keep` from previous)

**Scoring** (`clause_utils.go`): Visibility-based selectivity — constants weight 10x over available variables because they filter storage directly. Patterns scored 100+, OR at 80, expressions at 10, predicates at 5, NOT at 2, subqueries at -50. This naturally orders operations from most-selective data access down to expensive deferred operations.

**Phase boundaries**: A clause that needs symbols not yet available forces a new phase. This is the mechanism that prevents Cartesian products — symbols must flow through explicit `Keep` channels between phases.

**Output**: `RealizedPlan` — sequence of `RealizedPhase`, each containing:
- `Query`: Self-contained `*query.Query` fragment (`:find`, `:in`, `:where` clauses in execution order)
- `Available`: Exact phase input schema/environment
- `Provides`: Exact physical output schema of `Query`
- `Keep`: Exact materialized boundary schema; equal to `Provides` for non-final phases and empty for the final phase

**Key invariants**: phase metadata must match Datalog input/output schemas exactly; adjacent `Keep`/relation-input schemas must agree; runtime relations are checked against `Provides`.

**Plan caching** (`cache.go`): SHA256 of query structure → cached `RealizedPlan`. LRU eviction at 1000 plans, 5-minute TTL. 3x speedup on repeated queries. Hit/miss counters for monitoring.

**Explain support** (`explain_analysis.go`): Optional population of per-phase `Patterns`, `Expressions`, `Predicates`, `Subqueries` with index selection, selectivity scores, and binding analysis for plan inspection.

**NOT/OR/Subquery handling**: All treated as first-class clauses in the greedy algorithm. NOT requires all inner symbols bound (score +2, naturally deferred). OR provides the intersection of all branch outputs in union mode, or the union in fallback mode. Subqueries require all `:in` correlation symbols (score -50, executed last).

### Query Execution (`datalog/executor/`)

`DefaultQueryExecutor` processes clauses sequentially within each phase. After each clause, relation groups are collapsed — groups sharing symbols are joined, disjoint groups are kept separate. If disjoint groups remain after all clauses, it's an error (Cartesian product prevention).

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

### Public API (`datalog/db/`)

The recommended entry point for consumers. Provides `db.Open()` with functional options, type aliases (`DB = storage.Database`, `Transaction = storage.Transaction`), `MustParseQuery()` for compile-time query constants, and `Querier`/`EntityReader` interfaces. All methods on `*storage.Database` (`Query`, `QueryInto`, `Pull`, `AsOf`, `History`, etc.) are available directly on the `*DB` handle. For advanced use cases (custom planner options, annotation handlers, multi-source queries with `WithSources`), consumers can still use the `storage` package directly.

### Query Builder (`datalog/qb/`)

Type-safe query construction. Fluent API producing `*query.Query` directly (no EDN string intermediate). Supports patterns, predicates, expressions, subqueries, aggregations, pull expressions, NOT/OR clauses, multi-source (`Source()`, `PatFrom()`).

### Annotations (`datalog/annotations/`)

Performance monitoring via decorator pattern. `WrapMatcher()` wraps any `PatternMatcher` transparently. Zero overhead when handler is nil. Events for: pattern matching (index selection, scan size), join operations (type, sizes, reduction), expression evaluation, phase timing.

### Parser (`datalog/parser/`)

EDN parsing (Clojure-style syntax) and query transformation. Supports: patterns, predicates (variadic chained), expressions (arithmetic, string), aggregations, subqueries, order-by, NOT/OR clauses, pull expressions, time functions, history predicates.

### Other Packages

- **`datalog/constraints/`** — Time range constraints for predicate pushdown
- **`datalog/codec/`** — L85 encoding (sort-preserving Base85) and the LZ77+FSE compression codec (`compress.go`, `lz77.go`, `fse.go`, `sequences.go`)
- **`datalog/edn/`** — EDN lexer/parser
- **`datalog/experimental/`** — Retired Selinger-style decorrelation path; production decorrelation now lives in the algebra optimizer (active by default inside `EnableAlgebraOptimizer`)

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
| Per-combination | O(\|combos\| x T(sub)) | Nested `(q ...)` clauses |
| Parallel RelationInput iteration | O(\|inputs\| x T(query) / workers) | `EnableParallelSubqueries=true` |

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

### Implemented (~80% Datomic feature compatibility)

**Relational query algebra with Datalog syntax:**
- Patterns, joins, predicates, expressions, aggregations
- Subqueries (TupleBinding, RelationBinding)
- Order-by (multi-symbol, directional)
- NOT/OR clauses (`not`, `not-join`, `or`, `or-join`)

**Storage and data model:**
- Schema (types, cardinality, uniqueness)
- CRDT storage (LWW, add-wins, RGA)
- Time functions and history predicates
- Multi-source queries
- Database export/import (EDN)

**Entity access:**
- Pull API (attributes, wildcards, nested refs, cycle detection)
- QueryInto, Reflect API, Query Builder

### Not Implemented

**Recursive queries** (also limited in Datomic):
- No recursive rules or fixed-point evaluation (Datomic supports
  self-calling rules but without termination guarantees or semi-naïve
  evaluation)
- Non-recursive rules (named, reusable query fragments) are covered by
  Go functions + Query Builder, but not available through EDN queries

**Datomic features:**
- Transaction functions (`:db/fn`)
- Log API
- Lazy entity API

**Query extensions:**
- Window functions
- Distinct aggregation modifier
