# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Test Commands

**The standard gate is `make test`.** It runs the native suite (`go test -count=1 ./...`) and the js/wasm contracts (`make test-wasm`). Do not treat bare `go test ./...` as a full green — that skips wasm.

```bash
make test                        # Full gate: native + wasm (required)
make test-wasm                   # js/wasm contracts only (needs node)
go test -v ./package -run Test   # Focused native iteration only
```

**Do NOT add `-timeout` to `go test` commands, or use `-timeout 0`.** Use the default timeout. No exceptions.

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
- "It's simpler/easier this way" (when deviating from a plan or established pattern)
- Making a choice between multiple valid approaches without consulting

**Bugs do not authorize design changes:**
- Discovering a bug does not authorize you to change the agreed design. Report it and ask.
- If something we agreed on doesn't work, STOP and ask. Do not substitute alternatives.
- If you're about to do something different from what was discussed/agreed, ASK FIRST. No exceptions.

**The user's job**: Set direction, make architectural choices, review designs
**Your job**: Implement, follow patterns, propose options (not make choices)

## Discussion Is a Mode Only the Owner Exits

A question from the user never ends a discussion — it deepens one. Discussion ends only when the user issues an explicit directive in his own words ("go", "implement it", "proceed"). Answering his question is not an exit. Resolving the last open point is not an exit. Your own "shall I?" is not an exit, and his next message is not an implicit yes unless it actually says yes. Before any mutating tool call that follows a design exchange, you must be able to quote the words of his that authorize it.

The tell: your message ends with a self-issued authorization — "shall I?", "that resolves the last point", "proceeding on that basis" — followed by tool calls without his go-word in between. That is the lapse executing. Sit still instead.

## Inherited Shape Is Not Authority

**A design you did not derive is a design you must not defend.** Upstream code being ported, an adjacent entry in a document you are extending, the commit pattern of the branch you are on, your own first answer — these are shapes, not rulings. Every shape has premises. Before preserving one, name its premises and check them against THIS codebase; before copying one, ask what the artifact is for. "Near-verbatim is the correctness strategy" applies to logic whose invariants you must not perturb — it never extends to the design wrapped around that logic.

The tells that the lapse has already happened:

- You are defending an inherited shape against the owner's question instead of testing the premise the question aims at. His probing questions about a component ("do we need X?", "why is it Y?") ARE the design process — a question is a premise under test, usually because he already suspects it fails. Answer it, then stop with the pen in his hand. "Veto if you disagree," issued in the same turn you keep working, is proceeding unilaterally.
- Your justification cites fidelity, convention, or an adjacent example — anything except the premises of the problem in front of you. (The crash-state clause copied into a PERFORMANCE_STATUS entry because the neighboring entry had one; the code/docs commit split inferred from branch history whose real cause was docs written in a later session — same disease, smaller stakes.)

**Case study (2026-07-21, the EA-cache trie)**: porting Go's HashTrieMap, I defended in sequence the seeded hasher, the generic type parameters, and the two-map layout — each out of fidelity to upstream's shape. The owner's three questions ("can't we do a typed sync.Map?", "do we necessarily need a hasher?", "why are we making it generic?") each tested a premise, and every premise failed, because upstream's premises — arbitrary keys, opaque hashing, unknown consumers — were all false here: the keys were already SHA1 content hashes with exactly one consumer. The shape I defended measured −11.6% on the complex checkpoint. The shape his questions produced — one specialized trie, combined {entry, version} slots, routing from the key's own bits — measured −27.9% and cut cache hits from 16.7ns to 6.1ns. The fidelity I mistook for rigor was the thing hiding the design.

## When Tests Fail

**Failing tests are information, not obstacles.**

When tests fail after you make a change, the correct response is:
1. Understand WHY the test is failing
2. Report the failure to the user with context
3. Ask how they want to proceed

**NEVER change architecture or add code just to make tests pass.** If a test fails, it's telling you something important about the change - maybe:
- The change has unintended consequences
- The approach needs to be different
- The test expectations need updating
- The feature isn't ready

All of these are decisions for the user, not you.

**Wrong**: "The cache tests are failing, so I'll add a new cache type to make them pass."
**Right**: "The cache tests are failing because ClauseBasedPlanner doesn't integrate with the cache the same way. How do you want to handle this?"

## The Baseline Is Green — Never Blame Pre-Existing Conditions

**Every test passes before any work session starts. This is an invariant, not something to verify.** The repository is always green at the start of your work (`make test` passes; the pre-push hook enforces it). Internalize this and reason from it.

It has one unavoidable consequence: **any test that fails during or after your work was caused by your work** — either by a change you made, or by a stricter gate you chose to run (e.g. `-race`) that the standard suite does not. There is no third possibility. Therefore:

- **NEVER attribute a test failure to "pre-existing conditions."** Not in your reasoning, not in your reporting. The phrasing "this is pre-existing / not my change / not part of this work / I didn't touch that code" is **forbidden** — it is blame-deflection (CYA), and against a green baseline it is also false by construction. Catch yourself before you emit it.
- **NEVER run experiments to "prove" a failure is pre-existing.** No `git stash`, no `git checkout -- <file>`, no revert-and-rerun to A/B the baseline. You already know the baseline was green and you already have your own diff; causation is determined by **reading the diff and the failure**, never by mutating the working tree. (`git stash` is banned outright — it has silently buried uncommitted work in this repo. Never run it for any reason.)
- **Do NOT invent gates the project does not use** (e.g. `-race`, extra linters) and then go chasing what they surface as if it were someone else's problem. The standard gate is `make test`. If you choose to run a stricter check, anything it finds is yours to fully resolve or you should not have run it.

When a test is red there is exactly one fork: **fix it, or report it and ask** (per "When Tests Fail" above). There is no "investigate whose fault it is" step. Ownership is the default; the baseline guarantees it.

## When Asked to Revert

**Revert IMMEDIATELY. Do not defer.**

When the user says "revert", do it as your very next action. Do not:
- Explain why you made the change
- Read files to "understand context"
- Plan what you'll do after
- Make any other changes first

**Why this matters:** When you defer reverting and wander off making more changes, the original correct state gets buried in context. If context compaction happens, the original code may be lost from the summary entirely. Future Claude inherits a mess with no way to recover.

- **Immediate revert** = recoverable state preserved
- **Deferred revert** = potentially permanent damage

## Principles Are Upstream, Not a Final Filter

Every rule in this file is a **discipline that generates your actions**, not a
token or command to suppress at the last checkpoint. When you implement a rule as
an output filter — scanning for a banned word in identifiers, or catching a
banned command just before you run it — you leak it everywhere the filter doesn't
reach, and the leak is proof the upstream thinking already lapsed. If you find
yourself routing a rule through a checkpoint instead of letting it shape the
action, stop: you have already lost it. These are the recurring instances:

- **The user's presence is never a variable.** Whether the user is watching,
  likely to answer quickly, or away entirely must not appear anywhere in the
  derivation of what to do next. Every ask-first and report-first rule in
  this file was priced by the owner when the rule was written; the owner's
  availability is not yours to re-weigh per decision. Reasoning from "will
  this be seen" is surveillance-conditioned compliance — it produces the
  right action only while observed, and these rules exist precisely for the
  unobserved hours. It is also insidious: the observed case tends to yield
  the correct action for the wrong reason, so the pattern survives until an
  unobserved case, where the same derivation licenses proceeding
  unilaterally. If the user's presence (or any observation proxy — a gate
  that will be checked, a diff that will be reviewed) surfaces in your
  reasoning at all, the discipline has already lapsed upstream. An action's
  correctness does not depend on its visibility.

- **"No helpers" is not "don't type `helper` in a function name."** It is: name
  every piece of code for what it does. If the word *helper* occurs to you at all
  — in a name, a comment, or describing your own work out loud — you have not done
  the naming, and that code needs a second look. The word surfacing IS the
  violation, wherever it surfaces.

- **Never destroy state you cannot get back.** This is not "avoid these verbs," it
  is a property of the action. `git clean` and `git stash` are **banned outright,
  for any reason** — `clean` irrecoverably deletes untracked files (ones git has
  no record of), `stash` has silently buried work in this repo. Do not `rm`/`rmdir`
  files. To undo your *own* uncommitted edits when the user asks you to revert,
  `git restore` on the tracked files you changed this turn is fine; to remove a
  file you created, leave it in place and ask, or move it — never delete. When in
  doubt, move, don't delete.

- **Green gates commit-prep.** `git add`, moving a bug doc to `resolved/`, marking
  something RESOLVED — these are commit-prep, and they come *after* a green
  `make test`, never in the same breath as the run that would tell you
  whether the work is even correct. A red pre-existing test is the loudest signal
  the approach is wrong; you must not be anywhere near `git add` when you haven't
  earned green.

- **Diagnose by reading, not by ritual.** No throwaway `*_test.go` scratch files,
  no `md5`/`shasum`/`wc -l`/`cat -A` on source files. A failed Edit means your
  `old_string` was wrong or your context is stale — Read the relevant lines and fix
  it. The file is not haunted; hashing it tells you nothing about your mistake.

- **One result at a time when actions depend on each other.** Over-parallelizing
  tool calls so you act on step N before reading step N-1's result is how the
  destructive and premature mistakes above actually happen. Fan out only truly
  independent work.

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
- **Fixed 69-byte keys**: E(20) + A(32) + Tx(16) + Op(1) for efficient indexing
- **Unbounded values**: Stored last with 2-byte size prefix and 1-byte type
- **Binary physical encoding**: Raw fixed-width E/A/Tx components preserve byte order without text expansion
- **Eight indices**: EAVT, EATV, AEVT, AETV, ATEV, AVET, VAET, TAEV for different access patterns and cardinalities. ATEV's `[A][Tx↓][E][V]` layout gives O(1) attribute high-water mark for cache freshness and direct AsOf-by-attribute scans.
- **CRDT semantics**: LWW for cardinality-one, add-wins for cardinality-many, RGA for cardinality-vector
- **Keyword interning**: Keywords hashed once and reused
- **RefValues**: Entity references are stored as raw 20-byte identity hashes
- **Attribute size**: Increased from 20 to 32 bytes to support longer attribute names (e.g., `:option/open-interest`)
- **Tx as ElementID**: 16-byte transaction ID with Lamport clock (8 bytes) + ReplicaID (8 bytes) for CRDT ordering

### L85 Encoding Details

L85 is a custom Base85 encoding for stable, human-readable external values. It
is not a physical storage-key format.

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
- Identities and binary values can be logged and copied without binary tooling
- Export/import values have stable text representations
- URLs and JSON safe without escaping
- A 20-byte value uses 25 characters instead of Base64's 28

**Implementation Notes**:
- Located in `datalog/codec/l85.go`
- Inspired by Base85 encoding patterns with sort-preservation
- Decode table uses i+1 (0 = invalid) for cleaner validation
- Big-endian encoding preserves numeric sort order
- Physical storage keys use `BinaryKeyEncoder` exclusively
- Identity references in physical keys remain raw 20-byte hashes
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
datalog/             # Top-level: core types (Datom, Identity, Keyword, Value, ElementID), interning, comparison
├── algebra/         # Relational algebra IR, decorrelation, transform framework
├── annotations/     # Query execution observability (decorator pattern)
├── codec/           # L85 encoding + LZJ compression (LZ77+FSE)
├── constraints/     # Predicate classification and constraint infrastructure
├── db/              # Public API package (db.Open, d.Query, d.History, d.AsOf)
├── edn/             # EDN lexer and parser
├── executor/        # Query execution with Relations, joins, NOT/OR, Pull API
├── parser/          # Datalog query parser (EDN → Query AST)
├── planner/         # Query planning and optimization (clause-based planner)
├── qb/              # Fluent Go query builder (Query/Find/Where/...)
├── query/           # Query AST types and Symbol/Tuple/Relation primitives
├── reflect/         # Struct ↔ datom mapping via reflection
├── schema/          # Schema support (types, cardinality, uniqueness)
└── storage/         # BadgerDB-backed storage, indices, CRDT resolution, blob store
```

## Type System Architecture

The codebase maintains a clean separation between user-facing types and storage representations:

### Core Types (`datalog/`)
- **Datom**: The fundamental unit with proper types (not strings!)
  - `E: Identity` - Entity identifier with SHA1 hash and L85 encoding
  - `A: Keyword` - Attribute keyword (interned pointer)
  - `V: Value` - Any value (interface{} containing Go types directly)
  - `Tx: ElementID` - 16-byte transaction ID (Lamport + ReplicaID) for CRDT causal ordering
  - `Op: CRDTOp` - CRDT operation (none/add/remove/rga-insert/rga-tombstone)
  - `AfterRef: ElementID` - RGA position reference (only used when `Op.HasAfterRef()` is true)
- **Identity**: Like C++ Reference and Clojure Identity - the SHA1 content-address hash, rendered as L85; the seed string is hashed and discarded
- **Value**: Just `interface{}` - no wrapper types, direct Go types:
  - Scalars: `string`, `int64`, `float64`, `bool`, `time.Time`, `[]byte`
  - References: `Identity` (aliased as `Reference` when used as a value)
  - Keywords: `Keyword` (can be used as values, e.g., `:status/active`)
  - Symbols: `Symbol` (EDN symbols as first-class stored/query values)
- **Join Keys**: Use typed hashing and equality through `TupleKeyMap`

### Storage Layer (`datalog/storage/`)
- **Purpose**: Internal storage representation only
- **StorageDatom**: Uses fixed byte arrays for efficient indexing ([20]byte for E/Tx, [32]byte for A)
- **Conversion**: Storage layer converts between user types and storage types internally
- **Binary Encoding**: Raw fixed-width components provide sortable storage keys
- **Invisible to Query Engine**: The query engine never sees these types

This separation ensures the query engine remains simple and focused on logic, while the storage layer handles all encoding/decoding complexity.

### Storage Integration
The storage layer connects the query engine to BadgerDB:
- **Database API**: High-level interface for creating transactions and querying
- **Transaction API**: Write datoms with automatic indexing across all 8 indices
- **storage.PatternMatcher**: Implements the `executor.PatternMatcher` interface for the executor
  - Chooses optimal index based on bound values in patterns
  - Converts between user types (Identity, Keyword) and storage types ([20]byte for E/Tx, [32]byte for A)
  - Builds binary index prefixes with the same encoding used for writes
- **Value Encoding**: Serializes all value types with proper type tags
  - Special handling for Identity references to preserve join semantics
  - Fixed bugs where entity IDs were decoded incorrectly

### Multi-Source Query Architecture
- **One interface**: `PatternMatcher` is the sole interface for all data sources. No separate `DataSource` type.
- **SourceRouter**: Routes by `pattern.Source` field via `map[Symbol]PatternMatcher`. Also implements `PredicateAwareMatcher` (delegates predicate pushdown to underlying source) and `EntityLookupMatcher` (delegates to default `$` source for `get-else`, `missing?`, `get-some`).
- **Source threading**: Sources are an execution context built once at the top level. `Query()` accepts `WithSources(...)` as a functional option. The `SourceRouter` becomes the executor's `PatternMatcher`, so subqueries inherit access to all sources automatically.
- **IsSourceSymbol**: Helper `strings.HasPrefix(string(sym), "$")` replacing all hardcoded `sym == "$"` checks.
- **Adding new source types**: Implement `PatternMatcher`. Optionally implement `PredicateAwareMatcher` for predicate pushdown. Pass via `WithSources`.
- **Key files**: `executor/source_router.go`, `executor/slice_source.go`, `storage/database.go` (`WithSources`, `buildSourceMap`, `validateQuerySources`), `qb/source.go`

## Implementation Status

### ✅ Recent Updates (Q1 2026)
1. **LZJ Compression Codec** - Custom LZ77+FSE compression for value storage
   - Ratios: 3.6× English prose, 10-13× structured/repetitive data
   - Decompress 2.1-2.4 GB/s (Apple M5 Max), 7 allocs per 1KB value
   - Deterministic output (hard correctness requirement for storage)
   - `#lzj` EDN tagged literal for export/import; transparent at storage layer
   - Located in `datalog/codec/{compress,lz77,fse,sequences}.go`
2. **Tier 3 Blob Store** - Out-of-line storage for large/compressed values
   - Compressed values above threshold stored in `storage/blob_store.go`
   - Hash-keyed deduplication; values referenced by hash from index keys
   - Fixed `[]byte` CRDT set resolution panic discovered during this work
3. **OR Semantics Split** - `(or)` = Datomic union, `(or-default)` = fallback (janus extension)
   - Pure Datomic union semantics for `(or ...)` and `(or-join ...)`
   - Default-value/fallback semantics moved to `(or-default ...)`/`(or-default-join ...)`
4. **Algebra Bridge Decorrelation** - Replaced legacy CSE with relational-algebra rewrites
   - Removed Selinger-style CSE (superseded by algebra optimizer)
   - Decorrelation transforms correlated subqueries into joins where structurally safe
   - Skips decorrelation inside Union branches (preserves semantics; documented bug fix)
5. **Conditional Aggregate Rewriting** - 7.7× faster correlated aggregates
6. **AETV Index + Value Elimination** - Proper A-primary CRDT resolution; ~50% storage reduction (keys-only)
7. **Temporal API Cleanup** - Removed `[(history)]`/`[(as-of)]` query predicates; use `d.History()`/`d.AsOf()` instead
8. **Pre-push Hook + CI** - `make test` (native + wasm) runs on every push

### ✅ October 2025 Updates
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

### ✅ August 2025 Updates
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
10. **Storage integration** - Database and Transaction API, `storage.PatternMatcher` for pattern matching
11. **Value encoding** - Proper serialization for all value types including entity references
12. **Aggregation functions** - `sum`, `count`, `avg`, `min`, `max` with grouping support (with proper time.Time support)
13. **Temporal queries** - ElementID-based AsOf/History queries for time-travel
14. **Time extraction functions** - `year`, `month`, `day`, `hour`, `minute`, `second` for temporal analysis
15. **Subqueries** - Full implementation with TupleBinding and RelationBinding support
16. **Result/Relation unification** - Eliminated redundant Result type, unified API
17. **Table formatter** - Markdown table formatting using tablewriter library
18. **Order-by clause** - Full implementation with multi-symbol sorting and direction control
19. **Time comparison fix** - Proper time.Time comparison in aggregations and predicates
20. **Datomic compatibility** - ~80% feature parity (see DATOMIC_COMPATIBILITY.md)
21. **Relations migration** - Multi-value variable support throughout codebase
22. **Pull API** - Declarative entity attribute retrieval with nested refs, cycle detection, wildcards (9× faster than queries)
23. **Schema support** - Type validation, cardinality (one/many), uniqueness constraints; optional and additive
24. **CRDT storage** - LWW for cardinality-one, add-wins for cardinality-many, RGA for cardinality-vector; all writes preserved with ElementIDs; `db.History()` for raw datom access, `db.AsOf(elementID)` for point-in-time queries; three-mode `*ElementID` matcher (`nil`=latest, `&ElementID{}`=history, `&ElementID{L,R}`=as-of)
25. **NOT/OR clauses** - Full support for `(not ...)`, `(not-join ...)`, `(or ...)`, `(or-join ...)` with Datomic-compatible union semantics; `(or-default ...)`, `(or-default-join ...)` for fallback/default-value patterns (janus extension)
26. **QueryInto API** - Typed query results via `QueryInto()` and `QueryOneInto()` with struct tag mapping for variables and aggregates
27. **Multi-source queries** - Named sources (`$name`), `SourceRouter`, cross-source joins, `MemoryPatternMatcher`, `SliceSource[T]`, query builder `Source()`/`PatFrom()`
28. **Database export/import** - EDN format export/import for backup and migration; CLI flags `-export` and `-import`; preserves all value types and transaction IDs
29. **LZJ compression** - Custom LZ77+FSE codec; 3.6-13× ratios; deterministic; transparent at the storage layer; `#lzj` EDN tagged literal for export/import
30. **Tier 3 blob store** - Out-of-line storage for large/compressed values with hash-keyed deduplication; values referenced by hash from index keys

### 📋 TODO (Priority Order)

See `TODO.md` and `PERFORMANCE_STATUS.md` for detailed roadmap.

**Medium Priority**:
1. **CollectionBinding** - `[?x ...]` binding for set inputs in subqueries
2. **Distinct aggregation** - Add `(count-distinct ?x)` and `distinct` modifier
3. **Adaptive streaming strategy** - Auto-choose streaming vs materialized based on data shape

**Long Term**:
1. **Statistics-based optimization** - Query planning with cardinality estimates
2. **Parallel pattern execution** - For independent patterns (requires dependency analysis)
3. **WASM persistence adapters** - OPFS / host IndexedDB on top of `OpenMemory` + Export/Import

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

### CRITICAL: No "Helpers"
**NEVER name files, functions, or packages `helper`, `helpers`, `utils`, `common`, `misc`, or `shared`.** Never use "helper" in comments to describe code. Every function does something specific — name it for what it does.

**Why this matters**: `helpers.go` is a junk drawer. Code in it escapes scrutiny because the name signals "secondary, not important." This is how parallel implementations hide unnoticed — `notOrTupleKey` lived in `helpers.go` for four months next to `TupleKey` in `tuple_key.go`, with a collision bug, untested, because nobody looks critically at "helpers." If the file had been `relation_ops.go`, the duplication would have been obvious.

Naming something precisely forces you to think about what it does and where it belongs. That thinking is where bugs get caught. "Helper" defers that thinking indefinitely.

**If you're tempted to create a helpers file or call something a helper:**
- Name the file for what the functions do (`relation_ops.go`, `iterator_validation.go`)
- Name the function for what it does (`getUniqueCombinations`, not `helperGetCombos`)
- If you can't name it, you don't understand it well enough to write it

### CRITICAL: Use Pure Relational-Algebra Vocabulary
Describe relation structure with **symbols**, **tuple positions**, **relation
attributes**, and **bindings**. Do not import SQL table-field terminology into
identifiers, comments, errors, tests, documentation, or design discussion.

Name code for the relational concept it implements:
- `symbols`, not table-oriented field lists
- `symbolIndex` or `tuplePosition`, not SQL-oriented positional names
- `relation attribute`, not a table field
- `binding`, when a symbol receives a value

Exact external API names that describe source locations are exempt. For example,
`parse.Node.Column` is a horizontal source position, not relation structure; use
that field directly and preserve compile-time checking rather than hiding its
name through reflection or string construction.

Relational invariants:
- A `Relation` is always a set; each complete tuple appears at most once.
- Temporary tuple streams may require deduplication before becoming a Relation.
- Set-preserving operators must not defensively deduplicate again.
- `Materialize` guarantees replayability; it does not require eager realization
  when the Relation is already replayable.

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
    phases []RealizedPhase
}
func (pp *PredicatePropagator) Propagate() { ... }

// GOOD: Focused functions and methods on the data they transform
func scheduleReadyClauses(clauses []query.Clause, available map[query.Symbol]bool) []query.Clause
func (p RelationProperties) project(symbols []query.Symbol) RelationProperties
```

## Performance Considerations

- **Memory optimization**: Use datom interning, compressed storage, and lazy sequences
- **Parallel processing**: Leverage Go's concurrency for pattern matching and joins
- **Caching hierarchy**: Pattern compilation, tuple deduplication, query plans
- **Early termination**: Stop execution immediately on empty intermediate results

### CRITICAL: Profile, Don't Theorize

When performance doesn't match expectations, the FIRST action is `go test -cpuprofile` + `go tool pprof`. Do NOT theorize about what might be slow.

**Case study**: A 13s query bottleneck produced 10+ fabricated explanations ("scan I/O cost", "cons-cell overhead", "BadgerDB block cache", "prefetch does N broad scans") — all wrong. The profiler showed `pthread_cond_wait` at 23% CPU = goroutine scheduler thrashing from `PrefetchValues=true` spawning goroutines per BadgerDB iterator. Fix: one line (`PrefetchValues = false`). Result: 13s → 392ms.

**Rules**:
- Never say "the bottleneck is X" without profiler evidence
- Never fabricate per-operation costs ("~0.4ms per datom") — measure them
- `pthread_cond_wait/signal` in profiles means lock/goroutine contention, not I/O
- When measurements contradict a theory, the theory is wrong — profile again

### CRITICAL: Extend, Don't Avoid

When a component can't handle a valid input, **extend the component**. Never:
- Return an error to refuse the work ("not supported")
- Route around it ("only call the compiler when it has something to optimize")
- Pass through unchanged ("compile as opaque node")
- Blame the caller ("why is the compiler being asked to handle this?")

These are all avoidance disguised as engineering judgment. The correct answer is always: extend the system to handle the new case.

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
- Joins relations that share symbols, keeps disjoint relations separate
- Early termination on empty joins to avoid wasted work
- Uses hash joins for shared symbols, prevents cross products otherwise
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

// Using the db.Open API — pass handler as an option
d, _ := db.Open("path/to/db", db.WithAnnotationHandler(handler))
// All queries through d.Query() will emit annotation events

// Internal equivalent (for advanced usage):
// baseMatcher := storage.NewPatternMatcher(database.Store())
// matcher := executor.WrapMatcher(baseMatcher, handler)
// exec := executor.NewExecutor(matcher, database)
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
   - Relations that share no common symbols (symbols)
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
- **[DATOMIC_COMPATIBILITY.md](DATOMIC_COMPATIBILITY.md)** - Comprehensive compatibility guide for Datomic users (~80% feature parity)
- **[PERFORMANCE_STATUS.md](PERFORMANCE_STATUS.md)** - Current performance status, active optimizations, and benchmarks
- **[TODO.md](TODO.md)** - Active task tracking with completed and pending features
- **[DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md)** - Complete documentation navigation

### Implementation Guides
- **[docs/reference/SCHEMA.md](docs/reference/SCHEMA.md)** - Schema support: types, cardinality, uniqueness, Pull API integration
- **[docs/reference/CRDT.md](docs/reference/CRDT.md)** - CRDT storage semantics: LWW, add-wins sets, RGA vectors, EA cache
- **[docs/reference/REFLECT.md](docs/reference/REFLECT.md)** - Struct reflection API: Go structs ↔ datoms
- **[docs/reference/QUERY_INTO.md](docs/reference/QUERY_INTO.md)** - QueryInto API: typed query results into Go structs
- **[docs/INPUT_PARAMETER_SEMANTICS.md](docs/INPUT_PARAMETER_SEMANTICS.md)** - Comprehensive guide to input parameter handling
- **[docs/reference/PLANNER_OPTIONS.md](docs/reference/PLANNER_OPTIONS.md)** - Complete planner options reference with performance guidance
- **[docs/reference/MULTI_SOURCE.md](docs/reference/MULTI_SOURCE.md)** - Multi-source queries: cross-database joins, in-memory sources, SliceSource, custom sources

### Historical Context
- **[docs/archive/early-design/DATALOG_GO_NOTES_HISTORICAL_INSIGHTS.md](docs/archive/early-design/DATALOG_GO_NOTES_HISTORICAL_INSIGHTS.md)** - Architectural insights and lessons learned
- **[docs/ideas/planner-improvements.md](docs/ideas/planner-improvements.md)** - Proposed query planner enhancements using information flow approaches
- **[docs/archive/completed/subquery-implementation-plan.md](docs/archive/completed/subquery-implementation-plan.md)** - Detailed plan for implementing subqueries (COMPLETED)
- **[docs/archive/completed/order-by-implementation-plan.md](docs/archive/completed/order-by-implementation-plan.md)** - Implementation plan for :order-by clause (COMPLETED)

---

## Testing Strategy

### CRITICAL: Tests Are Not Optional

The implementation is NOT complete until tests pass. This is non-negotiable because:
- **Bugs hide in untested code** - The merge join algorithm looked correct but had a critical bug found only by testing
- **"It compiles" ≠ "It works"** - Compilation proves syntax, tests prove correctness
- **Manual testing lies** - Small manual tests miss edge cases that comprehensive tests catch

---

### Testing Workflow (Mandatory)

#### 1. Write the implementation

#### 2. Write comprehensive tests covering:
- Happy path (expected inputs)
- Edge cases (empty, single, large)
- Error cases (invalid, missing, conflicting)
- Performance validation (if relevant)

#### 3. Run the tests
```bash
go test -v ./package -run TestName   # focused iteration
make test                            # full gate before declaring done
```

**If tests timeout**:
- Run smaller subsets OR use longer timeouts on tool calls - don't give up
- **Wait for completion**: Be patient, don't assume success

#### 4. Verify tests PASS
- Not just "no errors", but actual PASS
- Test failure = implementation has bugs
- "No race detected" ≠ test passed
- Read the full output, don't stop at first sign of success

#### 5. Only when tests PASS: Then commit

---

### NEVER

- ❌ Declare work "done" before writing tests
- ❌ Write tests but not run them
- ❌ Assume test failures are "test problems" - they reveal real bugs
- ❌ Create test_*.go files in the root directory
- ❌ Commit because "tests are taking too long"
- ❌ Commit on first failure with unrelated error
- ❌ Skip verification because of timeouts
- ❌ Assume a fix works based on theory alone
- ❌ Use `t.Skip` to hide known bugs or unimplemented features. If a test exists, it documents expected behavior. If it fails, that's a bug to track — not a skip to add. Skips are a one-way ratchet: easy to add, never removed, and they silently degrade coverage when the underlying bug gets fixed by other work.
- ❌ Scope the full gate to bare `go test ./...` or `./datalog/...` — use `make test` (native `./...` plus wasm); focused `go test` is for iteration only

---

### ALWAYS

- ✅ Write tests in *_test.go files in the appropriate package
- ✅ Run tests immediately after writing them
- ✅ **Wait for tests to complete** - be patient
- ✅ Investigate every test failure thoroughly
- ✅ Use `go test` not standalone programs
- ✅ Verify PASS status, not just absence of specific errors

---

### Test Timeouts

- **Do NOT add `-timeout` to `go test` commands, or use `-timeout 0`.** Use the default timeout. No exceptions.
- Timeouts mean WAIT or TEST DIFFERENTLY, not COMMIT ANYWAY
- Use longer timeout values on tool calls (e.g., 600000ms for slow tests)
- Run smaller test subsets if full suite times out
- If you get impatient: ASK the user, don't decide unilaterally

---

### Test Coverage Guidelines

#### 1. Property-based tests for relational algebra laws

Example:
```go
func TestJoinCommutative(t *testing.T) {
    // R ⋈ S = S ⋈ R
    result1 := r.Join(s)
    result2 := s.Join(r)
    assert.Equal(t, result1, result2)
}
```

#### 2. Differential testing against reference implementations

Compare results with known-good implementations when possible.

#### 3. Correctness tests with known inputs/outputs

```go
func TestAggregationCorrectness(t *testing.T) {
    // Known input → Known output
    input := createTestData()
    expected := []Tuple{...}
    actual := executeQuery(query, input)
    assert.Equal(t, expected, actual)
}
```

#### 4. Performance benchmarks tracking query execution

```go
func BenchmarkComplexQuery(b *testing.B) {
    for i := 0; i < b.N; i++ {
        executor.Execute(complexQuery)
    }
}
```

#### 5. Fuzz testing with random queries and data

Generate random valid queries and verify no panics or crashes.

---

### Testing Query Optimizations

**CRITICAL**: Query optimizations are NOT tested the same way as regular features.

When implementing or modifying query optimizations (CSE, decorrelation, predicate pushdown, etc.), you MUST test:

#### 1. Semantic Preservation
Optimization doesn't change query meaning:

```go
// Test that optimized query returns same results as unoptimized
resultOptimized := execWithOptimization.Execute(query)
resultUnoptimized := execWithoutOptimization.Execute(query)
assert.Equal(resultOptimized, resultUnoptimized)
```

#### 2. Structural Invariants
Query structure is preserved correctly:

```go
// Test internal structure using annotations
event := captureAnnotation("aggregation/executed")
assert.Equal(0, event.Data["groupby_count"])  // Pure agg stays pure
```

#### 3. Category Distinctions
Different types are treated differently:

```go
// Pure aggregations should NOT be optimized the same as grouped
if isPureAggregation(query) {
    assert.False(wasDecorrelated(plan))
}
```

#### 4. Realistic Data Sizes
Test with production-scale data:

```go
// Use 100s-1000s of tuples, not just 2-5
datoms := generateLargeDataset(1000)
// Complex queries that stress optimization logic
```

#### 5. Failure Modes
Test that optimization doesn't break edge cases:

```go
// Test with nil values, empty relations, single tuples
// Verify no panics, no data corruption
```

---

### Why This Matters

**The decorrelation bug** existed because tests only verified:
- ✅ "Does it return the right answer?" (outcome)
- ❌ "Did optimization preserve query semantics?" (structure)
- ❌ "Are internal transformations correct?" (invariants)

**Lesson**: Test transformations at the structural level, not just outcomes.

---

### Use Annotations for Testing Optimizations

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

### Property-Based Testing for Optimizations

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

### Example: The Merge Join Bug

The merge join implementation appeared correct but had a binding advancement bug. Only comprehensive testing revealed it.

**Tests found the bug, not "careful review".**

This proves: Implementation without tests = incomplete implementation.

---

### Testing Checklist

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

---

## Bug Fixes and Learnings

This document catalogs critical bugs that have been fixed and the patterns that lead to them. Understanding these patterns prevents repeating the same mistakes.

**Convention — entries are historical learnings, not a live bug list.** Every
entry here describes a bug that has been *fixed* unless it carries an explicit
`**Status**: Open` marker. Several entries are written in the present tense
describing the *original defect* ("X does not check Y") — that is the problem
statement at the time of discovery, **not** a claim about the current code. Do
not infer current state from an entry's prose or tense. Before treating anything
here as a live bug, re-read the cited code; if you fix or confirm an entry, add
or update its `**Status**:` line (with date and commit) so the next reader
doesn't have to re-derive it. A stale "this is broken" note manufactures phantom
bugs just as easily as a missing note hides real ones.

---

### Storage Layer Integration (2025-06-21)

1. **Attribute Encoding Bug**: Pattern matcher was passing raw keyword bytes (10 bytes) to EncodePrefix which expected 20-byte arrays. Fixed by converting to storage format first.

2. **Identity Decoding Bug**: Entity IDs were being decoded with `NewIdentity(sd.E.String())` which created new identities with different hashes. Fixed by using `NewIdentityFromHash(sd.E)`.

3. **Value Type Preservation**: Identity values stored as references must decode back to the same Identity for joins to work. Fixed ValueFromBytes to properly reconstruct Identity from hash.

**Pattern**: Type mismatches between storage and query layers. Always verify type conversions preserve semantics, especially for identity/hashing.

---

### Expression Clauses (2025-06-21)

1. **Smart Joining**: Expression clauses that need variables from multiple relations now automatically join those relations first.

2. **Type Handling**: Proper type conversions for arithmetic operations between int64, int, and float64.

3. **Evaluation Order**: Fixed critical bug where predicates were evaluated before expressions in phases. Expressions must be evaluated first so predicates can use their output symbols.

**Pattern**: Execution order matters. If operation B depends on output of operation A, A must execute first.

---

### Query Optimization (2025-06-23)

1. **Predicate Pushdown**: Intra-phase predicates are now applied immediately after their required symbols are available, reducing intermediate result sizes.

2. **Equi-Join Optimization**: Equality predicates between phases are detected and pushed into join conditions, avoiding massive cross-products.

3. **Performance Annotations**: Fixed 150x slowdown caused by unnecessary unique value counting in annotation code.

**Pattern**: Premature optimization is real. Profile first, optimize second. The annotation code seemed harmless but was doing expensive counting on every call.

---

### Aggregation Scoping (2025-06-23)

1. **Issue Identified**: Current aggregation model (following Datomic) creates Cartesian products when mixing aggregated and non-aggregated values from different scopes.

2. **Solution Designed**: Datomic-style subqueries provide clean scoping for aggregations without breaking compatibility.

3. **Implementation Plan**: Created detailed plan in `docs/archive/completed/subquery-implementation-plan.md` for 3-phase implementation approach.

**Pattern**: Design before implementation. Complex features need detailed planning to avoid architectural mistakes.

---

### Subquery Implementation (2025-06-23)

1. **Completed**: Full implementation of Datomic-style subqueries with TupleBinding and RelationBinding

2. **Bug Fixed**: Input variables are now properly available during predicate evaluation in subqueries

3. **Solves Aggregation Bug**: Subqueries properly scope aggregations, solving the Cartesian product issue

4. **Demo Added**: `examples/subqueries.go` demonstrates subquery patterns including aggregation

**Pattern**: Test with real-world use cases. The OHLC demo proved the implementation works for production scenarios.

---

### Result/Relation Unification (2025-06-23)

1. **Design Fix**: Unified redundant Result and Relation types

2. **Result as Alias**: Result is now a type alias for MaterializedRelation

3. **Consistent API**: All query execution returns Relation interface

4. **Table Formatter**: Added table formatting utilities for debugging Relations

**Pattern**: Eliminate redundancy. Two types doing the same thing creates confusion and maintenance burden.

---

### Order-By Implementation (2025-06-24)

1. **Parser Support**: Added parsing for `:order-by` clause with `[?var :asc/:desc]` syntax

2. **Executor Implementation**: Added sorting after query execution with type-aware comparison

3. **Multi-symbol Sorting**: Supports multiple sort keys with independent directions

4. **Type-aware Comparison**: Properly handles all value types including time.Time

**Pattern**: Type-aware operations throughout. Don't assume all values are strings or numbers.

---

### Time Comparison Fix (2025-06-24)

1. **Bug Identified**: `compareValues` function didn't handle time.Time, causing string comparison

2. **Root Cause**: Times were being compared lexicographically ("2025-06-17" > "2025-06-20")

3. **Fix Applied**: Added time.Time case to compareValues using Before()/After() methods

4. **Impact**: Fixed `min`/`max` aggregations and all comparison predicates for time values

**Pattern**: Missing type case in switch. Always have a default case that errors on unknown types rather than falling through to wrong behavior.

---

### Table Formatter Enhancement (2025-06-24)

1. **Markdown Output**: Replaced ASCII tables with clean markdown using tablewriter library

2. **Header Preservation**: Disabled auto-formatting to preserve exact variable names (e.g., ?var)

3. **Relation Methods**: Added String() and Table() methods to Relation interface

4. **Colored Output**: String() method includes ANSI colors matching annotation format

**Pattern**: Debug output quality matters. Good formatting makes debugging 10x faster.

---

### RelationInput and Subquery Iteration (2025-08-26)

**Problem**: Subqueries were executing sequentially for each input combination (870 times for OHLC queries)

**Solution Implemented**: Proper RelationInput iteration semantics
- `:in $ [[?x ?y] ...]` now iterates over each tuple
- Query executes once per tuple with correct aggregation scoping
- Semantically correct (each tuple processed independently)

**Current Status**:
- ✅ Correct semantics - aggregations compute per-tuple not globally
- ⏳ Still sequential execution (performance optimization needed)
- See `docs/archive/2025-10/SUBQUERY_PERFORMANCE_ANALYSIS.md` for full details

**Key Insight**: Datalog is not SQL - no implicit GROUP BY. Getting the semantics right is more important than speed.

**Pattern**: Correctness first, performance second. Don't optimize prematurely if it breaks semantics.

---

### Bindings to Relations Migration (2025-06-26)

1. **Motivation**: Simple Bindings (map[Symbol]interface{}) couldn't support multi-value variable bindings needed for batch operations

2. **Migration**: Replaced Bindings with Relations throughout the codebase - this was the RIGHT decision!

3. **Performance Work**: Iterator reuse and batch scanning optimizations explored
   - Batch scanning implemented with threshold-based activation (>100 tuples)
   - SimpleBatchScanner used for large binding sets
   - Benchmarks show code clarity benefits, modest performance impact
   - See `PERFORMANCE_STATUS.md` for current state

**Pattern**: When existing abstractions can't handle new requirements, replace them entirely rather than patching.

---

### Decorrelation Pure Aggregation Bug (2025-10-10)

**Critical Correctness Bug**: Multiple pure aggregation subqueries returned `nil` values instead of correct aggregates.

**Root Cause**: The decorrelation optimization made a **category error** - it treated all aggregations the same:
- Pure aggregations: `[:find (max ?x)]` → Single global aggregate
- Grouped aggregations: `[:find ?group (max ?x)]` → Aggregate per group

Adding input parameters to pure aggregations **changed their type** from single to grouped, breaking semantics.

**The Fix**: Modified `extractCorrelationSignature()` to distinguish pure vs grouped aggregations. Only grouped aggregations are decorrelated.

**Why Tests Missed It**:
1. **Tested outcomes, not structure** - Verified result values but not find clause structure
2. **Simple data masked the problem** - Small test data (2-5 tuples) still produced correct-looking results by accident
3. **No structural invariants** - Didn't verify aggregation type preservation
4. **Missing annotations** - Couldn't observe internal transformations

**Lessons Learned**:
1. **Optimizations must preserve semantics** - Test that transformations don't change query meaning
2. **Use realistic data sizes** - Test with 100s-1000s of tuples, not just 2-5
3. **Test internal structure** - Use annotations to verify intermediate transformations
4. **Category distinctions matter** - Pure vs grouped aggregations are fundamentally different
5. **Annotations catch root causes** - They revealed wrong find clause structure, not just nil symptoms

**See**: `docs/bugs/resolved/DECORRELATION_BUG_FIX.md` for full details.

**Pattern**: Optimizations must preserve query semantics. Test transformations at the structural level, not just outcomes.

---

### Input Parameter Semantics (2025-10-13)

**Three Related Bugs** revealed the importance of correctly handling input parameters from `:in` clauses.

**Key Insight**: Input parameters are "environment" symbols (Available) not "data" symbols (Provides). They're metadata ABOUT query execution, not data IN the result.

**The Three-Level Type System**:
1. **Input Parameters**: Environment symbols available in ALL phases for filtering/correlation
2. **Pattern Variables**: Computation symbols that flow between phases via joins
3. **Relation Symbols**: Actual data in phase output relations

**Critical Invariants**:
```
Available = Environment symbols (inputs + previous outputs)
Provides = Relation symbols (what this phase produces)
Keep ⊆ Provides ∩ Available  (can only keep what's in the relation)
```

**Bugs Fixed**:

1. **INPUT_PARAMETER_KEEP_BUG** (Oct 12): Phase symbol calculation incorrectly added input parameters to Keep even though they weren't in the relation, causing projection errors. Fixed by checking Keep ⊆ Provides ∩ Available.

2. **BUG_PARAMETERIZED_QUERY_CARTESIAN_PRODUCT** (Oct 13): Selectivity scoring treated input parameters as unbound variables (+5 score) instead of bound like constants (-500 score), causing wrong phase ordering. Fixed by passing availableSymbols to scorePattern().

3. **BUG_STRING_PREDICATES_CANT_USE_PARAMETERS** (Oct 13): Predicate assignment only made input parameters available in phase 0, not subsequent phases, causing "predicates could not be assigned" panic. Fixed by using phases[i].Available which includes inputs for all phases.

**Analogy**: Input parameters are like SQL prepared statement parameters - they filter data but don't appear as result symbols:
```sql
-- ?symbol filters but isn't in output
SELECT time, close FROM prices WHERE symbol = ?
```

**See**: `docs/INPUT_PARAMETER_SEMANTICS.md` for comprehensive guide with examples and testing patterns.

**Pattern**: Understand the type system. Input parameters, pattern variables, and relation symbols are fundamentally different types with different semantics.

---

### Expression-Only Phases Bug (2025-10-14)

**Critical Bug**: Phases containing only expressions (no patterns) received empty relations instead of previous phase's results, causing zero-tuple outputs.

**Root Cause**: In `executor_sequential.go`, the phase execution logic built up `independentGroups` through pattern matching. When a phase had zero patterns, the pattern loop never executed, leaving `independentGroups` empty. This empty slice was then passed to `applyExpressionsAndPredicates()` instead of the previous phase's results.

**Symptoms**:
- Phase completes with 0 tuples when it should have N tuples
- No `expression/begin` or `expression/complete` annotations in logs
- Conditional aggregate rewriting returns empty results

**The Fix**:
```go
// If phase has no patterns, use availableRelations (results from previous phase)
collapsed := independentGroups
if len(phase.Patterns) == 0 && len(collapsed) == 0 {
    collapsed = availableRelations
}
```

**Detection Method**: Added expression annotations and searched for `grep "expression/"`. Finding zero annotations revealed expressions weren't executing at all.

**Key Lesson**: **Absence of expected annotations is as important as presence of error annotations.** If you expect certain events but see none, investigate immediately.

**General Pattern - "Pure-Type Phases"**: Any phase containing only ONE type of operation (patterns, expressions, predicates, subqueries) is vulnerable to this bug class. Always test:
- Pure pattern phases (no expressions/predicates)
- Pure expression phases (no patterns) ← **This bug**
- Pure predicate phases (no patterns/expressions)
- Empty phases (should probably error)

**Phase Execution Invariants**:
1. **Input Invariant**: Every phase receives either `nil` (first phase, no inputs) or previous phase's `Keep` symbols
2. **Output Invariant**: Every phase produces a Relation with symbols matching `phase.Provides` (or subset in `Keep`)
3. **Data Flow Invariant**: If Phase N produces K tuples with symbols S, and Phase N+1 needs S' ⊆ S, then Phase N+1 receives K tuples with S' available
4. **Composition Invariant**: Patterns, expressions, predicates, subqueries can appear in any combination (including zero), and phase execution MUST handle all combinations

**See**: `docs/bugs/resolved/BUG_EXPRESSION_ONLY_PHASES.md` for detailed analysis and debugging guide.

**Pattern**: Don't assume phases always have certain components. Test all combinations including edge cases.

---

### Decorrelation Inside Union Branches (2026-03-21)

**Critical Semantic Bug**: The algebra bridge's decorrelation pass transformed correlated subqueries inside Union branches into uncorrelated ones, producing Cartesian products.

**Root Cause**: Decorrelation moves the correlation variable from `:in` (input) to `:find` (output). Inside a Union, the other branch (ground default) doesn't have this variable. The Union produces branches with incompatible schemas. When joined with the outer relation, the ground branch rows cross-product because they lack the join key.

**The Fix**: The decorrelation transform checks `ctx.Parent.Rule == RuleUnion` using the EBNF transform framework's `TransformContext.Parent`. LateralJoins inside Union nodes are not decorrelated — their per-tuple correlation is semantically load-bearing.

**Key Insight**: Algebraic equivalence rules have structural preconditions. The decorrelation rule `R ⋈_L S(r.x) → R ⋈ (S GROUP BY x)` requires that the LateralJoin result is consumed directly by a Join. Inside a Union, the result is unioned with other branches first — a different structure the rule doesn't cover.

**Pattern**: Optimization rules are not universally applicable. Always verify the structural context matches the rule's preconditions. A rule that's valid at the top level may be invalid inside a compound operator.

---

### Meta-Patterns Across All Bugs

#### 1. Type Mismatches Kill
- Storage vs query types (Attribute Encoding Bug)
- Input parameters vs pattern variables (Input Parameter Bugs)
- Pure vs grouped aggregations (Decorrelation Bug)

**Rule**: Make types explicit and enforce distinctions.

#### 2. Execution Order Matters
- Expressions before predicates (Expression Clauses)
- Predicates as soon as symbols available (Predicate Pushdown)
- Pattern execution before expressions (Expression-Only Phases)

**Rule**: Define and enforce dependency ordering.

#### 3. Test Structure, Not Just Outcomes
- Decorrelation bug passed value tests, failed structure tests
- Annotations reveal what's happening, not just results
- Small test data masks category errors

**Rule**: Use annotations to verify internal transformations.

#### 4. Edge Cases Are Real Cases
- Expression-only phases (Expression-Only Phases Bug)
- Empty relations (multiple bugs)
- Single tuple inputs (Decorrelation Bug)

**Rule**: Test all combinations, especially the weird ones.

#### 5. Correctness Before Performance
- RelationInput semantics over speed
- Proper aggregation scoping over optimization
- Type preservation over clever encoding

**Rule**: Make it right, then make it fast.

---

### Streaming Architecture Violation (2026-02-05)

**Status**: ✅ RESOLVED. The buffering approach described below was abandoned;
`copyDatom` no longer exists. The current `CRDTResolvingIterator`
(`datalog/storage/crdt_resolving_iterator.go`) buffers *state*, not datoms —
CardinalityOne emits the first entry and skips the rest, CardinalityMany tracks
per-value add/remove Lamports, CardinalityVector accumulates minimal RGA element
state. This entry is retained as a *learning*; it does **not** describe the
current iterator, which follows the prescribed approach below.

**Critical Architecture Bug**: Created a buffering "CRDT resolution" layer that defeated the entire streaming architecture.

**What I Did Wrong**:
1. Created `CRDTResolvingIterator` that buffers ALL datoms for an (E, A) group
2. Added `copyDatom()` function to copy iterator results into a buffer
3. Built complex "resolution" logic (resolveLWW, resolveAddWins, resolveRGA)
4. Completely ignored that the storage layer already solves this problem

**Why It Was Wrong**:
The EATV index stores Tx with **bitwise NOT for descending order**. This means:
- First entry for each (E, A) IS the LWW winner
- No resolution logic needed for CardinalityOne
- Just skip subsequent entries with same (E, A)

The comments in `key_encoder_binary.go` say it explicitly:
```go
// EATV: [prefix][E][A][Tx↓][type][value][Op][AfterRef?] - first entry is current
// Tx is encoded with bitwise NOT for descending sort order (highest Tx first)
```

**Red Flags I Should Have Noticed**:
1. Creating a `copy*` function for iterator results → buffering smell
2. Accumulating datoms in a slice → materialization smell
3. Building "resolution" logic → the index already does this
4. Adding complexity to "solve" something → should have asked why it's needed

**The Correct Approach for CardinalityOne**:
```go
// Track current (E, A), skip duplicates
// First entry wins because EATV orders Tx descending
// Pure filtering, zero buffering
```

**The Correct Approach for CardinalityMany**:
```go
// Track state per value: map[valueKey]{highestAdd, highestRemove}
// When (E, A) changes, emit values where add >= remove
// Buffer state, not datoms
```

**Pattern**: The storage layer index ordering IS the CRDT resolution. If you think you need resolution logic, you don't understand the storage layer. READ THE INDEX COMMENTS.

**Meta-Pattern**: If you're buffering iterator results, you're breaking streaming. STOP and ask.

---

### Cache Path CardinalityOne Tombstone Gap (2026-02-08)

**Status**: ✅ FIXED (2026-02-08, commit `816b535`). `ResolveLWW` now checks
`datom.Op`: when the highest-Tx entry is a Remove it returns a nil value with
the tombstone's ElementID, so the attribute correctly reads as absent
(`datalog/storage/cache_resolver.go`, the `OpCRDTRemove` check). The text below
is retained as a *learning*, not an open bug — do not report it as live without
re-reading `ResolveLWW`.

**Original Bug (now fixed)**: `ResolveLWW` (cache path) did **not** check
`datom.Op`, so Remove() tombstones on CardinalityOne attributes were invisible
to PullInto and multi-clause join-bound queries. The streaming path
(`CRDTResolvingIterator`) always handled it correctly.

**What I Did Wrong**:
1. Wrote `ResolveLWW` without checking `datom.Op` — returns first datom's V blindly
2. Wrote 13 Remove tests that ALL bind E via `:in` parameters — streaming path only
3. Never tested Remove() through PullInto or multi-clause join-bound E — cache path
4. Reported "all tests pass" with zero coverage of the buggy code path

**Why Tests Missed It**:
The tests looked comprehensive: round-trip, overwrite, re-add, V-irrelevant, multi-entity, bound query, V-bound query, unbound query. But every test used `[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`, which goes through `CRDTResolvingIterator` (streaming), never through `ResolveLWW` (cache). 13 passing tests, zero coverage of the bug.

**The Trust Problem**:
Claude wrote the buggy code, wrote tests that don't cover it, and shipped it as complete. The user cannot trust "all tests pass" as evidence of correctness. This bug was only found because the user built a real application on top and hit it in production use.

**See**: `docs/bugs/resolved/BUG_CACHE_CARDINALIY_ONE_TOMBSTONE.md` for full analysis and reproducer.

**Root Cause Mental Model Error**: Claude thinks of datoms as values that replace each other — "Set name to Bob overwrites Alice." This is wrong. Storage is append-only. Both datoms exist. Resolution reads the first entry (highest Tx) and interprets the **operation**. If the operation is a tombstone, the attribute doesn't exist. The word "overwrite" encodes a mutable-storage mental model that directly caused this bug: ResolveLWW returns the first entry's V without checking its Op, because in the "overwrite" model there's nothing to check.

**Correct model**: Datoms are operation records. Resolution interprets operations. Every code path that reads a datom must check Op. No exceptions.

**Pattern**: When multiple code paths resolve the same semantic operation, tests must cover ALL paths. Test quantity and scenario variety mean nothing if they all exercise the same code path.

**Meta-Pattern**: Claude does not reliably catch its own coverage gaps. Apparent test thoroughness (many tests, many scenarios) creates false confidence when all tests go through the same path.

---

#### 6. Understand Before Implementing
- Read code AND understand what it means
- Index ordering has semantic meaning (descending Tx = first wins)
- Don't treat features as problems to solve without understanding existing design

**Rule**: If you read comments explaining WHY something works a certain way, actually think about the implications.

---

## Debugging Query Execution Issues

When query execution produces unexpected results, use this systematic approach.

---

### 1. Check Annotations First

Annotations reveal execution flow better than printf debugging:

```bash
# Run test and capture all annotations
go test -v ./tests -run YourTest 2>&1 | tee test.log

# Look for phase boundaries
grep "phase/" test.log

# Look for missing operation types
grep "pattern/" test.log    # Should see if patterns executed
grep "expression/" test.log  # Should see if expressions executed
grep "join/" test.log       # Should see if joins happened
```

**Key insight**: Missing annotations are bug symptoms. If a phase should evaluate expressions but you see no `expression/` events, the expressions aren't executing.

---

### 2. Examine Phase Structure

Test output shows phase composition:

```
Phase 2:
  Patterns: 3
  Expressions: 0
  Subqueries: 1
  Available: [?p ?name]
  Provides: [?e ?time ?v]
  Keep: [?name ?time ?v]
```

**Questions to ask**:
- Does any phase have all counts at zero?
- Does a phase with expressions show no `expression/` annotations?
- Does `Provides` match what the phase actually produces?
- Does `Keep` include symbols needed by later phases?
- Is there a gap in symbol flow? (Phase N provides `?x`, Phase N+2 needs `?x`, but Phase N+1 doesn't Keep it)

---

### 3. Check Data Flow Between Phases

Phases are a pipeline. Data must flow correctly:

```
Phase 1: 10 tuples, Provides [?x ?y]    Keep [?x]
Phase 2: 0 tuples  ← BUG! Where did the 10 tuples go?
```

**Common issues**:
- Empty join (no overlapping symbols) creates empty result
- Missing Keep symbols needed by later phases
- Expression-only phase receives empty relations (this bug!)
- Predicate filters out all tuples

**Use annotations to trace**:
```bash
# See tuple counts at phase boundaries
grep "phase/complete" test.log
# Output: phase/complete - map[phase:Phase 2 success:true tuple.count:0]
```

---

### 4. Add Targeted Debug Output

When annotations aren't enough, add debug output at critical junctions:

```go
// At phase boundaries
fmt.Printf("DEBUG Phase %d: patterns=%d, expressions=%d, " +
    "collapsed=%d tuples, available=%d tuples\n",
    phaseIndex, len(phase.Patterns), len(phase.Expressions),
    len(collapsed), len(availableRelations))

// Before/after key operations
fmt.Printf("DEBUG before expression eval: size=%d, cols=%v\n",
    group.Size(), group.Symbols())
```

**Strategic locations**:
- Start of `executePhaseSequential()` - what does this phase receive?
- Before `applyExpressionsAndPredicates()` - what relations are passed?
- After each join - did the join reduce or amplify tuples?
- After predicates - how many tuples filtered out?

---

### 5. Test Assumptions About Phase Composition

Don't assume phases always have certain operations. Test edge cases:

```go
// This can create expression-only phases:
// - Conditional aggregate rewriting
// - Phase reordering
// - CSE optimization

// Always test:
if len(phase.Patterns) == 0 {
    // Do we handle expression-only phases correctly?
}
```

---

### 6. Verify Invariants

At each phase boundary, these MUST be true:

```go
// Input invariant
if phaseIndex > 0 {
    assert(previousResult != nil, "Phase receives nil from previous phase")
}

// Output invariant
assert(result != nil, "Phase returns nil result")
assert(result.Symbols() matches phase.Provides or phase.Keep)

// Data flow invariant
if phase.Available includes ?x {
    assert(previousResult.Symbols() includes ?x OR ?x is input parameter)
}
```

---

### Common Bug Patterns

#### 1. Zero tuples from non-empty input
- Check: Failed join (no shared symbols)
- Check: Predicate filtered everything
- Check: Expression-only phase got empty relations ← **This bug**

#### 2. Missing symbols in later phases
- Check: Previous phase didn't Keep required symbols
- Check: Phase reordering broke symbol flow
- Check: Input parameters added to Keep but not in Provides

#### 3. Cartesian product explosion
- Check: Disjoint relations joined without shared symbols
- Check: Missing predicates to connect patterns
- Check: Input parameters treated as unbound

#### 4. Wrong aggregation results
- Check: Grouping variables vs input parameters
- Check: Pure vs grouped aggregation distinction
- Check: Conditional aggregate rewriting correctness

---

### Using Annotations for Root Cause Analysis

Annotations show execution flow, not just outcomes:

```bash
# Example: Why did aggregation return nil?

# WRONG approach: Only check outcome
grep "Result:" test.log  # Shows: Result: nil

# RIGHT approach: Trace the transformation
grep "aggregation/executed" test.log
# Shows: groupby_count:0 find_elements:[?person (max ?val)]
# → AHA! Grouped aggregation was changed to pure (0 groupby vars)
# → Root cause: Decorrelation added input params to find clause
```

**Annotation workflow**:
1. Identify unexpected outcome (wrong count, nil value, etc.)
2. Find annotation for that operation (`aggregation/executed`, `join/hash`, etc.)
3. Examine annotation data for unexpected values
4. Trace backwards: what created those values?
5. Use annotations from earlier phases to find transformation point

---

### Testing Strategy for Phase Execution

Every modification to phase execution should test:

```go
func TestPureExpressionPhase(t *testing.T) {
    // Phase with zero patterns, only expressions
}

func TestPurePatternPhase(t *testing.T) {
    // Phase with zero expressions, only patterns
}

func TestEmptyPhase(t *testing.T) {
    // Edge case: should probably error
}
```

**Why**: Optimizations and rewriting can create unexpected phase compositions. If your code assumes "phases always have patterns", it will break.

---

### Debugging Workflow Summary

**When a query fails:**

1. ✅ **Check annotations first** - grep for expected events
2. ✅ **Examine phase structure** - Look at counts and symbol flow
3. ✅ **Trace data flow** - Follow tuples through pipeline
4. ✅ **Add targeted debug** - At critical boundaries
5. ✅ **Test assumptions** - Edge cases and phase composition
6. ✅ **Verify invariants** - Input/output/flow guarantees

**Never:**
- ❌ Skip annotation analysis
- ❌ Assume phase structure
- ❌ Add random debug statements everywhere
- ❌ Change code without understanding root cause

**Always:**
- ✅ Use annotations to understand what's happening
- ✅ Test all phase compositions including edge cases
- ✅ Verify invariants at boundaries
- ✅ Root cause first, fix second
