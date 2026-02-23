# Janus Datalog: Architectural Philosophy and Design Patterns

This document captures the architectural insights and design philosophy behind Janus Datalog, complementing the [KEY_ENCODING_AND_CRDT.md](KEY_ENCODING_AND_CRDT.md) analysis of the storage layer.

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Query Execution: Not Quite Volcano](#query-execution-not-quite-volcano)
3. [The Planner: Dependency Resolution with Selectivity Heuristics](#the-planner-dependency-resolution-with-selectivity-heuristics)
4. [Streaming Architecture: Deferred Materialization](#streaming-architecture-deferred-materialization)
5. [The Pace Car Pattern](#the-pace-car-pattern)
6. [Proposals as a Storeroom](#proposals-as-a-storeroom)
7. [The CRDT Origin Story](#the-crdt-origin-story)
8. [The Simplification Pattern](#the-simplification-pattern)
9. [Summary: What Makes This Different](#summary-what-makes-this-different)

---

## Executive Summary

Janus Datalog's architecture reflects a consistent philosophy: **find the representation that makes complexity unnecessary**.

Most software adds abstraction layers to manage complexity. Janus removes complexity by finding the right foundation, making abstractions unnecessary:

- **Storage**: The key encoding IS the CRDT resolution (no separate layer)
- **History**: Same index, keep scanning (no separate segment)
- **Join ordering**: Runtime schema inspection (no cost-based optimizer)
- **Streaming**: First consumer builds cache, others benefit (invisible optimization)

The result: a system that's simpler AND faster than conventional approaches.

---

## Query Execution: Not Quite Volcano

### Classic Volcano Model

Traditional database query engines use the Volcano model (Graefe, 1990):
- Fixed operator tree determined at plan time
- Static pipeline: Scan → Filter → Join → Project
- Each operator pulls from its children
- Plan is "compiled" before execution

### What Janus Does Instead

Janus uses **schema-driven dataflow** with Volcano-style iterators:

```
Phase 1:
  Pattern [?p :person/name ?name] → Relation A {?p, ?name}
  Pattern [?p :person/email ?email] → Relation B {?p, ?email}
  Collapse([A, B]) → sees shared ?p → joins them → Relation C

Phase 2:
  Pattern [?p :person/dept ?d] with bindings from C
  Collapse() → joins on ?p

  Expression [(str ?name " works in " ?d) ?summary]
  → adds symbol, might bridge disjoint groups
```

### The Key Differences

| Volcano | Janus |
|---------|-------|
| Plan tree fixed at compile time | Join order determined at runtime by `Collapse()` |
| Operators statically linked | Relations are values, combined based on symbols |
| Single pipeline | Multiple disjoint groups that might merge later |
| Plan knows the shape | Shape emerges from what symbols exist |

### Dynamic Bridging

Expressions can create join symbols that bridge previously disjoint groups:

```
Group 1: {?x, ?y}     Group 2: {?a, ?b}
   ↓                     ↓
Expression: [(f ?y) ?z]   Expression: [(g ?b) ?z]
   ↓                     ↓
Group 1: {?x, ?y, ?z}  Group 2: {?a, ?b, ?z}
   ↓
   └──────── NOW they share ?z ────────┘
                    ↓
              Collapse() joins them
```

This isn't Volcano. Volcano doesn't rewire the plan based on what symbols emerge from expressions.

**The characterization:** Schema-driven dataflow with Volcano-style iterators.

---

## The Planner: Dependency Resolution with Selectivity Heuristics

### What the Planner Does NOT Do

- Cost-based optimization
- Statistics collection
- Join order selection
- Cardinality estimation

### What the Planner DOES Do

**1. Groups clauses into phases based on symbol dependencies:**

```
[?p :person/name ?name]      ← needs nothing, provides ?p, ?name
[?p :person/dept ?d]         ← needs ?p, provides ?d
[(> (count ?d) 5)]           ← needs ?d (filter)
[?d :dept/budget ?budget]    ← needs ?d, provides ?budget
```

**2. Orders clauses within phases by selectivity heuristics:**

| Clause Type | Score | Why |
|-------------|-------|-----|
| Pattern with 2 constants | 300 | Most selective - filters most data |
| Pattern with 1 constant + 1 bound var | 210 | Good selectivity + join hint |
| Pattern with 1 constant | 200 | Some filtering |
| Pattern with 2 bound vars | 120 | Join only, no filtering |
| OR clause | 80 | Data source but less predictable |
| Expression producing symbol | 10 | Computed symbol |
| Filter predicate | 5 | Just filters |
| NOT clause | 2 | Filters last |

**3. Computes which symbols to keep between phases**

### The Division of Labor

| Responsibility | Planner | Runtime Collapse() |
|----------------|---------|-------------------|
| **When** | Before execution | During execution |
| **Decides** | Which clauses can execute together | How to combine results |
| **Based on** | Symbol dependencies (static) | Actual symbol overlap (dynamic) |
| **Join order** | No | Yes |
| **Selectivity** | Orders clauses within phase | N/A |

### Why This Design?

Traditional query planners do more work at plan time. Janus deliberately defers join ordering to runtime, which means:
- Less optimization opportunity (no cost-based planning)
- More flexibility (handles dynamic schemas, expressions that create join symbols)
- Simpler planner (just dependency analysis + selectivity ordering)

**The trade-off:** Simplicity over optimization. The planner ensures you start with small result sets. `Collapse()` handles the mechanical joining.

---

## Streaming Architecture: Deferred Materialization

### The Core Principle

Materialization is **deferred and minimal**. The system:
1. Prefers streaming (symmetric hash join for stream-to-stream)
2. When forced to materialize, picks the smaller side
3. Probe side always streams
4. Final results can still stream to the caller

### When Materialization Happens

| Operation | What Gets Materialized | Why |
|-----------|----------------------|-----|
| Hash join (build side) | One relation | Need random access for hash table |
| Hash join (probe side) | Nothing | Can stream through |
| Symmetric hash join | Neither fully | Both sides build incrementally |
| Aggregation | All input | Need to see all groups |
| Sort | All input | Need to compare all tuples |
| Re-iteration | Caches on first pass | Can't rewind a stream |

### The Decision Flow

```
Pattern match → StreamingRelation (lazy)
      ↓
   Join?
      ↓
   ┌─────────────────────────────────────────────┐
   │ One side streaming, one materialized?       │
   │   → Use materialized as build, stream probe │
   │                                             │
   │ Both streaming?                             │
   │   → Symmetric hash join (both stay lazy)    │
   │   → OR materialize smaller one              │
   │                                             │
   │ Both materialized?                          │
   │   → Use smaller as build                    │
   └─────────────────────────────────────────────┘
      ↓
   Result: StreamingRelation (still lazy!)
```

### The Failure Modes Are Brutal

```go
// CRITICAL: Don't call IsEmpty() - it consumes streaming iterators!

// CRITICAL: Check if build relation was already consumed
if alreadyConsumed {
    panic("BUG: HashJoin received a StreamingRelation that was already consumed...")
}

// Check for illegal double-iteration without materialization
if r.iteratorCalled && !r.shouldCache {
    panic("StreamingRelation.Iterator() called multiple times without Materialize()...")
}
```

| Mistake | Consequence |
|---------|-------------|
| Call `Size()` on streaming relation | Forces materialization, may lose data |
| Call `IsEmpty()` to check | Consumes first tuple, silently wrong results |
| Iterate twice without cache | Panic (if caught) or silent data loss |
| Forget to copy tuple from reused workspace | Data corruption on next iteration |

The panics are *defensive* - better to crash than return wrong data.

---

## The Pace Car Pattern

### Current Implementation: Block Until Complete

```go
r.mu.Lock()
if r.cacheReady {
    r.mu.Unlock()
    return &sliceIterator{tuples: r.cache}  // Safe: cache complete
}
if r.cachingInProgress {
    completeChan := r.cacheComplete  // Capture before unlock!
    r.mu.Unlock()
    <-completeChan  // BLOCK until first iteration finishes
    return &sliceIterator{tuples: r.cache}
}
```

**What this does:**

```
Thread A (first):              Thread B (concurrent):
─────────────────              ──────────────────────
Iterator()
  → set cachingInProgress=true
  return CachingIterator       Iterator()
                                 cachingInProgress=true!
for it.Next() {                  <-completeChan  // BLOCKED
  // Each Next() appends        // ...waiting...
  // to cache                   // ...waiting...
}                               // ...waiting...

it.Close()
  cacheReady = true
  close(completeChan) ─────────→ // UNBLOCKED!
                                 return &sliceIterator{cache}
```

**The insight:** First consumer **does the work**, everyone else **waits for the free ride**.

- First caller iterates the raw stream, building cache as a side effect
- Concurrent callers block on a channel
- When first caller finishes, channel closes
- Everyone else gets the cached slice

**Why this design:**

| Alternative | Problem |
|-------------|---------|
| Materialize eagerly | Defeats streaming - pays cost even if never needed |
| Let everyone iterate raw | Can't - iterator is single-use |
| Copy iterator state | Expensive, complex, error-prone |
| Return error to concurrent callers | Bad UX - caller didn't do anything wrong |

The first consumer's iteration IS the materialization. Zero additional overhead for memoization.

### Future: Pipeline Parallelism (Proposed)

The [PACE_CAR_CACHING_ARCHITECTURE.md](../proposals/PACE_CAR_CACHING_ARCHITECTURE.md) proposal extends this:

**Current (Block Until Complete):**
```
Consumer 1 (pace car):  [====ITERATING====]
Consumer 2 (follower):  [BLOCKED..........][INSTANT REPLAY]
                                           ↑
                              Waits for COMPLETE cache
```

**Proposed (Follow Along):**
```
Consumer 1 (pace car):  [====ITERATING====]
Consumer 2 (follower):  [==CHASING========]
                         ↑
              Reads chunks AS THEY'RE BUILT
```

This would enable:
- Pipeline parallelism (phases overlap)
- Lower latency (first results available immediately)
- Bounded memory (grows with speed differential, not dataset size)

---

## Proposals as a Storeroom

### The Pattern

The repository contains 16 proposals covering:
- Pipeline parallelism (Pace Car)
- Schema selectivity hints
- Prepared queries
- Recursive rules
- Distributed Janus
- CRDT composable toolkit
- And more...

**But the current system is at ~85% production readiness with the simple versions.**

### The Philosophy

```
"Queries are slow when phases can't overlap"

Author: *reaches for shelf*
        "Ah yes, Pace Car Caching Architecture.
         I wrote that up months ago.
         Chunked signaling, Clojure-style.
         Here's the implementation plan,
         the risks, the alternatives I rejected,
         and why."
```

**The approach:**

1. **Build correct foundation first** - Get semantics right
2. **Document the vision** - Proposals show where it COULD go
3. **Defer complexity until proven necessary** - "We could do X, but let's see if we need it"
4. **Measure before optimizing** - Focus on proven bottlenecks

The proposals are a **design inventory**, not a TODO list. They're ready when needed, but not built until needed.

This is **premature documentation** instead of premature optimization - write down the sophisticated version so you don't forget it, but don't build it until the simple version proves insufficient.

---

## The CRDT Origin Story

### It Started with Vectors

> "Datalog has no ordered collections. Skills, inventory, event logs—anything that needs sequence—requires ugly workarounds."

**How most people would solve "I need vectors":**

| Approach | Problem |
|----------|---------|
| Serialize to JSON | Loses queryability |
| Index attributes (`:skill/0`, `:skill/1`) | Renumber on insert |
| Separate entity per element | N+1 queries, entity explosion |
| Different database for vectors | Two databases, no joins |
| Blob symbol | Complete surrender |

### The Rabbit Hole

From [CRDT_VECTOR_STORAGE.md](../proposals/CRDT_VECTOR_STORAGE.md):

> **The rabbit hole:** Implementing vectors properly meant rethinking the storage model. Append-at-position needs conflict resolution. Conflict resolution needs unique element identifiers. Unique identifiers need Lamport clocks. And once you have all that, you've accidentally built a CRDT-based system with benefits beyond just vectors.

**The domino chain:**

```
"I need ordered collections"
        ↓
"Vectors need append-at-position"
        ↓
"Append-at-position needs conflict resolution"
        ↓
"Conflict resolution needs unique element IDs"
        ↓
"Unique IDs need Lamport clocks"
        ↓
"Wait... I just built a CRDT system"
        ↓
"...which gives me history for free"
        ↓
"...and concurrent writes just work"
        ↓
"...and it's multi-node ready"
        ↓
"...and the key encoding makes resolution O(1)"
```

### The Result: Faster, Not Slower

**CRDT vs pre-CRDT performance: 1.9× faster**

Adding vectors didn't add overhead. The CRDT model **eliminated**:

| Before | After | Savings |
|--------|-------|---------|
| Read-before-write | Write-only | 1 storage lookup |
| Retraction datoms | None needed | Storage space, write I/O |
| Separate HISTORY index | Unified indices | Index maintenance |
| Different history code path | Same code path | Complexity, bugs |
| 8 index structures (Datomic-style) | 7 unified | 12.5% fewer indices |

**The feature that seemed like it would add complexity actually reduced it** because the new foundation was more coherent than the old one.

---

## The Simplification Pattern

### The Conventional Approach

```
Problem → Add abstraction layer → Problem "solved"

New problem → Add another layer → "Solved"

Repeat until you have:
  Controller → Service → Manager → Repository →
  DAO → Mapper → DTO → Entity → Value Object → ...
```

Each "what if" adds a layer:
- "What if we need to swap databases?" → Add repository abstraction
- "What if value types change?" → Add wrapper types
- "What if sources differ?" → Add adapter interfaces
- "What if resolution logic changes?" → Add strategy pattern

### The Janus Approach

```
Problem → What representation makes this problem disappear?

"Need resolution" → Key encoding IS resolution (no layer)
"Need history" → Same index, keep scanning (no layer)
"Need vectors" → Rethink storage model (remove layers)
```

### Examples Across the Codebase

**Type System:**
```go
// Most ORMs: wrapper types everywhere
type StringValue struct { value string }
type IntValue struct { value int64 }

// Janus: just interface{}
type Value = interface{}
// string is string, int64 is int64, no wrapping
```

**Data Sources:**
```go
// Most systems: different interfaces for different sources
type DatabaseSource interface { ... }
type MemorySource interface { ... }
type RemoteSource interface { ... }

// Janus: one interface
type PatternMatcher interface { ... }
// Database, memory, remote - all implement the same thing
```

**Identity:**
```go
// Most systems: multiple representations, convert constantly
entityID := "user-123"
entityHash := sha1(entityID)
entityDisplay := base64(entityHash)

// Janus: one type, three faces
type Identity struct {
    original string  // "user-123"
    hash     [20]byte // SHA1
    l85      string   // display
}
// Ask for what you need: id.String(), id.Bytes(), id.L85()
```

**Schema:**
```go
// Most systems: schema required upfront
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, ...)

// Janus: schema optional, additive
db.AddDatom(entity, ":name", "Alice")  // Works without schema
db.DefineAttribute(":name", TypeString, CardinalityOne)  // Add validation later
```

### The Principle

| Conventional | Janus |
|--------------|-------|
| Hide complexity behind abstractions | Remove complexity so abstractions unnecessary |
| "Flexibility for future changes" | "Simplicity for understanding now" |
| Layers protect from change | Foundation makes change unnecessary |
| Interface for everything | Interface only when polymorphism needed |

**Most abstractions exist because the foundation is wrong.** Fix the foundation and the abstractions become unnecessary.

---

## Summary: What Makes This Different

### The Author's Approach

1. **Deep domain understanding** - Spans CRDT theory, database internals, query optimization, and storage engines simultaneously

2. **Implementation-level thinking** - "What happens when I literally scan bytes in this order?"

3. **Simplification over abstraction** - "What representation makes this problem disappear?"

4. **Deferred complexity** - Document solutions in proposals, implement when proven necessary

5. **Foundation-first** - Will rethink entire storage model if it makes the system simpler

### The Results

| Metric | Result |
|--------|--------|
| LWW resolution | 965ns (O(1) from key encoding) |
| CRDT overhead | Negative (1.9× faster than pre-CRDT) |
| Streaming speedup | 2.22× with 52% memory reduction |
| Index count vs Datomic | 6 unified vs 8 segmented |
| Production readiness | ~85% with simple foundation |

### The Philosophy in One Line

**Don't add abstractions to manage complexity. Find the representation that makes complexity unnecessary.**

---

## References

- [KEY_ENCODING_AND_CRDT.md](KEY_ENCODING_AND_CRDT.md) - Storage layer analysis
- [PACE_CAR_CACHING_ARCHITECTURE.md](../proposals/PACE_CAR_CACHING_ARCHITECTURE.md) - Pipeline parallelism proposal
- [CRDT_VECTOR_STORAGE.md](../proposals/CRDT_VECTOR_STORAGE.md) - CRDT origin story
- [CLAUDE.md](../../CLAUDE.md) - Project guidelines and architecture overview
- [PERFORMANCE_STATUS.md](../../PERFORMANCE_STATUS.md) - Verified benchmarks
