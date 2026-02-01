# CRDT Storage Semantics

This document describes the CRDT (Conflict-free Replicated Data Type) storage semantics in janus-datalog. The system supports three cardinalities, each with distinct conflict resolution behavior.

## Overview

| Cardinality | CRDT Type | Conflict Resolution | Use Case |
|-------------|-----------|---------------------|----------|
| One | LWW Register | Highest ElementID wins | Single values (name, age) |
| Many | Add-Wins Set | Add beats concurrent remove | Tags, roles, categories |
| Vector | RGA | Deterministic ordering | Ordered lists, sequences |

All writes are preserved with unique ElementIDs. The "current value" is computed by applying CRDT resolution rules to the full history.

## ElementID: The Ordering Primitive

Every write gets a unique ElementID:

```go
type ElementID struct {
    Lamport   uint64  // Logical timestamp (monotonically increasing)
    ReplicaID uint64  // Database instance identifier
}
```

**Comparison**: `(Lamport, ReplicaID)` lexicographic ordering provides total order across all writes from all replicas.

**Key encoding**: ElementIDs are encoded with bitwise NOT so highest sorts first in storage. Forward scan returns newest entries.

---

## Cardinality-One (LWW Register)

**Semantics**: Last-Writer-Wins. The write with the highest ElementID is the current value.

```go
tx.Set(entity, ":person/name", "Alice")  // ElementID L1
tx.Commit()

tx2.Set(entity, ":person/name", "Bob")   // ElementID L2 > L1
tx2.Commit()

// Query returns "Bob" (L2 > L1)
```

**Index**: Uses EATV (E → A → Tx → V). First entry in forward scan = highest Tx = current value.

**Resolution**: O(1) - just read first entry.

---

## Cardinality-Many (Add-Wins Set)

**Semantics**: Add-wins at concurrent Lamport timestamps. If add and remove have the same Lamport (happened on different replicas before sync), add wins.

```go
tx.Add(entity, ":person/tags", "admin")    // L1, OpAdd
tx.Commit()

tx2.Remove(entity, ":person/tags", "admin") // L2, OpRemove
tx2.Commit()

// If L2 > L1: "admin" is NOT in set (remove is later)
// If L2 == L1: "admin" IS in set (add-wins at same Lamport)
```

**Index**: Uses EAVT (E → A → V → Tx). Groups entries by value for efficient resolution.

**Resolution**: For each unique value, compare highest add Lamport vs highest remove Lamport.

---

## Cardinality-Vector (RGA)

**Semantics**: Replicated Growable Array. Ordered collection with deterministic merge across replicas.

### RGA Data Model

Each vector element has:
- **ID** (ElementID): Unique identifier for this element
- **Value**: The actual data
- **AfterRef** (ElementID): Which element this was inserted after
- **Tombstone** (optional ElementID): If deleted, the deletion's ID

The vector is a linked list where each element points to its predecessor.

### Storage Format

```
Key:   [E][A][V][Tx↓][Op][AfterRef?]
Value: (empty - all data in key)

Op values:
  OpRGAInsert (3)    - new element, AfterRef = predecessor
  OpRGATombstone (4) - deletion, AfterRef = element being deleted
```

**V stores the raw value** (not RGA metadata). This enables AVET lookups like "find entities where vector contains X".

### RGA Reconstruction Algorithm

```
1. Load all elements for (E, A) from EATV index
2. Build adjacency map: AfterRef → [children]
3. DFS from HEAD (zero ElementID), visiting children in ElementID order
4. Skip tombstoned elements
5. Result: ordered list of values
```

**Determinism**: Children of each node are sorted by ElementID, ensuring all replicas produce identical order after merge.

### Concurrent Append Behavior

When two replicas append concurrently (before sync):

```
Replica A (ReplicaID=100):          Replica B (ReplicaID=200):
tx.Add(e, :skills, "stealth")       tx.Add(e, :skills, "magic")
  AfterRef = HEAD                     AfterRef = HEAD
  ID = (Lamport=5, Replica=100)       ID = (Lamport=5, Replica=200)
tx.Commit()                         tx.Commit()
```

Both replicas independently append to an empty vector. Both elements have `AfterRef = HEAD` because neither replica saw the other's write.

**After merge (bidirectional sync)**, both replicas have both elements. RGA reconstruction:

1. Build adjacency map: `HEAD → [element_A, element_B]`
2. **Sort children by ElementID** for determinism
3. DFS from HEAD, visiting children in ElementID order

Since `(5, 100) < (5, 200)` (same Lamport, lower ReplicaID wins):

**Result on ALL replicas**: `["stealth", "magic"]`

### Concurrent Update to Same Position

When two replicas modify the same position concurrently:

```
Initial: ["a", "b", "c"] (on both replicas)

Replica A:                              Replica B:
tx.Set(e, :skills, ["a", "X", "c"])     tx.Set(e, :skills, ["a", "Y", "c"])
  // Change middle to "X"                 // Change middle to "Y"
tx.Commit()                             tx.Commit()
```

Each replica:
1. Tombstones original "b" (and "c" due to RGA prefix constraint)
2. Inserts new elements after "a"

**After merge**:
- Both "X" and "Y" are inserted after "a" (both have AfterRef = E1)
- "X" and "Y" are sorted by ElementID
- Result: `["a", "X", "Y", "c"]` or `["a", "Y", "X", "c"]` depending on ReplicaID ordering

**Both values are preserved** - RGA does not silently discard concurrent writes.

### Key Properties of Concurrent Vector Writes

| Property | Behavior |
|----------|----------|
| **Deterministic** | Same result on all replicas after merge |
| **No data loss** | All concurrent writes are preserved |
| **Order by ElementID** | Lower `(Lamport, ReplicaID)` comes first |
| **Not temporal** | Order is NOT "who wrote first in wall-clock time" |
| **Convergent** | All replicas converge to identical state after sync |

### Important: This is NOT Last-Writer-Wins

Unlike cardinality-one (LWW), vectors preserve ALL concurrent writes. The semantics are:

- **Cardinality-One**: Concurrent writes → highest ElementID wins, others discarded
- **Cardinality-Vector**: Concurrent writes → ALL preserved, ordered by ElementID

If you need strict temporal ordering or "only one writer wins" semantics for ordered collections, you would need:
- External coordination (distributed lock)
- Explicit position/sequence attributes with LWW semantics
- Application-level conflict resolution after merge

---

## EA Cache Architecture

The cache provides O(1) access to resolved CRDT views.

### Cache Entry Structure

```go
type CacheEntry struct {
    version     ElementID           // Max ElementID when computed
    cardinality Cardinality

    // One of these is populated based on cardinality:
    oneValue    any                 // Cardinality-One: current value
    manySet     map[any]bool        // Cardinality-Many: set members
    vectorList  []any               // Cardinality-Vector: ordered values
    vectorIndex []ElementID         // Cardinality-Vector: position → ElementID
}
```

### Cache Key

```go
type CacheKey struct {
    E Entity    // 20-byte entity hash
    A Attribute // 32-byte attribute
}
```

### Read Flow

```
Query: [?e :character/skills ?skills]
                │
                ▼
     ┌────────────────────┐
     │ cache.GetOrResolve │
     └────────────────────┘
                │
      ┌─────────┴─────────┐
      │                   │
  Cache Hit?          Cache Miss
  (version == max)        │
      │                   ▼
      │        ┌─────────────────────┐
      │        │ rebuildVector()     │
      │        │  1. Scan EATV       │
      │        │  2. RGA reconstruct │
      │        │  3. Store in cache  │
      │        └─────────────────────┘
      │                   │
      ▼                   ▼
   Return vectorList (values)
```

### Freshness Tracking

```go
// Per (E,A) max ElementID - updated on every write
maxVersions sync.Map  // map[CacheKey]ElementID

// Freshness check is O(1):
if entry.version == maxVersions[key] {
    return entry  // Fresh
}
// Stale - rebuild
```

### Write Flow

```
tx.Add(e, :skills, "archery")
            │
            ▼
    ┌───────────────────┐
    │ Generate ElementID│
    │ Write to storage  │
    └───────────────────┘
            │
            ▼
    ┌───────────────────┐
    │ Commit():         │
    │ UpdateMaxVersion()│ ← Marks cache stale
    │ Invalidate(key)   │ ← Removes entry
    └───────────────────┘
```

Next read triggers rebuild with fresh data.

### Cache Invalidation

**Per-Key Granularity**: Writes only affect the specific (E, A) key being written:

```go
// Writing to (Entity1, :name) does NOT invalidate (Entity2, :skills)
tx.Set(entity1, ":name", "Alice")
tx.Commit()
// Only maxVersions[(entity1, :name)] is updated
// Cache entry for (entity2, :skills) remains fresh
```

**Unbounded Cache**: The current implementation has no eviction policy:
- No LRU eviction
- No size limits
- No TTL expiration
- Entries remain until explicitly invalidated by writes

For databases with millions of entities, this could consume significant memory. Future implementations may add configurable eviction policies.

**Process Restart**: Cache is empty after restart:
- `maxVersions` map starts empty
- First access to each (E, A) triggers rebuild from storage
- Use `db.WarmCache(attributes)` for performance-critical attributes

---

## Vector Operations and Cache

### Add (Append)

```go
tx.Add(entity, ":skills", "stealth")
```

1. Generate new ElementID
2. Set AfterRef = last element from this transaction (or HEAD if first)
3. Write OpRGAInsert to storage
4. On commit: invalidate cache

**Note**: Add does NOT read from storage. AfterRef tracks within the transaction only. Cross-transaction ordering is determined by ElementID sorting during reconstruction.

### Set (Replace)

```go
tx.Set(entity, ":skills", []any{"stealth", "magic"})
```

1. **Read current from cache**: Get `vectorList` and `vectorIndex`
2. **Compute diff**: Find common prefix, elements to tombstone, elements to insert
3. **Write tombstones**: For each removed element, write OpRGATombstone with AfterRef = element's ID
4. **Write inserts**: For each new element, write OpRGAInsert chained after appropriate predecessor

**Why vectorIndex matters**: To tombstone element at position N, you need its ElementID. The cache's `vectorIndex` provides O(1) lookup: `vectorIndex[N]` = ElementID to tombstone.

### Set() Diff Algorithm

For efficiency, Set() computes minimal diff rather than full replacement.

#### Why Prefix-Only (RGA Constraint)

In RGA, each element chains off its predecessor via AfterRef. When you tombstone an element, **its descendants become orphaned** during DFS reconstruction:

```
Old: [a, b, c] with chain: HEAD → a → b → c

If we tombstone "b":
- children[a] = [] (b is filtered out as tombstoned)
- children[b] = [c] (c still points to b)
- DFS from HEAD visits: a, then a's children (empty)
- "c" is NEVER visited - it's orphaned

Result: [a] - we lost "c"!
```

**This means we can only preserve a PREFIX** - elements that chain from HEAD through kept elements. A common suffix cannot be preserved because suffix elements chain from tombstoned elements.

#### Algorithm

```go
// 1. Find common prefix
commonPrefix := 0
for i := 0; i < min(len(old), len(new)); i++ {
    if old[i] != new[i] { break }
    commonPrefix++
}

// 2. Tombstone old[commonPrefix:] - these and their descendants are removed
// 3. Insert new[commonPrefix:] chained after old[commonPrefix-1] (or HEAD)
```

#### Examples by Operation Type

| Operation | Old | New | Writes |
|-----------|-----|-----|--------|
| **Append** | `[a, b, c]` | `[a, b, c, d]` | 1 insert |
| **Append multiple** | `[a]` | `[a, b, c]` | 2 inserts |
| **Change last** | `[a, b, c]` | `[a, b, X]` | 1 tombstone + 1 insert |
| **Change middle** | `[a, b, c]` | `[a, X, c]` | 2 tombstones + 2 inserts* |
| **Prepend** | `[b, c]` | `[a, b, c]` | 2 tombstones + 3 inserts |
| **No change** | `[a, b, c]` | `[a, b, c]` | 0 writes |
| **Full replace** | `[a, b, c]` | `[x, y, z]` | 3 tombstones + 3 inserts |
| **Truncate** | `[a, b, c]` | `[a]` | 2 tombstones |
| **Clear** | `[a, b, c]` | `[]` | 3 tombstones |

*Change middle: prefix is `[a]`, so we tombstone `[b, c]` and insert `[X, c]`.

#### Detailed Example: Change Middle Element

```
Old: [a, b, c]  with ElementIDs: [E1, E2, E3]
     Chain: HEAD → E1(a) → E2(b) → E3(c)

New: [a, X, c]

Step 1: Find common prefix
  old[0]="a" == new[0]="a" ✓
  old[1]="b" != new[1]="X" ✗
  commonPrefix = 1

Step 2: Tombstone old[1:] = [b, c]
  Write: OpRGATombstone for E2
  Write: OpRGATombstone for E3

Step 3: Insert new[1:] = [X, c] after E1
  Write: OpRGAInsert "X" with AfterRef=E1, gets E4
  Write: OpRGAInsert "c" with AfterRef=E4, gets E5

Result chain: HEAD → E1(a) → E4(X) → E5(c)
              E2(b) and E3(c) are tombstoned

Total: 2 tombstones + 2 inserts = 4 writes
```

#### Performance by Pattern

| Pattern | Writes | Notes |
|---------|--------|-------|
| Append (most common) | O(k) | k = new elements |
| Modify end | O(k) | k = changed elements |
| Modify beginning | O(n+m) | Worst case - full rewrite |
| No change | O(1) | Detected, skipped |

**Optimization insight**: Structure updates to modify the end when possible. Appending is O(1), prepending is O(n).

---

## Vector Query Integration

### Bound Entity Query

```clojure
[:find ?skills :where [?e :character/name "Alice"] [?e :character/skills ?skills]]
```

1. First pattern binds `?e` via name lookup
2. Second pattern uses bound `?e` → cache lookup → returns `vectorList`

### Unbound Entity Query

```clojure
[:find ?e ?skills :where [?e :character/skills ?skills]]
```

1. Scan all entities with `:character/skills` attribute
2. For each entity, resolve vector via cache
3. Return (entity, vector) pairs

---

## CacheResolver Interface

The cache is decoupled from storage via interface:

```go
type CacheResolver interface {
    GetCardinality(a Attribute) Cardinality

    // Cardinality-One: LWW resolution
    ResolveLWW(e Entity, a Attribute) (value any, maxID ElementID, err error)

    // Cardinality-Many: Add-wins resolution
    ResolveAddWins(e Entity, a Attribute) (members map[any]bool, maxID ElementID, err error)

    // Cardinality-Vector: RGA reconstruction
    ResolveRGA(e Entity, a Attribute) (values []any, positions []ElementID, maxID ElementID, err error)
}
```

`BadgerMatcher` implements this interface.

---

## Performance Characteristics

### Read Performance

| Operation | Cache Hit | Cache Miss |
|-----------|-----------|------------|
| Get cardinality-one value | O(1) | O(1) scan first entry |
| Get cardinality-many set | O(1) | O(n) add-wins resolution |
| Get vector | O(1) | O(n log n) RGA reconstruction |
| Get vector[i] | O(1) | O(n log n) then O(1) |
| Get vector length | O(1) | O(n log n) then O(1) |

### Write Performance

| Operation | Complexity |
|-----------|------------|
| Set cardinality-one | O(1) - append only |
| Add to set | O(1) - append only |
| Remove from set | O(1) - append tombstone |
| Append to vector | O(1) - append only |
| Set vector (diff) | O(k) where k = changed elements |
| Set vector (full replace) | O(n + m) tombstones + inserts |

### Memory

Cache entries are rebuilt on demand. After process restart, first access to each (E, A) triggers rebuild. Use `db.WarmCache(attributes)` at startup for performance-critical attributes.

---

## Index Selection by Cardinality

| Cardinality | Primary Index | Reason |
|-------------|---------------|--------|
| One | EATV | Tx before V: first entry = current value |
| Many | EAVT | Group by V for add-wins resolution |
| Vector | EATV | Load all elements, reconstruct in memory |

For value lookups across entities:
- AVET: Find entities where attribute has specific value
- Works for all cardinalities (V is raw value, not wrapped metadata)

---

## History and Time-Travel

All writes are preserved. Use history predicates for time-travel queries:

```clojure
;; All historical values
[:find ?name ?tx :where [?e :person/name ?name ?tx] [(history)]]

;; Value as of specific Lamport time
[:find ?name :where [?e :person/name ?name ?tx] [(as-of ?tx 5000)]]
```

The cache stores current value only. History queries bypass the cache and scan storage directly.
