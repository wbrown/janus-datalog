# CRDT Storage Semantics

This document describes the CRDT (Conflict-free Replicated Data Type) storage semantics in janus-datalog. The system supports three cardinalities, each with distinct conflict resolution behavior.

## Overview

| Cardinality | CRDT Type | Conflict Resolution | Use Case |
|-------------|-----------|---------------------|----------|
| One | LWW Register | Highest ElementID wins | Single values (name, age) |
| Many | Add-Wins Set | Add beats concurrent remove | Tags, roles, categories |
| Vector | RGA | Deterministic ordering | Ordered lists, sequences |
| Vector + UniqueElements | RGA-OrderedSet | RGA + duplicate rejection | Ordered unique collections |

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

**Unique variant**: when a cardinality-one attribute is declared
`:db.unique/value` or `:db.unique/identity`, an additional `(A, V)`-LWW
resolution rule applies at read time (see
[Unique Attributes](#unique-attributes-av-lww-with-walk-fallback)
below). The simple first-entry shortcut is replaced by a walk that
falls back to older assertions when the latest is superseded by
another entity's claim.

---

## Unique Attributes: (A, V)-LWW with walk fallback

For cardinality-one attributes declared unique in the schema, janus
applies an additional CRDT rule at read time so that lookup-by-value
returns exactly one canonical entity.

**Why read-time**: in this codebase's concurrent-write CRDT model, a
write-time uniqueness gate cannot work without imposing coordination
that contradicts the rest of the architecture. Uniqueness is expressed
instead as a merge rule: every assertion is a fact, and resolution
determines which one wins per `(A, V)`. See
[CRDT_UNIQUE_SEMANTICS.md](./CRDT_UNIQUE_SEMANTICS.md) for the full
design discussion, including why each alternative was rejected.

**Walk rule** (per-entity):

Walk E's EATV history in descending Tx order. For each entry
`(V_i, T_i, op)`:

  1. `Remove(V_i, T_i)` — record `retracted[V_i] = max(retracted[V_i], T_i)`.
  2. `Set(V_i, T_i)` — skip if `retracted[V_i] > T_i` (cancelled).
  3. `Set(V_i, T_i)` — skip if any OTHER entity has asserted `V_i` with
     `Tx > T_i` (superseded).
  4. Otherwise — emit `V_i` as E's current value.

If no entry qualifies, E has no current value for this attribute.

**Symmetric views**:

| View | Question | Answer |
|---|---|---|
| Entity | "What is E's value?" | The walk's emission. |
| Value | "Who owns V?" | The entity whose walk emits V (at most one). |

V-view (`LookupByUnique(a, v)`) and entity-view always agree because
they derive from the same walk rule.

**Operational cost**:

- Non-unique cardinality-one reads: O(1) EATV first-entry (unchanged).
- Unique reads, uncontested: O(1) — walk emits the first entry.
- Unique reads, contested: O(history-depth × AVET-candidates-per-V).
  Common case (rare takeovers): still near-O(1). Worst case: walk
  proceeds until a non-superseded entry is found.

**Lookup-ref API** (`UniqueIdentity`):

```go
e, err := d.LookupByUnique(":user/email", "alice@example.com")
// Returns the canonical owner, or (nil, nil) if no entity owns V.
```

`LookupByUnique` is the primitive for natural-key lookup; application-
layer upsert is built on top.

**Storage**:

No new indices. The walk uses the existing EATV for per-entity history
and AVET for per-(A, V) supersession checks. Writes remain simple
append-only datom inserts; the resolution complexity lives entirely in
the read path.

**Tombstones and retractions**:

Remove operations participate in the walk: a `Remove(V)` at higher Tx
cancels a `Set(V)` at lower Tx within the same entity's history. This
preserves the property that `Set → Remove` produces no current value,
while `Set → Remove → Set` correctly re-asserts V at the highest Tx.

**History and AsOf**:

`d.History()` bypasses the walk entirely — all raw assertions are
visible, including superseded ones. `d.AsOf(tx)` applies the walk with
`Tx ≤ target` restricting both the walked entity's history and the
supersession check against other entities.

**Cache invalidation**:

Writes to a unique attribute can silently stale cached values for
other entities whose walks may now produce different results. The
cache invalidates all `(E, A)` entries for the attribute on any commit
that writes it (conservative strategy per design D3).

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

## Vector with UniqueElements (OrderedSet)

**Semantics**: RGA vector with duplicate rejection. Combines ordered semantics with set-like uniqueness.

### Schema Definition

```go
// Schema builder
builder.Attribute(":character/prefs").Type(schema.TypeString).OrderedSet().Add()

// Or explicitly
builder.Attribute(":character/prefs").Type(schema.TypeString).Vector().UniqueElements(true).Add()
```

### Duplicate Rejection

When `UniqueElements` is true, `Add()` operations check for existing values:

```go
tx.Add(entity, ":prefs", "dark-mode")   // Added
tx.Add(entity, ":prefs", "compact")     // Added
tx.Add(entity, ":prefs", "dark-mode")   // No-op: already exists
```

The uniqueness check examines:
1. Pending datoms in the current transaction
2. Committed data via cache

### Using with Reflect Package

The `datalog.OrderedSet[T]` type maps to `Vector().UniqueElements(true)`:

```go
type Character struct {
    ID    datalog.Identity           `datalog:"-,id"`
    Name  string                     `datalog:"name"`
    Prefs datalog.OrderedSet[string] `datalog:"prefs"`
}

// Create and populate
prefs := datalog.NewOrderedSet[string]()
prefs.Append("dark-mode")
prefs.Append("compact")
prefs.Append("dark-mode")  // No-op: duplicate

char := Character{Name: "Alice", Prefs: *prefs}
tx.SaveStruct(&char)
// Stores: ["dark-mode", "compact"]
```

### Query Behavior

Vectors (including OrderedSet) return as array values in queries:

```clojure
[:find ?prefs :where [?e :character/name "Alice"] [?e :character/prefs ?prefs]]
;; Returns: [[["dark-mode" "compact"]]]
;; Note: Single tuple with array value, not multiple tuples
```

### Concurrent Write Behavior

Duplicate rejection is **not distributed-safe**. If two replicas add the same value concurrently (before sync), both will succeed locally and both inserts will appear after merge:

```
Replica A: tx.Add(e, :prefs, "X")  // Succeeds (no local "X")
Replica B: tx.Add(e, :prefs, "X")  // Succeeds (no local "X")

After sync: ["X", "X"]  // Both inserts preserved by RGA
```

For strict uniqueness across replicas, application-level conflict resolution is required after merge.

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
- Use `d.WarmCache(attributes)` for performance-critical attributes

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

## Asymptotic Complexity Analysis

This section provides formal complexity analysis for all CRDT operations.

### Definitions

| Symbol | Meaning |
|--------|---------|
| **n** | Total operations (writes + tombstones) for an (E, A) pair |
| **k** | Live elements in collection (after CRDT resolution) |
| **d** | Pending datoms in current transaction |
| **N** | Total datoms in database |
| **p** | Common prefix length (for Set() diff algorithm) |

### Cardinality-One (LWW Register)

| Operation | Time | Space | Notes |
|-----------|------|-------|-------|
| **Write** `Set(e, a, v)` | O(1) | O(1) | Append-only, no read required |
| **Read** (cache hit) | O(1) | O(1) | Return cached value |
| **Read** (cache miss) | O(1) | O(1) | EATV index: first entry = max ElementID |
| **Cache rebuild** | O(1) | O(1) | Single index seek |
| **History query** | O(n) | O(n) | Scan all writes for (E, A) |

**Why O(1) cache miss?** EATV index orders by Tx descending (bitwise NOT encoding). First entry is always the highest ElementID, which is the current value. No scan needed.

### Cardinality-Many (Add-Wins Set)

| Operation | Time | Space | Notes |
|-----------|------|-------|-------|
| **Write** `Add(e, a, v)` | O(1) | O(1) | Append OpAdd datom |
| **Write** `Remove(e, a, v)` | O(1) | O(1) | Append OpRemove datom |
| **Read** (cache hit) | O(1) | O(1) | Return cached set |
| **Read** (cache miss) | O(n) | O(k) | Add-wins resolution |
| **Cache rebuild** | O(n) | O(k) | Group by V, compare Lamport per value |
| **Membership test** | O(1) | O(1) | Hash lookup in cached set |
| **History query** | O(n) | O(n) | Scan all operations |

**Resolution algorithm**: For each unique value V, find max add Lamport and max remove Lamport. Value is in set if `maxAdd >= maxRemove` (add-wins at equal Lamport).

### Cardinality-Vector (RGA)

| Operation | Time | Space | Notes |
|-----------|------|-------|-------|
| **Write** `Add(e, a, v)` (append) | O(1) | O(1) | Single RGAInsert datom |
| **Write** `Set(e, a, vals)` (replace) | O(k + k') | O(k') | See diff algorithm below |
| **Read** (cache hit) | O(1) | O(1) | Return cached vectorList |
| **Read** (cache miss) | O(n log n) | O(n) | RGA reconstruction |
| **Cache rebuild** | O(n log n) | O(k) | Sort + DFS traversal |
| **Index access** `vec[i]` | O(1) | O(1) | Cached vectorList[i] |
| **Length** | O(1) | O(1) | len(vectorList) |
| **History query** | O(n) | O(n) | Scan all operations |

**RGA reconstruction**: O(n) to load all elements, O(n log n) to sort children by ElementID at each node, O(k) DFS to produce ordered output.

**Set() diff complexity**:

| Scenario | Tombstones | Inserts | Total |
|----------|------------|---------|-------|
| Append k' elements | 0 | k' | O(k') |
| Change last element | 1 | 1 | O(1) |
| Change at position p | k - p | k' - p | O(k + k' - 2p) |
| Full replace (p=0) | k | k' | O(k + k') |
| No change | 0 | 0 | O(1) |

### Vector + UniqueElements (OrderedSet)

| Operation | Time | Space | Notes |
|-----------|------|-------|-------|
| **Write** `Add(e, a, v)` | O(d + k) | O(1) | Uniqueness check + insert |
| **Write** `Set(e, a, vals)` | O(k + k') | O(k') | Same as Vector |
| **Read** (cache hit) | O(1) | O(1) | Same as Vector |
| **Read** (cache miss) | O(n log n) | O(n) | Same as Vector |
| **Uniqueness check** | O(d + k) | O(1) | Scan pending + cached |

**Uniqueness check breakdown**:
- Scan pending transaction datoms: O(d)
- Cache lookup: O(1) hit, O(n log n) miss
- Scan cached vectorList: O(k)
- Total: O(d + k) assuming cache hit

**Trade-off**: Regular Vector Add is O(1). OrderedSet Add is O(d + k) due to uniqueness enforcement. For small vectors (k < 100), overhead is negligible.

### Index Operations

| Operation | Time | Notes |
|-----------|------|-------|
| Point lookup (any index) | O(log N) | BadgerDB LSM-tree seek |
| Range scan | O(log N + r) | r = results in range |
| EAVT scan for (E, A) | O(log N + n) | n = operations for that (E, A) |
| AVET value lookup | O(log N + m) | m = entities with that value |
| VAET reverse ref | O(log N + m) | m = references to entity |

### Cache Operations

| Operation | Time | Notes |
|-----------|------|-------|
| Cache lookup | O(1) | Hash map by (E, A) |
| Freshness check | O(1) | Compare version to maxVersions |
| Invalidation | O(1) | Delete entry, update maxVersions |
| Full rebuild (One) | O(1) | Single index seek |
| Full rebuild (Many) | O(n) | Add-wins resolution |
| Full rebuild (Vector) | O(n log n) | RGA reconstruction |

### Space Complexity

**Per (E, A) pair**:

| Cardinality | Storage | Cache |
|-------------|---------|-------|
| One | O(n) all writes | O(1) single value |
| Many | O(n) all writes | O(k) set members |
| Vector | O(n) all writes | O(k) values + O(k) position index |
| Vector + UniqueElements | O(n) all writes | O(k) values + O(k) position index |

**Storage note**: All historical writes are preserved (append-only). Storage grows with n (total operations), not k (current size). Compaction/GC is not currently implemented.

**Cache note**: Cache stores only resolved current state, not history. Memory usage is O(k) per cached (E, A) pair.

### Summary: Write vs Read Trade-offs

| Cardinality | Write | Read (miss) | Read (hit) | Trade-off |
|-------------|-------|-------------|------------|-----------|
| One | O(1) | O(1) | O(1) | Optimal for single values |
| Many | O(1) | O(n) | O(1) | Write-optimized, read rebuilds |
| Vector | O(1) | O(n log n) | O(1) | Write-optimized, expensive rebuild |
| OrderedSet | O(d+k) | O(n log n) | O(1) | Uniqueness costs at write time |

**Key insight**: The system is write-optimized. Writes are always O(1) or O(d+k) for OrderedSet. Read performance depends on cache hit rate. For read-heavy workloads, use `d.WarmCache()` at startup.

---

## Performance Characteristics

### Memory

Cache entries are rebuilt on demand. After process restart, first access to each (E, A) triggers rebuild. Use `d.WarmCache(attributes)` at startup for performance-critical attributes.

---

## Index Selection by Cardinality

### E-Primary Indices (EATV)

| Cardinality | Primary Index | Reason |
|-------------|---------------|--------|
| One | EATV | Tx before V: first entry = current value |
| Many | EAVT | Group by V for add-wins resolution |
| Vector | EATV | Load all elements, reconstruct in memory |

### A-Primary Index (AETV)

When E is bound via input (not constant) and A is constant, the query engine uses AETV instead of AEVT:

| Index | Ordering | Use Case |
|-------|----------|----------|
| AEVT | A → E → V → Tx | Value lookups, no CRDT resolution |
| AETV | A → E → Tx↓ → V | A-primary queries with CRDT resolution |

**Why AETV?** The CRDTResolvingIterator requires Tx-descending order to apply "first entry wins" logic. AEVT has Tx ascending (oldest first), which breaks LWW resolution. AETV stores Tx with bitwise NOT for descending order.

### Value Lookups

For value lookups across entities:
- AVET: Find entities where attribute has specific value
- Works for all cardinalities (V is raw value, not wrapped metadata)

---

## Streaming CRDT Resolution

When the cache is bypassed (e.g., unbound E scans, `DisableCache: true`), CRDT resolution is applied at the storage scan level via `CRDTResolvingIterator`.

**Key insight**: The EATV/AETV index ordering IS resolution. Tx is stored with bitwise NOT for descending order, so forward scan returns highest Tx first. For LWW (CardinalityOne), the first entry is the winner.

| Cardinality | Resolution Strategy |
|-------------|---------------------|
| One | Emit first datom per (E, A), skip rest |
| Many | Track emitted values + tombstones, emit immediately |
| Vector | Accumulate minimal state, reconstruct at (E, A) boundary |

See [CRDT_STREAMING_RESOLUTION.md](CRDT_STREAMING_RESOLUTION.md) for detailed design.

---

## History and Time-Travel

All writes are preserved. Use database-level temporal modes for time-travel queries:

```go
import "github.com/wbrown/janus-datalog/datalog/db"

d, _ := db.Open("/path/to/data")
defer d.Close()

// History mode — all raw datoms, no CRDT resolution
hist := d.History()
history, _ := hist.Query(
    `[:find ?name ?tx :where [?e :person/name ?name ?tx]]`)

// As-of mode — CRDT-resolved state at a specific point in time
asOf := d.AsOf(txID)  // txID is datalog.ElementID from Commit()
asOfResult, _ := asOf.Query(
    `[:find ?name :where [?e :person/name ?name]]`)
```

**Three database modes** (controlled via `*datalog.ElementID` pointer on `BadgerMatcher.txID`):

| Mode | API | `txID` value | CRDT resolution | Tx filtering |
|------|-----|-------------|-----------------|--------------|
| Latest | `d.Query(q)` | `nil` | Yes | No |
| As-of | `d.AsOf(elemID).Query(q)` | `&ElementID{L,R}` | Yes | Yes |
| History | `d.History().Query(q)` | `&ElementID{}` | No | No |

The cache stores current value only. History and as-of queries bypass the cache and scan storage directly.

---

## Pull API and CRDT Resolution

The Pull API (`d.Pull()`, `d.PullInto()`, and pull expressions in queries) uses CRDT resolution to return current values.

### EntityResolver Interface

Pull operations use the `EntityResolver` interface to resolve attributes with proper CRDT semantics:

```go
type EntityResolver interface {
    ResolveAllAttributes(entity datalog.Identity) (map[datalog.Keyword]interface{}, error)
}
```

The Database implements this interface, delegating to `ResolveEntityAttributes` which uses the EA cache for efficient resolution.

### Wildcard Pulls

Wildcard pulls (`[*]`) resolve all attributes for an entity:

```go
result, _ := d.Pull(entity, "[*]")
// Returns CRDT-resolved values:
// - CardinalityOne: single value (LWW)
// - CardinalityMany: []interface{} (add-wins set)
// - CardinalityVector: []interface{} (RGA ordered list)
```

**With schema**: Uses schema attributes as the resolution list, leveraging the cache when data is already cached.

**Without schema**: Scans EAVT to discover all attributes for the entity.

### Pull Expressions in Queries

Pull expressions in queries also use CRDT resolution:

```clojure
[:find (pull ?e [*]) :where [?e :task/name "test"]]
```

Both standalone `d.PullInto()` and query pull expressions use the same resolution path through `EntityResolver`.

### Return Types by Cardinality

| Cardinality | Pull Return Type | Example |
|-------------|------------------|---------|
| One | Single value | `"Alice"` |
| Many | `[]interface{}` | `["admin", "user"]` |
| Vector | `[]interface{}` (ordered) | `["Go", "Python", "Rust"]` |
