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

An inserted element is guaranteed only to land somewhere after the element
its AfterRef names. The elements inserted after one element are ordered by
ElementID, whichever writers wrote them. A tombstoned element keeps its place,
so the elements after it stay where they are.

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

Two transactions open at once on one database behave as two replicas do. Each writes against the vector its read session opened on, with its own writes applied. An element the other commits after that is one it never saw, and the merge keeps both writers' elements.

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

The uniqueness check finds the value when the vector the transaction holds
has a live element carrying it. It reads the newest live element of the
value, which [Asymptotic Complexity Analysis](#asymptotic-complexity-analysis)
defines.

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

An `Add` also writes a second copy of a value that another transaction open on the same database commits after this one opened (see [Concurrent Append Behavior](#concurrent-append-behavior)).

For strict uniqueness across replicas or concurrent transactions, application-level conflict resolution is required after merge.

---

## EA Cache Architecture

The cache provides O(1) access to resolved CRDT views.

### Cache Entry Structure

```go
type CacheEntry struct {
    version     ElementID           // Highest ElementID among the datoms the resolution read
    fence       ElementID           // In an as-of view's cache: the read point the resolution began at
    cardinality Cardinality

    // One of these is populated based on cardinality:
    oneValue    any                 // Cardinality-One: current value
    manySet     map[string]any      // Cardinality-Many: member key → member
    vectorList  []any               // Cardinality-Vector: ordered values
    vectorIndex []ElementID         // Cardinality-Vector: position → ElementID
}
```

### Cache Key

```go
type CacheKey struct {
    E datalog.Identity // the interned entity identity a reader holds
    A datalog.Keyword  // the interned attribute keyword a reader holds
}
```

The key compares by pointer, and the trie routes it on the entity hash's first word folded with the keyword's FNV-1a hash, computed once where the keyword is interned.

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
  (no arrival above       │
   the read point)        │
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

The datoms of a key can arrive below the highest ElementID among those already present: two transactions open at once, an import, and a merge each bring them. So the highest ElementID among a key's datoms does not identify the set of them, and the cache records each commit's arrival instead.

- **Arrival**: once a commit's datoms are readable, it draws an ElementID from the database's clock and raises the version of each key it touched to that ElementID. Recording the arrival clears the key's entry.
- **Read point**: a reader takes its read point from the clock before it reads, or before its read session opens.
- **Serve**: an entry serves a reader when no arrival on its key lies above the reader's read point. An arrival at or below the read point was readable before the reader's session opened, so the session holds it. The entry holds every arrival on its key, so it is the value the session holds.
- **Store**: a resolution is stored only when no arrival on its key lies above the reader's read point. A key with no slot reads the cache's floor, which starts at the clock's read point when the cache is made and rises before `Clear`, `InvalidateAttribute` and `InvalidateRewind` drop slots.

### As-of Views

An as-of handle (`d.AsOf(target)`) and a matcher's as-of copy keep their resolutions in a cache of their own (`Cache.ForView`), which holds only resolutions as of the view's target and is freed with the view. It records no arrival: it reads the database cache's record of each key. Each entry records the read point its resolution began at, and it serves a reader while no arrival on its key, and no drop of the key's slot, lies above that read point or above the reader's. With the database's cache disabled, an as-of handle has none.

### Write Flow

```
tx.Add(e, :skills, "archery")
            │
            ▼
    ┌──────────────────────────────┐
    │ Draw ElementID, buffer datom │
    └──────────────────────────────┘
            │
            ▼
    ┌────────────────────────────────┐
    │ Commit():                      │
    │ BeginInFlight(keys)            │ ← readers resolve from storage
    │ write to storage               │
    │ UpdateMaxVersion(key, arrival) │ ← records the arrival, clears the entry
    │ Invalidate(keys)               │ ← clears the in-flight sentinels
    └────────────────────────────────┘
```

Next read triggers rebuild with fresh data.

### Cache Invalidation

**Per-Key Granularity**: Writes only affect the specific (E, A) key being written:

```go
// Writing to (Entity1, :name) does NOT invalidate (Entity2, :skills)
tx.Set(entity1, ":name", "Alice")
tx.Commit()
// Only the slot of (entity1, :name) records the arrival
// Cache entry for (entity2, :skills) remains fresh
```

**Unbounded Cache**: The current implementation has no eviction policy:
- No LRU eviction
- No size limits
- No TTL expiration
- Entries remain until explicitly invalidated by writes

For databases with millions of entities, this could consume significant memory. Future implementations may add configurable eviction policies.

**Process Restart**: Cache is empty after restart:
- The cache starts with no slots, its floor at the clock's read point when the database opens
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

1. **Read the vector the transaction holds**, each value with its ElementID, as [Asymptotic Complexity Analysis](#asymptotic-complexity-analysis) defines it
2. **Compute diff**: Find common prefix, elements to tombstone, elements to insert
3. **Write tombstones**: For each removed element, write OpRGATombstone with AfterRef = element's ID
4. **Write inserts**: For each new element, write OpRGAInsert chained after appropriate predecessor

**Why the ElementIDs matter**: To tombstone the element at position N, you need its ElementID, which the resolved vector carries beside each value.

### Set() Diff Algorithm

For efficiency, Set() computes minimal diff rather than full replacement.

#### Why Prefix-Only

An element inserted after the prefix's last element is guaranteed only to land somewhere after it, so an element kept after the prefix can end up on either side of it:

```
Old: [a, b, c] with chain: HEAD → a → b → c
New: [a, X, c], keeping "c" and inserting "X" after "a"

- children[a] = [b, X], with c following b
- DFS from HEAD visits: a, b (tombstoned), c, X

Result: [a, c, X] - not the new vector
```

**So Set preserves only a PREFIX**: it tombstones every element after the prefix and inserts the rest of the new vector after the prefix's last element. A tombstoned element keeps its place, so nothing after it is lost. A kept suffix stays where it was, ahead of the new elements.

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

`storage.PatternMatcher` implements this interface.

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
| **Write** `Set(e, a, vals)` (replace) | O(d + k + k') from a fresh cache entry, O(n log n + d + k') otherwise | O(n + d + k') | Reads the vector the transaction holds, then the diff below |
| **Write** `Remove(e, a, v)` | O(d + n_v) | O(d + n_v) | Reads the newest live element of v, n_v = inserts and tombstones of the elements holding v |
| **Read** (cache hit) | O(1) | O(1) | Return cached vectorList |
| **Read** (cache miss) | O(n log n) | O(n) | RGA reconstruction |
| **Cache rebuild** | O(n log n) | O(k) | Sort + DFS traversal |
| **Index access** `vec[i]` | O(1) | O(1) | Cached vectorList[i] |
| **Length** | O(1) | O(1) | len(vectorList) |
| **History query** | O(n) | O(n) | Scan all operations |

**RGA reconstruction**: O(n) to load all elements, O(n log n) to sort children by ElementID at each node, O(k) DFS to produce ordered output.

**The vector the transaction holds** is the vector its read session opened on, less the elements its own tombstones name, followed by its own inserts in the order it wrote them, less those its tombstones name. `Set` reads it.

- Every element the transaction writes is drawn after every element its session holds, so it lands after its anchor's whole subtree.
- Everything after that subtree is already tombstoned in the transaction's view. The anchor is HEAD for the transaction's first `Add`, the element its last `Add` or `Set` ended on, or a `Set`'s prefix end, after which that `Set` tombstoned the rest.
- An element another transaction commits after the session opened is not in its view.
- The session's vector is the cached entry when no datom arrived on (E, A) after the transaction's read point, O(1). Otherwise it is resolved through the session, O(n log n).
- Dropping the tombstoned elements and appending the transaction's inserts is O(k + d).

**Set() diff complexity**, after reading that vector:

| Scenario | Tombstones | Inserts | Total |
|----------|------------|---------|-------|
| Append k' elements | 0 | k' | O(k') |
| Change last element | 1 | 1 | O(1) |
| Change at position p | k - p | k' - p | O(k + k' - 2p) |
| Full replace (p=0) | k | k' | O(k + k') |
| No change | 0 | 0 | O(1) |

**The newest live element of a value** is what `Remove` and an ordered set's `Add` read.

- When the transaction holds a live insert of v, its newest one is the answer, since every element the transaction writes outranks every element its session holds: O(d).
- Otherwise it is the first insert on v's EAVT run in the session that neither a tombstone in the session nor one of the transaction's own names. The run is read newest first: O(d + n_v).
- A tombstone carries the value of the element it names and is written after that element's insert, so the run reads it before the insert.

### Vector + UniqueElements (OrderedSet)

| Operation | Time | Space | Notes |
|-----------|------|-------|-------|
| **Write** `Add(e, a, v)` | O(d + n_v) | O(d + n_v) | Uniqueness check + insert |
| **Write** `Set(e, a, vals)` | O(d + k + k') from a fresh cache entry, O(n log n + d + k') otherwise | O(n + d + k') | Same as Vector |
| **Read** (cache hit) | O(1) | O(1) | Same as Vector |
| **Read** (cache miss) | O(n log n) | O(n) | Same as Vector |
| **Uniqueness check** | O(d + n_v) | O(d + n_v) | Reads the newest live element of v |

**Trade-off**: Regular Vector Add is O(1). OrderedSet Add reads the datoms of the value it adds to check uniqueness, so its cost grows with the inserts and tombstones of that value.

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
| Cache lookup | O(1) | Hash trie by (E, A) |
| Freshness check | O(1) | Compare the slot's version, at least every arrival on the key, to the read point |
| Invalidation | O(1) | Raise the slot's version to the arrival, clear the entry |
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

**Cache note**: A database's cache stores resolved latest state, not history, and an as-of view's cache stores its view's. Memory usage is O(k) per cached (E, A) pair.

### Summary: Write vs Read Trade-offs

| Cardinality | Write | Read (miss) | Read (hit) | Trade-off |
|-------------|-------|-------------|------------|-----------|
| One | O(1) | O(1) | O(1) | Optimal for single values |
| Many | O(1) | O(n) | O(1) | Write-optimized, read rebuilds |
| Vector | O(1) | O(n log n) | O(1) | Write-optimized, expensive rebuild |
| OrderedSet | O(d + n_v) | O(n log n) | O(1) | Uniqueness costs at write time |

**Key insight**:

- A vector's `Add` writes without reading, in O(1).
- A vector's `Remove`, and an ordered set's `Add`, read the datoms of one value, in O(d + n_v).
- A vector's `Set` reads the vector the transaction holds, in O(d + k) from a fresh cache entry and O(n log n + d) when it resolves the vector through its read session.
- Read performance depends on cache hit rate. For read-heavy workloads, use `d.WarmCache()` at startup.

---

## Performance Characteristics

### Memory

Cache entries are rebuilt on demand. After process restart, first access to each (E, A) triggers rebuild. Use `d.WarmCache(attributes)` at startup for performance-critical attributes.

---

## Index Selection by Cardinality

### E-Primary Indices (EATV)

| Cardinality | V bound? | Primary Index | Reason |
|-------------|----------|---------------|--------|
| One | either | EATV | Tx before V: first entry = current value |
| Many | no | EAVT | Group by V for add-wins resolution |
| Many | yes | AEVT | A → E → V is the leading prefix, so the run covers every Tx for that one value |
| Vector | either | EATV | Load all elements, reconstruct in memory |
| Unknown / schemaless | either | EATV | Resolves as CardinalityOne (LWW) at every layer |

A bound V joins the scan prefix **only** for cardinality-many. Its adds and
removes all carry its own value, so the resolution decision for that value is
contained in the V-filtered run. For cardinality-one the winner may carry a
*different* V, and for vector the datom holds one element while the pattern's V
names the whole collection — in both cases V is compared downstream of the scan,
never used to narrow it.

### A-Primary Index (AETV)

When E is bound via input (not constant) and A is constant, the index depends on
the attribute's cardinality — **AETV for LWW, AEVT for add-wins**:

| Index | Ordering | Use Case |
|-------|----------|----------|
| AETV | A → E → Tx↓ → V | LWW resolution: cardinality-one, vector, and unknown |
| AEVT | A → E → V → Tx | **Add-wins resolution**: cardinality-many, where values must group |

**Why AETV for LWW?** `CRDTResolvingIterator` applies "first entry wins", so the
newest write for an (E, A) must sort first. AETV puts `Tx↓` immediately after
`[A][E]`, so it does.

**Every index encodes `Tx↓`.** What differs is Tx's *position*. AEVT orders
`[A][E][V][Tx↓]`, so the first entry under `[A][E]` is the lowest **value**, not the
newest write — newest-first within each value, which is what add-wins needs and
what LWW cannot use.

**Why AEVT for add-wins?** Add-wins is decided *per value*, not per (E, A): a value
is present if its highest add outranks its highest remove. AEVT makes each value's
datoms contiguous, so the decision is made as the scan passes each one. This is not
"no CRDT resolution" — it is the layout that add-wins resolution requires.

### Value Lookups

For value lookups across entities (A and V bound, E unbound):
- AVET: find entities where the attribute has a specific value
- AVET is a **candidate** index for every cardinality, but candidates are not
  answers. For cardinality-many and vector the CRDT iterator resolves them
  directly. For **cardinality-one and unknown/schemaless**, each candidate must be
  post-validated against the current winner on EATV: the LWW winner for that
  (E, A) may carry a different V, so the winner *within the V-filtered run* is not
  necessarily the winner at all. See `INDEX_SELECTION_PROOF.md` Theorems 2 and 3b.

---

## Streaming CRDT Resolution

When the cache is bypassed (e.g., unbound E scans, `DisableCache: true`), CRDT resolution is applied at the storage scan level via `CRDTResolvingIterator`.

**Key insight**: The EATV/AETV index ordering IS resolution. Tx is stored with bitwise NOT for descending order, so forward scan returns highest Tx first. For LWW (CardinalityOne), the first entry is the winner.

| Cardinality | Resolution Strategy |
|-------------|---------------------|
| One | Emit first datom per (E, A), skip rest |
| Many | Track emitted values + tombstones, emit immediately |
| Vector | Accumulate minimal state, reconstruct at (E, A) boundary |
| Unknown / schemaless | Same as One — LWW, first entry wins |

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

**Three database modes**, carried as `mode` and `asOf` on `Database` and on the `PatternMatcher` it mints:

| Mode | API | `mode` | `asOf` | CRDT resolution | Tx filtering |
|------|-----|--------|--------|-----------------|--------------|
| Latest | `d.Query(q)` | `latestMode` | zero | Yes | No |
| As-of | `d.AsOf(elemID).Query(q)` | `asOfMode` | `elemID` | Yes | Yes |
| History | `d.History().Query(q)` | `historyMode` | zero | No | No |

A database's EA cache holds the latest resolutions. An as-of handle keeps its resolutions in a cache of its own (see [As-of Views](#as-of-views)). History queries bypass the cache and scan storage directly.

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

The Database implements this interface. A latest or as-of read walks the entity's EATV range once, discovering and resolving every attribute from the same scan and populating the EA cache as it goes. A history read discovers the attributes by an EAVT scan and resolves each through `ResolveEntityAttributes`.

### Wildcard Pulls

Wildcard pulls (`[*]`) resolve all attributes for an entity:

```go
result, _ := d.Pull(entity, "[*]")
// Returns CRDT-resolved values:
// - CardinalityOne: single value (LWW)
// - CardinalityMany: []interface{} (add-wins set)
// - CardinalityVector: []interface{} (RGA ordered list)
```

**With declarations**: the attributes the entity carries among the stored declarations and the engine's words. An attribute the entity carries that the store does not declare is not answered, and a definition inferred from the ops is not a declaration.

**Without declarations**: every attribute the entity carries.

**The id**: beside the attributes, the entity under `db/id`, which a wildcard names as it names every attribute. An entity carrying no attribute answers nil, so no id stands alone under a wildcard.

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
