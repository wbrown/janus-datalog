# Janus Datalog Key Encoding: Designed for CRDTs

This document provides a comprehensive analysis of how Janus Datalog's key encoding is uniquely designed to support CRDT (Conflict-free Replicated Data Type) semantics, including comparisons with other CRDT systems.

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [The Storage Foundation](#the-storage-foundation)
3. [Key Structure](#key-structure)
4. [The Key IS the Value](#the-key-is-the-value)
5. [The Bitwise NOT Trick](#the-bitwise-not-trick)
6. [Seven Indices for CRDT Semantics](#seven-indices-for-crdt-semantics)
7. [Unified Current and History](#unified-current-and-history)
8. [L85 Encoding: Representation Only](#l85-encoding-representation-only)
9. [Comparison with Other CRDT Systems](#comparison-with-other-crdt-systems)
10. [The Cache Architecture](#the-cache-architecture)
11. [Why Not Write-Through Caching](#why-not-write-through-caching)
12. [Performance Analysis](#performance-analysis)
13. [Summary](#summary)

---

## Executive Summary

Most CRDT systems treat storage as a **serialization problem**: "How do we efficiently encode and replay an operation log?"

Janus treats it as an **indexing problem**: "How do we structure keys so CRDT resolution is just a prefix scan?"

**The key insight: The key IS the value.**

In traditional databases, keys point to data stored elsewhere. In Janus, the key contains the complete datom: `[E][A][V][Tx][Op]`. There's no pointer chasing, no separate row fetch. Every index is a covering index by default. This is why CRDT resolution can be O(1) - the answer is in the key itself, not behind a pointer.

The result:
- **O(1) LWW resolution** even without caching (965ns to resolve current value when 1000 historical versions exist)
- **CRDT semantics with zero overhead** (actually 1.9× faster than pre-CRDT implementation)
- **No reconstruction step** for Last-Writer-Wins operations
- **Unified current/history storage** - no separate segments like Datomic

---

## The Storage Foundation

### Why LSM-Tree (BadgerDB)

Janus uses BadgerDB, an LSM-tree (Log-Structured Merge-tree) based key-value store.

| Aspect | B-tree | LSM-tree (BadgerDB) |
|--------|--------|---------------------|
| **Write pattern** | In-place updates | Append-only |
| **CRDT fit** | Poor - overwrites history | Perfect - all writes preserved |
| **Write amplification** | High for random writes | Low - sequential writes to memtable |
| **Range scans** | Good | Good (with sorted SSTables) |
| **Concurrency** | Page-level locking | Lock-free writes |

CRDTs require **append-only semantics** - every write is preserved with its ElementID. B-trees do in-place updates which would overwrite history. LSM-trees naturally preserve all versions because writes go to memtables and get flushed as immutable SSTables.

---

## Key Structure

### Fixed-Size Components

The key structure uses fixed-size components for efficient indexing:

```
E (Entity)     : 20 bytes  (SHA1 hash)
A (Attribute)  : 32 bytes  (direct string or SHA256 if longer)
Tx (ElementID) : 16 bytes  (Lamport clock + ReplicaID)
Op             : 1 byte    (CRDT operation type)
V (Value)      : Variable  (sole variable-length field, with 1-byte type prefix)
AfterRef       : 16 bytes  (optional, only for RGA operations)
```

**V is the only variable-length field.** Its position varies by index type (first in VAET, middle in EAVT, etc.), but because all other components are fixed-size, decoding can determine V's boundaries by subtracting known sizes from the key length.

### ElementID: The CRDT Heart

The 16-byte `Tx` field is an **ElementID** with two parts:

```go
type ElementID struct {
    Lamport   uint64  // Logical timestamp (8 bytes, big-endian)
    ReplicaID uint64  // Database instance identifier (8 bytes, big-endian)
}
```

**Properties:**
- **Total ordering**: Lexicographic on (Lamport, ReplicaID)
- **Deterministic tie-breaking**: When replicas have same Lamport, ReplicaID breaks ties
- **Fixed size**: 16 bytes regardless of replica count (unlike vector clocks)

### Why Fixed Sizes?

1. **Predictable prefix ranges**: Scanning all values for `(E, A)` needs exactly 52 bytes of prefix (20 + 32). No length prefixes, no parsing.

2. **Variable-length V is decodable**: Since V is the *only* variable-length field, its boundaries can be computed by subtracting known sizes:
   ```go
   // For EAVT: [prefix:1][E:20][A:32][V:?][Tx:16][Op:1][AfterRef?:16]
   // V starts at offset 53, ends at len(key) - 16 - 1 - (16 if hasAfterRef)
   vStart := 1 + 20 + 32
   vEnd := len(key) - 16 - 1
   if hasAfterRef { vEnd -= 16 }
   value := key[vStart:vEnd]
   ```

3. **Efficient comparisons**: Comparing keys is just `bytes.Compare()` - no decoding needed for sort order.

4. **Index selection by cardinality**: The system chooses between EAVT and EATV based on whether V or Tx comes first. Fixed sizes make this reordering trivial - just rearrange the concatenation order.

### Why Not Vector Clocks?

Vector clocks grow with replica count: `{replicaA: 42, replicaB: 17, replicaC: 99, ...}`

| Replicas | Vector Clock Size | ElementID Size |
|----------|-------------------|----------------|
| 2 | ~32 bytes | 16 bytes |
| 10 | ~160 bytes | 16 bytes |
| 100 | ~1.6 KB | 16 bytes |
| 1000 | ~16 KB | 16 bytes |

**Trade-off**: ElementID requires Lamport clock coordination (replicas must see each other's writes to advance). Vector clocks allow fully independent progress. For a database (vs. real-time text editing), the coordination requirement is acceptable.

### CRDT Operation Types

```go
type CRDTOp uint8

const (
    OpNone         CRDTOp = 0  // Cardinality-one (LWW register)
    OpCRDTAdd      CRDTOp = 1  // Set add operation
    OpCRDTRemove   CRDTOp = 2  // Set remove operation
    OpRGAInsert    CRDTOp = 3  // Vector element insert
    OpRGATombstone CRDTOp = 4  // Vector element deletion
)

// HasAfterRef makes Op self-describing
func (op CRDTOp) HasAfterRef() bool {
    return op == OpRGAInsert || op == OpRGATombstone
}
```

The Op byte in the key tells decoders whether 16 more bytes (AfterRef) follow. No external metadata needed - keys are self-describing.

---

## The Key IS the Value

This is perhaps the most fundamental design insight in Janus, and it's easy to miss.

### Traditional Database Model

```
Index Key → Pointer → Row/Document → Extract Field
```

In a traditional database:
- The index key contains just enough to locate data
- Following the pointer requires a separate I/O operation
- "Covering index" is a special optimization where you copy fields into the index

### Janus Model

```
Key: [prefix][E][A][V][Tx][Op][AfterRef?] → Done. That's the data.
```

In Janus:
- The key contains the COMPLETE datom
- The BadgerDB "value" portion is essentially empty
- There's nothing to dereference - the key IS the data

### Why This Matters

| Aspect | Traditional | Janus |
|--------|-------------|-------|
| Read one datom | Key lookup + row fetch | Key lookup only |
| Index overhead | Keys + pointers + separate data | Just keys (which ARE data) |
| Covering index | Special optimization | Default for ALL indices |
| Range scan | Fetch keys, then fetch each row | Scan keys, done |

### The CRDT Connection

This is why CRDT resolution can be truly O(1):

**Traditional approach:**
1. Find key for (E, A) → pointer
2. Fetch value via pointer
3. Resolve CRDT semantics
4. Return result

**Janus approach:**
1. Seek to (E, A) prefix
2. Read first key → **the key contains the answer**

There's no step 2-3. The key's sort order IS the resolution, and the key contains the value.

### The "6× Indexing Overhead" Reframe

The seven indices aren't "7× overhead pointing to data stored elsewhere."

Each index IS a complete copy of the data in a different sort order. There's no separate "row store" being indexed. The storage cost is:

```
7 indices × datom size = total storage
```

Not:

```
1 row store + 6 indices pointing to it = total storage
```

This is more like 6 different sort orders of the same data than 6 indices plus a base table. The mental model from relational databases ("indices are overhead on top of tables") doesn't apply.

### What This Enables

1. **Every access pattern is O(log N) + O(k)** - seek to prefix, scan k results, done
2. **No "index-only scan" optimization needed** - all scans are index-only
3. **CRDT resolution in the key layer** - no post-fetch processing
4. **Predictable performance** - no variance from pointer chasing

---

## The Bitwise NOT Trick

The critical CRDT enabler is encoding Tx with bitwise NOT:

```go
func txToDescending(tx [16]byte) [16]byte {
    var result [16]byte
    binary.BigEndian.PutUint64(result[0:8], ^binary.BigEndian.Uint64(tx[0:8]))
    binary.BigEndian.PutUint64(result[8:16], ^binary.BigEndian.Uint64(tx[8:16]))
    return result
}
```

### What This Achieves

- Higher ElementIDs (newer writes) sort **lower** in byte order
- Forward scans encounter **newest entries first**
- **O(1) current value lookup** - first entry = highest ElementID = current value

### Example

```
ElementID L100@R5 → Lamport=100 → encoded as ^100 → 0xFFFFFFFFFFFFFF9B
ElementID L200@R5 → Lamport=200 → encoded as ^200 → 0xFFFFFFFFFFFFFF37

L200 > L100, but ^200 < ^100 in unsigned byte comparison
Forward scan: encounters L200 first (the current value)
```

### Why This Matters for CRDT

- **Last-Writer-Wins Resolution**: For cardinality-one attributes, the first entry in EATV index is automatically the highest ElementID (current value)
- **No separate "latest version" index**: The sort order IS the resolution
- **O(1) without caching**: Just seek and read first entry

---

## Seven Indices for CRDT Semantics

The system maintains seven different index orderings, each optimized for specific CRDT operations:

| Index | Order | CRDT Use | When Selected |
|-------|-------|----------|---------------|
| **EATV** | E→A→Tx↓→V | Cardinality-One (LWW), E-primary | E+A bound, cardinality=one |
| **EAVT** | E→A→V→Tx↓ | Cardinality-Many (add-wins) | E+A bound, cardinality=many |
| **AETV** | A→E→Tx↓→V | Cardinality-One (LWW), A-primary | A bound, E from input, cardinality=one |
| **AEVT** | A→E→V→Tx↓ | Attribute scans (no CRDT resolution) | A bound, E unbound, cardinality=many |
| **AVET** | A→V→E→Tx↓ | Value lookups | A+V bound |
| **VAET** | V→A→E→Tx↓ | Reverse reference lookup | V bound (for refs) |
| **TAEV** | Tx↓→A→E→V | Transaction log / time-travel | Time-based queries |

### The Key Insight: V vs Tx Position

The position of **V relative to Tx** determines CRDT semantics:

- **EATV** (Tx before V): First entry after `(E,A)` prefix = current value (LWW)
- **EAVT** (V before Tx): Same values grouped together for add/remove comparison (add-wins)

### Key Formats (Binary Encoder)

```
EAVT: [prefix][E][A][type+value][Tx↓][Op][AfterRef?]  - groups by value for add-wins
EATV: [prefix][E][A][Tx↓][type+value][Op][AfterRef?]  - first entry is current (LWW), E-primary
AETV: [prefix][A][E][Tx↓][type+value][Op][AfterRef?]  - first entry is current (LWW), A-primary
AEVT: [prefix][A][E][type+value][Tx↓][Op][AfterRef?]  - by attribute (Tx ascending)
AVET: [prefix][A][type+value][E][Tx↓][Op][AfterRef?]  - value lookup
VAET: [prefix][type+value][A][E][Tx↓][Op][AfterRef?]  - reverse refs (V first!)
TAEV: [prefix][Tx↓][A][E][type+value][Op][AfterRef?]  - transaction log

AfterRef? = 16 bytes present only if Op ∈ {OpRGAInsert(3), OpRGATombstone(4)}
```

**Note:** `type+value` is the sole variable-length component (1-byte type prefix + variable data). Its position varies by index, but decoding works because:
- All other components have known fixed sizes
- The decoder subtracts fixed sizes from key length to find V's boundaries
- Example for EAVT: `V = key[1+20+32 : len(key)-16-1-maybeAfterRef]`

---

## Unified Current and History

### Datomic's Segmented Approach

Datomic separates storage into two segments:

```
Current Segment: EAVT, AEVT, AVET, VAET (4 indices)
    ↓ on update, old values move to ↓
History Segment: Separate storage with its own indices
```

**On every write:**
1. Write new value to current segment (4 index updates)
2. Move old value to history segment (additional writes)
3. Both segments maintain their own index structures

**To query current state:** Query the current segment
**To query history:** Use `d/history` to access the history segment

This means Datomic effectively maintains **8 index structures** (4 current + 4 history), plus the overhead of moving data between segments on updates.

### Janus's Unified Approach

Janus stores ALL datoms (current and historical) in the SAME indices:

```
Single Index Set: EAVT, EATV, AEVT, AETV, AVET, VAET, TAEV (7 indices)
    Contains: All datoms, all time, one structure
```

**On every write:**
1. Write new datom with new ElementID (7 index updates)
2. That's it. No data movement.

**To query current state:** Seek to (E,A), read first entry (bitwise NOT makes newest sort first)
**To query history:** Same index, keep scanning past the first entry

### The Comparison

| Aspect | Datomic | Janus |
|--------|---------|-------|
| Index structures | 4 current + 4 history = 8 | 6 unified |
| On write | Update current + move old to history | Write once |
| Data movement | Yes (current → history) | None |
| Current lookup | Query current segment | First entry in EATV |
| History lookup | Separate `d/history` call | Same index, scan |
| Storage duplication | Some (during transitions) | None |

### Why Unified Is Better for CRDTs

**The bitwise NOT trick does double duty:**

1. **Provides O(1) current value** (what Datomic's current segment provides)
   - Seek to (E,A) prefix, first entry = highest ElementID = current value

2. **Provides full history** (what Datomic's history segment provides)
   - Same index, keep scanning = all historical values in descending time order

**One structure, both access patterns.** No segment management, no data movement, no separate history indices.

### The Real Index Count Comparison

| System | What You Get | Actual Index Structures |
|--------|--------------|------------------------|
| Datomic | Current + History | 8 (4 per segment) |
| Janus | Current + History + CRDT Resolution | 6 (unified) |

Janus provides MORE functionality (CRDT semantics, time-travel via TAEV) with FEWER index structures because the unified design with bitwise NOT eliminates the need for segmentation.

---

## L85 Encoding: Representation Only

### Common Misconception

L85 is often assumed to be the storage format. **It is not.**

### What L85 Actually Is

L85 is a custom Base85 encoding with sort-order preservation, used for **human-readable representation**:

```go
const L85Alphabet = "!$%&()+,-./" +
    "0123456789:;<=>@" +
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ[]_`" +
    "abcdefghijklmnopqrstuvwxyz{}"
```

**Properties:**
- 25% overhead (better than Base64's 33%)
- Lexicographic sort of encoded strings matches byte order
- Terminal safe (all printable ASCII, no quotes/spaces/backslashes)
- Fixed output: 20 bytes → 25 characters, 16 bytes → 20 characters

### Where L85 Is Used

- `Identity.L85()` method for human-readable display
- Export/import format: `#identity "L85hash"`
- Available as alternative encoder for debugging (`L85Strategy`)

### What Production Actually Uses

```go
// From database.go:103
store, err := NewBadgerStore(opts.Path, NewKeyEncoder(BinaryStrategy))
```

**Production uses raw binary (`BinaryStrategy`)** - 25% smaller keys, faster comparisons, no encode/decode overhead in hot paths.

### The CRDT Benefits Come From

1. **Key structure** (fixed-size components, component ordering)
2. **Bitwise NOT on Tx** (descending sort = newest first)
3. **Index selection** (EATV vs EAVT based on cardinality)
4. **LSM-tree properties** (append-only preserves all writes)

Not from encoding format.

---

## Comparison with Other CRDT Systems

### System Overview

| System | Primary CRDT Mechanism | Ordering Primitive | Storage Model |
|--------|----------------------|-------------------|---------------|
| **Janus** | Key structure + indices | ElementID (Lamport + ReplicaID) | LSM-tree with 6 indices |
| **Automerge** | Operation log + columnar encoding | OpID (Actor + Counter) | Compressed chunks |
| **Yjs** | YATA algorithm | Vector clocks per document | Binary deltas |
| **Riak** | Dotted Version Vectors | DVV (dot + vector clock) | Bitcask/LevelDB per vnode |

### Automerge: Operation-Centric Columnar Encoding

[Automerge](https://automerge.org/automerge-binary-format-spec/) treats documents as operation logs with heavy compression:

```
Operation = (OpID, ObjID, Key, Value, Predecessors, Successors)
OpID = (ActorID, Counter)
```

**Storage approach:**
- Operations stored as columnar tables (like Parquet)
- Run-length + delta encoding: `[1,2,3,5,7]` → `[(3,+1),(2,+2)]`
- Chunks identified by `[docID, chunk-type, chunk-hash]`
- DEFLATE compression on top

**Trade-off:** Optimized for bandwidth and storage size, but requires decompression and reconstruction to query. You can't efficiently ask "what's the current value of field X?" without loading the document.

### Yjs: Vector Clocks + Binary Deltas

[Yjs architecture](https://www.bartoszsypytkowski.com/yrs-architecture/) uses per-document vector clocks:

```
StateVector = { replicaA: 42, replicaB: 17, ... }
Delta = operations where op.clock > peer.stateVector[op.replica]
```

**Storage approach:**
- Deltas encoded in custom binary format
- Sync protocol: send StateVector → receive missing delta
- Optimized for real-time text collaboration (YATA algorithm)

**Trade-off:** Excellent for collaborative editing, but vector clocks grow with replica count. Not designed for database-style queries.

### Riak: Dotted Version Vectors

[Riak](https://docs.riak.com/riak/kv/latest/developing/data-types/index.html) introduced DVVs to fix sibling explosion:

```
DVV = { dot: (replica, counter), vector: {replicaA: n, ...} }
```

**Storage approach:**
- Key-value with version metadata per key
- Operations-based API (increment counter, add to set)
- Delta-state internally for efficiency

**Trade-off:** Mature distributed system, but CRDT types are separate from regular data. You choose "counter" or "set" at schema time.

### Janus: CRDT Semantics in the Key Structure

Janus takes a different approach - **the key encoding IS the CRDT mechanism**:

```
Key = [Index][E:20][A:32][Tx↓:16][V:var][Op:1][AfterRef?:16]
Tx = (Lamport:8, ReplicaID:8) with bitwise NOT
```

**What makes this unique:**

1. **No separate operation log** - datoms ARE the operations, stored directly in indices

2. **Index layout determines CRDT semantics:**
   - EATV (Tx before V): First entry = current value (LWW)
   - EAVT (V before Tx): Same values grouped (add-wins comparison)
   - No runtime interpretation needed

3. **O(1) resolution via sort order:**
   ```
   // Automerge: load doc → decompress → replay ops → get value
   // Yjs: load doc → apply deltas → traverse structure
   // Janus: seek to (E,A) prefix → read first entry → done
   ```

4. **Queryable history without reconstruction:**
   ```clojure
   ;; Get all historical values - just scan the index
   [:find ?name ?tx :where [?e :person/name ?name ?tx] [(history)]]
   ```

5. **Scalar ElementID vs Vector Clocks:**
   - Automerge/Yjs: Vector grows with replicas
   - Janus: Fixed 16 bytes regardless of replica count

### Feature Comparison

| Aspect | Automerge | Yjs | Riak | Janus |
|--------|-----------|-----|------|-------|
| **Current value lookup** | O(n) reconstruct | O(n) traverse | O(1) but separate type | O(1) first entry |
| **History query** | Full doc load | Not designed for | Limited | Native index scan |
| **Ordering size** | O(replicas) | O(replicas) | O(replicas) | O(1) fixed 16 bytes |
| **Compression** | Heavy (columnar+DEFLATE) | Binary deltas | None | LSM compaction |
| **Query model** | JSON paths | Text positions | Key-value | Datalog |
| **CRDT types** | JSON structure | Text/Array focused | Explicit (counter/set/map) | Schema-driven (one/many/vector) |

---

## The Cache Architecture

### What "Cache" Means Per System

| System | "Cache" Is... | Authoritative Source |
|--------|---------------|---------------------|
| **Automerge** | In-memory document (operations + materialized) | The document itself |
| **Yjs** | In-memory document (YATA structure) | The document itself |
| **Riak** | Per-key in-memory or Bitcask/LevelDB | Distributed vnodes |
| **Janus** | Resolved views in `sync.Map` | BadgerDB storage |

### Janus Cache Structure

```go
type Cache struct {
    // Per-(E,A) resolved views
    entries sync.Map // map[CacheKey]*CacheEntry

    // Per-(E,A) max ElementID tracking - updated on every write
    maxVersions sync.Map // map[CacheKey]ElementID
}

type CacheEntry struct {
    version     ElementID           // Max ElementID when computed
    cardinality Cardinality

    // ONE of these populated based on cardinality:
    oneValue    any                 // LWW: single value
    manySet     map[any]bool        // Add-wins: set members
    vectorList  []any               // RGA: ordered elements
    vectorIndex []ElementID         // RGA: position → ElementID
}
```

**Key insight:** The cache stores **resolved views**, not operation history.

### Cache Lifecycle

```
WRITE PATH (Transaction.Commit):
┌─────────────────────────────────────────────────────────────┐
│ for each datom:                                             │
│   1. Write to BadgerDB (all 6 indices)                      │
│   2. cache.UpdateMaxVersion(key, elemID)  ← Track newest    │
│                                                             │
│ After all writes:                                           │
│   3. cache.Invalidate(touched)  ← Remove stale entries      │
└─────────────────────────────────────────────────────────────┘

READ PATH (with cache):
┌─────────────────────────────────────────────────────────────┐
│ cache.GetOrResolve(key, resolver):                          │
│   1. Load entry from cache                                  │
│   2. Check: entry.version == maxVersions[key]?              │
│      YES → Return cached (O(1))                             │
│      NO  → Rebuild from storage, store in cache             │
└─────────────────────────────────────────────────────────────┘

READ PATH (without cache - direct storage):
┌─────────────────────────────────────────────────────────────┐
│ Direct storage scan:                                        │
│   LWW: Seek EATV(E,A) → read first entry → done            │
│   Set: Scan AEVT(A,E) → add-wins resolution                │
│   Vector: Scan EATV(E,A) → RGA reconstruction              │
└─────────────────────────────────────────────────────────────┘
```

### The Freshness Check (O(1))

```go
// On every write:
cache.UpdateMaxVersion(key, elemID)  // O(1) map update

// On every read:
if entry.version == maxVersions[key] {
    return entry  // Fresh!
}
// Stale - rebuild from storage
```

No storage seek needed to check freshness. The `maxVersions` map is updated atomically on commit.

### Why the Cache Exists (Asymmetric by Cardinality)

| Cardinality | Without Cache | With Cache | Cache Benefit |
|-------------|---------------|------------|---------------|
| **One (LWW)** | O(log N) seek + O(1) read | O(1) map lookup | Nice-to-have (~10× faster) |
| **Many (add-wins)** | O(n) scan + resolution | O(1) map lookup | **Critical** (avoids O(n)) |
| **Vector (RGA)** | O(n log n) reconstruction | O(1) map lookup | **Critical** (avoids O(n log n)) |

For LWW, the cache is optional optimization. For sets and vectors, **the cache is essential for repeated access**.

---

## Why Not Write-Through Caching

### The Question

Why does Janus use invalidation-based caching instead of write-through (updating cache on write)?

### The Cache Stores Resolved Views, Not Operations

```go
type CacheEntry struct {
    oneValue    any              // Just the resolved value
    manySet     map[any]bool     // Just the set members (not add/remove history)
    vectorList  []any            // Just the ordered elements (not the RGA graph)
    vectorIndex []ElementID      // Position mapping
}
```

Write-through would require either:
1. Re-doing full resolution on every write (expensive)
2. Storing enough metadata to incrementally update (complex)

### Why Write-Through Fails Per Cardinality

#### For LWW (Cardinality-One)

Write-through seems simple:
```go
// Hypothetical write-through for LWW
if newElemID > cachedEntry.version {
    cachedEntry.oneValue = newValue
    cachedEntry.version = newElemID
}
```

But this breaks with concurrent replicas:
```
Replica A cache: version=L100, value="Alice"
Replica B writes: L101, value="Bob" → syncs to A
Replica A writes: L102, value="Carol"

Without seeing B's write first, A's cache is wrong.
The storage has all three writes; the cache has only what A saw.
```

The cache can't know if a higher-ElementID write exists that it hasn't seen yet. Only storage is authoritative.

#### For Sets (Cardinality-Many)

Write-through is impossible without storing more:

```go
// Cache stores:
manySet map[any]bool  // Just: {"admin": true, "user": true}

// Write-through for Add("admin"):
// Can we just do manySet["admin"] = true?
// NO - what if there's a Remove("admin") with higher Lamport?

// Write-through for Remove("admin"):
// Can we just delete manySet["admin"]?
// NO - what if there's an Add("admin") with same Lamport? (add-wins)
```

To do write-through, the cache would need:
```go
// Hypothetical write-through cache for sets:
type SetCacheEntry struct {
    perValueMaxAdd    map[any]uint64  // value → highest add Lamport
    perValueMaxRemove map[any]uint64  // value → highest remove Lamport
    resolvedSet       map[any]bool    // derived from above
}
```

That's essentially duplicating the storage structure.

#### For Vectors (Cardinality-Vector)

Write-through is O(n log n) - same as rebuild:

```go
// Cache stores:
vectorList  []any        // ["a", "b", "c"]
vectorIndex []ElementID  // [E1, E2, E3]

// Write-through for Insert("X", AfterRef=E1):
// Where does "X" go?
// - Find E1's position
// - Find all other elements with AfterRef=E1
// - Sort by ElementID to find insertion point
// This IS RGA reconstruction
```

There's no incremental update for RGA that's cheaper than rebuild.

### The Trade-off

| Approach | Write Cost | Read (miss) Cost | Complexity |
|----------|------------|------------------|------------|
| **Write-through** | O(resolution) per write | O(1) | High (CRDT logic in cache) |
| **Invalidate** | O(1) per write | O(resolution) on miss | Low (cache is just a map) |

### Why Janus Chose Invalidation

1. **Writes are frequent, reads are repeated** - Better to pay O(n) once on first read than O(n) on every write

2. **Storage is authoritative** - With replicas, cache can't know if it's seen all writes. Rebuild from storage guarantees correctness.

3. **Simplicity** - Cache is just `sync.Map`. No CRDT logic in cache layer.

4. **Asymmetric benefit** - LWW doesn't need cache (storage is O(1)). Sets/vectors benefit from caching reads, not writes.

### How Other Systems Handle This

**Automerge/Yjs**: They essentially DO write-through - they keep the full document (operation log) in memory and apply operations incrementally. But:
- They're not caches, they're the primary data structure
- They store operations, not resolved views
- Document IS the source of truth (no separate storage layer)

**Janus**: Storage is source of truth. Cache is derived. Invalidate-on-write is the only correct approach when:
1. Cache stores resolved views (not operations)
2. Multiple writers might exist (replicas)
3. Storage might have writes cache hasn't seen

---

## Performance Analysis

**Note:** All measured times are **per-operation** (ns/op from Go benchmarks). For example, "965ns" for LWW means each individual lookup takes 965ns, not that 1000 lookups complete in 965ns total.

### Storage Type Comparison

**Critical context:** These systems have fundamentally different storage models:

| System | Storage Type | Durability | Persistence |
|--------|-------------|------------|-------------|
| **Automerge** | Memory-only (in-process) | None until explicit save | Requires external storage layer |
| **Yjs** | Memory-only (in-process) | None until explicit save | Requires external storage layer |
| **Riak** | Distributed durable (Bitcask/LevelDB) | Immediate (replicated) | Built-in |
| **Janus** | Local durable (BadgerDB LSM) | Immediate (WAL + fsync) | Built-in |

**This means:**
- Automerge/Yjs "writes" are memory operations - fast but not durable
- Janus writes go to WAL (Write-Ahead Log) immediately - durable on commit
- Comparing Automerge/Yjs write speed to Janus is comparing memory ops to disk I/O
- For fair comparison, add Automerge/Yjs persistence layer overhead (~1-10ms to save)

### Definitions

| Symbol | Meaning |
|--------|---------|
| **N** | Total items in database/document |
| **n** | Operations for specific (E,A) pair (Janus) or document history (Automerge/Yjs) |
| **k** | Current live elements (after CRDT resolution) |
| **s** | Siblings/concurrent versions (Riak) |

### Cold Start (Nothing in Memory)

| System | Operation | Complexity | Measured Time | What's Happening |
|--------|-----------|------------|---------------|------------------|
| **Automerge** | Load document | O(n) | 10-100ms typical | Decompress chunks, build op tree |
| **Yjs** | Load document | O(n) | 5-50ms typical | Parse binary, build YATA structure |
| **Riak** | First read | O(s × log N) | ~1-10ms | Fetch from vnode, DVV resolution |
| **Janus LWW** | First read | O(log N) | **~1µs** | LSM seek, read 1 entry |
| **Janus Set (k=10)** | First read | O(log N + n) | ~3-4µs | LSM seek, scan n ops, resolve |
| **Janus Set (k=100)** | First read | O(log N + n) | ~27µs | Same, more ops |
| **Janus Vector (k=10)** | First read | O(log N + n log n) | ~21µs | LSM seek, RGA reconstruction |
| **Janus Vector (k=100)** | First read | O(log N + n log n) | ~205µs | Same, more elements |

### Warm State (Document/Cache Loaded)

| System | Operation | Complexity | Measured Time | What's Happening |
|--------|-----------|------------|---------------|------------------|
| **Automerge** | Read field | O(1) to O(log k) | ~100ns-1µs | Tree traversal in memory |
| **Yjs** | Read position | O(1) | ~50-100ns | Direct index access |
| **Riak** | Read (cached) | O(1) | ~100ns-1µs | Memory lookup |
| **Janus LWW** | Read (cached) | O(1) | ~100ns | `sync.Map` lookup |
| **Janus Set** | Read (cached) | O(1) | ~100ns | `sync.Map` lookup |
| **Janus Vector** | Read (cached) | O(1) | ~100ns | `sync.Map` lookup |

### Write Path

| System | Operation | Durability | Measured Time | What's Happening |
|--------|-----------|------------|---------------|------------------|
| **Automerge** | Mutate field | Memory only | ~1-10µs | Append op to in-memory log |
| **Yjs** | Insert/delete | Memory only | ~1-5µs | YATA insertion in memory |
| **Riak** | Write | Durable (replicated) | ~1-10ms | Vnode write, DVV update, replication |
| **Janus** | Write (any) | Durable (WAL) | **~25-35µs** | WAL append + memtable insert × 6 indices |

**Note on Janus write performance:**
- Writes go to WAL (Write-Ahead Log) first - this is fast sequential I/O
- Memtable insertions are in-memory
- LSM compaction (merging SSTables) happens in background, doesn't block writes
- The 25-35µs includes fsync for durability guarantee
- Without fsync (memory-only mode), Janus would be comparable to Automerge/Yjs

### Cache Miss After Writes

| System | Scenario | Complexity | Measured Time | What's Happening |
|--------|----------|------------|---------------|------------------|
| **Automerge** | Reload after eviction | O(n) | 10-100ms | Full reconstruction |
| **Yjs** | Reload after eviction | O(n) | 5-50ms | Full reconstruction |
| **Riak** | Sibling resolution | O(s) | ~100µs-1ms | Compare s version vectors |
| **Janus LWW (1000 versions)** | Cache miss | O(log N) | **965ns/lookup** | Seek + read first entry |
| **Janus Set (50 members, clean)** | Cache miss | O(n) | ~14µs | Scan + add-wins |
| **Janus Set (100 adds, 50 removes)** | Cache miss | O(n) | ~38µs | Scan + tombstone filtering |
| **Janus Vector (10 elements)** | Cache miss | O(n log n) | ~21µs | RGA reconstruction |

### The Hidden Constants

| Operation | O(1) Actually Means... |
|-----------|------------------------|
| **Cache lookup** | Hash computation + map probe + pointer chase (~50-100ns) |
| **LSM seek** | Binary search memtable + check bloom filters + SSTable seek (~500ns-2µs) |
| **Read 1 entry** | Key decode + value decode (~200-500ns) |
| **Freshness check** | Two map lookups + ElementID comparison (~50ns) |

### What "n" Looks Like in Practice

| Scenario | Automerge/Yjs n | Janus n |
|----------|-----------------|---------|
| **User profile** | All edits to entire document | Edits to that one (E,A) pair |
| **Collaborative doc (1 hour)** | ~1,000-10,000 ops | ~10-100 per field |
| **Long-lived entity (1 year)** | ~100,000+ ops | ~100-1,000 per field |

**Key insight**: Janus's n is per-(E,A), not per-document. A database with 1M datoms might have n=10 for a specific (E,A) pair.

### Reading One Field: Apples-to-Apples

| System | Cold | Warm | Notes |
|--------|------|------|-------|
| **Automerge** | O(n) doc load + O(1) field | O(1) | Must load entire document first |
| **Yjs** | O(n) doc load + O(1) field | O(1) | Must load entire document first |
| **Janus LWW** | O(log N) | O(1) | Only touches that (E,A) |
| **Janus Set** | O(log N + n) | O(1) | n = ops for that (E,A) only |
| **Janus Vector** | O(log N + n log n) | O(1) | n = ops for that (E,A) only |

### When Each System Wins

| Scenario | Winner | Why |
|----------|--------|-----|
| **Cold read of single LWW field** | Janus | ~1µs vs 10-100ms document load |
| **Cold read of small set** | Janus | ~3-4µs vs document load |
| **Hot read of any field** | Tie | All ~100ns from cache/memory |
| **Bulk read of many fields (same entity)** | Automerge/Yjs | One document load serves all fields |
| **Memory-only write throughput** | Automerge/Yjs | No durability overhead |
| **Durable write throughput** | Janus | 25-35µs vs Riak's 1-10ms |
| **Large document, read one field** | Janus | Don't pay for unrequested data |
| **Real-time collaboration** | Yjs | Optimized for keystroke-level ops |
| **Query across entities** | Janus | Has indices; others must scan |
| **Crash recovery** | Janus/Riak | Durable storage; Automerge/Yjs lose uncommitted |

### The Real Trade-off

| System | Storage Model | Optimized For | Pays Cost When |
|--------|---------------|---------------|----------------|
| **Automerge/Yjs** | Memory + external persistence | Real-time collaboration, single document hot | Durability, querying across docs, cold starts |
| **Riak** | Distributed durable | High availability, partition tolerance | Latency (network hops), complexity |
| **Janus** | Local durable (LSM) | Queryable CRDT database, cold reads, durability | Requires separate replication layer for distribution |

---

## Summary

### Key Design Choices

| Design Choice | Why It Matters |
|---------------|----------------|
| **The key IS the value** | No pointer chasing, every index is covering, O(1) data access |
| **Fixed 16-byte ElementID** | O(1) size regardless of replica count |
| **Bitwise NOT on Tx** | Forward scan = newest first = O(1) current value |
| **Unified current/history** | 7 indices vs Datomic's 8, no data movement on writes |
| **Seven indices** | Cardinality-aware access patterns |
| **LSM-tree (BadgerDB)** | Natural append-only for CRDT history |
| **Value types for hot paths** | No heap allocation in decode/encode |
| **No separate CRDT layer** | Resolution IS the index access |
| **Invalidation-based cache** | Simple, correct, asymmetric benefit |

### Verified Performance (from PERFORMANCE_STATUS.md)

| Metric | Result | Notes |
|--------|--------|-------|
| **LWW resolution (1000 versions)** | 965ns/lookup | Without cache, direct from storage |
| **Add-wins (50 members)** | 13.8µs | No tombstones |
| **Add-wins (100 adds, 50 removes)** | 37.9µs | With tombstone filtering |
| **RGA vector (10 elements)** | 21.2µs | Reconstruction |
| **RGA vector (100 elements)** | 204.7µs | Linear scaling |
| **Write (any cardinality)** | 25-35µs | Consistent across types |
| **CRDT vs pre-CRDT performance** | **1.9× faster** | After optimization work |

### The Bottom Line

Most databases ask "how do we index our data?" Janus asks "what if the index IS the data?"

Most CRDT systems require loading and reconstructing documents to access data. Janus's key encoding makes CRDT resolution a property of the index structure itself:

- **The key contains the complete datom** - no pointer chasing, no separate row fetch
- **LWW**: Seek + read first key = current value is IN the key (O(1) in practice)
- **Sets**: Scan keys grouped by value + add-wins comparison
- **Vectors**: Load keys + RGA graph traversal
- **Unified storage**: Current and historical values in same indices (vs Datomic's 8 index structures, Janus uses 6)

The cache is optional for LWW, essential for repeated set/vector access, and uses invalidation (not write-through) because it stores resolved views rather than operations.

The result: CRDT semantics with **zero overhead** (actually negative - the engine is faster than before CRDTs were added). This isn't just clever encoding - it's a fundamentally different mental model that required deep understanding of both CRDT theory and storage engine implementation.

---

## References

- [Automerge Binary Document Format](https://automerge.org/automerge-binary-format-spec/)
- [Automerge Storage](https://automerge.org/docs/reference/under-the-hood/storage/)
- [Yrs (Yjs Rust) Architecture](https://www.bartoszsypytkowski.com/yrs-architecture/)
- [Riak Data Types](https://docs.riak.com/riak/kv/latest/developing/data-types/index.html)
- [CRDT Implementations Overview](https://crdt.tech/implementations)
- [CRDT.md](./CRDT.md) - Janus CRDT storage semantics
- [PERFORMANCE_STATUS.md](../../PERFORMANCE_STATUS.md) - Verified benchmarks
