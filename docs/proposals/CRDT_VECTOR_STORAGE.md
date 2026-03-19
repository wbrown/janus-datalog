# CRDT-Based Storage Model

**Status:** Proposal
**Author:** wbrown
**Date:** 2026-01-29

---

## Abstract

**The problem:** Datalog has no ordered collections. Skills, inventory, event logs—anything that needs sequence—requires ugly workarounds that lose queryability or explode into N+1 queries.

**The solution:** Native vector support as a third cardinality type.

**The rabbit hole:** Implementing vectors properly meant rethinking the storage model. Append-at-position needs conflict resolution. Conflict resolution needs unique element identifiers. Unique identifiers need Lamport clocks. And once you have all that, you've accidentally built a CRDT-based system with benefits beyond just vectors:

- **Ordered collections (vectors)** — The original goal
- **History for free** — Append-only means every change is preserved
- **Concurrent writes just work** — CRDTs merge automatically, no lost updates
- **Simpler API** — `Set`, `Add`, `Remove`, `Append`; no manual retractions
- **Multi-node ready** — The same model works distributed

---

## Motivation

### The Ordering Problem

Datalog is fundamentally set-based. Ordered data requires workarounds:

| Current Workaround | Problem |
|-------------------|---------|
| String serialization (`"[\"a\",\"b\",\"c\"]"`) | Loses queryability, parse overhead |
| Indexed entities (`[item :order 0]`) | Entity explosion, N+1 loads |
| Cardinality-many + client sort | No ordering guarantee, sort overhead |

This is the primary motivator for this proposal. Skills, inventory, event logs—anything that needs sequence—currently requires ugly workarounds.

### The History Problem

Current Janus supports history via separate `_HISTORY` indices with an Op flag. This works but:

- Doubles index storage
- Requires explicit `RetractHistory` mode
- History queries use different code path

### The Retraction Problem

Current `tx.Add` for cardinality-one is confusing:

```go
tx.Add(entity, ":name", "Alice")
tx.Add(entity, ":name", "Bob")
tx.Commit()
// Result: TWO datoms exist (bug-prone)
// User expected: just "Bob"
```

Users must manually retract, which is error-prone and loses history.

### The Concurrency Bonus

While solving the ordering problem with CRDTs, we get concurrent-write safety for free. The current storage model uses read-modify-write:

```go
// Current pattern in SaveStruct (writer.go)
existing := db.Load(entity, ":tags")   // READ
toAdd, toRemove := diff(existing, new) // COMPUTE
tx.Retract(entity, ":tags", removed)   // WRITE
tx.Add(entity, ":tags", added)         // WRITE
```

This breaks under concurrent writes:

```
Goroutine A                    Goroutine B
-----------                    -----------
Read tags: ["x", "y"]          Read tags: ["x", "y"]
Add "a", remove "y"            Add "b", remove "x"
Write: ["x", "a"]              Write: ["y", "b"]

Result: Either ["x", "a"] or ["y", "b"] — one writer's changes lost
Expected: ["a", "b"] — both additions, both removals
```

CRDTs eliminate this class of bug—concurrent writes merge automatically.

---

## Background: Lamport Clocks and CRDTs

*This section provides background for readers unfamiliar with distributed systems concepts.*

### The Concurrent Write Problem

When two writers modify the same data simultaneously, we have a conflict:

```
Writer A                           Writer B
--------                           --------
Read vector: ["a", "b"]            Read vector: ["a", "b"]
Append "c"                         Append "d"
Write: ["a", "b", "c"]             Write: ["a", "b", "d"]
```

Who wins? With naive last-write-wins, one append is lost. We want both: `["a", "b", "c", "d"]`.

This happens even in single-process systems (concurrent goroutines) and becomes critical in distributed systems (multiple nodes).

### Lamport Clocks

A **Lamport clock** is a logical timestamp that establishes causal ordering without synchronized physical clocks.

**Rules:**
1. Each process maintains a counter `L`, starting at 0
2. Before each event (read, write), increment: `L = L + 1`
3. When sending data, include your `L` value
4. When receiving data with timestamp `L_remote`: `L = max(L, L_remote) + 1`

**Key property:** If event A *caused* event B (A happened before B in a causal chain), then `L(A) < L(B)`.

```
Process 1              Process 2
---------              ---------
L=1: write X
L=2: send X to P2 ───► L=3: receive X (max(0,2)+1)
                       L=4: write Y (caused by X)
```

We know X happened before Y because `L(X)=2 < L(Y)=4`.

**Limitation:** If `L(A) < L(B)`, A *might* have caused B, or they might be concurrent. Lamport clocks don't distinguish. For that, you need vector clocks (but we don't need them here).

### Unique Identifiers: (Lamport, NodeID)

Lamport clocks can produce the same value on different nodes. To get globally unique IDs:

```
ElementID = (Lamport: uint64, NodeID: uint64)
```

**Comparison (total ordering):**
```go
func (a ElementID) Less(b ElementID) bool {
    if a.Lamport != b.Lamport {
        return a.Lamport < b.Lamport
    }
    return a.NodeID < b.NodeID  // Tiebreaker
}
```

This gives deterministic ordering even for concurrent events.

### CRDTs: Conflict-Free Replicated Data Types

A **CRDT** is a data structure designed so that concurrent modifications automatically merge without conflicts. The key insight: instead of "last write wins," design operations that *commute* (order doesn't matter).

**Example: Grow-only Set (G-Set)**
- Operation: Add(element)
- Merge: Union
- Concurrent adds: both elements appear

```
Node A: Add("x") → {x}
Node B: Add("y") → {y}
Merge: {x} ∪ {y} = {x, y}  // No conflict!
```

**Example: Counter**
- Instead of `value = 5`, store `{A: 3, B: 2}` (who added what)
- Merge: take max per node, sum totals
- Concurrent increments: both count

### RGA: Replicated Growable Array

For ordered sequences (vectors), we use **RGA** (Replicated Growable Array).

**Key idea:** Each element stores *what it was inserted after*, not its position.

```
Element 1: {id: (L1, N1), value: "a", after: HEAD}
Element 2: {id: (L2, N1), value: "b", after: (L1, N1)}
Element 3: {id: (L3, N1), value: "c", after: (L2, N1)}
```

**Reconstruction:** Follow the "after" chain from HEAD.

**Concurrent inserts resolve automatically:**

```
Both nodes insert after element 2:

Node 1: {id: (L4, N1), value: "x", after: (L2, N1)}
Node 2: {id: (L4, N2), value: "y", after: (L2, N1)}

Conflict! Both claim to be after (L2, N1).
Resolution: Sort by ElementID → (L4, N1) < (L4, N2) if N1 < N2

Result: ["a", "b", "x", "y", "c"]  // Deterministic on all nodes
```

**Why this works:**
1. Each element has a globally unique ID
2. The "after" reference is immutable once written
3. Conflicts (same "after") are resolved by sorting IDs
4. All nodes apply the same algorithm → same result

**Tombstones for deletion:**

```
Element 2: {id: (L2, N1), value: "b", after: (L1, N1), tombstone: (L5, N2)}
```

The element stays (for causal ordering) but is skipped during reconstruction.

### Complexity Cost

| Component | Lines of Code | Difficulty |
|-----------|---------------|------------|
| Lamport clock | ~30 | Trivial |
| ElementID comparison | ~10 | Trivial |
| RGA reconstruction | ~60 | One algorithm |
| Tombstone filtering | ~20 | Simple |
| **Total** | **~120** | Well-understood |

The complexity is in *understanding*, not implementation. Once you grasp the model, the code is straightforward.

---

## Design

### Core Insight: ElementID as First-Class Component

Datoms become 5-tuples:

```
(E, A, I, V, T)
 │  │  │  │  └─ Transaction ID (when written to storage)
 │  │  │  └──── Value
 │  │  └─────── ElementID (Lamport, NodeID) - the "Index" slot
 │  └────────── Attribute
 └───────────── Entity
```

The **I** component (historically called "Index") now contains an `ElementID(Lamport, NodeID)` pair (see Index Encoding below for full definition). ElementID encoding varies by cardinality:

| Cardinality | ElementID Encoding | Purpose |
|-------------|-------------------|---------|
| One | Descending `^(Lamport, NodeID)` | First in scan = current value |
| Many | Descending `^(Lamport, NodeID)` | First per value = latest add/remove |
| Vector | Ascending `(Lamport, NodeID)` | RGA traversal order |

**All cardinalities use Lamport clocks.** This provides:
- Causal ordering across concurrent writers
- Deterministic conflict resolution via NodeID tiebreaker
- Multi-node ready from day one

### Index Encoding

**ElementID: The Universal Index Component**

```go
type ElementID struct {
    Lamport uint64  // Logical timestamp (causal ordering)
    NodeID  uint64  // Writer identifier (tiebreaker)
}

// Total ordering - used for ALL conflict resolution
func (a ElementID) Less(b ElementID) bool {
    if a.Lamport != b.Lamport {
        return a.Lamport < b.Lamport
    }
    return a.NodeID < b.NodeID
}

// Encode as 16 bytes
func EncodeElementID(id ElementID) []byte {
    buf := make([]byte, 16)
    binary.BigEndian.PutUint64(buf[0:8], id.Lamport)
    binary.BigEndian.PutUint64(buf[8:16], id.NodeID)
    return buf
}

// Descending encoding for One/Many (higher = first in scan)
func EncodeElementIDDescending(id ElementID) []byte {
    buf := make([]byte, 16)
    binary.BigEndian.PutUint64(buf[0:8], ^id.Lamport)
    binary.BigEndian.PutUint64(buf[8:16], ^id.NodeID)
    return buf
}
```

**Cardinality-One: Highest ElementID Wins**

```go
// Index = ^(Lamport, NodeID) - descending
// First entry in scan = latest value

// Concurrent writes at same Lamport:
//   Node 1 (L5, N1) → "Alice"
//   Node 2 (L5, N2) → "Bob"
// Winner: higher NodeID. If N2 > N1, "Bob" wins.
```

**Cardinality-Many: Add-Wins Semantics**

```go
const OpAdd    = uint8(0)
const OpRemove = uint8(1)

// Index = ^(Lamport, NodeID) ++ OpBit
// OpBit in value, not key - same ElementID can have add and remove

type SetEntry struct {
    ID     ElementID
    Op     uint8  // OpAdd or OpRemove
}

// Concurrent add+remove at same Lamport:
//   Node 1: add "warrior" at L5
//   Node 2: remove "warrior" at L5
// Policy: Add-wins (safer, data preserved)
// "warrior" is present.
```

**Why add-wins?** It's the standard CRDT choice. Deletion is recoverable (just remove again with higher Lamport). Lost additions are not.

**Why descending for One/Many:**

BadgerDB uses lexicographic key ordering. With descending encoding:

| ElementID | Encoded | Key Order |
|-----------|---------|-----------|
| (L3000, N1) | ^(L3000, N1) | First (smallest) |
| (L2000, N1) | ^(L2000, N1) | Second |
| (L1000, N1) | ^(L1000, N1) | Third (largest) |

**First entry in ascending scan = latest value.** O(1) to get current.

**Cardinality-Vector: Ascending ElementID + RGA**

Vector elements use ascending order (not descending) because RGA needs to traverse elements in insertion order to build the "after" chain.

```go
// Storage: ascending ElementID
// [E][:skills][(L1,N1)] = {value: "a", after: HEAD}
// [E][:skills][(L2,N1)] = {value: "b", after: (L1,N1)}

// Reconstruction uses "after" references, not storage order
// ElementID comparison is tiebreaker for concurrent inserts
```

### Storage Indices

Six indices, with I (ElementID) positioned for optimal access patterns:

```
EAIVT: [E:20][A:32][I:16][type:1][V:var][T:20]
       │           └─ ElementID: (Lamport:8)(NodeID:8)
       └─ Iterate values by recency (One/Many) or insertion order (Vector)

EAVIT: [E:20][A:32][type:1][V:var][I:16][T:20]
       └─ Find ElementID of specific value

AEIVT: [A:32][E:20][I:16][type:1][V:var][T:20]
       └─ All entities with attribute, by recency

AVEIT: [A:32][type:1][V:var][E:20][I:16][T:20]
       └─ Find entities with value (membership queries)

VAEIT: [type:1][V:var][A:32][E:20][I:16][T:20]
       └─ Reverse reference traversal

TAEIV: [T:20][A:32][E:20][I:16][type:1][V:var]
       └─ Transaction log
```

Note: I is 16 bytes (two uint64s: Lamport + NodeID), not 8.

### Cardinality Semantics

#### Cardinality-One: Highest ElementID Wins

```go
// Update name (no retraction needed)
tx.Add(entity, ":person/name", "Alice")  // (L1, N1)
tx.Add(entity, ":person/name", "Bob")    // (L2, N1)

// Storage (EAIVT with descending ElementID):
// [E][:name][^(L2,N1)]["Bob"]    ← First in scan = current
// [E][:name][^(L1,N1)]["Alice"]  ← History preserved
```

**Concurrent writes resolve deterministically:**

```go
// Node 1 and Node 2 both write at Lamport 5:
// Node 1: (L5, N1) → "Alice"
// Node 2: (L5, N2) → "Bob"

// Storage (both entries exist):
// [E][:name][^(L5,N2)]["Bob"]    ← First if N2 > N1 (winner)
// [E][:name][^(L5,N1)]["Alice"]  ← Second (superseded)

// All nodes see "Bob" - deterministic, no coordination needed
```

**Query "current":** Scan E+A, take first entry.

```go
func (m *Matcher) loadOne(e, a Identity) (any, error) {
    iter := m.store.ScanEAIVT(e, a)
    defer iter.Close()

    if iter.Next() {
        return iter.Datom().V, nil  // First = highest ElementID = current
    }
    return nil, nil
}
```

#### Cardinality-Many: Add-Wins Tombstones

```go
// Add to set
tx.Add(entity, ":tags", "warrior")     // (L1, N1)
tx.Add(entity, ":tags", "veteran")     // (L2, N1)

// Remove from set (tombstone)
tx.Remove(entity, ":tags", "warrior")  // (L5, N1)

// Re-add
tx.Add(entity, ":tags", "warrior")     // (L8, N1)

// Storage (EAVIT, grouped by value, descending ElementID):
// [E][:tags]["veteran"][(L2,N1), Add]       ← Current (no removal)
// [E][:tags]["warrior"][(L8,N1), Add]       ← Current (highest)
// [E][:tags]["warrior"][(L5,N1), Remove]    ← Historical
// [E][:tags]["warrior"][(L1,N1), Add]       ← Historical
```

**Concurrent add+remove with add-wins:**

```go
// Node 1 adds, Node 2 removes, same Lamport:
// Node 1: add "warrior" at (L5, N1)
// Node 2: remove "warrior" at (L5, N2)

// Both entries exist. Query logic:
// - Find all entries for "warrior"
// - Check highest ElementID
// - If it's an Add, value is present
// - If it's a Remove, check if any Add at same Lamport exists → Add-wins

// Result: "warrior" is present (add-wins policy)
```

**Query "current set":** Scan, resolve add-wins per value.

```go
func (m *Matcher) loadMany(e, a Identity) ([]any, error) {
    iter := m.store.ScanEAVIT(e, a)  // Grouped by V, descending ElementID
    defer iter.Close()

    var result []any
    var currentV any
    var highestLamport uint64
    var hasAddAtHighest bool

    for iter.Next() {
        d := iter.Datom()
        entry := d.V.(SetEntry)

        if entry.Value != currentV {
            // New value - emit previous if it had an add
            if currentV != nil && hasAddAtHighest {
                result = append(result, currentV)
            }
            // Start tracking new value
            currentV = entry.Value
            highestLamport = entry.ID.Lamport
            hasAddAtHighest = (entry.Op == OpAdd)
        } else if entry.ID.Lamport == highestLamport && entry.Op == OpAdd {
            // Add-wins: add at same Lamport as remove → add wins
            hasAddAtHighest = true
        }
        // Entries with lower Lamport are superseded, ignore them
    }
    // Don't forget last value
    if currentV != nil && hasAddAtHighest {
        result = append(result, currentV)
    }
    return result, nil
}
```

#### Cardinality-Vector: RGA (CRDT)

Vectors use RGA (Replicated Growable Array) semantics for conflict-free concurrent modifications.

**Element structure:**

```go
type RGAElement struct {
    ID        ElementID  // This element's unique identifier
    Value     any        // The actual value
    AfterRef  ElementID  // What this element was inserted after
    Tombstone *ElementID // Non-nil if deleted (who deleted it)
}

// ElementID defined in Index Encoding section above

const HEAD = ElementID{0, 0}  // Sentinel for "beginning of list"
```

**Storage encoding:**

The Index (I) component stores the ElementID. The Value stores the RGAElement:

```
EAIVT key: [E][A][(Lamport:8)(NodeID:8)][type][V][Tx]
Value contains: AfterRef + optional Tombstone
```

**Example - building a vector:**

```go
// Initial: append "stealth", "archery", "lockpicking"
tx.Append(entity, ":skills", "stealth")     // L1,N1 after HEAD
tx.Append(entity, ":skills", "archery")     // L2,N1 after L1,N1
tx.Append(entity, ":skills", "lockpicking") // L3,N1 after L2,N1

// Storage:
// [E][:skills][(L1,N1)] = {value: "stealth", after: HEAD}
// [E][:skills][(L2,N1)] = {value: "archery", after: (L1,N1)}
// [E][:skills][(L3,N1)] = {value: "lockpicking", after: (L2,N1)}
```

**Concurrent appends resolve automatically:**

```go
// Node 1 and Node 2 both append after "lockpicking" simultaneously:

// Node 1 (Lamport 4):
// [E][:skills][(L4,N1)] = {value: "swimming", after: (L3,N1)}

// Node 2 (Lamport 4):
// [E][:skills][(L4,N2)] = {value: "climbing", after: (L3,N1)}

// Both claim to be after (L3,N1). Resolution: sort by ElementID.
// If N1 < N2: result is ["stealth", "archery", "lockpicking", "swimming", "climbing"]
// Deterministic - all nodes converge to same order.
```

**Query reconstructs via topological sort:**

```go
func (m *Matcher) loadVector(e, a Identity) (Vector, error) {
    elements := m.store.ScanVectorElements(e, a)
    return reconstructRGA(elements), nil
}

func reconstructRGA(elements []RGAElement) []any {
    // Build children map: afterRef -> []elements
    children := make(map[ElementID][]RGAElement)
    for _, e := range elements {
        if e.Tombstone == nil {  // Skip deleted
            children[e.AfterRef] = append(children[e.AfterRef], e)
        }
    }

    // Sort each child list by ElementID for deterministic order
    for k := range children {
        sort.Slice(children[k], func(i, j int) bool {
            return children[k][i].ID.Less(children[k][j].ID)
        })
    }

    // DFS from HEAD
    var result []any
    var walk func(id ElementID)
    walk = func(id ElementID) {
        for _, child := range children[id] {
            result = append(result, child.Value)
            walk(child.ID)
        }
    }
    walk(HEAD)
    return result
}
```

**Deletion via tombstone:**

When an element is removed, it receives a tombstone marker rather than being deleted:

```
// Element with tombstone (internal representation)
[E][:skills][(L2,N1)] = {value: "archery", after: (L1,N1), tombstone: (L5,N2)}
```

The element remains in storage (needed for causal ordering) but is skipped during reconstruction. Users remove elements via `tx.Set(e, a, newSlice)` with the desired elements.

#### Vector Read Optimization: Materialized Position Index

RGA reconstruction is O(n). For read-heavy workloads, we maintain a **materialized position index** that provides O(1) random access when fresh.

**Dual storage:**

```
// Source of truth: RGA elements (CRDT, conflict-free)
[E][:skills:rga][(L1,N1)] = {value: "a", after: HEAD}
[E][:skills:rga][(L2,N1)] = {value: "b", after: (L1,N1)}
[E][:skills:rga][(L3,N1)] = {value: "c", after: (L2,N1)}

// Materialized view: position index (rebuilt on demand)
[E][:skills:pos:version] = (L3,N1)   // "valid as of this ElementID"
[E][:skills:pos:length] = 3
[E][:skills:pos:0] = (L1,N1)         // position 0 → element ID
[E][:skills:pos:1] = (L2,N1)         // position 1 → element ID
[E][:skills:pos:2] = (L3,N1)         // position 2 → element ID
```

**Write path (unchanged, O(1)):**

```go
func (tx *Transaction) Append(e, a, v) {
    // Just write RGA element - don't touch position index
    // Position index is lazily invalidated
    tx.writeRGAElement(e, a, elemID, value, afterRef)
}
```

**Read path (O(1) when fresh):**

```go
func (db *Database) GetVectorNth(e, a, n) any {
    // 1. Check freshness: O(1)
    posVersion := db.get(e, a, "pos:version")
    rgaMaxID := db.reverseSeek(e, a, "rga")  // Highest ElementID, O(1)

    if posVersion == rgaMaxID {
        // Fresh - direct lookup: O(1)
        elemID := db.get(e, a, "pos", n)
        return db.getRGAValue(e, a, elemID)
    }

    // 2. Stale - rebuild then lookup: O(n) + O(1)
    db.rebuildPositionIndex(e, a)
    return db.GetVectorNth(e, a, n)
}

func (db *Database) GetVectorLength(e, a) int {
    if db.positionIndexFresh(e, a) {
        return db.get(e, a, "pos:length")  // O(1)
    }
    db.rebuildPositionIndex(e, a)
    return db.get(e, a, "pos:length")
}
```

**Performance with position index:**

| Operation | No writes since last read | After concurrent writes |
|-----------|---------------------------|------------------------|
| Get vector | O(n) | O(n) rebuild |
| Get length | **O(1)** | O(n) rebuild, then O(1) |
| Get nth | **O(1)** | O(n) rebuild, then O(1) |
| Append | O(1) | O(1) |

The rebuild happens **once** after writes, then subsequent reads are O(1) until the next write. This gives us both CRDT conflict-freedom AND fast random access.

### Pattern Syntax

Patterns remain 4 elements. Index is NOT exposed in patterns.

```clojure
[?e ?a ?v]       ;; 3-element
[?e ?a ?v ?tx]   ;; 4-element (optional tx)
```

Vectors are returned as values. Index access via functions:

```clojure
;; Get skills as vector
[:find ?skills :where [?e :character/skills ?skills]]
;; → [[["stealth" "archery" "lockpicking"]]]

;; Decompose vector with enumerate
[:find ?idx ?skill
 :where
 [?e :character/skills ?skills]
 [(enumerate ?skills) ?idx ?skill]]
;; → [[0 "stealth"] [1 "archery"] [2 "lockpicking"]]

;; Access specific index
[:find ?skill
 :where
 [?e :character/skills ?skills]
 [(nth ?skills 0) ?skill]]
;; → [["stealth"]]

;; Membership test (uses AVEIT, efficient)
[:find ?e
 :where
 [?e :character/tags ?tags]
 [(contains? ?tags "warrior")]]
```

### Storage Layer Reconstruction

The pattern matcher reconstructs vectors and resolves "current" values:

```go
func (m *BadgerMatcher) Match(pattern *DataPattern, bindings Relations) (Relation, error) {
    e, a, v := extractEAV(pattern)

    switch m.schema.Cardinality(a) {
    case CardinalityOne:
        // Return single current value (first in descending scan)
        return m.matchOne(e, a, v, bindings)

    case CardinalityMany:
        // Return current set (first per V, skip tombstones)
        return m.matchMany(e, a, v, bindings)

    case CardinalityVector:
        // Return reconstructed vector
        return m.matchVector(e, a, v, bindings)
    }
}
```

### Transaction API

The transaction API provides two levels:

1. **High-level:** `SaveStruct` for most users (handles everything automatically)
2. **Low-level:** `Set`, `Add`, `Remove`, `Append` for fine-grained control

#### High-Level API: SaveStruct

Most developers should use `SaveStruct`, which handles cardinality detection and CRDT operations automatically:

```go
type Character struct {
    ID     datalog.Identity   `db:"id"`
    Name   string             `db:":character/name"`           // Cardinality-One
    Tags   []string           `db:":character/tags,many"`      // Cardinality-Many
    Skills []string           `db:":character/skills,vector"`  // Cardinality-Vector
}

character := &Character{
    Name:   "Alice",
    Tags:   []string{"warrior", "veteran"},
    Skills: []string{"stealth", "archery", "lockpicking"},
}

// SaveStruct detects cardinality from schema/tags, handles all CRDT ops
id, err := tx.SaveStruct(character)
```

`SaveStruct` behavior:
- **Cardinality-One fields:** Writes new version (no read needed)
- **Cardinality-Many fields:** When given a complete slice, diffs to minimize operations. For conflict-free individual operations, use `Add()`/`Remove()` directly.
- **Cardinality-Vector fields:** Replaces entire vector (use `Append()` for conflict-free appends)

#### Low-Level API: Set, Add, Remove, Append

For fine-grained control, four primitive operations:

```go
// Set: Replace entire value (works for all cardinalities)
tx.Set(entity, ":name", "Carol")                    // One: new version
tx.Set(entity, ":tags", []string{"a", "b"})         // Many: replace entire set
tx.Set(entity, ":skills", []string{"x", "y", "z"})  // Vector: replace entire vector

// Add: Add to set (Cardinality-Many only)
tx.Add(entity, ":tags", "newtag")  // Adds "newtag" to set

// Remove: Remove from set (Cardinality-Many only)
tx.Remove(entity, ":tags", "oldtag")  // Tombstones "oldtag"

// Append: Append to vector (Cardinality-Vector only)
tx.Append(entity, ":skills", "newskill")  // Appends to end
```

**Method validity by cardinality:**

| Method | One | Many | Vector |
|--------|-----|------|--------|
| `Set(e, a, v)` | New version | Replace set | Replace vector |
| `Add(e, a, v)` | Error¹ | Add to set | Error² |
| `Remove(e, a, v)` | Error | Tombstone value | Error² |
| `Append(e, a, v)` | Error | Error | Append element |

¹ Use `Set` for cardinality-one
² Use `Append` for vectors, or `Set` to replace entirely

**Note:** ElementID is internal to the CRDT implementation and never exposed to users.

**Implementation:**

```go
func (tx *Transaction) Set(e Identity, a Keyword, v any) error {
    card := tx.db.schema.Cardinality(a)

    switch card {
    case CardinalityOne:
        // Append new version with descending ElementID
        tx.datoms = append(tx.datoms, Datom{
            E:         e,
            A:         a,
            ElementID: tx.NextElementID(),
            V:         v,
        })

    case CardinalityMany:
        // Replace set: read current, tombstone removed, add new
        slice := v.([]any)  // Must be slice
        return tx.replaceSet(e, a, slice)

    case CardinalityVector:
        // Replace vector: tombstone all existing, write new RGA elements
        slice := v.([]any)  // Must be slice
        return tx.replaceVector(e, a, slice)
    }
    return nil
}

func (tx *Transaction) Add(e Identity, a Keyword, v any) error {
    card := tx.db.schema.Cardinality(a)
    if card != CardinalityMany {
        return fmt.Errorf("Add only valid for cardinality-many (got %v)", card)
    }

    // Add to set: append with new ElementID
    tx.datoms = append(tx.datoms, Datom{
        E:         e,
        A:         a,
        ElementID: tx.NextElementID(),
        V:         SetEntry{Value: v, Op: OpAdd},
    })
    return nil
}

func (tx *Transaction) Remove(e Identity, a Keyword, v any) error {
    card := tx.db.schema.Cardinality(a)
    if card != CardinalityMany {
        return fmt.Errorf("Remove only valid for cardinality-many (got %v)", card)
    }

    // Tombstone: append with new ElementID and OpRemove
    tx.datoms = append(tx.datoms, Datom{
        E:         e,
        A:         a,
        ElementID: tx.NextElementID(),
        V:         SetEntry{Value: v, Op: OpRemove},
    })
    return nil
}

func (tx *Transaction) Append(e Identity, a Keyword, v any) error {
    card := tx.db.schema.Cardinality(a)
    if card != CardinalityVector {
        return fmt.Errorf("Append only valid for cardinality-vector (got %v)", card)
    }

    // RGA append: new element after last known
    elemID := tx.NextElementID()
    afterRef := tx.lastKnownElement(e, a)  // Track locally, no DB read

    tx.datoms = append(tx.datoms, Datom{
        E:         e,
        A:         a,
        ElementID: elemID,
        V:         RGAElement{Value: v, AfterRef: afterRef},
    })

    tx.trackLastElement(e, a, elemID)
    return nil
}
```

#### Lamport Clock Management

```go
type Transaction struct {
    db          *Database
    id          uint64              // Transaction ID
    lamport     uint64              // Current Lamport clock value
    nodeID      uint64              // This node's ID
    datoms      []Datom
    lastElement map[EntityAttr]ElementID  // Track last vector element per (E,A)
}

// NextElementID generates a unique ID for any write operation
func (tx *Transaction) NextElementID() ElementID {
    tx.lamport++
    return ElementID{Lamport: tx.lamport, NodeID: tx.nodeID}
}
```

### Read Requirements by Operation

A key design goal is minimizing reads during writes. The current reflection-based API (`SaveStruct`) always reads before writing to compute diffs. The new proposal eliminates reads for most operations.

#### Current API (writer.go)

The current `SaveStruct` implementation reads before every update:

```go
// Cardinality-One: read → compare → retract if different → add
func (sw *StructWriter) updateField(...) error {
    existingVal, found := lookup.LookupAttribute(entity, kw)  // READ
    if found && !datalog.ValuesEqual(existingVal, newVal) {
        tx.Retract(entity, kw, existingVal)  // RETRACT old
    }
    return tx.Add(entity, kw, newVal)
}

// Cardinality-Many: read all → diff → retract removed → add new
func (sw *StructWriter) updateSliceField(...) error {
    existingVals := lookup.LookupAllAttributes(entity, kw)  // READ ALL
    // Retract values in existing but not in new
    // Add values in new but not in existing
}
```

**Every update requires a read**, even when the value hasn't changed.

#### New Proposal Read Requirements

| Cardinality | Operation | Read Required? | Why |
|-------------|-----------|----------------|-----|
| **One** | Update | **No** | Just append; descending index makes new value "first" |
| **Many** | Add to set | **No** | Just append with new ElementID |
| **Many** | Remove from set | **No** | Just append tombstone |
| **Many** | Replace set (diff) | **Yes** | Need existing values to compute diff |
| **Vector** | Append | **No** | CRDT: generate ElementID, write with after=HEAD or last known |
| **Vector** | Insert after X | **No** | CRDT: generate ElementID, write with after=X |
| **Vector** | Remove element | **No** | CRDT: write tombstone to element's ID |
| **Vector** | Get current | **O(n)** | Must scan all elements and reconstruct RGA |

#### Vector Operations: CRDT Eliminates Read-Before-Write

With RGA, vector mutations don't require reading current state:

**Append (no read):**
```go
func (tx *Transaction) Append(e Identity, a Keyword, v any) error {
    elemID := tx.NextElementID()  // Lamport++, unique

    // Find last element we know about (from our local state, or HEAD)
    afterRef := tx.lastKnownElement(e, a)  // Local tracking, not a DB read

    tx.datoms = append(tx.datoms, Datom{
        E:   e,
        A:   a,
        ElementID: elemID,
        V:         RGAElement{Value: v, AfterRef: afterRef},
    })

    tx.trackLastElement(e, a, elemID)  // Remember for next append in this tx
    return nil
}
```

**Concurrent appends just work:**
```
Goroutine A                          Goroutine B
-----------                          -----------
Append("x")                    Append("y")
  elemID = (L1, N1)                    elemID = (L1, N2)
  afterRef = HEAD                      afterRef = HEAD
  write element                        write element

// Both elements exist, both after HEAD
// RGA reconstruction sorts by ElementID: ["x", "y"] or ["y", "x"]
// Deterministic based on NodeID comparison
```

No lost updates. No read-modify-write race. Both elements survive.

#### Summary: Reads Eliminated

| Operation | Current API | New Proposal |
|-----------|-------------|--------------|
| Update cardinality-one | Read + compare + maybe retract | **Append only (no read)** |
| Add to set | Read all + check membership | **Append only (no read)** |
| Remove from set | Read all + find + retract | **Append tombstone (no read)** |
| Vector append | N/A | **CRDT append (no read)** |
| Vector replace | N/A | **Tombstone all + append (no read)** |

The big wins:
- **Cardinality-one updates: 0 reads** (was 1 read + conditional retract)
- **Set add/remove: 0 reads** (was full set read for diff)
- **Vector mutations: 0 reads** (CRDT handles conflicts automatically)
- **Concurrent writes: no lost updates** (all mutations survive and merge)

### History Queries

Built-in history without separate indices:

```clojure
;; Get all historical values for name (with transaction ID)
[:find ?name ?tx
 :where [?e :person/name ?name ?tx]
 [(history)]]  ;; Marker to return all versions, not just current

;; Get value as of specific transaction
[:find ?name
 :where [?e :person/name ?name ?tx]
 [(as-of ?tx 5000)]]  ;; Filter to Tx <= 5000
```

Note: `as-of` uses Transaction ID (T), not ElementID. Transaction ID is the user-facing temporal marker ("when was this committed?"). ElementID is internal for CRDT conflict resolution.

**Implementation:**

```go
func (m *Matcher) matchOneWithHistory(e, a Identity) (Relation, error) {
    iter := m.store.ScanEAIVT(e, a)
    defer iter.Close()

    var tuples []Tuple
    for iter.Next() {
        d := iter.Datom()
        tuples = append(tuples, Tuple{
            d.V,
            d.Tx,  // Transaction ID - user-facing temporal marker
        })
    }
    return NewRelation([]Symbol{symV, symTx}, tuples), nil
}

func (m *Matcher) matchOneAsOf(e, a Identity, asOfTx uint64) (any, error) {
    iter := m.store.ScanEAIVT(e, a)
    defer iter.Close()

    for iter.Next() {
        d := iter.Datom()
        if d.Tx <= asOfTx {
            return d.V, nil  // First entry with Tx <= target
        }
    }
    return nil, nil
}
```

### Vector Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `enumerate` | `[(enumerate ?vec) ?idx ?val]` | Decompose to (index, value) pairs |
| `nth` | `[(nth ?vec ?n) ?val]` | Access element at index |
| `contains?` | `[(contains? ?vec ?val)]` | Membership test (boolean) |
| `count` | `[(count ?vec) ?n]` | Vector length |
| `first` | `[(first ?vec) ?val]` | First element |
| `last` | `[(last ?vec) ?val]` | Last element |
| `index-of` | `[(index-of ?vec ?val) ?idx]` | Find index of value |
| `subvec` | `[(subvec ?vec ?start ?end) ?sub]` | Slice of vector |

**Implementation:**

```go
func init() {
    RegisterFunction("enumerate", builtinEnumerate)
    RegisterFunction("nth", builtinNth)
    RegisterFunction("contains?", builtinContains)
    // ...
}

func builtinEnumerate(args []any) (Relation, error) {
    vec, ok := args[0].(Vector)
    if !ok {
        return nil, fmt.Errorf("enumerate requires vector")
    }

    tuples := make([]Tuple, len(vec))
    for i, v := range vec {
        tuples[i] = Tuple{int64(i), v}
    }
    return NewRelation([]Symbol{symIdx, symVal}, tuples), nil
}

func builtinNth(args []any) (any, error) {
    vec := args[0].(Vector)
    idx := args[1].(int64)

    if idx < 0 || idx >= int64(len(vec)) {
        return nil, nil  // Out of bounds
    }
    return vec[idx], nil
}

func builtinContains(args []any) (bool, error) {
    vec := args[0].(Vector)
    val := args[1]

    for _, v := range vec {
        if v == val {
            return true, nil
        }
    }
    return false, nil
}
```

### Schema Declaration

```go
// Cardinality constants
const (
    CardinalityOne    Cardinality = iota  // Single value, versioned
    CardinalityMany                        // Set with tombstones
    CardinalityVector                      // Ordered list
)

// Schema definition
schema := NewSchema()
schema.AddAttribute(Attribute{
    Ident:       ":person/name",
    ValueType:   TypeString,
    Cardinality: CardinalityOne,
    Doc:         "Person's name (versioned)",
})

schema.AddAttribute(Attribute{
    Ident:       ":character/tags",
    ValueType:   TypeString,
    Cardinality: CardinalityMany,
    Doc:         "Character tags (set with history)",
})

schema.AddAttribute(Attribute{
    Ident:       ":character/skills",
    ValueType:   TypeString,
    Cardinality: CardinalityVector,
    Doc:         "Character skills (ordered list)",
})
```

---

## Architecture

### Layer Structure

```
┌─────────────────────────────────────────────────────────────┐
│  Query Executor / Reflection API                            │
│  (PullInto, SaveStruct, ExecuteQuery)                       │
├─────────────────────────────────────────────────────────────┤
│  PatternMatcher (BadgerMatcher)                             │
│  - Executes Datalog patterns                                │
│  - Cardinality-aware resolution                             │
│  - Uses Cache for resolved views                            │
├─────────────────────────────────────────────────────────────┤
│  Cache                                        ◄── NEW       │
│  - Stores resolved CRDT views per (E, A)                    │
│  - Invalidated on transaction commit                        │
├─────────────────────────────────────────────────────────────┤
│  Database                                                   │
│  - Owns Store, Schema, Cache                                │
│  - Transaction management                                   │
│  - Lamport clock / NodeID                                   │
├─────────────────────────────────────────────────────────────┤
│  Store (BadgerStore)                                        │
│  - Raw key-value operations                                 │
│  - Index scans                                              │
│  - No CRDT awareness (just bytes)                           │
└─────────────────────────────────────────────────────────────┘
```

### Unified Cache

The Cache stores **resolved CRDT views**, not raw datoms. Each cardinality has different resolution logic, but the cache provides a unified interface.

**What gets cached:**

| Cardinality | Raw Storage | Cached Resolved View |
|-------------|-------------|---------------------|
| One | Multiple entries (history) | Single current value |
| Many | Adds + tombstones | Resolved set membership |
| Vector | RGA elements | Ordered list + position index |

**Cache structure:**

```go
type Cache struct {
    // Using sync.Map for lock-free reads in the common case.
    // Read-heavy workload with disjoint keys (different entities) fits sync.Map well.
    entries      sync.Map  // map[CacheKey]*CacheEntry
    attrVersions sync.Map  // map[datalog.Keyword]ElementID
}

type CacheKey struct {
    E datalog.Identity
    A datalog.Keyword
}

type CacheEntry struct {
    version     ElementID   // Max ElementID when computed
    cardinality Cardinality

    // Resolved view (one of these populated based on cardinality)
    oneValue    any              // Cardinality-One: single current value
    manySet     map[any]bool     // Cardinality-Many: current set members
    vectorList  []any            // Cardinality-Vector: ordered elements
    vectorIndex []ElementID      // Cardinality-Vector: position → ElementID
}
```

**Cache operations:**

```go
// Called by PatternMatcher to get resolved value
func (c *Cache) GetOrResolve(e, a datalog.Identity, store Store, schema SchemaProvider) *CacheEntry {
    key := CacheKey{e, a}

    // Fast path: load existing entry
    if val, ok := c.entries.Load(key); ok {
        entry := val.(*CacheEntry)
        currentMax := store.MaxElementID(e, a)  // O(1) reverse seek
        if entry.version == currentMax {
            return entry  // Cache hit, fresh
        }
        // Stale - fall through to rebuild
    }

    // Slow path: rebuild and store
    // Note: Two goroutines might both rebuild for the same key.
    // That's fine - CRDT resolution is deterministic, both compute same result.
    entry := c.rebuild(e, a, store, schema)
    c.entries.Store(key, entry)
    return entry
}

// Called by Database on tx.Commit()
func (c *Cache) Invalidate(touched []CacheKey) {
    for _, key := range touched {
        c.entries.Delete(key)
    }
    // Note: attrVersions invalidation is implicit -
    // next IsAttributeFresh() call will fetch current max from store
}

// Fast check for A-bound queries: is entire attribute fresh?
func (c *Cache) IsAttributeFresh(a datalog.Keyword, store Store) bool {
    val, ok := c.attrVersions.Load(a)
    if !ok {
        return false
    }
    cachedMax := val.(ElementID)
    storeMax := store.MaxElementIDForAttribute(a)  // O(1) reverse seek
    return cachedMax == storeMax
}
```

**Rebuild by cardinality:**

```go
func (c *Cache) rebuild(e, a datalog.Identity, store Store, schema SchemaProvider) *CacheEntry {
    card := schema.Cardinality(a)

    switch card {
    case CardinalityOne:
        // Scan EAIVT, first entry (descending) is current
        iter := store.ScanEAIVT(e, a)
        if iter.Next() {
            return &CacheEntry{
                version:  iter.ElementID(),
                oneValue: iter.Value(),
            }
        }

    case CardinalityMany:
        // Scan EAVIT grouped by value, descending ElementID
        // For each value: if highest entry is Add (or Add exists at same Lamport as Remove), include it
        set, maxSeen := resolveAddWinsSet(store, e, a)
        return &CacheEntry{
            version: maxSeen,
            manySet: set,
        }

    case CardinalityVector:
        // Scan all RGA elements, reconstruct
        elements := store.ScanRGAElements(e, a)
        ordered := reconstructRGA(elements)
        positions := buildPositionIndex(elements)
        return &CacheEntry{
            version:     maxSeen,
            vectorList:  ordered,
            vectorIndex: positions,
        }
    }
}
```

### PatternMatcher Integration

The PatternMatcher becomes cardinality-aware and uses the Cache for all value resolution.

**Required changes to BadgerMatcher:**

```go
type BadgerMatcher struct {
    store   *BadgerStore
    schema  schema.SchemaProvider  // ADD: for cardinality lookup
    cache   *Cache                 // ADD: for resolved views
    txID    uint64
    // ... existing fields
}
```

**Query patterns vs cache utility:**

| Pattern | E | A | Cache Role |
|---------|---|---|------------|
| `[123 :name ?v]` | bound | bound | Direct (E,A) lookup |
| `[123 ?a ?v]` | bound | unbound | Scan A's, resolve each via cache |
| `[?e :name "Bob"]` | unbound | bound | Scan E's, resolve each via cache |
| `[?e ?a ?v]` | unbound | unbound | Full scan, minimal cache benefit |

**Key insight:** Cache doesn't replace index scanning. It accelerates *resolution* of individual (E, A) pairs after candidates are found.

**Pattern: Scan for candidates, resolve through cache**

```go
func (m *Matcher) matchPattern(e, a, v PatternElement) Relation {
    switch {
    case e.IsBound() && a.IsBound():
        // Direct cache lookup
        entry := m.cache.GetOrResolve(e.Value(), a.Value(), m.store, m.schema)
        return m.filterByValue(entry, v)

    case a.IsBound(): // E unbound
        // Scan index for candidate E's, resolve each through cache
        return m.scanAndResolve(a.Value(), v)

    case e.IsBound(): // A unbound
        // Scan index for candidate A's, resolve each through cache
        return m.scanAttributesForEntity(e.Value(), v)

    default: // Both unbound
        // Full scan
        return m.fullScan(v)
    }
}

func (m *Matcher) scanAndResolve(a datalog.Keyword, v PatternElement) Relation {
    var results []Tuple
    seen := make(map[datalog.Identity]bool)

    // Scan index to find candidate E's
    iter := m.store.ScanAEIVT(a)
    for iter.Next() {
        candidate := iter.Entity()

        // Skip if already processed (index has multiple entries per E)
        if seen[candidate] {
            continue
        }
        seen[candidate] = true

        // Resolve through cache (handles all CRDT semantics)
        entry := m.cache.GetOrResolve(candidate, a, m.store, m.schema)

        // Filter by value pattern
        if m.matchesValue(entry, v) {
            results = append(results, m.buildTuple(candidate, a, entry))
        }
    }

    return NewRelation(results)
}
```

**Why the `seen` map?**

The index contains multiple entries per (E, A):
- Cardinality-One: history entries (multiple versions)
- Cardinality-Many: set members + tombstones
- Vector: RGA elements

We scan to find unique E candidates, then resolve each once through the cache.

**Visualization:**

```
Index Scan                         Cache
──────────                         ─────
E=1, A=:name, (L5,N1)  ──┐
E=1, A=:name, (L3,N1)  ──┼──► seen[1]=true ──► GetOrResolve(1, :name)
E=1, A=:name, (L1,N1)  ──┘                            │
                                                      ▼
E=2, A=:name, (L4,N2)  ──┐                    [Check version]
E=2, A=:name, (L2,N2)  ──┼──► seen[2]=true          │
                                    │          [Fresh? Return cached]
                                    │          [Stale? Rebuild]
                                    │                │
                                    └──► GetOrResolve(2, :name)
                                                      │
                                                      ▼
                                              Resolved values
                                              (One: single value)
                                              (Many: set)
                                              (Vector: ordered list)
```

### Schemaless Mode

When running without a schema (`schema == nil` or `schema.HasSchema() == false`):

- All attributes treated as Cardinality-One
- No CRDT semantics (no add-wins, no RGA)
- Cache still works (caches first entry as "current value")
- Vectors and Cardinality-Many require schema to function

```go
func (c *Cache) rebuild(e, a, store, schema) *CacheEntry {
    var card Cardinality = CardinalityOne  // Default

    if schema != nil && schema.HasSchema() {
        card = schema.Cardinality(a)
    }

    // ... proceed with cardinality-specific logic
}
```

### Transaction Flow

**Write path:**

```go
func (tx *Transaction) Commit() error {
    // 1. Assign Lamport timestamps to all datoms
    for i := range tx.datoms {
        tx.datoms[i].ElementID = tx.NextElementID()
    }

    // 2. Write to storage
    if err := tx.store.Assert(tx.datoms); err != nil {
        return err
    }

    // 3. Collect touched (E, A) pairs
    touched := make([]CacheKey, 0, len(tx.datoms))
    for _, d := range tx.datoms {
        touched = append(touched, CacheKey{d.E, d.A})
    }

    // 4. Invalidate cache
    tx.db.cache.Invalidate(touched)

    return nil
}
```

**Read path:**

```go
func (m *Matcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
    e, a, v := extractEAV(pattern)

    // Schema lookup for cardinality
    var card Cardinality = CardinalityOne
    if m.schema != nil && a.IsBound() {
        card = m.schema.Cardinality(a.Value())
    }

    // Dispatch to cardinality-specific matching
    switch card {
    case CardinalityOne:
        return m.matchOne(e, a, v, bindings)
    case CardinalityMany:
        return m.matchMany(e, a, v, bindings)
    case CardinalityVector:
        return m.matchVector(e, a, v, bindings)
    }
}
```

## Performance Characteristics

### Read Operations

| Cardinality | Operation | Complexity |
|-------------|-----------|------------|
| One | Get current | O(1) - first entry in descending scan |
| One | Get history | O(versions) - full scan |
| Many | Get current set | O(n) - scan, first per V, skip tombstones |
| Many | Check membership | O(1) - AVEIT index lookup |
| Vector | Get vector | O(n) - scan all elements, RGA reconstruct |
| Vector | Get length | O(n) - must reconstruct to count non-tombstoned |
| Vector | Get nth element | O(n) - must reconstruct, then index |

*Note: Vector reads are O(n) because RGA reconstruction requires seeing all elements. This is the trade-off for conflict-free writes. Caching reconstructed vectors is recommended for read-heavy workloads.*

### Write Operations

| Cardinality | Operation | Read Required | Write Complexity |
|-------------|-----------|---------------|------------------|
| One | Update | **None** | O(1) append |
| Many | Add to set | **None** | O(1) append |
| Many | Remove from set | **None** | O(1) append tombstone |
| Many | Replace set (diff) | O(n) read | O(n) tombstones + adds |
| Vector | Append | **None (CRDT)** | O(1) append |
| Vector | Insert after | **None (CRDT)** | O(1) append |
| Vector | Remove element | **None (CRDT)** | O(1) tombstone |

### Concurrent Write Behavior

All cardinalities use `(Lamport, NodeID)` for conflict resolution:

| Cardinality | Concurrent Writes | Result |
|-------------|-------------------|--------|
| One | A writes "x" at (L5,N1), B writes "y" at (L5,N2) | Higher NodeID wins (deterministic) |
| One | A writes "x" at L5, B writes "y" at L6 | Higher Lamport wins ("y") |
| Many | A adds "x", B adds "y" | Both in set (different values) |
| Many | A adds "x" at L5, B removes "x" at L5 | **Add-wins** ("x" present) |
| Many | A adds "x" at L5, B removes "x" at L6 | Higher Lamport wins ("x" removed) |
| Vector | A appends "x", B appends "y" | Both in vector (RGA merge) |
| Vector | A inserts after Z, B inserts after Z | Both inserted, ordered by ElementID |

**Key policies:**
- **Cardinality-One:** Highest `(Lamport, NodeID)` wins. Simple last-write-wins with deterministic tiebreaker.
- **Cardinality-Many:** Add-wins at same Lamport. Deletion requires strictly higher Lamport to take effect.
- **Cardinality-Vector:** All concurrent operations merge via RGA. No lost updates.

### History Access

| Query | Complexity |
|-------|------------|
| All versions of One | O(versions) |
| Value as-of Tx | O(versions) worst, often O(1) |
| Set membership history | O(versions for that V) |

### Storage Overhead

| Change | Impact |
|--------|--------|
| 6 indices (was 5) | +20% index storage |
| 8-byte index per datom | +8 bytes per datom |
| No separate history indices | -50% for history mode |
| No retractions (append-only) | +storage for versions |

Net: Similar storage, simpler model, built-in history.

---

## Implementation Roadmap

### Phase 1: Index as First-Class Component
- [ ] Add `ElementID` field to Datom struct
- [ ] Update key encoding for all 6 indices
- [ ] Update KeyEncoder interface

### Phase 2: Lamport Clocks and CRDT Foundation
- [ ] Implement `ElementID` type (Lamport, NodeID)
- [ ] Add Lamport clock to Database/Transaction
- [ ] NodeID generation/configuration
- [ ] ElementID comparison (total ordering)
- [ ] Descending encoding for `^(Lamport, NodeID)`

### Phase 3: Cardinality-One Semantics
- [ ] Update `tx.Set` for cardinality-one (append new version, no retract)
- [ ] Update matcher to return first entry (current)
- [ ] History query support

### Phase 4: Cardinality-Many with Add-Wins
- [ ] Implement `SetEntry` type (value, op bit)
- [ ] Add-wins conflict resolution logic
- [ ] `tx.Add` - add to set
- [ ] `tx.Remove` - tombstone value
- [ ] `tx.Set` - replace entire set (diff-based)
- [ ] Set membership queries via AVEIT

### Phase 5: Cardinality-Vector (RGA)
- [ ] Implement `RGAElement` type (value, afterRef, tombstone)
- [ ] RGA reconstruction algorithm
- [ ] `tx.Append` - append with CRDT semantics
- [ ] `tx.Set` for vectors - replace entire vector
- [ ] Materialized position index for O(1) random access

### Phase 6: Unified Cache
- [ ] Implement `Cache` struct with `sync.Map`
- [ ] `CacheEntry` for all cardinalities (oneValue, manySet, vectorList)
- [ ] `GetOrResolve(e, a)` with freshness check via `MaxElementID`
- [ ] `Invalidate(touched)` on transaction commit
- [ ] Vector position index caching
- [ ] Thread schema through to PatternMatcher

### Phase 7: Simplified Transaction API
- [ ] `tx.Set(e, a, v)` - replace value (all cardinalities)
- [ ] `tx.Add(e, a, v)` - add to set (many only)
- [ ] `tx.Remove(e, a, v)` - tombstone (many only)
- [ ] `tx.Append(e, a, v)` - append to vector (vector only)
- [ ] Update `SaveStruct` to use new primitives
- [ ] Method validation (error on invalid cardinality/method combo)

### Phase 8: Query Integration
- [ ] Update PatternMatcher to use Cache for resolution
- [ ] "Scan for candidates, resolve through cache" pattern
- [ ] `seen` map for deduplicating index scan results
- [ ] Implement vector functions (enumerate, nth, etc.)
- [ ] History/as-of query predicates

### Phase 9: Multi-Node Preparation
- [ ] Lamport clock synchronization on message receive
- [ ] NodeID assignment for distributed deployment
- [ ] Document CRDT merge semantics for operators

---

## Open Design Questions

### Ordered Vector vs Ordered Set

The current design uses RGA for ordered collections, but there's a fundamental choice:

**Ordered Set (no duplicates):**
- Values are unique within the collection
- Enables clean value-based mutation API:
  ```go
  tx.InsertAfter(e, ":skills", "archery", "lockpicking")  // Insert after value
  tx.Remove(e, ":skills", "stealth")                       // Remove by value
  ```
- No ElementID exposure needed
- Sufficient for most use cases (skills, tags, inventory items)

**Ordered Vector (duplicates allowed):**
- Same value can appear multiple times
- Cannot identify elements by value alone
- Limited API: only `Append` and `Set` (replace all)
- Insert/remove by position would require exposing ElementID or position

**Trade-offs:**

| Aspect | Ordered Set | Ordered Vector |
|--------|-------------|----------------|
| Duplicates | No | Yes |
| Insert/Remove API | By value (clean) | By position (exposes internals) or not supported |
| Use cases | Skills, tags, inventory | Event logs, repeated items |

**Question:** Do we need both? Or is ordered set sufficient for the application use cases?

**Recommendation:** Start with ordered set semantics. If duplicate support is needed later, it can be added as a fourth cardinality type.

### NodeID Assignment

ElementID requires a NodeID for conflict resolution tiebreaking. Open questions:

1. **Single-node case:** Use NodeID=0? NodeID=1? Random?
2. **Multi-node assignment:** Configured? Auto-assigned from coordinator? Random with collision detection?
3. **Persistence:** How is NodeID preserved across restarts?

**For single-node (current use case):** NodeID=0 is sufficient. All operations from the same node, Lamport clock provides ordering.

**For future distribution:** Requires explicit design. Options include:
- Configuration file with unique NodeID per deployment
- Raft-style leader assignment
- Random uint64 (collision probability negligible)

---

## Alternatives Considered

### Alternative 1: Separate Vector Index

Store vectors as opaque blobs with separate position index.

**Rejected:** Loses element-level queryability (e.g., "find entities with tag X").

### Alternative 2: Order Attribute on Elements

`[element :order 0]` pattern with separate entities.

**Rejected:** Entity explosion, N+1 queries, complex updates.

### Alternative 3: Keep Separate History Indices

Maintain current `_HISTORY` indices alongside current-state indices.

**Rejected:** Doubles storage, two code paths, less elegant.

### Alternative 4: Require Explicit Retraction

Keep current semantics where users must retract before adding.

**Rejected:** Error-prone, loses history by default.

---

## Contrast with Original History Mechanism

The original Janus Datalog history preservation works fundamentally differently. This section contrasts the two approaches.

### Original Mechanism (RetractHistory Mode)

**Architecture:**

```
Current-State Indices (5, mutated):
  EAVT, AEVT, AVET, VAET, TAEV

History Indices (5, append-only):
  EAVT_HISTORY, AEVT_HISTORY, AVET_HISTORY, VAET_HISTORY, TAEV_HISTORY
```

**How it works:**

1. **RetractMode selection at store creation:**
   ```go
   // Default: RetractDelete - retractions actually delete datoms
   store := NewBadgerStore(path, encoder)

   // Opt-in: RetractHistory - keep full history
   store := NewBadgerStoreWithRetractMode(path, encoder, RetractHistory)
   ```

2. **Assertion flow (RetractHistory mode):**
   ```go
   func (s *BadgerStore) assertDatom(txn *badger.Txn, d *datalog.Datom) error {
       // Write to all 5 current-state indices
       for _, idx := range CurrentStateIndices {
           key := s.encoder.EncodeKey(idx, d)
           txn.Set(key, value)
       }

       // ALSO write to all 5 history indices with Op=true
       if s.retractMode == RetractHistory {
           s.writeToHistoryIndices(txn, d, OpAssert)  // Op byte = 0x01
       }
   }
   ```

3. **Retraction flow (RetractHistory mode):**
   ```go
   func (s *BadgerStore) retractDatom(txn *badger.Txn, d *datalog.Datom) error {
       // DELETE from all 5 current-state indices
       for _, idx := range CurrentStateIndices {
           key := s.encoder.EncodeKey(idx, storedDatom)
           txn.Delete(key)
       }

       // APPEND to history indices with Op=false
       if s.retractMode == RetractHistory {
           s.writeToHistoryIndices(txn, retractDatom, OpRetract)  // Op byte = 0x00
       }
   }
   ```

4. **Op flag in history indices:**
   ```go
   type Op bool
   const (
       OpAssert  Op = true   // 0x01
       OpRetract Op = false  // 0x00
   )
   ```

**Query paths diverge:**

| Query | Which Indices | Complexity |
|-------|---------------|------------|
| Current value | Current-state (EAVT) | O(1) - single lookup |
| All history | History (EAVT_HISTORY) | O(n) - scan all, interpret Op |
| Value at Tx | History indices | O(n) - scan, replay assertions/retractions |

### New Proposal (Unified Indices)

**Architecture:**

```
Unified Indices (6, append-only):
  EAIVT, EAVIT, AEIVT, AVEIT, VAEIT, TAEIV

No separate history indices.
No Op flag.
Index (I) component encodes version.
```

**How it works:**

1. **No mode selection - history is always preserved:**
   ```go
   // History is built-in, not opt-in
   store := NewBadgerStore(path, encoder)
   ```

2. **Assertion flow (cardinality-one):**
   ```go
   func (tx *Transaction) Set(e Identity, a Keyword, v any) error {
       // Append new datom with new ElementID
       tx.datoms = append(tx.datoms, Datom{
           E:         e,
           A:         a,
           ElementID: tx.NextElementID(),  // (Lamport++, NodeID)
           V:         v,
       })
       // NO retraction needed - old version stays in place
   }
   ```

3. **No retraction for updates - just append:**
   ```go
   // Update name three times (same node, incrementing Lamport)
   tx.Set(entity, ":name", "Alice")  // ElementID = (L1, N1)
   tx.Set(entity, ":name", "Bob")    // ElementID = (L2, N1)
   tx.Set(entity, ":name", "Carol")  // ElementID = (L3, N1)

   // Storage (EAIVT scan order due to descending encoding):
   // [E][:name][^(L3,N1)]["Carol"]  ← First in scan = current
   // [E][:name][^(L2,N1)]["Bob"]    ← History
   // [E][:name][^(L1,N1)]["Alice"]  ← History
   ```

4. **Tombstones replace retraction (cardinality-many only):**
   ```go
   func (tx *Transaction) Remove(e Identity, a Keyword, v any) error {
       // Add tombstone entry
       tx.datoms = append(tx.datoms, Datom{
           E:         e,
           A:         a,
           ElementID: tx.NextElementID(),
           V:         SetEntry{Value: v, Op: OpRemove},
       })
   }
   ```

**Query paths unified:**

| Query | Which Indices | Complexity |
|-------|---------------|------------|
| Current value | Same (EAIVT) | O(1) - first entry |
| All history | Same (EAIVT) | O(n) - scan all |
| Value at Tx | Same (EAIVT) | O(k) - scan until Tx ≤ target |

### Side-by-Side Comparison

| Aspect | Original | New Proposal |
|--------|----------|--------------|
| **Index count** | 10 (5 current + 5 history) | 6 (unified) |
| **History mode** | Opt-in (`RetractHistory`) | Built-in (always) |
| **Current value** | Look up in current-state | First entry in descending scan |
| **Retraction** | Delete from current-state, append Op=false to history | Append tombstone (many) or nothing (one) |
| **Op tracking** | Explicit Op byte (assert/retract) | Implicit in index position and tombstone bit |
| **Storage overhead** | 2x indices for history mode | Index component adds 16 bytes per datom |
| **Code paths** | Separate for current vs history queries | Single code path with filters |
| **Cardinality-one update** | Must retract old value | Append only, old value preserved |

### Why the New Approach is Better

1. **Simpler mental model:**
   - Original: "Current state is mutable, history is separate"
   - New: "Everything is append-only, first entry is current"

2. **No forgotten history:**
   - Original: Must opt-in to `RetractHistory` mode, and must remember to retract
   - New: History preserved automatically, updates just append

3. **Unified query path:**
   - Original: Different code for `query()` vs `history()`
   - New: Same scan, different filter

4. **O(1) current value access:**
   - Original: Current-state lookup is O(1), but requires maintaining mutated indices
   - New: First entry in descending scan is O(1), no mutation needed

5. **Better for distribution:**
   - Original: Retraction requires finding and deleting existing datoms (read-modify-write)
   - New: All writes are appends (conflict-free in distributed context)

---

## References

- [Datomic Information Model](https://docs.datomic.com/pro/architecture.html) - Inspiration for immutable facts
- [CRDTs and Tombstones](https://crdt.tech/) - Tombstone pattern for distributed sets
- [Fractional Indexing](https://www.figma.com/blog/realtime-editing-of-ordered-sequences/) - Alternative for insert-heavy vectors
- [Lamport Clocks](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) - Original paper on logical timestamps
- [RGA: Replicated Growable Array](https://hal.inria.fr/inria-00555588/document) - CRDT for ordered sequences
- [A Comprehensive Study of CRDTs](https://arxiv.org/abs/1805.06358) - Survey of CRDT theory and practice
