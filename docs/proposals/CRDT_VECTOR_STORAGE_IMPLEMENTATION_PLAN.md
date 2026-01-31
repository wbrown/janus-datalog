# CRDT Vector Storage - Implementation Plan

**Status:** In Progress (Phases 0-4 Complete - All Phase 4 Bugs Fixed)
**Based On:** CRDT_VECTOR_STORAGE.md Proposal
**Created:** 2026-01-30
**Updated:** 2026-01-30 (All 3 Phase 4 bugs fixed with streaming iterators)

---

## Implementation Deviations

This section documents differences between the plan and the actual implementation, with rigorous correctness analysis.

### Summary

| Deviation | Classification | Correct? | Action Required |
|-----------|---------------|----------|-----------------|
| 1. Slice vs Map return | **Lazy shortcut** | Functionally yes, architecturally no | ✅ FIXED - changed to map |
| 2. Raw vs Encoded value param | Design mismatch | Yes | Maybe - depends on encoding strategy |
| 3. Per-tx vs Per-op Lamport | **Semantic difference** | Different behavior | ✅ FIXED - per-operation |
| 4. Datom type | Architectural constraint | Yes | No - would require major refactor |

### Phase 4 Deviations

#### 1. `resolveAddWinsSet` Return Type - LAZY SHORTCUT

| Aspect | Plan | Implementation |
|--------|------|----------------|
| Return type | `(map[any]bool, ElementID, error)` | `(*SetResolutionResult, error)` |
| Set representation | `map[any]bool` for O(1) membership | `Members []interface{}` - O(n) lookup |

**Why I did it:** Easier to `append()` during iteration than to build a map.

**Correctness Analysis:** Functionally correct for returning results. But **wrong for the system design**:
- The cache (Phase 6) stores `manySet map[any]bool`
- Membership queries need O(1) lookup
- Current slice requires O(n) conversion or O(n) lookup

**Verdict:** Lazy shortcut. Should have used `map[any]bool` as specified. The plan's design is better.

**Action:** Change to return `map[any]bool` to match plan.

#### 2. `checkMembership` Signature - DESIGN MISMATCH

| Aspect | Plan | Implementation |
|--------|------|----------------|
| Function name | `checkMembership` | `checkSetMembership` |
| Value parameter | `v []byte` (pre-encoded) | `v interface{}` (raw value) |

**Why I did it:** The caller (matcher) had raw values from query patterns, not encoded bytes.

**Correctness Analysis:** Functionally correct. But inconsistent with plan's encoding architecture:
- Plan assumes encoding happens early, at call site
- Plan's `m.encoder.EncodePrefix(EAVT, e, a, v)` expects encoded v
- When cache has encoded values, calling this function requires decode→re-encode

**Verdict:** Not a shortcut per se, but adapted to existing codebase rather than following plan's encoding strategy.

**Action:** Evaluate during Phase 6 cache implementation.

#### 3. Per-Operation vs Per-Transaction Lamport Timestamps - SEMANTIC DIFFERENCE

| Aspect | Plan | Implementation |
|--------|------|----------------|
| When Lamport assigned | Each `Add()`/`Remove()` calls `clock.Next()` | Single `clock.Next()` at `Commit()` time |
| Granularity | Per-operation unique Lamport | Per-transaction shared Lamport |

**Why I did it:** Followed the existing `Commit()` pattern in the codebase without recognizing the semantic implications.

**Correctness Analysis:** **This produces DIFFERENT RESULTS:**

```go
tx.Add(e, :tags, "foo")
tx.Remove(e, :tags, "foo")
tx.Commit()
```

| Approach | Add Lamport | Remove Lamport | Result |
|----------|-------------|----------------|--------|
| **Plan** | 1 | 2 | Remove wins (L2 > L1) → NOT in set |
| **Implementation** | 1 | 1 | Same Lamport → add-wins → IN set |

**Verdict:** Semantic difference that changes behavior. I followed existing code instead of the plan without asking. This is a design decision that should have been escalated.

**Action:** **RESOLVED** - Per-operation Lamport is correct.

**Analysis of distributed systems:**

| System Type | Timestamp Granularity | Rationale |
|-------------|----------------------|-----------|
| Pure CRDTs (Automerge, Yjs, Riak) | Per-operation | Each op independent, causal ordering preserved |
| Database transactions (Datomic, Spanner) | Per-transaction | ACID atomicity ("all or nothing") |

**Key insight:** CRDT semantics define "concurrent" as operations from different replicas that haven't synced - NOT operations from the same replica in sequence.

When a user does:
```go
tx.Add(e, :tags, "foo")
tx.Remove(e, :tags, "foo")
tx.Commit()
```

These are **causally ordered**, not concurrent. The user did Add, *then* Remove. They're from the same replica, in sequence.

| Approach | Behavior | Correctness |
|----------|----------|-------------|
| Per-operation | Remove (L=2) > Add (L=1) → Remove wins | ✓ Honors user's intent |
| Per-transaction | Both L=1 → add-wins → Add wins | ✗ Contradicts user's action sequence |

**Decision:** Fix to use per-operation Lamport timestamps. This:
- Preserves causal ordering within a session
- Reserves "concurrent" for true cross-replica conflicts
- Honors user's explicit action sequence

#### 4. StorageDatom vs datalog.Datom - ARCHITECTURAL CONSTRAINT

| Aspect | Plan | Implementation |
|--------|------|----------------|
| Type used in Transaction | `StorageDatom` with encoded fields | `datalog.Datom` with raw types |
| When encoding happens | At `Add()`/`Remove()` time | Deferred to storage layer |

**Why I did it:** The existing `Transaction` type uses `[]datalog.Datom`. Changing it would require modifying the Transaction struct and all its methods.

**Correctness Analysis:** Functionally correct - encoding happens in `Assert()` instead of at `Add()` time. The plan assumed a different architecture than what exists in the codebase.

**Verdict:** Not a shortcut - this is how the codebase is structured. Following existing patterns was appropriate here.

**Action:** None required.

### Confirmed Correct (Not Deviations)

#### Lamport-Only Comparison for Add-Wins

The plan's comments said "Higher Tx wins" but the plan's code used `.Lamport` comparison only. This was clarified and documented in "ElementID Comparison: LWW vs Add-Wins" section. Lamport-only comparison is correct for add-wins semantics.

---

## Known Bugs (Phase 4 Audit - 2026-01-30)

This section documents bugs discovered during semantic correctness audit of Phase 4 implementation.

### Bug Summary

| Bug | Severity | Status | Impact |
|-----|----------|--------|--------|
| 1. Duplicate writes in Set() | Minor | ✅ Fixed | Wasteful but correct result |
| 2. Unbound E queries broken | **Critical** | ✅ Fixed | Silent wrong answers |
| 3. Set() ignores pending ops | Medium | ✅ Fixed | Surprising behavior |

### Bug #1: Duplicate Values in Set() Slice Cause Duplicate Writes

**Location:** `database.go` lines 1459-1471

**Problem:** When `Set()` is called with a slice containing duplicates, each duplicate generates a separate Add operation with its own Lamport timestamp.

```go
// Current code iterates the slice (which may have duplicates)
for _, val := range newSlice {
    if !currentResult.Members[val] {
        elemID := t.db.clock.Next()
        // ... generates Add for each duplicate
    }
}
```

**Example:**
```go
tx.Set(e, attr, []interface{}{"a", "b", "a"})  // "a" appears twice
// Generates: Add("a") at L1, Add("b") at L2, Add("a") at L3
```

**Impact:** Wasteful storage writes. Result is still correct (both adds for "a" are adds, so "a" is in set), but uses unnecessary Lamport values and storage space.

**Fix:** Iterate `newSet` (the deduplicated map) instead of `newSlice`:
```go
for val := range newSet {  // Iterate deduplicated set
    if !currentResult.Members[val] {
        // ...
    }
}
```

**Resolution:** ✅ Fixed in `database.go` - changed `for _, val := range newSlice` to `for val := range newSet`.

---

### Bug #2: Queries with Unbound E Don't Work for Cardinality-Many (CRITICAL)

**Location:** `matcher_relations.go` lines 172-198

**Problem:** The add-wins resolution and membership check only trigger when **both** E and A are bound:

```go
if e != nil && a != nil {  // BOTH must be bound
    // ... set useAddWinsResolution or useMembershipCheck
}
```

When E is unbound, the code falls through to the normal scan path, which:
1. Looks for V="warrior" encoded as TypeString
2. But stored V is SetEntry bytes encoded as TypeBytes
3. Type mismatch means **no keys match**

**Affected Query Patterns:**

| Query | Expected | Actual |
|-------|----------|--------|
| `[?e :person/tags "warrior"]` | All entities with "warrior" tag | Empty (wrong) |
| `[?e :person/tags ?v]` | All (entity, tag) pairs | Raw SetEntry bytes (wrong) |
| `[:find ?e :where [?e :person/tags "warrior"]]` | Entities with tag | Empty (wrong) |

**Root Cause:** The normal scan path has no awareness of cardinality-many encoding. It compares query values directly against stored values, but for cardinality-many, stored values are SetEntry-encoded bytes, not the raw user values.

**Impact:** **Silent wrong answers.** Users get empty results for valid queries without any error.

**Fix Required:** When A is bound and cardinality is Many:
1. If E unbound, V bound: Scan all entities and filter by membership (expensive but correct)
2. If E unbound, V unbound: Scan all entities and resolve each set (very expensive)
3. Consider adding a reverse index `(A, V) → set of E's` for efficient lookup

**Resolution:** ✅ Fixed in `matcher_relations.go`:
- Added `useAddWinsScanAllEntities` and `useAddWinsScanAllEntitiesWithValue` flags
- Added `cardinalityManyScanAllEntitiesIterator` streaming iterator for [?e :attr ?v] patterns
- Added `cardinalityManyFindEntitiesWithValueIterator` streaming iterator for [?e :attr "value"] patterns
- Both use streaming at entity level (not materialized), maintaining the codebase's streaming architecture

---

### Bug #3: Set() Reads Committed State Only (Ignores Pending Transaction Ops)

**Location:** `database.go` lines 1434-1442

**Problem:** `Set()` calls `resolveAddWinsSet()` which reads from the committed store state. It does not see pending Add/Remove operations from earlier in the same transaction.

**Example:**
```go
tx.Add(e, attr, "foo")                           // Pending, not committed
tx.Set(e, attr, []interface{}{"bar"})            // Reads committed state (no "foo")
tx.Commit()
// Result: {"foo", "bar"} - NOT {"bar"} as user might expect
```

**Analysis:** Set() diffs against committed state:
- Current committed set: `{}`
- New desired set: `{"bar"}`
- Diff: Add "bar" (no removes needed)
- But pending Add("foo") also gets committed
- Final: `{"foo", "bar"}`

**Semantic Question:** Is this a bug or intended behavior?

| Interpretation | Behavior | Argument |
|----------------|----------|----------|
| Bug | Set() should see pending ops | User expects Set() to replace everything |
| Intended | Set() only sees committed | CRDT ops are independent; each has own Lamport |

**Current Behavior Classification:** Surprising but arguably correct for CRDT semantics. Each operation (Add, Set) is independent. Set() generates ops based on committed state at the time of the call.

**Recommendation:** If we want Set() to be a true "replace" operation:
1. Track pending adds/removes in transaction
2. When Set() is called, clear pending ops for that (E, A)
3. Or: Document that Set() should not be mixed with Add/Remove in same transaction

**Resolution:** ✅ Fixed in `database.go`:
- Set() now scans `t.datoms` for pending Add/Remove ops for the same (E, A) pair
- Applies pending ops on top of committed state to compute "effective" current set
- Uses effective set for diff calculation
- Tests added: `TestCardinalityManySetSeesPendingOps` and `TestCardinalityManySetSeesCommittedAndPending`

---

### Testing Gaps Identified

These bugs reveal missing test coverage:

| Test Case | Status |
|-----------|--------|
| `[?e :attr "value"]` with cardinality-many | **Missing** |
| `[?e :attr ?v]` with cardinality-many, E unbound | **Missing** |
| `Set()` with duplicate values in slice | **Missing** |
| `Add()` then `Set()` in same transaction | **Missing** |

---

## Why This Project?

### The Problem

The current janus-datalog storage layer has fundamental limitations:

1. **No multi-writer support** - A single database instance assumes exclusive write access. Multiple writers (replicas, concurrent processes) can corrupt data or produce undefined results.

2. **No ordered collections** - Cardinality-many attributes are unordered sets. There's no way to represent sequences where order matters (skill progressions, event logs, document sections).

3. **Brittle conflict handling** - When conflicts occur (same E/A written twice), behavior depends on write order, which is non-deterministic across replicas.

### What This Enables

1. **Multi-replica architectures** - Independent database instances can accept writes offline and merge later with deterministic conflict resolution. Enables edge computing, mobile-first, and distributed deployments.

2. **Native vector/list support** - Ordered collections as a first-class cardinality. Skills, events, and sequences maintain their order through concurrent modifications.

3. **Predictable semantics** - Every write gets a globally-ordered ElementID. Conflicts resolve deterministically: LWW for scalars, add-wins for sets, RGA for vectors.

4. **Time-travel queries** - ElementID-based ordering enables "as-of" queries and audit trails across the entire database, not just within a single transaction.

### Why CRDTs?

CRDTs (Conflict-free Replicated Data Types) provide **mathematically proven** merge semantics. When two replicas diverge and later sync:

- The merge is **deterministic** - same inputs always produce same output
- The merge is **commutative** - order of receiving updates doesn't matter
- The merge is **idempotent** - replaying the same update is safe

This eliminates an entire class of distributed systems bugs. We're not inventing conflict resolution - we're implementing well-studied algorithms (LWW registers, add-wins sets, RGA sequences).

---

## Executive Summary

This plan details the implementation of CRDT-based storage with native vector support for janus-datalog. The change is **foundational** - it **unifies Tx with ElementID**, so the storage model remains 4-tuples `(E, A, V, Tx)` but Tx becomes a 16-byte `ElementID` containing `(Lamport uint64, ReplicaID uint64)` instead of the previous 20-byte transaction ID.

**Key Design Decisions:**

1. **Tx = ElementID** - Transaction ID is unified with CRDT logical clock. No separate "I" field.
2. **Per-database Lamport clock** - Single clock shared across all attributes for global temporal ordering.
3. **LWW (Last-Writer-Wins) semantics** - Higher (Lamport, ReplicaID) wins. Not vector clocks.
4. **Remove `tx.SetTime()` capability** - Business timestamps become regular attributes, not Tx metadata.
5. **Tx field shrinks** - From 20 bytes to 16 bytes (Lamport + ReplicaID).

**Backward compatibility is NOT a concern.** This is a clean-sheet replacement of the storage layer.

**Key Principles:**
1. **Correctness before performance** - Get semantics right first
2. **Exhaustive testing** - Every component tested in isolation and integration
3. **Incremental delivery** - Each phase produces working, testable code
4. **Replace, don't extend** - No dual-mode operation or migration paths

> **⚠️ IMPORTANT: Vector Append Semantics**
>
> Vector `Append()` does NOT mean "append to end of current list." It means "insert after my last known element in this transaction."
>
> **Cross-replica behavior:** When two replicas concurrently append to the same vector, both elements are preserved but their final order is determined by ElementID sorting, NOT by "who appended first." For example:
> - Replica A appends "x" at Lamport 5
> - Replica B appends "y" at Lamport 5 (concurrent, before merge)
> - After merge: order is `["x", "y"]` if ReplicaID(A) < ReplicaID(B)
>
> This is deterministic and consistent across all replicas, but may not match user expectations of "earlier write = earlier position." Applications requiring strict temporal ordering should use explicit position attributes or timestamps.
>
> See [Phase 5.3](#53-vector-transaction-operations) for detailed semantics.

> **✅ Clock Restoration: IMPLEMENTED**
>
> Clock restoration on database open is complete. The implementation:
> 1. **Phase 2.2**: ✅ Tx field is 16 bytes (ElementID)
> 2. **Phase 2.3**: ✅ TAEV index encodes ElementID with descending sort order (bitwise NOT)
> 3. **Phase 1.3**: ✅ `store.MaxElementID()` + `clock.Restore()` called on database open
>
> The Lamport clock is now correctly restored to a value greater than any existing data.

---

## Design Decisions

### LWW Semantics (Not Vector Clocks)

This implementation uses **Lamport clocks with Last-Writer-Wins (LWW)** semantics, not vector clocks.

**What this means:**
- Concurrent writes across replicas are resolved deterministically: higher `(Lamport, ReplicaID)` wins
- We do NOT detect true concurrency (unlike Riak's vector clocks which can identify siblings)
- If two replicas write concurrently, one value wins silently - no user notification

**Multi-replica support:**
- The design fully supports multiple replicas with different ReplicaIDs
- Replicas can diverge (independent writes) and merge (via `Receive()`)
- `ReplicaID` provides deterministic tiebreaking when replicas generate the same Lamport value
- Within a single replica, the atomic clock serializes all goroutines - each write gets a unique Lamport value, so ReplicaID tiebreaking never activates locally

**Why Lamport over Vector Clocks:**
- Simpler implementation - single counter vs map of counters per replica
- Total ordering - any two ElementIDs are comparable (vector clocks give partial order)
- CRDT semantics (add-wins for sets, RGA for vectors) provide meaningful conflict resolution beyond just "latest wins"
- Vector clocks detect concurrency but require application-level sibling resolution
- Can be redesigned later if concurrent-write detection becomes necessary

**Trade-off:** Vector clocks would allow detecting "these writes were concurrent" and surfacing both values as siblings for user resolution. LWW picks a winner deterministically, which may silently discard a concurrent write. For most use cases, the CRDT-level conflict resolution (add-wins, RGA ordering) provides sufficient semantics without needing sibling detection.

### ElementID Comparison: LWW vs Add-Wins

**CRITICAL DISTINCTION**: The comparison strategy differs by cardinality:

| Cardinality | Strategy | Comparison | Rationale |
|-------------|----------|------------|-----------|
| **One** | LWW (Last-Writer-Wins) | Full ElementID: `(Lamport, ReplicaID)` | Need total order to pick ONE winner |
| **Many** | Add-Wins | Lamport only | Concurrent = same Lamport; add wins at concurrent |
| **Vector** | RGA | ElementID for ordering | Deterministic element ordering |

**Why Add-Wins uses Lamport-only:**

For cardinality-many sets, "add-wins" means: if add and remove are **concurrent**, add wins.

"Concurrent" in Lamport clock terms means: same Lamport value. Two operations at the same Lamport could not have observed each other (they happened on different replicas before any sync).

If we used full ElementID comparison for add-wins:
```
Add at (Lamport=5, ReplicaID=100)
Remove at (Lamport=5, ReplicaID=200)

Full comparison: (5, 200) > (5, 100) → Remove wins
Lamport-only:    Same Lamport → Add wins (correct add-wins semantics)
```

Full comparison would make the higher ReplicaID "win" conflicts, which:
1. Violates the add-wins contract (remove can beat add)
2. Introduces arbitrary replica bias (higher ID = more power)
3. Makes conflict resolution depend on random ID assignment

**Summary:**
- **LWW (cardinality-one)**: Full ElementID comparison provides total order for picking one winner
- **Add-wins (cardinality-many)**: Lamport-only comparison correctly identifies concurrent operations

This is why `resolveAddWinsSet` and `checkSetMembership` compare `.Lamport` values, while cardinality-one resolution uses full `ElementID.Compare()`.

### Removing `tx.SetTime()` Capability

The ability for users to set transaction timestamps is **removed**. Tx is now the system-controlled Lamport clock.

**Migration path:**
- Business timestamps (e.g., "when did this price occur?") become regular attributes
- Example: Instead of `tx.SetTime(priceTime)`, store `[:db/add entity :price/time priceTime]`

**Impact:**
- `as-of` queries now mean "database state at Lamport time L" (system ordering)
- For "what was the price on date X?", query the `:price/time` attribute
- Time-range queries on business time use predicate filtering (no automatic index)

**Rationale:**
- Lamport clock invariants require system control - user-settable timestamps would break causality guarantees
- Conflating system time with business time was a shortcut that obscured semantics
- Cleaner data model: business time is data (stored as attributes), system time is metadata (Tx/Lamport)

### Nil Values Not Allowed

`nil` is NOT a valid value in this system. This follows from relational algebra semantics where NULL/nil represents "unknown" or "missing", not a concrete value.

**Implications:**
- `Set(e, a, nil)` → Error
- `Add(e, a, nil)` → Error
- `Append(e, a, nil)` → Error

**To represent "no value":**
- Don't write the attribute (absence = no value)
- For cardinality-one: the attribute simply doesn't exist for the entity
- For cardinality-many: empty set (no members)
- For cardinality-vector: empty vector (no elements)

**To "delete" a value:**
- Cardinality-one: Write a new value (old value becomes history) or use schema-level deletion
- Cardinality-many: `Remove(e, a, v)` tombstones the value
- Cardinality-vector: `Set(e, a, newSlice)` replaces entire vector

This is consistent with Datalog semantics where facts are positive assertions, not nullable fields.

### Schema Cardinality Migration

When an attribute's cardinality changes in the schema, existing data is automatically migrated on first read.

**Migration Semantics:**

| From | To | Migration Behavior |
|------|-----|-------------------|
| One | Many | Existing value becomes single-element set (wrapped with OpAdd) |
| One | Vector | Existing value becomes single-element vector (wrapped as RGA element after HEAD) |
| Many | One | Highest ElementID value becomes current; others become history |
| Many | Vector | Set elements become vector elements (ordered by ElementID) |
| Vector | One | Last element (highest position) becomes current; others become history |
| Vector | Many | Vector elements become set members (order lost) |

**Implementation:**

```go
// migrateCardinality converts existing data when schema cardinality changes
func (db *Database) migrateCardinality(e, a []byte, from, to Cardinality) error {
    switch {
    case from == CardinalityOne && to == CardinalityMany:
        // Read current value
        iter, _ := db.store.Scan(EATV, EncodePrefix(EATV, e, a), ...)
        if iter.Next() {
            datom := decodeDatom(iter.Key(), iter.Value())
            // Rewrite as set entry with OpAdd
            elemID := db.clock.Next()
            newDatom := StorageDatom{
                E:  e,
                A:  a,
                V:  EncodeSetEntry(SetEntry{Value: datom.V, Op: OpAdd}),
                Tx: elemID.Encode(),
            }
            db.writeDatom(newDatom)
        }
        iter.Close()

    case from == CardinalityOne && to == CardinalityVector:
        // Read current value
        iter, _ := db.store.Scan(EATV, EncodePrefix(EATV, e, a), ...)
        if iter.Next() {
            datom := decodeDatom(iter.Key(), iter.Value())
            // Rewrite as RGA element after HEAD
            elemID := db.clock.Next()
            newDatom := StorageDatom{
                E:  e,
                A:  a,
                V:  EncodeRGAElement(RGAElement{ID: elemID, Value: datom.V, AfterRef: HEAD}),
                Tx: elemID.Encode(),
            }
            db.writeDatom(newDatom)
        }
        iter.Close()

    // ... other cases follow similar patterns
    }
    return nil
}
```

**When Migration Occurs:**
- On first read of an (E, A) pair after schema change
- During cache rebuild for that (E, A)
- Lazy: only migrated data is touched, not entire attribute

**Important Considerations:**
- Migration is one-way per read - old format data coexists until read
- `Many → One` and `Vector → One` are lossy (some values become history-only)
- Bulk migration can be triggered via `db.WarmCache([]Keyword{attr})`

**Tasks:**
- [ ] Implement `migrateCardinality()` for all 6 transitions
- [ ] Integrate migration into cache rebuild path
- [ ] Add schema version tracking to detect changes
- [ ] Handle concurrent migration (idempotent writes)

**Tests:** `datalog/storage/migration_test.go`
- [ ] `TestMigrateOneToMany` - value wrapped as set entry
- [ ] `TestMigrateOneToVector` - value wrapped as RGA element
- [ ] `TestMigrateManyToOne` - highest ElementID wins
- [ ] `TestMigrateManyToVector` - preserves all values, orders by ElementID
- [ ] `TestMigrateVectorToOne` - last element wins
- [ ] `TestMigrateVectorToMany` - preserves all values, loses order
- [ ] `TestMigrationIdempotent` - migrating twice is safe
- [ ] `TestMigrationLazy` - only touched data migrated

### EDN Serialization Format

Tx is serialized as a `[lamport replica]` tuple in EDN export/import:

```edn
;; New format
{:e "entity-hash" :a :foo/bar :v 42 :tx [12345 0]}

;; Old format (auto-detected on import for backward compatibility)
{:e "entity-hash" :a :foo/bar :v 42 :tx 12345}
```

**Import logic:**
- If `:tx` is an integer → Old format, convert to `[value 0]`
- If `:tx` is a vector → New format, use `[lamport replica]` directly

**After import:** `clock.Restore(maxSeen)` ensures new writes have higher Lamport values.

### Database Export/Import

**File:** `datalog/storage/export.go` (MODIFY)

Export and import use the `Merge()` API to preserve ElementIDs, enabling cross-replica data sharing.

**Export:**

```go
// Export writes all datoms to EDN format, preserving ElementIDs.
// The exported data can be imported into any replica.
func (db *Database) Export(w io.Writer) error {
    iter, err := db.store.Scan(TAEV, nil, nil)  // Full scan
    if err != nil {
        return err
    }
    defer iter.Close()

    encoder := edn.NewEncoder(w)
    for iter.Next() {
        datom := db.decodeDatom(iter.Key(), iter.Value())
        // Serialize with ElementID as [lamport, replica] tuple
        if err := encoder.Encode(datom); err != nil {
            return err
        }
    }
    return nil
}
```

**Import:**

```go
// Import reads EDN datoms and merges them into the database.
// Uses Merge() to preserve source ElementIDs, enabling cross-replica sync.
//
// IMPORTANT: This is a merge operation, not a replace operation.
// - Existing datoms are preserved
// - Imported datoms are added with their original ElementIDs
// - Conflicts are resolved by CRDT semantics (LWW, add-wins, RGA)
func (db *Database) Import(r io.Reader) error {
    decoder := edn.NewDecoder(r)
    var datoms []Datom

    for {
        var d Datom
        err := decoder.Decode(&d)
        if err == io.EOF {
            break
        }
        if err != nil {
            return err
        }
        datoms = append(datoms, d)
    }

    // Use Merge to preserve ElementIDs and update clock
    return db.Merge(datoms)
}
```

**Cross-Replica Sync Pattern:**

```go
// Replica A exports its data
file, _ := os.Create("replica-a-export.edn")
replicaA.Export(file)
file.Close()

// Replica B imports, merging with its own data
file, _ = os.Open("replica-a-export.edn")
replicaB.Import(file)  // Uses Merge(), preserving ElementIDs
file.Close()

// Both replicas now have union of data
// Conflicts resolved by CRDT semantics
```

**Why Merge() instead of fresh import:**
- Preserves causal ordering (ElementIDs reflect when data was created)
- Enables offline-first workflows (sync when connected)
- Idempotent: importing same data twice is safe
- Supports partial sync (export subset, import into different replica)

**Tasks:**
- [ ] Update `Export()` to emit ElementID as `[lamport replica]` tuple
- [ ] Update `Import()` to use `Merge()` instead of direct writes
- [ ] Handle old format detection (integer Tx → convert to ElementID)
- [ ] Add progress callback for large imports

**Tests:** `datalog/storage/export_import_test.go`
- [ ] `TestExportImportRoundTrip` - data survives export/import
- [ ] `TestImportPreservesElementIDs` - ElementIDs match after import
- [ ] `TestImportMergesWithExisting` - doesn't overwrite local data
- [ ] `TestImportUpdatesLocalClock` - clock advances past imported data
- [ ] `TestImportOldFormat` - handles legacy integer Tx format
- [ ] `TestImportIdempotent` - importing twice is safe

### ElementID Literal Syntax

**File:** `datalog/edn/reader.go` (MODIFY)

ElementID has a tagged literal syntax for use in queries and data:

```edn
;; Syntax: #eid [lamport replica-id]
#eid [1234 5678]

;; In query predicates
[:find ?e
 :where
 [?e :person/name ?name ?tx]
 [(> ?tx #eid [1000 0])]]

;; As attribute value
{:e "abc" :a :audit/created-at :v #eid [500 42] :tx [600 0]}
```

**Parser Implementation:**

```go
// RegisterTaggedReader adds #eid support
func init() {
    RegisterTaggedReader("eid", func(form any) (any, error) {
        vec, ok := form.([]any)
        if !ok || len(vec) != 2 {
            return nil, fmt.Errorf("#eid requires [lamport replica-id] vector")
        }
        lamport, ok1 := vec[0].(int64)
        replica, ok2 := vec[1].(int64)
        if !ok1 || !ok2 {
            return nil, fmt.Errorf("#eid requires integer values")
        }
        return ElementID{Lamport: uint64(lamport), ReplicaID: uint64(replica)}, nil
    })
}
```

**Tasks:**
- [ ] Add `#eid` tagged reader to EDN parser
- [ ] Update EDN writer to emit `#eid` for ElementID values
- [ ] Handle ElementID in query parser (predicates, expressions)

### Merge API

When merging data from another replica, use the `Merge` API to import datoms while preserving their original ElementIDs and updating the local clock.

**File:** `datalog/storage/database.go`

```go
// Merge imports datoms from another replica, preserving their ElementIDs.
// The local clock is updated via Receive() to maintain causality.
func (db *Database) Merge(datoms []Datom) error {
    touched := make([]CacheKey, 0, len(datoms))

    for _, d := range datoms {
        // Update local clock: L = max(L, L_remote) + 1
        db.clock.Receive(d.Tx)

        // Store datom with its original ElementID (no new clock.Next())
        sd := StorageDatom{
            E:  d.E.Hash(),
            A:  encodeAttribute(d.A),
            V:  encodeValue(d.V),  // or encoded set/vector entry
            Tx: d.Tx.Encode(),
        }

        // Write to all 6 indices
        if err := db.store.Put(sd); err != nil {
            return err
        }

        // Track touched keys and update max versions for cache freshness
        key := CacheKey{E: sd.E, A: sd.A}
        touched = append(touched, key)
        db.cache.UpdateMaxVersion(key, d.Tx)
    }

    // Invalidate cache for all touched (E, A) pairs
    db.cache.Invalidate(touched)
    return nil
}
```

**Key points:**
- **Preserves original ElementIDs** - Merged datoms keep their source replica's timestamps
- **Updates local clock via Receive()** - Ensures future local writes have higher Lamports
- **No conflict resolution during merge** - All datoms are stored; resolution happens at query time
- **Updates maxVersions** - Ensures cache freshness checks work correctly for merged data
- **Cache invalidation** - Ensures queries see merged data

**Merge vs Normal Write:**

| Operation | ElementID Source | Clock Update | Cache Update |
|-----------|------------------|--------------|--------------|
| `Set/Add/Append` | `clock.Next()` (new) | Implicit in Next() | `UpdateMaxVersion()` in Commit() |
| `Merge` | Preserved from source | `clock.Receive()` | `UpdateMaxVersion()` per datom |

**Example merge scenario:**

```go
// Replica A has: [:person/name "Alice" (Lamport=5, Replica=A)]
// Replica B has: [:person/name "Bob" (Lamport=3, Replica=B)]

// B merges from A:
replicaB.Merge(datomsFromA)
// B's clock: Receive((5, A)) → L = max(3, 5) + 1 = 6
// B now has both datoms; queries return "Alice" (higher Lamport wins)

// A merges from B:
replicaA.Merge(datomsFromB)
// A's clock: Receive((3, B)) → L = max(5, 3) + 1 = 6
// A now has both datoms; queries return "Alice" (same result)
```

**Convergence guarantee:** After bidirectional merge, both replicas have identical data and return identical query results.

### Startup Clock Recovery

On database open, the Lamport clock must be restored to a value greater than any existing data.

**Implementation:** Forward scan on TAEV index (Tx-first, descending order):

```go
func (db *Database) recoverClock() error {
    // TAEV index: Tx is first component, descending order
    // Forward scan gives us highest Tx first (= highest Lamport)
    iter := db.store.Iterator(TAEV, nil)  // Forward scan
    defer iter.Close()

    if iter.Valid() {
        maxTx := decodeElementID(iter.Key()[1:17])  // Skip prefix byte
        db.clock.Restore(maxTx)
    }
    return nil
}
```

**Why TAEV?** It's the only index with Tx as the first component. With descending encoding, first entry = highest Tx = O(1) max lookup.

**Why `Restore()` not `Receive()`?**
- `Restore()` sets clock to value without incrementing
- `Receive()` is for causal message receipt: `L = max(L, L_remote) + 1`
- Database recovery is not a causal event - we're just restoring state

---

## Risk Assessment

### High Risk Areas

| Area | Risk | Mitigation |
|------|------|------------|
| **Key Encoding** | Incorrect sort order | Exhaustive sort order tests before integration |
| **RGA Reconstruction** | O(n) read performance | Position index cache, benchmark early |
| **Add-Wins Logic** | Incorrect conflict resolution | Property-based concurrent write tests |
| **Type Preservation** | Values don't round-trip | Test all 8 value types through new encoding |
| **Schema Dependency** | Cardinality-Vector requires schema | Graceful fallback to One for schemaless |

### Critical Invariants to Preserve

From CLAUDE_BUGS.md:
1. **Type preservation** - Identity/Keyword/Reference must round-trip perfectly
2. **Execution order** - Expressions before predicates in phases
3. **Symbol semantics** - Available vs Provides vs Keep distinctions
4. **Set semantics** - Queries return deduplicated results

### Storage Size Impact Analysis

With Tx = ElementID (16 bytes), keys actually **shrink by 4 bytes** compared to current (Tx was 20 bytes):

| Index | Old Size | New Size | Change |
|-------|----------|----------|--------|
| EAVT | `[1][E:20][A:32][type:1][V:var][Tx:20]` = 74+ bytes | `[1][E:20][A:32][type:1][V:var][Tx:16]` = 70+ bytes | **-5%** |
| All 5 indices | ~370 bytes/datom | ~350 bytes/datom | **-5% total** |

**Additional Benefits:**
- History no longer requires separate `_HISTORY` indices (-50% for history mode)
- No Op flag byte needed (was 1 byte per history entry)
- Single index set vs dual (current + history)
- Tx field shrinks from 20 bytes (SHA1 hash) to 16 bytes (ElementID)

**Net impact:** Storage decreases slightly. The 16-byte ElementID is smaller than the old 20-byte Tx field.

### API Limitations to Document

**Vector Remove Limitation:**

The `Remove(e, a, v)` method is **NOT valid for Vector cardinality**. The only way to remove elements from a vector is `Set(e, a, newSlice)` which requires:

1. Read current vector: O(n)
2. Filter out unwanted element(s)
3. Write new vector (tombstones all existing + writes new elements)

This is intentional - RGA doesn't support efficient individual element removal by value because elements are identified by ElementID, not value. To remove efficiently, you'd need to expose ElementID to users, which breaks the abstraction.

| Method | One | Many | Vector |
|--------|-----|------|--------|
| `Set(e, a, v)` | ✓ New version | ✓ Replace set | ✓ Replace vector |
| `Add(e, a, v)` | ✗ Error | ✓ Add to set | ✗ Error |
| `Remove(e, a, v)` | ✗ Error | ✓ Tombstone value | ✗ Error |
| `Append(e, a, v)` | ✗ Error | ✗ Error | ✓ Append element |

### Deferred: Compaction and Garbage Collection

**Status:** Intentionally deferred to future work.

This implementation stores **all historical values indefinitely**. For cardinality-one attributes with frequent updates, this means unbounded storage growth over time.

**Why deferred:**
- Core CRDT semantics must be correct first
- History is valuable for audit, debugging, and time-travel queries
- Premature optimization before understanding real-world usage patterns

**Future considerations when implementing:**
- Optional compaction policy: keep only N most recent versions per (E, A)
- Lamport-age-based GC: remove entries older than L_current - threshold
- Tombstone cleanup for cardinality-many: remove tombstones after all replicas have synced
- Per-attribute retention policies via schema metadata

**Current mitigation:** For attributes with very high update frequency, consider:
- Using cardinality-many with explicit versioning (each version as a set member)
- External archival of old data with periodic database rebuild
- Monitoring storage growth and planning capacity accordingly

---

## Phase 0: Foundation (Prerequisites)

Before any implementation, establish the testing and compatibility foundation.

> **STATUS: ✅ COMPLETE**

### 0.1 Schema Infrastructure Audit

**Goal:** Ensure schema layer can support new cardinality.

> **STATUS: ✅ DONE**

**Implemented in `schema/types.go`:**
```go
const (
    CardinalityOne    Cardinality = "db.cardinality/one"
    CardinalityMany   Cardinality = "db.cardinality/many"
    CardinalityVector Cardinality = "db.cardinality/vector" // Ordered collection (RGA)
)
```

**Tasks:**
- [x] Audit `schema/types.go` - `CardinalityVector` constant added
- [x] Audit `schema/builder.go` - `.Vector()` builder method added
- [ ] Audit `schema/validation.go` - vector type validation (deferred to Phase 5)
- [ ] Audit `reflect/reader.go` and `reflect/writer.go` - struct tag support (deferred to Phase 7)

**Test:**
```go
func TestCardinalityVectorSchemaDeclaration(t *testing.T) {
    s, err := schema.NewBuilder().
        Attribute(":character/skills").
        Type(schema.TypeString).
        Vector().  // NEW
        Add().
        Build()

    require.NoError(t, err)
    attr, ok := s.Get(datalog.NewKeyword(":character/skills"))
    require.True(t, ok)
    assert.Equal(t, schema.CardinalityVector, attr.Cardinality)
}
```

### 0.2 ReplicaID Configuration

**Goal:** Add ReplicaID to database options for CRDT operation.

> **STATUS: ✅ DONE**

**Implementation:**
```go
// datalog/storage/options.go
type DatabaseOptions struct {
    // ... existing options ...

    ReplicaID uint64  // For new DBs: 0 = auto-generate random; non-zero = use specified. Ignored for existing DBs.
}
```

**Tasks:**
- [x] Add `ReplicaID` to `DatabaseOptions`
- [x] Generate random 64-bit ReplicaID on database creation if not specified
- [x] Store ReplicaID in database metadata (persist across restarts)
- [ ] Document: ReplicaID is auto-generated; explicit assignment only needed for deterministic testing

---

## Phase 1: ElementID and Lamport Clocks

**Goal:** Implement the core primitives without changing storage.

> **STATUS: ✅ COMPLETE**
> - 1.1 ElementID: ✅ Type exists in `datalog/element_id.go`, key encoding with bitwise NOT in `storage/element_id.go`. Tests exist in `storage/element_id_test.go`.
> - 1.2 LamportClock: ✅ Implementation complete in `storage/lamport_clock.go`. Tests exist in `storage/lamport_clock_test.go`.
> - 1.3 Database Integration: ✅ DONE including clock restoration.

### 1.1 ElementID Type

**File:** `datalog/storage/element_id.go` (NEW)

```go
package storage

import (
    "encoding/binary"
)

// ElementID uniquely identifies a datom version across all replicas.
//
// Lamport: Logical timestamp from this replica's clock. Provides causal ordering.
// ReplicaID: Identifies which Database instance generated this timestamp. Used for
//            tiebreaking when two replicas generate the same Lamport value.
//
// Within a single Database instance:
//   - All goroutines share one atomic clock
//   - Every write gets a unique Lamport value (atomic increment)
//   - ReplicaID tiebreaking never activates (same ReplicaID, different Lamports)
//
// Across multiple Database instances (replication/merge scenarios):
//   - Each instance should have a unique ReplicaID
//   - Independent clocks may generate the same Lamport value
//   - ReplicaID provides deterministic tiebreaking for LWW resolution
//
// ReplicaID is auto-generated (random 64-bit) on database creation, ensuring
// independently created databases can be safely merged without coordination.
type ElementID struct {
    Lamport   uint64  // Logical timestamp (causal ordering)
    ReplicaID uint64  // Database instance identifier (tiebreaker when merging)
}

// Zero value represents HEAD sentinel for RGA
var HEAD = ElementID{0, 0}

// Less implements total ordering for ElementIDs
func (a ElementID) Less(b ElementID) bool {
    if a.Lamport != b.Lamport {
        return a.Lamport < b.Lamport
    }
    return a.ReplicaID < b.ReplicaID
}

// IsZero returns true for the HEAD sentinel
func (a ElementID) IsZero() bool {
    return a.Lamport == 0 && a.ReplicaID == 0
}

// Encode writes 16 bytes for storage keys.
// Uses bitwise NOT so highest ElementID sorts first (first entry = current value).
func (a ElementID) Encode() []byte {
    buf := make([]byte, 16)
    binary.BigEndian.PutUint64(buf[0:8], ^a.Lamport)
    binary.BigEndian.PutUint64(buf[8:16], ^a.ReplicaID)
    return buf
}

// DecodeElementID reads 16 bytes from storage key format.
func DecodeElementID(buf []byte) ElementID {
    return ElementID{
        Lamport: ^binary.BigEndian.Uint64(buf[0:8]),
        ReplicaID:  ^binary.BigEndian.Uint64(buf[8:16]),
    }
}

// String returns human-readable representation: "L1234@R5678"
func (a ElementID) String() string {
    return fmt.Sprintf("L%d@R%d", a.Lamport, a.ReplicaID)
}
```

#### ElementID as First-Class Value Type

**Why?** ElementID is the fundamental ordering primitive in a CRDT system. Users need to:

1. **Store ElementIDs as attribute values** - "When was this entity created?" → `:entity/created-at` stores the ElementID
2. **Query by ElementID ranges** - "Find all changes between Tx 1000 and 2000"
3. **Compare ElementIDs in predicates** - `[(> ?tx1 ?tx2)]` for causal ordering
4. **Return ElementIDs in query results** - Expose transaction metadata to applications
5. **Reference specific points in time** - Correlate events across entities

Without first-class support, users would need to manually extract and compare Lamport/ReplicaID integers, losing the semantic meaning and type safety.

**File:** `datalog/types.go` (MODIFY)

ElementID is exposed as a native type throughout the stack, similar to Identity and Keyword. Users can store, retrieve, and compare ElementIDs as values.

**Value Type Registration:**

```go
// ElementID is a recognized value type (like Identity, Keyword, string, int64, etc.)
// Type tag for value encoding: TypeElementID = 0x09
const TypeElementID byte = 0x09

// EncodeValue handles ElementID as a value (different from key encoding)
func EncodeValue(v any) []byte {
    switch val := v.(type) {
    // ... existing cases ...
    case ElementID:
        buf := make([]byte, 17)
        buf[0] = TypeElementID
        binary.BigEndian.PutUint64(buf[1:9], val.Lamport)
        binary.BigEndian.PutUint64(buf[9:17], val.ReplicaID)
        return buf
    }
}

// DecodeValue handles ElementID values
func DecodeValue(buf []byte) any {
    switch buf[0] {
    // ... existing cases ...
    case TypeElementID:
        return ElementID{
            Lamport:   binary.BigEndian.Uint64(buf[1:9]),
            ReplicaID: binary.BigEndian.Uint64(buf[9:17]),
        }
    }
}
```

**IMPORTANT: Key vs Value Encoding**

ElementID appears in two contexts with different byte encodings, but decoding ALWAYS returns the natural value:

| Context | Encode | Decode | Scan Order |
|---------|--------|--------|------------|
| Tx in index keys | `^Lamport, ^ReplicaID` | `^` to reverse | Highest first |
| ElementID as value | Natural | Natural | Lowest first |

The `^` is symmetric - applied on encode, reversed on decode. Both paths return the same natural ElementID. The difference is **scan order in storage**:

```go
// KEY: ^Lamport, ^ReplicaID on encode → highest Tx first in scans
// Decode reverses ^ to get natural value
func DecodeElementID(buf []byte) ElementID {
    return ElementID{
        Lamport:   ^binary.BigEndian.Uint64(buf[0:8]),
        ReplicaID: ^binary.BigEndian.Uint64(buf[8:16]),
    }
}

// VALUE: Natural order on encode → lowest first in AVET scans
// Decode is just reading natural order
func decodeElementIDValue(buf []byte) ElementID {
    return ElementID{
        Lamport:   binary.BigEndian.Uint64(buf[0:8]),
        ReplicaID: binary.BigEndian.Uint64(buf[8:16]),
    }
}
```

**Why?**
- Key Tx with `^`: Forward scan returns newest entries first (current value optimization)
- Value with natural: AVET range scans on ElementID values work intuitively (lowest to highest)

**EDN Representation:**

```clojure
;; ElementID literal syntax: #eid [lamport replica-id]
#eid [1234 5678]

;; In queries - bind and compare ElementIDs
[:find ?e ?tx
 :where
 [?e :person/name ?name ?tx]
 [(> ?tx #eid [1000 0])]]   ;; Filter by ElementID

;; Store ElementID as attribute value
[:find ?e
 :where
 [?e :audit/created-by ?creator-tx]
 [(= ?creator-tx #eid [500 42])]]
```

**Comparison Support:**

```go
// compareValues in executor handles ElementID
func compareValues(a, b any) int {
    switch av := a.(type) {
    // ... existing cases ...
    case ElementID:
        bv := b.(ElementID)
        if av.Less(bv) {
            return -1
        }
        if bv.Less(av) {
            return 1
        }
        return 0
    }
}
```

**Use Cases:**

1. **Audit trails** - Store which transaction created/modified an entity
2. **Causal references** - "This event happened after transaction X"
3. **Merge conflict resolution** - Compare ElementIDs to determine winner
4. **Debugging** - Query for specific transaction ranges

> **STATUS: ✅ CORE COMPLETE**
> - ✅ ElementID type in `datalog/element_id.go` with Less(), IsZero(), String(), Bytes(), Compare()
> - ✅ Key encoding with bitwise NOT in `storage/element_id.go`: EncodeElementIDForKey(), DecodeElementID()
> - ✅ compareValues() supports ElementID comparison
> - ✅ Tests exist in `storage/element_id_test.go`
> - ⚠️ ElementID as first-class VALUE type (TypeElementID, AVET storage) - deferred to later phase

**Tests:** `datalog/storage/element_id_test.go` ✅ EXISTS
- [x] `TestElementIDOrdering` - verify Less() for all edge cases
- [x] `TestElementIDEncodeDecode` - round-trip key encode/decode
- [x] `TestElementIDSortOrderPreservation` - encoded bytes sort correctly (highest first)
- [x] `TestElementIDZero` - HEAD sentinel behavior
- [x] `TestElementIDString` - String() format "L1234@R5678"

**Tests:** `datalog/value_encoding_test.go` (ADD)
- [ ] `TestElementIDValueEncode` - round-trip value encode/decode (natural order)
- [ ] `TestElementIDValueType` - TypeElementID tag correct
- [ ] `TestElementIDKeyVsValueEncoding` - verify key uses `^`, value uses natural order
- [ ] `TestElementIDValueRangeScan` - AVET scan with ElementID values sorts correctly

**Tests:** `datalog/parser/edn_test.go` (ADD)
- [ ] `TestParseElementIDLiteral` - parse `#eid [1234 5678]`
- [ ] `TestElementIDInQuery` - ElementID in where clause

**Tests:** `datalog/executor/compare_test.go` (ADD)
- [ ] `TestCompareElementID` - comparison operators work
- [ ] `TestElementIDPredicates` - `<`, `>`, `=` in queries

### 1.2 Lamport Clock

**File:** `datalog/storage/lamport_clock.go` (NEW)

**Design Decision: Per-Database Clock (not per-attribute)**

The Lamport clock is shared across the entire database, not partitioned by entity or attribute. This enables:

1. **Point-in-time snapshots** - "What was the entity's state at Lamport time T?" requires comparing timestamps across all attributes of an entity. A global clock makes this query trivial.

2. **Causal ordering across attributes** - If setting `:person/name` causally precedes setting `:person/address` (e.g., same transaction, or user saw the name before changing address), the global clock preserves `L(name) < L(address)`.

3. **As-of query support** - Existing `as-of` functionality depends on global temporal ordering. Per-attribute clocks would break this.

4. **Transaction reconstruction** - Logical transactions touching multiple attributes maintain consistent ordering.

5. **Merge reasoning** - When merging from replicas, global ordering lets you reason about the relative order of all operations, not just within single attributes.

**Trade-off**: Single atomic counter could be a bottleneck under extreme write contention. In practice, atomic increment is nanoseconds—not a concern for typical workloads.

```go
package storage

import "sync/atomic"

// LamportClock implements Lamport's logical clock algorithm.
//
// IMPORTANT: One clock per Database instance, shared across all attributes.
// This provides global temporal ordering for point-in-time queries, causal
// debugging, and as-of functionality.
//
// Rules (from Lamport 1978):
// 1. Before each local event: L = L + 1
// 2. When sending: include L in message
// 3. When receiving message with L_remote: L = max(L, L_remote) + 1
//
// This ensures: if A causally precedes B, then L(A) < L(B)
type LamportClock struct {
    counter   uint64
    replicaID uint64
}

// NewLamportClock creates a clock for the given replica
func NewLamportClock(replicaID uint64) *LamportClock {
    return &LamportClock{replicaID: replicaID}
}

// Next generates the next ElementID for a local event.
// Implements rule 1: L = L + 1 before each event.
func (c *LamportClock) Next() ElementID {
    next := atomic.AddUint64(&c.counter, 1)
    return ElementID{Lamport: next, ReplicaID: c.replicaID}
}

// Receive updates the clock when receiving data from another node.
// Implements rule 3: L = max(L, L_remote) + 1
//
// This is the critical Lamport property: receiving a message is an event,
// and the receiver's clock must be greater than both its previous value
// AND the sender's timestamp.
func (c *LamportClock) Receive(remote ElementID) {
    for {
        current := atomic.LoadUint64(&c.counter)
        // L = max(L, L_remote) + 1
        newVal := max(current, remote.Lamport) + 1
        if atomic.CompareAndSwapUint64(&c.counter, current, newVal) {
            return
        }
        // CAS failed, retry
    }
}

// Restore sets the clock to a known value without incrementing.
// Used ONLY for restoring state on database reopen, NOT for receiving messages.
func (c *LamportClock) Restore(maxSeen ElementID) {
    for {
        current := atomic.LoadUint64(&c.counter)
        if maxSeen.Lamport <= current {
            return // Already at or past this value
        }
        if atomic.CompareAndSwapUint64(&c.counter, current, maxSeen.Lamport) {
            return
        }
    }
}

// Current returns the current clock value without incrementing
func (c *LamportClock) Current() uint64 {
    return atomic.LoadUint64(&c.counter)
}

// ReplicaID returns this clock's replica identifier
func (c *LamportClock) ReplicaID() uint64 {
    return c.replicaID
}

// Note: Uses Go 1.21+ builtin max() function
```

> **STATUS: ✅ COMPLETE**
> - LamportClock type exists in `datalog/storage/lamport_clock.go`
> - All methods implemented: Next(), Receive(), Restore(), Current(), ReplicaID()
> - Tests exist in `datalog/storage/lamport_clock_test.go`

**Tests:** `datalog/storage/lamport_clock_test.go` ✅ EXISTS
- [x] `TestLamportClockNext` - monotonically increasing
- [x] `TestLamportClockReceive` - L = max(L, L_remote) + 1
- [x] `TestLamportClockReceiveCausality` - receiving always advances past remote
- [x] `TestLamportClockRestore` - restores without incrementing
- [x] `TestLamportClockConcurrency` - safe under concurrent Next()/Receive()

### 1.3 Integration with Database

> **⚠️ DEPENDENCY: Clock restoration requires Phase 2 to be complete.**
>
> The `clock.Restore(maxID)` call below requires `store.MaxElementID()` which depends on:
> - Phase 2.2: StorageDatom.Tx changes from 20 bytes to 16 bytes
> - Phase 2.3: TAEV index uses 16-byte ElementID with descending sort order
>
> **Do NOT implement workarounds.** Complete Phase 2 first, then implement clock restoration.

**File:** `datalog/storage/database.go` (MODIFY)

```go
type Database struct {
    // ... existing fields ...

    clock     *LamportClock  // NEW: for CRDT mode
    replicaID uint64         // NEW: this replica's ID
}

func NewDatabaseWithOptions(opts DatabaseOptions) (*Database, error) {
    // ... existing code ...

    // Determine ReplicaID: stored (existing DB) vs new (creating DB)
    storedReplicaID, exists := db.store.GetMetadata("replica_id")

    var replicaID uint64
    if exists {
        // Existing database: use stored ReplicaID (opts.ReplicaID ignored)
        replicaID = storedReplicaID
    } else {
        // New database: generate or use specified
        if opts.ReplicaID == 0 {
            replicaID = rand.Uint64()
        } else {
            replicaID = opts.ReplicaID
        }
        db.store.SetMetadata("replica_id", replicaID)
    }

    db.replicaID = replicaID
    db.clock = NewLamportClock(replicaID)

    // Restore clock state from stored max ElementID (not a receive event)
    // REQUIRES Phase 2: TAEV index with 16-byte ElementID encoding
    maxID := db.store.MaxElementID()
    db.clock.Restore(maxID)

    return db, nil
}
```

> **STATUS: ✅ DONE**

**Tasks:**
- [x] Add `clock` and `replicaID` fields to Database (DONE)
- [x] Add metadata storage methods: `GetMetadataUint64`, `SetMetadataUint64` (DONE)
- [x] On new DB: generate random ReplicaID (or use opts if specified), persist to metadata (DONE)
- [x] On existing DB: load ReplicaID from metadata, ignore opts.ReplicaID (DONE)
- [x] Restore Lamport clock from max ElementID on open (DONE)

---

## Phase 2: Key Encoding with ElementID

**Goal:** Modify existing key encoding to include ElementID component.

> **STATUS: ✅ COMPLETE**
> - 2.1 Modify Datom Type: ✅ DONE (Datom.Tx is now ElementID)
> - 2.2 Modify StorageDatom: ✅ DONE (Tx is [16]byte)
> - 2.3 Index Types: ✅ DONE (EATV index exists in store.go, key_encoder_binary.go)
> - 2.4 Modify Key Encoder: ✅ DONE (16-byte Tx with bitwise NOT via EncodeElementIDForKey/EncodeElementIDInto)
> - 2.5 Modify Store Interface: ✅ DONE (MaxElementID() and Iterator.ElementID() implemented)
> - 2.6 Cardinality-Aware Index Selection: ✅ DONE (matcher.go uses EATV for one/vector, EAVT for many)

### 2.1 Modify Datom Type

**File:** `datalog/types.go` (MODIFY)

> **STATUS: ✅ DONE** - Datom.Tx is now `datalog.ElementID`

Tx becomes ElementID (unified):

```go
// Datom is the fundamental unit of data
type Datom struct {
    E  Identity    // Entity
    A  Keyword     // Attribute
    V  Value       // Value
    Tx ElementID   // Transaction/CRDT version (was uint64)
}

// For backward compatibility in user-facing APIs
func (d Datom) TxLamport() uint64   { return d.Tx.Lamport }
func (d Datom) TxReplicaID() uint64 { return d.Tx.ReplicaID }
```

### 2.2 Modify StorageDatom

**File:** `datalog/storage/types.go` (MODIFY)

> **STATUS: ✅ DONE** - Tx changed from [20]byte to [16]byte

```go
// StorageDatom is the storage representation
type StorageDatom struct {
    E  [20]byte    // Entity hash
    A  [32]byte    // Attribute
    V  []byte      // Value (with type prefix)
    Tx [16]byte    // ElementID: Lamport (8) + ReplicaID (8)
}

// Tx field encoding:
// Bytes 0-7:  Lamport (big-endian uint64)
// Bytes 8-15: ReplicaID (big-endian uint64)
```

### 2.3 Index Types

**File:** `datalog/storage/index_types.go` (MODIFY)

> **STATUS: ✅ DONE** - All 6 indices exist (EAVT, EATV, AEVT, AVET, VAET, TAEV) in store.go and key_encoder_binary.go.

Six indices to support cardinality-aware CRDT resolution. Tx is now 16-byte ElementID:

```go
const (
    EAVT IndexType = iota  // E → A → V → Tx: Cardinality-Many (group by V for add-wins)
    EATV                   // E → A → Tx → V: Cardinality-One (first = current value)
    AEVT                   // A → E → V → Tx: All entities with attribute
    AVET                   // A → V → E → Tx: Find entities with specific value
    VAET                   // V → A → E → Tx: Reverse reference lookup
    TAEV                   // Tx → A → E → V: Transaction log, startup recovery
)
```

**Key changes from current 5-index system:**
- Added `EATV` for efficient cardinality-one lookups (Tx before V enables O(1) current value)
- "T" in all index names now means 16-byte ElementID (Lamport + ReplicaID)
- Tx encoded so highest sorts first (first entry = current value)
- For TAEV chronological scans, use reverse iteration

### 2.4 Modify Key Encoder

**File:** `datalog/storage/key_encoder_binary.go` (MODIFY)

> **STATUS: ✅ DONE**
> - ✅ Tx encoding changed from 20 bytes to 16 bytes
> - ✅ Tx encoded with bitwise NOT via `EncodeElementIDForKey()` / `EncodeElementIDInto()` in `storage/element_id.go`
> - ✅ Existing tests updated for 16-byte Tx
> - ✅ EATV index encoding implemented in key_encoder_binary.go

Update Tx encoding from 20 bytes to 16 bytes (ElementID). All 6 index layouts:

```
EAVT: [prefix:1][E:20][A:32][type:1][V:var][Tx:16↓]     Cardinality-Many
EATV: [prefix:1][E:20][A:32][Tx:16↓][type:1][V:var]     Cardinality-One (Tx before V!)
AEVT: [prefix:1][A:32][E:20][type:1][V:var][Tx:16↓]     By attribute
AVET: [prefix:1][A:32][type:1][V:var][E:20][Tx:16↓]     Value lookup
VAET: [prefix:1][type:1][V:var][A:32][E:20][Tx:16↓]     Reverse refs
TAEV: [prefix:1][Tx:16↓][A:32][E:20][type:1][V:var]     Transaction log

↓ = Tx encoded so highest sorts first (^Lamport, ^ReplicaID)

Forward scan: newest entries first (highest Tx)
Reverse scan: oldest entries first (chronological order)
```

**Tasks:**
- [x] Change Tx encoding from 20 bytes to 16 bytes in all 6 indices
- [x] Add EATV index for cardinality-one optimization
- [x] Update `EncodeKey` to use ElementID.Encode() with bitwise NOT for descending sort
- [x] Update `DecodeKey` to use DecodeElementID() with bitwise NOT reversal

**Tests:** `datalog/storage/key_encoder_test.go` (MODIFY)
- [x] Update existing tests for 16-byte Tx
- [x] Round-trip tests for ElementID in Tx position (element_id_test.go)
- [x] Sort order verification (element_id_test.go TestElementIDSortOrderPreservation)
- [x] Edge cases: max ElementID, zero values (element_id_test.go)

### 2.5 Modify Store Interface

> **STATUS: ✅ DONE**
> - ✅ `MaxElementID()` added to Store interface (returns global max, not per E/A)
> - ✅ `MaxElementID()` implemented in BadgerStore using reverse iteration
> - ✅ `Iterator.ElementID()` method added to interface and implemented on BadgerIterator, KeyMaskFilterWrapper

**File:** `datalog/storage/badger_store.go` (MODIFY)

Update existing store to handle ElementID:

```go
// Add to existing Store interface
type Store interface {
    // ... existing methods ...

    MaxElementID(e, a []byte) (ElementID, error)
}

// Modify Iterator to expose ElementID
type Iterator interface {
    // ... existing methods ...

    ElementID() ElementID
}
```

### 2.6 Cardinality-Aware Index Selection

> **STATUS: ✅ DONE**
> - ✅ EATV index exists (6 indices: EAVT, EATV, AEVT, AVET, VAET, TAEV)
> - ✅ Cardinality-aware index selection implemented in matcher.go chooseIndex()
> - ✅ Matcher uses EATV for cardinality-one/vector, EAVT for cardinality-many when E+A bound

**Critical:** Index selection depends on BOTH bound components AND attribute cardinality.

**Note:** With Tx = ElementID unified, "Tx" in index names represents the 16-byte `(Lamport, ReplicaID)` tuple. The key ordering within indices determines CRDT resolution behavior.

**Index Purpose by Query Pattern:**

| Index | Key Order | Primary Use Case |
|-------|-----------|------------------|
| EAVT | E → A → V → Tx | Cardinality-Many (group by V for add-wins) |
| EATV | E → A → Tx → V | Cardinality-One (first = current) |
| AEVT | A → E → V → Tx | All entities with attribute |
| AVET | A → V → E → Tx | Find entities with specific value |
| VAET | V → A → E → Tx | Reverse reference lookup |
| TAEV | Tx → A → E → V | Transaction log / startup recovery |

**Tx encoding:** All indices encode Tx so highest sorts first. First entry in forward scan = current value.

**TAEV scan directions:**
- **Forward scan**: Newest entries first (highest Tx) - used for clock recovery
- **Reverse scan**: Oldest entries first (chronological order) - used for transaction history/audit log

**Tx range scans:** Since Tx encoding inverts sort order, the query layer rewrites range bounds:

```go
// User writes: [(tx-between ?tx 100 200)]
// Query rewriter inverts bounds for storage scan

func rewriteTxRange(low, high uint64) (startKey, endKey []byte) {
    // High Tx encodes to smaller value, so it's the start bound
    return ElementID{Lamport: high}.Encode(),  // start (smallest encoded)
           ElementID{Lamport: low}.Encode()    // end (largest encoded)
}
```

User sees intuitive `(low, high)` semantics; the inversion is handled internally.

**Why EAVT for Cardinality-Many (add-wins):**

For add-wins resolution, we need entries grouped by value:

```
EAVT scan for [E][:tags] (cardinality-many):
[E][:tags]["veteran"][Tx_high, Add]      ← First for "veteran"
[E][:tags]["warrior"][Tx_high, Add]      ← First for "warrior"
[E][:tags]["warrior"][Tx_mid, Remove]    ← Second for "warrior"
[E][:tags]["warrior"][Tx_low, Add]       ← Third for "warrior"

Processing: For each value, check if highest Tx is Add (or Add exists at same Lamport)
```

**Why EATV for Cardinality-One:**

First entry after E+A prefix is current value (highest Tx wins):

```
EATV scan for [E][:name] (cardinality-one):
[E][:name][Tx_high]["Bob"]    ← First = current
[E][:name][Tx_mid]["Alice"]   ← Superseded
[E][:name][Tx_low]["???"]     ← Superseded
```

**Index Selection Logic:**

**File:** `datalog/storage/matcher.go` (MODIFY)

```go
func (m *BadgerMatcher) selectIndex(e, a, v PatternElement) IndexType {
    eBound := e.IsBound()
    aBound := a.IsBound()
    vBound := v.IsBound()

    // Determine cardinality (default One for schemaless)
    var card Cardinality = CardinalityOne
    if aBound && m.schema != nil {
        if attr, ok := m.schema.Get(a.Value().(datalog.Keyword)); ok {
            card = attr.Cardinality
        }
    }

    switch {
    // E and A bound: cardinality determines index
    case eBound && aBound && !vBound:
        if card == CardinalityMany {
            return EAVT  // Group by V for add-wins resolution
        }
        return EATV  // One: first entry is current; Vector: all elements

    // E, A, and V bound: membership check
    case eBound && aBound && vBound:
        if card == CardinalityMany {
            return EAVT  // Direct lookup: E+A+V prefix
        }
        return EATV  // One/Vector: scan and filter

    // A and V bound: find entities with value
    case aBound && vBound:
        return AVET

    // Only A bound: all entities with attribute
    case aBound:
        return AEVT

    // Only V bound: reverse reference lookup
    case vBound:
        return VAET

    // Only E bound: all attributes for entity
    case eBound:
        return EATV

    // Nothing bound: full scan
    default:
        return EATV
    }
}
```

**Schemaless Mode:**

When schema is nil or attribute not in schema:
- Default to `CardinalityOne`
- Use `EATV` for (E, A) queries
- First entry treated as "current" value
- No add-wins or RGA semantics

**Membership Query Optimization:**

For `[E A "specific-value"]` with cardinality-many:
- Use EAVT with `E+A+V` prefix
- Directly find entries for that specific value
- Apply add-wins logic to just those entries
- O(k) where k = entries for that value, not O(n) for all values

```go
func (m *BadgerMatcher) checkMembership(e, a, v []byte) (bool, error) {
    // EAVT scan with E+A+V prefix - only entries for this value
    iter, err := m.store.Scan(EAVT,
        m.encoder.EncodePrefix(EAVT, e, a, v),
        m.encoder.EncodePrefixRange(EAVT, e, a, v))
    if err != nil {
        return false, err
    }
    defer iter.Close()

    var highestLamport uint64
    var hasAddAtHighest bool

    for iter.Next() {
        datom := m.decodeDatom(iter)
        entry := decodeSetEntry(datom.V)

        if datom.ElementID.Lamport > highestLamport {
            highestLamport = datom.ElementID.Lamport
            hasAddAtHighest = (entry.Op == OpAdd)
        } else if datom.ElementID.Lamport == highestLamport && entry.Op == OpAdd {
            hasAddAtHighest = true  // Add-wins at same Lamport
        }
    }

    return hasAddAtHighest, nil
}
```

**Tests:** `datalog/storage/index_selection_test.go`
- [ ] `TestSelectIndexCardinalityOne` - uses EATV
- [ ] `TestSelectIndexCardinalityMany` - uses EAVT
- [ ] `TestSelectIndexCardinalityVector` - uses EATV
- [ ] `TestSelectIndexSchemaless` - defaults to EATV
- [ ] `TestSelectIndexValueBound` - uses AVET
- [ ] `TestSelectIndexReverse` - uses VAET
- [ ] `TestMembershipQueryEfficiency` - O(k) not O(n)

---

## Phase 3: Cardinality-One CRDT Semantics

**Goal:** Implement "highest ElementID wins" for cardinality-one attributes.

### 3.1 Update Transaction.Set for Cardinality-One

**File:** `datalog/storage/transaction.go` (MODIFY)

```go
func (tx *Transaction) Set(e datalog.Identity, a datalog.Keyword, v any) error {
    card := tx.db.schema.Cardinality(a)

    switch card {
    case schema.CardinalityOne:
        // Just append - no retraction needed
        elemID := tx.db.clock.Next()
        tx.datoms = append(tx.datoms, StorageDatom{
            E:  e.Hash(),
            A:  encodeAttribute(a),
            V:  encodeValue(v),
            Tx: elemID.Encode(),  // ElementID encoded for index
        })

    default:
        return fmt.Errorf("Set for cardinality %v not yet implemented", card)
    }

    return nil
}
```

### 3.2 Update Matcher for Cardinality-One Resolution

**File:** `datalog/storage/matcher.go` (MODIFY)

Add cardinality-aware resolution:

```go
func (m *BadgerMatcher) matchOne(e, a []byte) (any, error) {
    // Scan EATV with descending ElementID - first entry is current
    iter, err := m.store.Scan(EATV,
        m.encoder.EncodePrefix(EATV, e, a),
        m.encoder.EncodePrefixRange(EATV, e, a))
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    if iter.Next() {
        datom, err := m.encoder.DecodeKey(EATV, iter.Key())
        if err != nil {
            return nil, err
        }
        return decodeValue(datom.V)
    }

    return nil, nil  // No value
}
```

### 3.3 History Query Support

**File:** `datalog/storage/matcher_history.go` (NEW)

```go
func (m *BadgerMatcher) matchOneWithHistory(e, a []byte) (Relation, error) {
    iter, err := m.store.Scan(EATV,
        m.encoder.EncodePrefix(EATV, e, a),
        m.encoder.EncodePrefixRange(EATV, e, a))
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    var tuples []executor.Tuple
    for iter.Next() {
        datom, err := m.encoder.DecodeKey(EATV, iter.Key())
        if err != nil {
            return nil, err
        }

        v, err := decodeValue(datom.V)
        if err != nil {
            return nil, err
        }

        tuples = append(tuples, executor.Tuple{v, datom.Tx})
    }

    return executor.NewMaterializedRelation(
        []datalog.Symbol{symV, symTx}, tuples), nil
}
```

**Tests:** `datalog/storage/crdt_one_test.go` ✅ ALL PASS
- [x] `TestCardinalityOneCurrentValue` - returns highest ElementID
- [x] `TestCardinalityOneHistory` - returns all versions
- [x] `TestCardinalityOneConcurrentWrites` - ReplicaID tiebreaker works
- [x] `TestCardinalityOneAsOf` - time travel works
- [x] `TestCardinalityOneNoRead` - Set doesn't read existing value
- [x] `TestSetCardinalityValidation` - Set rejects cardinality-many (bonus test)
- [x] `TestSchemalessDefaultsToCardinalityOne` - schemaless defaults work (bonus test)

---

## Phase 4: Cardinality-Many with Add-Wins

**Goal:** Implement set semantics with tombstones and add-wins conflict resolution.

### 4.1 SetEntry Type

**File:** `datalog/storage/set_entry.go` (NEW)

**CRITICAL: Value MUST come before Op in encoding.**

For add-wins resolution via EAVT index, entries for the same logical value must sort together.
If Op came first, `[OpAdd]["warrior"]` and `[OpRemove]["warrior"]` would NOT be adjacent,
breaking the grouping algorithm.

```go
package storage

// Op constants are ordered so Add < Remove.
// With key order [Value][Op][Tx↓], this means:
//   - For same Value: all Adds sort before all Removes
//   - At same (Value, Tx): Add sorts first (add-wins semantics)
const (
    OpAdd    uint8 = 0  // Sorts first - wins ties
    OpRemove uint8 = 1
)

type SetEntry struct {
    Value any
    Op    uint8
}

func EncodeSetEntry(entry SetEntry) []byte {
    // [Value:var][Op:1] - Value FIRST so same values sort together in EAVT
    buf := encodeValue(entry.Value)
    buf = append(buf, entry.Op)
    return buf
}

func DecodeSetEntry(data []byte) (SetEntry, error) {
    if len(data) < 2 {  // At minimum: 1 byte type + 1 byte op
        return SetEntry{}, fmt.Errorf("set entry too short")
    }
    // Value is everything except last byte (Op)
    v, err := decodeValue(data[:len(data)-1])
    if err != nil {
        return SetEntry{}, err
    }
    return SetEntry{Value: v, Op: data[len(data)-1]}, nil
}
```

**Why this ordering matters:**

```
EAVT scan for [E][:tags] with Value-first encoding:
[E][:tags][type:str]["veteran"][Op:Add][Tx_high]    ← Same value grouped
[E][:tags][type:str]["veteran"][Op:Remove][Tx_low]  ← Same value grouped
[E][:tags][type:str]["warrior"][Op:Add][Tx_high]    ← Different value
```

With Op-first encoding (WRONG), entries would sort by Op, then Value - breaking grouping.

### 4.2 Add/Remove Operations

**File:** `datalog/storage/transaction.go` (MODIFY)

```go
func (tx *Transaction) Add(e datalog.Identity, a datalog.Keyword, v any) error {
    card := tx.db.schema.Cardinality(a)
    if card != schema.CardinalityMany {
        return fmt.Errorf("Add only valid for cardinality-many, got %v", card)
    }

    elemID := tx.db.clock.Next()
    tx.datoms = append(tx.datoms, StorageDatom{
        E:  e.Hash(),
        A:  encodeAttribute(a),
        V:  EncodeSetEntry(SetEntry{Value: v, Op: OpAdd}),
        Tx: elemID.Encode(),
    })

    return nil
}

func (tx *Transaction) Remove(e datalog.Identity, a datalog.Keyword, v any) error {
    card := tx.db.schema.Cardinality(a)
    if card != schema.CardinalityMany {
        return fmt.Errorf("Remove only valid for cardinality-many, got %v", card)
    }

    elemID := tx.db.clock.Next()
    tx.datoms = append(tx.datoms, StorageDatom{
        E:  e.Hash(),
        A:  encodeAttribute(a),
        V:  EncodeSetEntry(SetEntry{Value: v, Op: OpRemove}),
        Tx: elemID.Encode(),
    })

    return nil
}
```

### 4.3 Add-Wins Resolution Logic

**File:** `datalog/storage/set_resolution.go` (NEW)

```go
package storage

// resolveAddWinsSet scans entries for (E,A) and resolves current set membership
// using add-wins CRDT semantics.
//
// Key order is [E][A][type][encoded_value][Op][Tx↓], so for each value we see:
//   1. All Adds, highest Tx first (OpAdd = 0 sorts before OpRemove = 1)
//   2. All Removes, highest Tx first
//
// Resolution: Compare highest Add Lamport vs highest Remove Lamport
//   - Higher Lamport wins
//   - Same Lamport (concurrent operations): Add wins
//
// NOTE: We compare Lamport values only, NOT full ElementID.
// ReplicaID is irrelevant for add-wins - see "ElementID Comparison: LWW vs Add-Wins"
func (m *BadgerMatcher) resolveAddWinsSet(e, a []byte) (map[any]bool, ElementID, error) {
    iter, err := m.store.Scan(EAVT,
        m.encoder.EncodePrefix(EAVT, e, a),
        m.encoder.EncodePrefixRange(EAVT, e, a))
    if err != nil {
        return nil, ElementID{}, err
    }
    defer iter.Close()

    result := make(map[any]bool)
    var maxSeen ElementID

    // Per-value tracking
    var currentValue any
    var currentValueBytes []byte
    var highestAddTx ElementID
    var highestRemoveTx ElementID
    var hasAdd, hasRemove bool

    emitValue := func() {
        if currentValue == nil {
            return
        }
        // Determine if value is in set
        inSet := false
        if hasAdd && !hasRemove {
            inSet = true  // Only adds, no removes
        } else if hasAdd && hasRemove {
            // Compare highest timestamps
            if highestAddTx.Lamport > highestRemoveTx.Lamport {
                inSet = true  // Add is more recent
            } else if highestAddTx.Lamport == highestRemoveTx.Lamport {
                inSet = true  // Same Lamport: add-wins
            }
            // else: Remove is more recent, not in set
        }
        // If only removes (no adds), not in set

        if inSet {
            result[currentValue] = true
        }
    }

    for iter.Next() {
        datom, err := m.encoder.DecodeKey(EAVT, iter.Key())
        if err != nil {
            return nil, ElementID{}, err
        }

        entry, err := DecodeSetEntry(datom.V)
        if err != nil {
            return nil, ElementID{}, err
        }

        // Track global max ElementID
        if !datom.ElementID.Less(maxSeen) {
            maxSeen = datom.ElementID
        }

        // Extract value bytes for comparison (everything except Op byte)
        valueBytes := datom.V[:len(datom.V)-1]

        if !bytes.Equal(valueBytes, currentValueBytes) {
            // New value - emit previous
            emitValue()

            // Reset tracking for new value
            currentValue = entry.Value
            currentValueBytes = valueBytes
            hasAdd = false
            hasRemove = false
            highestAddTx = ElementID{}
            highestRemoveTx = ElementID{}
        }

        // Record first (= highest Tx) entry for each Op type
        // Due to [Op][Tx↓] ordering, first Add has highest Add Tx, etc.
        if entry.Op == OpAdd && !hasAdd {
            highestAddTx = datom.ElementID
            hasAdd = true
        } else if entry.Op == OpRemove && !hasRemove {
            highestRemoveTx = datom.ElementID
            hasRemove = true
        }
    }

    // Emit final value
    emitValue()

    return result, maxSeen, nil
}
```

**Tests:** `datalog/storage/crdt_many_test.go`
- [ ] `TestCardinalityManyAddRemove` - basic add/remove works
- [ ] `TestCardinalityManyAddWins` - concurrent add+remove at same Lamport, add wins
- [ ] `TestCardinalityManyReplicaIDTiebreaker` - same Lamport, different ReplicaID, higher wins
- [ ] `TestCardinalityManyAddThenRemove` - add at T1, remove at T2 (T2 > T1) → not in set
- [ ] `TestCardinalityManyRemoveThenAdd` - remove at T1, add at T2 (T2 > T1) → in set
- [ ] `TestCardinalityManyMultipleValues` - add/remove different values independently
- [ ] `TestCardinalityManyEmptySet` - remove all values → empty set returned
- [ ] `TestCardinalityManyHistory` - all operations preserved in history
- [ ] `TestCardinalityManyMembership` - query specific value membership
- [ ] `TestCardinalityManyReplaceSet` - Set() replaces entire set
- [ ] `TestAddCardinalityValidation` - Add() rejects cardinality-one attributes
- [ ] `TestRemoveCardinalityValidation` - Remove() rejects cardinality-one/vector attributes

---

## Phase 5: Cardinality-Vector (RGA)

**Goal:** Implement Replicated Growable Array for ordered collections.

### 5.1 RGAElement Type

**File:** `datalog/storage/rga_element.go` (NEW)

```go
package storage

type RGAElement struct {
    ID        ElementID   // This element's unique identifier
    Value     any         // The actual value
    AfterRef  ElementID   // What this element was inserted after
    Tombstone *ElementID  // Non-nil if deleted
}

func EncodeRGAElement(elem RGAElement) []byte {
    // [AfterRef:16][Tombstone:17 (1 byte present flag + 16 bytes)][Value:var]
    buf := elem.AfterRef.Encode()

    if elem.Tombstone != nil {
        buf = append(buf, 1)  // Present flag
        buf = append(buf, elem.Tombstone.Encode()...)
    } else {
        buf = append(buf, 0)  // Not present
    }

    buf = append(buf, encodeValue(elem.Value)...)
    return buf
}

func DecodeRGAElement(id ElementID, data []byte) (RGAElement, error) {
    if len(data) < 17 {  // 16 (AfterRef) + 1 (tombstone flag)
        return RGAElement{}, fmt.Errorf("rga element too short")
    }

    afterRef := DecodeElementID(data[0:16])

    var tombstone *ElementID
    if data[16] == 1 {
        if len(data) < 33 {
            return RGAElement{}, fmt.Errorf("rga element tombstone incomplete")
        }
        ts := DecodeElementID(data[17:33])
        tombstone = &ts
        data = data[33:]
    } else {
        data = data[17:]
    }

    value, err := decodeValue(data)
    if err != nil {
        return RGAElement{}, err
    }

    return RGAElement{
        ID:        id,
        Value:     value,
        AfterRef:  afterRef,
        Tombstone: tombstone,
    }, nil
}
```

### 5.2 RGA Reconstruction Algorithm

**File:** `datalog/storage/rga_reconstruct.go` (NEW)

```go
package storage

import "sort"

// reconstructRGA builds ordered list from RGA elements
func reconstructRGA(elements []RGAElement) []any {
    // Build children map: afterRef -> []elements
    children := make(map[ElementID][]RGAElement)
    for _, e := range elements {
        if e.Tombstone == nil {  // Skip deleted elements
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

> **Design Note: Vector Size Expectations and Complexity**
>
> Vectors are intended for **small to medium ordered collections** (tens to hundreds of elements), such as:
> - Character skill progressions
> - Document section orderings
> - Event sequences within a session
>
> **Large vectors (thousands of elements) are considered a misuse of this feature.** If you need large ordered collections, consider:
> - Using a separate entity per element with explicit ordering attributes
> - Pagination patterns with multiple smaller vectors
> - Different storage strategies optimized for large sequences
>
> **Algorithmic Complexity:**
>
> | Operation | Cache Hit | Cache Miss |
> |-----------|-----------|------------|
> | Get full vector | O(1) | O(n log n) |
> | Get nth element | O(1) | O(n log n) |
> | Get length | O(1) | O(n log n) |
> | Append | O(1) | O(1) |
> | Set (replace) | O(m) writes | O(m) writes |
>
> Where n = number of elements (including tombstones), m = new vector size.
>
> **Cache Miss Breakdown:**
> 1. Load all RGA elements from storage: O(n) sequential scan
> 2. Build children map (afterRef → elements): O(n)
> 3. Sort each child list by ElementID: O(n log n) total
> 4. DFS traversal to build ordered list: O(n)
> 5. Build position index: O(n)
>
> **Memory Usage on Cache Miss:** O(n) for children map, element slice, and position index.
>
> **When Cache Misses Occur:**
> - First access after process restart
> - After cache eviction (if LRU eviction is implemented)
> - After any write to that (E, A) pair (cache invalidated)
>
> For the intended use cases (tens to hundreds of elements), cache miss overhead is negligible. Performance degrades linearly with vector size beyond the intended range.

### 5.3 Vector Transaction Operations

**File:** `datalog/storage/transaction.go` (MODIFY)

**CRITICAL: Append Semantics (HEAD + Local Tracking)**

The proposal specifies that Append does NOT read from the database. Instead:

1. **First append in transaction:** `afterRef = HEAD` (element goes at logical beginning)
2. **Subsequent appends in same tx:** `afterRef = previous element's ID` (chains correctly)

This is intentional CRDT behavior:
- Appends within a single transaction chain correctly
- Appends from different transactions get sorted by ElementID, NOT append order
- This preserves determinism across all nodes without coordination

```go
type Transaction struct {
    // ... existing fields ...
    lastElement map[entityAttrKey]ElementID  // Track last appended element per (E,A)
}

func (tx *Transaction) Append(e datalog.Identity, a datalog.Keyword, v any) error {
    card := tx.db.schema.Cardinality(a)
    if card != schema.CardinalityVector {
        return fmt.Errorf("Append only valid for cardinality-vector, got %v", card)
    }

    elemID := tx.db.clock.Next()

    // Get afterRef from LOCAL tracking only - NO database read
    key := entityAttrKey{e.Hash(), encodeAttribute(a)}
    afterRef := tx.lastElement[key]
    // If zero (first append in this tx), use HEAD - element will be sorted by ElementID
    // This is correct RGA semantics: concurrent appends to HEAD get sorted deterministically

    tx.datoms = append(tx.datoms, StorageDatom{
        E:  e.Hash(),
        A:  encodeAttribute(a),
        V:  EncodeRGAElement(RGAElement{ID: elemID, Value: v, AfterRef: afterRef}),
        Tx: elemID.Encode(),
    })

    // Track for subsequent appends in this transaction
    tx.lastElement[key] = elemID

    return nil
}
```

**Concurrent Append Behavior (Cross-Replica Scenario):**

This example shows two SEPARATE Database instances (different replicas with different ReplicaIDs: N1 and N2) appending concurrently. Within a single Database instance, all goroutines share one clock, so concurrent appends get different Lamport values and this scenario doesn't occur.

```
Replica N1:                       Replica N2:
tx1.Append(e, :skills, "a")       tx2.Append(e, :skills, "x")
  afterRef = HEAD                   afterRef = HEAD
  elemID = (L1, N1)                 elemID = (L1, N2)  ← Same Lamport, different ReplicaID
tx1.Append(e, :skills, "b")       tx2.Append(e, :skills, "y")
  afterRef = (L1, N1)               afterRef = (L1, N2)
  elemID = (L2, N1)                 elemID = (L2, N2)
tx1.Commit()                      tx2.Commit()

// After bidirectional merge, both replicas have all 4 elements
// RGA reconstruction (same result on both replicas):
// - "a" and "x" both have afterRef=HEAD, sorted by ElementID
// - "b" chains from (L1, N1), "y" chains from (L1, N2)
// - Result (if N1 < N2): ["a", "b", "x", "y"]
// - Result is DETERMINISTIC: same on all nodes after merge
```

**Within single replica:** Concurrent goroutine appends get strictly increasing Lamports (L1 < L2 < L3...), so they naturally chain correctly without ReplicaID tiebreaking.

> **⚠️ CRITICAL: This is NOT "append to end" semantics**
>
> `Append()` means "insert after my last known element in this transaction." True append-to-end would require reading current state, which violates CRDT write-without-read.
>
> **Implication:** Cross-replica concurrent appends produce deterministic order based on ElementID, NOT based on "who appended first" in wall-clock time. Applications expecting strict temporal ordering should use explicit position attributes.

### 5.4 Vector Matcher

**File:** `datalog/storage/matcher_vector.go` (NEW)

```go
func (m *BadgerMatcher) matchVector(e, a []byte) ([]any, error) {
    elements, err := m.loadRGAElements(e, a)
    if err != nil {
        return nil, err
    }

    return reconstructRGA(elements), nil
}

func (m *BadgerMatcher) loadRGAElements(e, a []byte) ([]RGAElement, error) {
    iter, err := m.store.Scan(EATV,
        m.encoder.EncodePrefix(EATV, e, a),
        m.encoder.EncodePrefixRange(EATV, e, a))
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    var elements []RGAElement
    for iter.Next() {
        datom, err := m.encoder.DecodeKey(EATV, iter.Key())
        if err != nil {
            return nil, err
        }

        elem, err := DecodeRGAElement(datom.ElementID, datom.V)
        if err != nil {
            return nil, err
        }

        elements = append(elements, elem)
    }

    return elements, nil
}
```

**Tests:** `datalog/storage/crdt_vector_test.go`
- [ ] `TestVectorAppend` - basic append works
- [ ] `TestVectorReconstruction` - RGA reconstruction correct
- [ ] `TestVectorConcurrentAppends` - both appends survive
- [ ] `TestVectorTombstone` - deleted elements hidden
- [ ] `TestVectorNoReadAppend` - append doesn't read whole vector
- [ ] `TestVectorDeterministicOrder` - same result on all nodes
- [ ] `TestVectorCrossTransactionOrder` - concurrent tx appends merge correctly

### 5.5 Vector Position Index (Cache-Only)

**Goal:** Provide O(1) random access for read-heavy workloads while maintaining CRDT write semantics.

**Key Design Decision:** The position index is **cache-only** - it lives in the `CacheEntry` struct, NOT in the database. This ensures:
- Reads have no side effects (no database writes)
- Writes remain simple appends
- Position index is just a computed view, rebuilt on cache miss

**Storage:** Only RGA elements are stored in the database:

```
// Source of truth: RGA elements (CRDT, conflict-free)
// Stored in EATV index - this is ALL that's persisted
[E][:skills][(L1,N1)] = {value: "stealth", after: HEAD}
[E][:skills][(L2,N1)] = {value: "archery", after: (L1,N1)}
[E][:skills][(L3,N1)] = {value: "lockpicking", after: (L2,N1)}
```

**Cache Entry (includes position index):**

```go
type CacheEntry struct {
    version     ElementID
    cardinality Cardinality

    // ... other fields ...

    // Vector-specific: computed during cache rebuild, NOT persisted
    vectorList  []any        // Ordered elements after RGA reconstruction
    vectorIndex []ElementID  // position → ElementID mapping
}
```

**Cache Rebuild (computes position index):**

```go
func (c *Cache) rebuildVector(key CacheKey, store Store) *CacheEntry {
    // 1. Load all RGA elements from storage
    elements := loadRGAElements(store, key.E[:], key.A[:])

    // 2. Find max ElementID for version tracking
    var maxID ElementID
    for _, elem := range elements {
        if !elem.ID.Less(maxID) {
            maxID = elem.ID
        }
    }

    // 3. Reconstruct ordered list (filters tombstones)
    ordered := reconstructRGAWithIDs(elements)  // []struct{ID ElementID, Value any}

    // 4. Build position index in memory
    vectorList := make([]any, len(ordered))
    vectorIndex := make([]ElementID, len(ordered))
    for i, elem := range ordered {
        vectorList[i] = elem.Value
        vectorIndex[i] = elem.ID
    }

    return &CacheEntry{
        version:     maxID,
        cardinality: CardinalityVector,
        vectorList:  vectorList,
        vectorIndex: vectorIndex,
    }
}
```

**Access via Cache:**

```go
// GetVectorNth uses cache for O(1) access when fresh
func (db *Database) GetVectorNth(e, a Identity, n int64) (any, error) {
    key := CacheKey{e.Hash(), encodeAttribute(a)}
    entry := db.cache.GetOrResolve(key, db.store, db.schema)

    if entry.cardinality != CardinalityVector {
        return nil, fmt.Errorf("not a vector attribute")
    }

    if n < 0 || n >= int64(len(entry.vectorList)) {
        return nil, nil  // Out of bounds
    }

    return entry.vectorList[n], nil
}

// GetVectorLength uses cache
func (db *Database) GetVectorLength(e, a Identity) (int64, error) {
    key := CacheKey{e.Hash(), encodeAttribute(a)}
    entry := db.cache.GetOrResolve(key, db.store, db.schema)

    if entry.cardinality != CardinalityVector {
        return 0, fmt.Errorf("not a vector attribute")
    }

    return int64(len(entry.vectorList)), nil
}
```

**Performance Characteristics:**

| Operation | Cache hit | Cache miss |
|-----------|-----------|------------|
| Get full vector | O(1) return cached | O(n) reconstruct + cache |
| Get length | O(1) | O(n) reconstruct + cache |
| Get nth element | O(1) | O(n) reconstruct + cache |
| Append | O(1) write + invalidate | O(1) write + invalidate |

**Trade-off:** Position index is lost on process restart and rebuilt on first access. This is acceptable because:
- RGA reconstruction is already O(n)
- Cache miss only happens once per (E,A) after restart
- No database write side-effects from reads

**Tests:** `datalog/storage/cache_vector_test.go`
- [ ] `TestVectorCacheHit` - O(1) access when cached
- [ ] `TestVectorCacheMiss` - rebuilds correctly on miss
- [ ] `TestVectorCacheInvalidation` - invalidates on write
- [ ] `TestVectorNthFromCache` - correct element at each position
- [ ] `TestVectorLengthFromCache` - correct length
- [ ] `TestVectorCacheTombstones` - excludes deleted elements

---

## Phase 6: Unified Cache

**Goal:** Implement caching layer for resolved CRDT views.

### Query Resolution Model

The cache is the **primary query resolution mechanism** for current-value queries. Understanding the relationship between indices and cache clarifies why no additional indices are needed:

**For "get current value of (E, A)"** - the common case:
- **Runtime**: Look up EA cache directly → O(1)
- **Cache miss**: Scan EATV index, first entry is current (descending Tx) → populate cache
- **No AVET scan involved**

**For "find all entities where A = V" (value lookup)**:
- AVET scan returns all entities that EVER had that value (historical)
- Each result requires EA cache lookup to verify currency
- If fast current-value lookup is needed, add reverse index to cache: `(A, V) → set of E's`
- This is maintained as cache entries change, not persisted

**Component Roles:**

| Component | Purpose | When Used |
|-----------|---------|-----------|
| **EA Cache** | O(1) current value lookup | All runtime queries for (E, A) |
| **EATV Index** | Cache population | Startup, cache misses |
| **EAVT Index** | Cardinality-Many resolution | Add-wins set membership queries |
| **AVET Index** | Historical value queries | "Who ever had this value?" |
| **VAET Index** | Reverse references | "What references this entity?" |
| **TAEV Index** | Clock recovery, audit log | Startup, transaction history |

**Key Insight:** The indices support storage and cache population. The cache handles runtime queries. This is why AVET being expensive for current-value queries is acceptable—we don't use AVET for that purpose.

### 6.1 Cache Implementation

**File:** `datalog/storage/cache.go` (NEW)

```go
package storage

import "sync"

type Cache struct {
    // Per-(E,A) resolved views
    entries      sync.Map  // map[CacheKey]*CacheEntry

    // Per-(E,A) max ElementID tracking - updated atomically on every write
    // This avoids storage seeks for freshness checks
    maxVersions  sync.Map  // map[CacheKey]ElementID

    // Per-attribute version tracking for fast A-bound query freshness checks
    // When querying [?e :name "Bob"], we can check if ANY :name has changed
    // without checking every individual (E, :name) pair
    attrVersions sync.Map  // map[[32]byte]ElementID
}

type CacheKey struct {
    E [20]byte
    A [32]byte
}

type CacheEntry struct {
    version     ElementID    // Max ElementID when this entry was computed
    cardinality Cardinality

    // Resolved views (one populated based on cardinality)
    oneValue    any              // Cardinality-One: single current value
    manySet     map[any]bool     // Cardinality-Many: current set members
    vectorList  []any            // Cardinality-Vector: ordered elements
    vectorIndex []ElementID      // Cardinality-Vector: position → ElementID for O(1) access
}

func NewCache() *Cache {
    return &Cache{}
}

// GetOrResolve returns cached entry if fresh, rebuilds if stale
//
// Freshness is tracked via maxVersions sync.Map, updated atomically on every write.
// This provides O(1) freshness checks without any storage seeks.
// False negatives (returning stale data) are NOT acceptable.
func (c *Cache) GetOrResolve(key CacheKey, store Store, schema schema.SchemaProvider) *CacheEntry {
    // Fast path: load existing entry
    if val, ok := c.entries.Load(key); ok {
        entry := val.(*CacheEntry)

        // Check freshness: compare stored version with maxVersions (O(1) map lookup)
        if maxVal, ok := c.maxVersions.Load(key); ok {
            currentMax := maxVal.(ElementID)
            if entry.version == currentMax {
                return entry  // Fresh - cache hit
            }
        }
        // Stale or no max tracked - fall through to rebuild
    }

    // Slow path: rebuild and store
    // Note: Two goroutines might both rebuild for the same key.
    // That's fine - CRDT resolution is deterministic, both compute same result.
    entry := c.rebuild(key, store, schema)
    c.entries.Store(key, entry)
    return entry
}

// UpdateMaxVersion updates the max ElementID for a (E,A) pair.
// Called by Transaction.Commit() for every written datom.
// This enables O(1) cache freshness checks without storage seeks.
func (c *Cache) UpdateMaxVersion(key CacheKey, elemID ElementID) {
    for {
        val, loaded := c.maxVersions.Load(key)
        if !loaded {
            // No existing value - try to store
            if c.maxVersions.CompareAndSwap(key, nil, elemID) {
                return
            }
            // CAS failed, retry
            continue
        }
        current := val.(ElementID)
        if !current.Less(elemID) {
            return  // Current is already >= new value
        }
        // Try to update to new max
        if c.maxVersions.CompareAndSwap(key, current, elemID) {
            return
        }
        // CAS failed, retry
    }
}

// Invalidate removes cached entries for the given keys.
// Called by Database on tx.Commit() with all touched (E,A) pairs.
// Note: maxVersions is NOT cleared here - it's updated by UpdateMaxVersion()
// during commit, preserving the max for freshness checks.
func (c *Cache) Invalidate(touched []CacheKey) {
    for _, key := range touched {
        c.entries.Delete(key)
    }
    // Note: attrVersions invalidation is implicit -
    // next IsAttributeFresh() call will fetch current max from store
}

// IsAttributeFresh checks if the entire attribute is fresh in cache
// Used for A-bound queries like [?e :name "Bob"] to avoid checking every entity
//
// IMPLEMENTATION NOTE: MaxElementIDForAttribute() performs an O(1) reverse seek
// on the AEVT index to find the highest Tx for any entity with this attribute.
//
// EDGE CASE - Initial population after restart:
// After process restart, attrVersions is empty. The first A-bound query will:
// 1. Return false from IsAttributeFresh (no cached version)
// 2. Trigger resolution of all entities for that attribute
// 3. Call UpdateAttributeVersion with the max seen
//
// For attributes with millions of entities, this first query after restart
// may be slow. Subsequent queries use the cached attrVersions and are O(1).
//
// MITIGATION: For performance-critical attributes, consider:
// - Warming the cache on startup for known high-cardinality attributes
// - Using per-(E,A) queries instead of A-bound queries after restart
func (c *Cache) IsAttributeFresh(a [32]byte, store Store) bool {
    val, ok := c.attrVersions.Load(a)
    if !ok {
        return false  // No cached version - first query after restart will be slow
    }
    cachedMax := val.(ElementID)
    storeMax, err := store.MaxElementIDForAttribute(a[:])  // O(1) reverse seek on AEVT
    if err != nil {
        return false
    }
    return cachedMax == storeMax
}

// UpdateAttributeVersion updates the cached version for an attribute
// Called after resolving all entities for an attribute
func (c *Cache) UpdateAttributeVersion(a [32]byte, version ElementID) {
    c.attrVersions.Store(a, version)
}

func (c *Cache) rebuild(key CacheKey, store Store, schema schema.SchemaProvider) *CacheEntry {
    card := schema.Cardinality(decodeAttribute(key.A))

    switch card {
    case CardinalityOne:
        // Scan EATV with descending ElementID, first entry is current
        iter, _ := store.Scan(EATV, EncodePrefix(EATV, key.E[:], key.A[:]), ...)
        defer iter.Close()

        if iter.Next() {
            datom := decodeDatom(iter.Key(), iter.Value())
            return &CacheEntry{
                version:     datom.ElementID,
                cardinality: CardinalityOne,
                oneValue:    datom.V,
            }
        }
        return &CacheEntry{version: ElementID{}, cardinality: CardinalityOne}

    case CardinalityMany:
        // Add-wins resolution: scan EAVT grouped by value
        set, maxSeen := resolveAddWinsSet(store, key.E[:], key.A[:])
        return &CacheEntry{
            version:     maxSeen,
            cardinality: CardinalityMany,
            manySet:     set,
        }

    case CardinalityVector:
        // RGA reconstruction + position index
        elements := loadRGAElements(store, key.E[:], key.A[:])
        ordered := reconstructRGA(elements)
        positions := extractPositionIndex(elements)
        maxSeen := findMaxElementID(elements)
        return &CacheEntry{
            version:     maxSeen,
            cardinality: CardinalityVector,
            vectorList:  ordered,
            vectorIndex: positions,
        }
    }

    return nil
}
```

**Store Interface Addition:**

```go
// Add to Store interface for attribute-level freshness
type Store interface {
    // ... existing methods ...

    // MaxElementIDForAttribute returns highest ElementID for any (E, A) with this A
    // Used for fast cache freshness checks on A-bound queries
    MaxElementIDForAttribute(a []byte) (ElementID, error)
}
```

**Tests:** `datalog/storage/cache_test.go`
- [ ] `TestCacheFreshness` - returns cached when fresh
- [ ] `TestCacheInvalidation` - invalidate removes entry
- [ ] `TestCacheRebuild` - stale entry triggers rebuild
- [ ] `TestCacheConcurrency` - safe under concurrent access
- [ ] `TestCacheAttributeFreshness` - IsAttributeFresh works correctly
- [ ] `TestCacheAttributeInvalidation` - attribute version updates on write
- [ ] `TestUpdateMaxVersion` - correctly tracks max ElementID per (E,A)
- [ ] `TestUpdateMaxVersionConcurrency` - safe under concurrent updates

### 6.2 Cache Warmup API

**Goal:** Provide explicit cache warming for performance-critical attributes after process restart.

**File:** `datalog/storage/database.go` (ADD method)

```go
// WarmCache pre-populates cache entries for the specified attributes.
// Use this after process restart for attributes with high query frequency
// to avoid cold-start latency on first access.
//
// For each attribute, this scans all entities and populates:
// - Per-(E,A) cache entries with resolved CRDT values
// - Attribute-level version tracking for freshness checks
//
// This is O(n) where n = total datoms for the specified attributes.
// Call during application startup, not on the hot path.
func (db *Database) WarmCache(attributes []datalog.Keyword) error {
    for _, attr := range attributes {
        a := encodeAttribute(attr)

        // Scan AEVT for all entities with this attribute
        iter, err := db.store.Scan(AEVT,
            db.encoder.EncodePrefix(AEVT, a),
            db.encoder.EncodePrefixRange(AEVT, a))
        if err != nil {
            return fmt.Errorf("warming cache for %s: %w", attr, err)
        }

        var maxAttrVersion ElementID
        seenEntities := make(map[[20]byte]bool)

        for iter.Next() {
            datom, err := db.encoder.DecodeKey(AEVT, iter.Key())
            if err != nil {
                iter.Close()
                return err
            }

            // Track max version for attribute-level freshness
            if !datom.ElementID.Less(maxAttrVersion) {
                maxAttrVersion = datom.ElementID
            }

            // Resolve each (E, A) once
            if !seenEntities[datom.E] {
                seenEntities[datom.E] = true
                key := CacheKey{E: datom.E, A: [32]byte(a)}
                db.cache.GetOrResolve(key, db.store, db.schema)
            }
        }
        iter.Close()

        // Update attribute-level version
        db.cache.UpdateAttributeVersion([32]byte(a), maxAttrVersion)
    }

    return nil
}
```

**Usage:**

```go
// At application startup, after opening database
db, err := storage.OpenDatabase(path, opts)
if err != nil {
    return err
}

// Warm cache for frequently-queried attributes
err = db.WarmCache([]datalog.Keyword{
    datalog.NewKeyword(":person/name"),
    datalog.NewKeyword(":person/email"),
    datalog.NewKeyword(":order/status"),
})
if err != nil {
    log.Printf("cache warmup failed: %v", err)
    // Non-fatal: queries will work, just slower on first access
}
```

**Tests:** `datalog/storage/cache_warmup_test.go`
- [ ] `TestWarmCacheSingleAttribute` - populates cache entries
- [ ] `TestWarmCacheMultipleAttributes` - handles multiple attributes
- [ ] `TestWarmCacheEmptyAttribute` - no error for attributes with no data
- [ ] `TestWarmCacheUpdatesAttrVersion` - attribute freshness works after warmup
- [ ] `TestWarmCacheIdempotent` - calling twice is safe

---

## Phase 7: Transaction API Finalization

**Goal:** Complete the new transaction API with proper validation.

### 7.0 Write Indexing (All 6 Indices)

When a transaction commits, each StorageDatom is written to ALL 6 indices with appropriate key encoding.

**File:** `datalog/storage/transaction.go` (Commit method)

```go
func (tx *Transaction) Commit() error {
    for _, sd := range tx.datoms {
        // Write to all 6 indices with index-specific key encoding
        for _, idx := range []IndexType{EAVT, EATV, AEVT, AVET, VAET, TAEV} {
            key := tx.encoder.EncodeKey(idx, sd)
            if err := tx.store.Put(key, sd.V); err != nil {
                return err
            }
        }
    }

    // Update cache: track max versions and invalidate stale entries
    touched := make([]CacheKey, 0, len(tx.datoms))
    for _, sd := range tx.datoms {
        key := CacheKey{E: sd.E, A: sd.A}
        touched = append(touched, key)

        // Update maxVersions for O(1) freshness checks (no storage seeks)
        elemID := DecodeElementID(sd.Tx[:])
        tx.db.cache.UpdateMaxVersion(key, elemID)
    }
    tx.db.cache.Invalidate(touched)

    return nil
}
```

**Key encoding:** All 6 indices encode Tx so highest sorts first. Forward scan returns newest entries.

### 7.1 Method Validation

```go
func (tx *Transaction) validateCardinality(method string, a datalog.Keyword, allowed ...Cardinality) error {
    card := tx.db.schema.Cardinality(a)
    for _, c := range allowed {
        if card == c {
            return nil
        }
    }
    return fmt.Errorf("%s not valid for cardinality %v (allowed: %v)", method, card, allowed)
}
```

### 7.2 SaveStruct Integration

**File:** `datalog/reflect/writer.go` (MODIFY)

Update `SaveStruct` to use new primitives based on cardinality:

```go
func (sw *StructWriter) writeField(tx *Transaction, e Identity, a Keyword, v any, card Cardinality) error {
    switch card {
    case schema.CardinalityOne:
        return tx.Set(e, a, v)

    case schema.CardinalityMany:
        slice := v.([]any)
        // For full slice replacement, diff against existing
        // For SaveStruct, we replace the entire set
        return tx.Set(e, a, slice)

    case schema.CardinalityVector:
        slice := v.([]any)
        return tx.Set(e, a, slice)
    }
    return nil
}
```

**Tests:** `datalog/reflect/writer_crdt_test.go`
- [ ] `TestSaveStructCardinalityOne` - Set used, no read
- [ ] `TestSaveStructCardinalityMany` - Add/Remove used
- [ ] `TestSaveStructCardinalityVector` - Append used

---

## Phase 8: Query Integration

**Goal:** Integrate CRDT resolution into pattern matching.

### 8.1 PatternMatcher Cardinality Dispatch

**File:** `datalog/storage/matcher.go` (MODIFY)

```go
func (m *BadgerMatcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
    e, a, v := extractEAV(pattern)

    // Determine cardinality
    var card Cardinality = CardinalityOne  // Default for schemaless
    if m.schema != nil && a.IsBound() {
        card = m.schema.Cardinality(a.Value().(datalog.Keyword))
    }

    switch card {
    case CardinalityOne:
        return m.matchOneCRDT(e, a, v, bindings)
    case CardinalityMany:
        return m.matchManyCRDT(e, a, v, bindings)
    case CardinalityVector:
        return m.matchVectorCRDT(e, a, v, bindings)
    }

    return nil, fmt.Errorf("unknown cardinality: %v", card)
}
```

### 8.2 Pull API Integration

**Goal:** Ensure Pull API works correctly with all three cardinalities.

**File:** `datalog/executor/pull.go` (MODIFY)

Pull already resolves attributes via the matcher, which now uses the cache. The key changes are how values are returned:

| Cardinality | Pull Return Type | Resolution |
|-------------|------------------|------------|
| One | Single value | Cache → oneValue |
| Many | `[]any` (set as slice) | Cache → manySet keys |
| Vector | `[]any` (ordered list) | Cache → vectorList |

**Implementation:**

```go
func (p *PullExecutor) resolveAttribute(e Identity, a Keyword) (any, error) {
    card := p.schema.Cardinality(a)
    entry := p.db.cache.GetOrResolve(CacheKey{e.Hash(), encodeAttribute(a)}, p.db.store, p.schema)

    switch card {
    case CardinalityOne:
        return entry.oneValue, nil  // Single value or nil

    case CardinalityMany:
        // Convert map to slice for Pull output
        result := make([]any, 0, len(entry.manySet))
        for v := range entry.manySet {
            result = append(result, v)
        }
        return result, nil  // May be empty slice

    case CardinalityVector:
        return entry.vectorList, nil  // Ordered slice, may be empty
    }

    return nil, fmt.Errorf("unknown cardinality")
}
```

**Pull Pattern Examples:**

```clojure
;; Pull with all cardinalities
(pull db [:person/name          ;; One: "Alice"
          :person/tags          ;; Many: ["warrior" "veteran"]
          :person/skills])      ;; Vector: ["stealth" "archery" "lockpicking"]

;; Result:
{:person/name "Alice"
 :person/tags ["warrior" "veteran"]      ;; Set as slice (unordered)
 :person/skills ["stealth" "archery" "lockpicking"]}  ;; Vector (ordered)
```

**Tests:** `datalog/executor/pull_crdt_test.go`
- [ ] `TestPullCardinalityOne` - returns single value
- [ ] `TestPullCardinalityMany` - returns set as slice
- [ ] `TestPullCardinalityVector` - returns ordered slice
- [ ] `TestPullEmptySet` - returns empty slice for empty set
- [ ] `TestPullEmptyVector` - returns empty slice for empty vector
- [ ] `TestPullMissingAttribute` - returns nil for missing

### 8.3 Vector Functions

**File:** `datalog/executor/vector_functions.go` (NEW)

All 8 vector functions from the proposal:

```go
package executor

import (
    "fmt"
)

func init() {
    RegisterFunction("enumerate", builtinEnumerate)
    RegisterFunction("nth", builtinNth)
    RegisterFunction("contains?", builtinContains)
    RegisterFunction("length", builtinLength)
    RegisterFunction("first", builtinFirst)
    RegisterFunction("last", builtinLast)
    RegisterFunction("index-of", builtinIndexOf)
    RegisterFunction("subvec", builtinSubvec)
}

// enumerate decomposes vector into (index, value) pairs
// Usage: [(enumerate ?vec) ?idx ?val]
// Returns: Relation with columns [?idx, ?val]
func builtinEnumerate(args []any) (Relation, error) {
    vec, ok := args[0].([]any)
    if !ok {
        return nil, fmt.Errorf("enumerate requires vector, got %T", args[0])
    }

    tuples := make([]Tuple, len(vec))
    for i, v := range vec {
        tuples[i] = Tuple{int64(i), v}
    }

    return NewMaterializedRelation(
        []datalog.Symbol{datalog.NewSymbol("?idx"), datalog.NewSymbol("?val")},
        tuples), nil
}

// nth returns element at index, or nil if out of bounds
// Usage: [(nth ?vec ?n) ?val]
func builtinNth(args []any) (any, error) {
    vec, ok := args[0].([]any)
    if !ok {
        return nil, fmt.Errorf("nth requires vector as first arg, got %T", args[0])
    }

    idx, ok := args[1].(int64)
    if !ok {
        // Try int
        if i, ok := args[1].(int); ok {
            idx = int64(i)
        } else {
            return nil, fmt.Errorf("nth requires int as second arg, got %T", args[1])
        }
    }

    if idx < 0 || idx >= int64(len(vec)) {
        return nil, nil  // Out of bounds returns nil (not error)
    }
    return vec[idx], nil
}

// contains? returns true if value is in vector
// Usage: [(contains? ?vec ?val)]
func builtinContains(args []any) (bool, error) {
    vec, ok := args[0].([]any)
    if !ok {
        return false, fmt.Errorf("contains? requires vector, got %T", args[0])
    }
    val := args[1]

    for _, v := range vec {
        if valuesEqual(v, val) {
            return true, nil
        }
    }
    return false, nil
}

// length returns vector length
// Usage: [(length ?vec) ?n]
func builtinLength(args []any) (int64, error) {
    vec, ok := args[0].([]any)
    if !ok {
        return 0, fmt.Errorf("length requires vector, got %T", args[0])
    }
    return int64(len(vec)), nil
}

// first returns first element, or nil if empty
// Usage: [(first ?vec) ?val]
func builtinFirst(args []any) (any, error) {
    vec, ok := args[0].([]any)
    if !ok {
        return nil, fmt.Errorf("first requires vector, got %T", args[0])
    }

    if len(vec) == 0 {
        return nil, nil
    }
    return vec[0], nil
}

// last returns last element, or nil if empty
// Usage: [(last ?vec) ?val]
func builtinLast(args []any) (any, error) {
    vec, ok := args[0].([]any)
    if !ok {
        return nil, fmt.Errorf("last requires vector, got %T", args[0])
    }

    if len(vec) == 0 {
        return nil, nil
    }
    return vec[len(vec)-1], nil
}

// index-of returns first index of value, or -1 if not found
// Usage: [(index-of ?vec ?val) ?idx]
func builtinIndexOf(args []any) (int64, error) {
    vec, ok := args[0].([]any)
    if !ok {
        return -1, fmt.Errorf("index-of requires vector, got %T", args[0])
    }
    val := args[1]

    for i, v := range vec {
        if valuesEqual(v, val) {
            return int64(i), nil
        }
    }
    return -1, nil  // Not found
}

// subvec returns slice of vector from start (inclusive) to end (exclusive)
// Usage: [(subvec ?vec ?start ?end) ?sub]
func builtinSubvec(args []any) ([]any, error) {
    vec, ok := args[0].([]any)
    if !ok {
        return nil, fmt.Errorf("subvec requires vector, got %T", args[0])
    }

    start, ok := args[1].(int64)
    if !ok {
        if i, ok := args[1].(int); ok {
            start = int64(i)
        } else {
            return nil, fmt.Errorf("subvec requires int start, got %T", args[1])
        }
    }

    end, ok := args[2].(int64)
    if !ok {
        if i, ok := args[2].(int); ok {
            end = int64(i)
        } else {
            return nil, fmt.Errorf("subvec requires int end, got %T", args[2])
        }
    }

    // Clamp bounds
    if start < 0 {
        start = 0
    }
    if end > int64(len(vec)) {
        end = int64(len(vec))
    }
    if start >= end {
        return []any{}, nil  // Empty slice
    }

    // Return copy to avoid aliasing
    result := make([]any, end-start)
    copy(result, vec[start:end])
    return result, nil
}

// valuesEqual compares two values for equality (handles type variations)
func valuesEqual(a, b any) bool {
    // Use existing datalog.ValuesEqual for proper comparison of all types
    // including slices, Identity, Keyword, time.Time, etc.
    return datalog.ValuesEqual(a, b)
}
```

**Query Examples:**

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

;; Check membership
[:find ?e
 :where
 [?e :character/skills ?skills]
 [(contains? ?skills "archery")]]
;; → [[entity-id]]

;; Get first and last
[:find ?first ?last
 :where
 [?e :character/skills ?skills]
 [(first ?skills) ?first]
 [(last ?skills) ?last]]
;; → [["stealth" "lockpicking"]]

;; Slice vector
[:find ?subset
 :where
 [?e :character/skills ?skills]
 [(subvec ?skills 1 3) ?subset]]
;; → [[["archery" "lockpicking"]]]
```

**Tests:** `datalog/executor/vector_functions_test.go`
- [ ] `TestEnumerate` - produces index/value pairs
- [ ] `TestEnumerateEmpty` - empty vector returns empty relation
- [ ] `TestNth` - correct element at each position
- [ ] `TestNthOutOfBounds` - returns nil for invalid index
- [ ] `TestContains` - finds existing values
- [ ] `TestContainsNotFound` - returns false for missing
- [ ] `TestVectorLength` - correct length
- [ ] `TestVectorLengthEmpty` - returns 0 for empty
- [ ] `TestFirst` - returns first element
- [ ] `TestFirstEmpty` - returns nil for empty
- [ ] `TestLast` - returns last element
- [ ] `TestLastEmpty` - returns nil for empty
- [ ] `TestIndexOf` - finds correct index
- [ ] `TestIndexOfNotFound` - returns -1 for missing
- [ ] `TestSubvec` - correct slice
- [ ] `TestSubvecBounds` - clamps out-of-bounds indices

### 8.4 History Query Predicates

**Goal:** Implement `[(history)]` and `[(as-of ?tx N)]` predicates for time-travel queries.

**Parser Changes:**

**File:** `datalog/parser/predicate_parser.go` (MODIFY)

Add parsing for history predicates:

```go
// History predicates have special forms:
// [(history)]        - marker to return all versions
// [(as-of ?tx 5000)] - filter to Tx <= target

func (p *Parser) parseHistoryPredicate(elements []any) (*query.HistoryPredicate, error) {
    if len(elements) < 1 {
        return nil, fmt.Errorf("empty predicate")
    }

    name, ok := elements[0].(Symbol)
    if !ok {
        return nil, nil  // Not a history predicate
    }

    switch name.String() {
    case "history":
        if len(elements) != 1 {
            return nil, fmt.Errorf("history takes no arguments")
        }
        return &query.HistoryPredicate{Type: query.HistoryAll}, nil

    case "as-of":
        if len(elements) != 3 {
            return nil, fmt.Errorf("as-of requires 2 arguments: (as-of ?tx N)")
        }
        txVar, ok := elements[1].(Symbol)
        if !ok {
            return nil, fmt.Errorf("as-of first arg must be variable")
        }
        target, ok := elements[2].(int64)
        if !ok {
            return nil, fmt.Errorf("as-of second arg must be integer transaction ID")
        }
        return &query.HistoryPredicate{
            Type:          query.HistoryAsOf,
            TxVar:         query.NewSymbol(txVar.String()),
            TargetLamport: uint64(target),
        }, nil
    }

    return nil, nil  // Not a history predicate
}
```

**Query Types:**

**File:** `datalog/query/history.go` (NEW)

```go
package query

type HistoryPredicateType int

const (
    HistoryAll  HistoryPredicateType = iota  // [(history)] - all versions
    HistoryAsOf                               // [(as-of ?tx N)] - Tx <= N
)

type HistoryPredicate struct {
    Type          HistoryPredicateType
    TxVar         Symbol  // For as-of: which variable holds Tx
    TargetLamport uint64  // For as-of: filter to Lamport <= this value
}

// IsHistoryQuery returns true if query contains history predicates
func (q *Query) IsHistoryQuery() bool {
    for _, clause := range q.Where {
        if hp, ok := clause.(*HistoryPredicate); ok && hp != nil {
            return true
        }
    }
    return false
}

// GetHistoryPredicate returns the history predicate if present
func (q *Query) GetHistoryPredicate() *HistoryPredicate {
    for _, clause := range q.Where {
        if hp, ok := clause.(*HistoryPredicate); ok && hp != nil {
            return hp
        }
    }
    return nil
}
```

**Executor Changes:**

**File:** `datalog/storage/matcher.go` (MODIFY)

```go
// matchOneCRDT now checks for history mode
func (m *BadgerMatcher) matchOneCRDT(e, a, v PatternElement, bindings Relations, historyMode *query.HistoryPredicate) (Relation, error) {
    if historyMode == nil {
        // Normal mode: return only current value (first in descending scan)
        return m.matchOneCurrent(e, a, v, bindings)
    }

    // History mode: return all versions
    switch historyMode.Type {
    case query.HistoryAll:
        return m.matchOneWithHistory(e, a, v, bindings)

    case query.HistoryAsOf:
        return m.matchOneAsOf(e, a, v, bindings, historyMode.TargetLamport)
    }

    return nil, fmt.Errorf("unknown history mode")
}

// matchOneWithHistory returns all versions with their transaction IDs
func (m *BadgerMatcher) matchOneWithHistory(e, a []byte, v PatternElement, bindings Relations) (Relation, error) {
    iter, err := m.store.Scan(EATV,
        m.encoder.EncodePrefix(EATV, e, a),
        m.encoder.EncodePrefixRange(EATV, e, a))
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    var tuples []executor.Tuple
    for iter.Next() {
        datom, err := m.encoder.DecodeKey(EATV, iter.Key())
        if err != nil {
            return nil, err
        }

        val, err := decodeValue(datom.V)
        if err != nil {
            return nil, err
        }

        // Include value and transaction ID
        tuples = append(tuples, executor.Tuple{val, datom.Tx})
    }

    return executor.NewMaterializedRelation(
        []datalog.Symbol{symV, symTx}, tuples), nil
}

// matchOneAsOf returns value as of specific Lamport time
func (m *BadgerMatcher) matchOneAsOf(e, a []byte, v PatternElement, bindings Relations, targetLamport uint64) (Relation, error) {
    iter, err := m.store.Scan(EATV,
        m.encoder.EncodePrefix(EATV, e, a),
        m.encoder.EncodePrefixRange(EATV, e, a))
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    // Find first entry with Lamport <= target (descending ElementID order)
    // Since entries are descending by Tx (= ElementID), the first entry
    // with Lamport <= target is the value that was current at that time
    for iter.Next() {
        datom, err := m.encoder.DecodeKey(EATV, iter.Key())
        if err != nil {
            return nil, err
        }

        if datom.Tx.Lamport <= targetLamport {
            val, err := decodeValue(datom.V)
            if err != nil {
                return nil, err
            }
            return executor.NewMaterializedRelation(
                []datalog.Symbol{symV},
                []executor.Tuple{{val}}), nil
        }
    }

    // No value existed at that Lamport time
    return executor.EmptyRelation([]datalog.Symbol{symV}), nil
}
```

**Query Examples:**

```clojure
;; Get all historical values for name (with transaction ID)
[:find ?name ?tx
 :where
 [?e :person/name ?name ?tx]
 [(history)]]
;; → [["Alice" 1000] ["Bob" 2000] ["Carol" 3000]]

;; Get value as of specific transaction
[:find ?name
 :where
 [?e :person/name ?name ?tx]
 [(as-of ?tx 2500)]]
;; → [["Bob"]]  (value at Tx 2500 was "Bob", before "Carol" at Tx 3000)

;; Combine with other patterns
[:find ?e ?name ?tx
 :where
 [?e :person/age 30]
 [?e :person/name ?name ?tx]
 [(history)]]
;; → All name history for entities with age 30
```

**Important:** With Tx = ElementID unified, the Lamport component is the user-facing temporal marker for history queries. `[(as-of ?tx 5000)]` filters to entries with `Lamport <= 5000`. The ReplicaID component is only used for LWW tiebreaking, not for history filtering.

**Tests:** `datalog/executor/history_test.go`
- [ ] `TestHistoryPredicateParsing` - [(history)] parses correctly
- [ ] `TestAsOfPredicateParsing` - [(as-of ?tx N)] parses correctly
- [ ] `TestHistoryQueryAllVersions` - returns all historical values
- [ ] `TestAsOfQuery` - returns correct value at transaction
- [ ] `TestAsOfQueryBeforeFirst` - returns empty before first write
- [ ] `TestHistoryWithOtherPatterns` - combines correctly with other clauses
- [ ] `TestHistoryCardinalityMany` - works with set cardinality
- [ ] `TestHistoryCardinalityVector` - works with vector cardinality

### 8.5 Tx Range Query Rewriting

**File:** `datalog/planner/tx_range_rewriter.go` (NEW)

Since Tx is encoded so highest sorts first, range scans require bound inversion. The query layer handles this transparently:

```go
package planner

// RewriteTxRange inverts bounds for storage scan.
// User specifies intuitive (low, high); storage needs (encoded_high, encoded_low).
func RewriteTxRange(low, high uint64) (startKey, endKey []byte) {
    // High Tx encodes to smaller value (sorts first)
    // Low Tx encodes to larger value (sorts last)
    return ElementID{Lamport: high}.Encode(),  // start bound
           ElementID{Lamport: low}.Encode()    // end bound
}

// RewriteTxPredicate transforms user-friendly Tx predicates to storage bounds.
// [(tx-between ?tx 100 200)] → scan from encoded(200) to encoded(100)
func RewriteTxPredicate(pred *query.Predicate) (*StorageRange, error) {
    if pred.Name != "tx-between" {
        return nil, nil  // Not a Tx range predicate
    }

    low := pred.Args[1].(int64)
    high := pred.Args[2].(int64)
    start, end := RewriteTxRange(uint64(low), uint64(high))

    return &StorageRange{
        Index:    TAEV,
        StartKey: start,
        EndKey:   end,
    }, nil
}
```

**Usage in queries:**

```clojure
;; User writes intuitive range (low to high)
[:find ?e ?a ?v ?tx
 :where
 [?e ?a ?v ?tx]
 [(tx-between ?tx 1000 2000)]]  ;; "Tx between 1000 and 2000"

;; Internally rewritten to scan TAEV from encoded(2000) to encoded(1000)
;; Results returned in descending Tx order (newest first within range)
```

**Tasks:**
- [ ] Implement `RewriteTxRange` function
- [ ] Add `tx-between` predicate to parser
- [ ] Integrate rewriter into query planner
- [ ] Handle edge cases (low > high, zero bounds)

**Tests:** `datalog/planner/tx_range_rewriter_test.go`
- [ ] `TestRewriteTxRange` - bounds correctly inverted
- [ ] `TestTxBetweenPredicate` - end-to-end query works
- [ ] `TestTxRangeEdgeCases` - handles invalid ranges

---

## Phase 9: Cleanup and Finalization

**Goal:** Remove legacy code and finalize the new storage model.

### 9.1 Remove Legacy Storage Code

**Tasks:**
- [ ] Remove `Retract` method from Transaction (replaced by tombstones for Many)
- [ ] Remove `RetractHistory` mode (history is now built-in)
- [ ] Remove `_HISTORY` index handling
- [ ] Remove old test fixtures that reference legacy indices

### 9.2 Verify Index Migration

All code should now use the 6 new indices (Tx = ElementID, 16 bytes):

```go
const (
    EAVT IndexType = iota  // E → A → V → Tx: Cardinality-Many (group by V for add-wins)
    EATV                   // E → A → Tx → V: Cardinality-One (first = current value)
    AEVT                   // A → E → V → Tx: All entities with attribute
    AVET                   // A → V → E → Tx: Find entities with specific value
    VAET                   // V → A → E → Tx: Reverse reference lookup
    TAEV                   // Tx → A → E → V: Transaction log, startup recovery
)
```

Note: This matches Phase 2.3. "Tx" in all names means the 16-byte ElementID (Lamport + ReplicaID).

### 9.3 Update All Tests

- [ ] Update test helpers to include ElementID in Datom construction
- [ ] Remove tests for legacy storage paths
- [ ] Ensure all 6 indices covered in encoder tests
- [ ] Verify all cardinality-specific tests pass

### 9.4 Storage Diagnostics API

**Goal:** Provide on-demand storage statistics for monitoring and capacity planning.

**File:** `datalog/storage/database.go` (ADD method)

```go
// StorageStats contains on-demand storage metrics.
// Computed by scanning indices - not continuously tracked.
type StorageStats struct {
    TotalDatoms      int64                       // Total datom count across all indices
    PerAttribute     map[datalog.Keyword]int64   // Datom count per attribute
    PerCardinality   map[Cardinality]int64       // Datom count per cardinality type
    MaxLamport       uint64                      // Highest Lamport value in database
    UniqueEntities   int64                       // Count of distinct entity IDs
}

// StorageStats computes storage metrics by scanning indices.
// This is an O(n) operation where n = total datoms. Use sparingly.
//
// Use this for:
// - Capacity planning before compaction is implemented
// - Debugging storage growth patterns
// - Identifying high-churn attributes
//
// Do NOT use on hot path - this scans the entire database.
func (db *Database) StorageStats() (StorageStats, error) {
    stats := StorageStats{
        PerAttribute:   make(map[datalog.Keyword]int64),
        PerCardinality: make(map[Cardinality]int64),
    }

    entities := make(map[[20]byte]bool)

    // Scan TAEV for complete coverage (includes all datoms)
    iter, err := db.store.Scan(TAEV, nil, nil)  // Full scan
    if err != nil {
        return stats, err
    }
    defer iter.Close()

    for iter.Next() {
        datom, err := db.encoder.DecodeKey(TAEV, iter.Key())
        if err != nil {
            return stats, err
        }

        stats.TotalDatoms++

        // Track per-attribute
        attr := decodeAttribute(datom.A)
        stats.PerAttribute[attr]++

        // Track per-cardinality
        card := db.schema.Cardinality(attr)
        stats.PerCardinality[card]++

        // Track max Lamport
        if datom.ElementID.Lamport > stats.MaxLamport {
            stats.MaxLamport = datom.ElementID.Lamport
        }

        // Track unique entities
        entities[datom.E] = true
    }

    stats.UniqueEntities = int64(len(entities))
    return stats, nil
}
```

**Usage:**

```go
// Periodic monitoring (e.g., daily cron job, admin endpoint)
stats, err := db.StorageStats()
if err != nil {
    log.Printf("failed to get storage stats: %v", err)
    return
}

log.Printf("Total datoms: %d", stats.TotalDatoms)
log.Printf("Unique entities: %d", stats.UniqueEntities)
log.Printf("Max Lamport: %d", stats.MaxLamport)

// Identify high-churn attributes
for attr, count := range stats.PerAttribute {
    if count > 100000 {
        log.Printf("High-churn attribute %s: %d datoms", attr, count)
    }
}
```

**Tests:** `datalog/storage/stats_test.go`
- [ ] `TestStorageStatsEmpty` - returns zeros for empty database
- [ ] `TestStorageStatsCounts` - correct counts after writes
- [ ] `TestStorageStatsPerAttribute` - breaks down by attribute
- [ ] `TestStorageStatsPerCardinality` - breaks down by cardinality
- [ ] `TestStorageStatsMaxLamport` - tracks highest Lamport

---

## CRDT Test Coverage Analysis

### What IS Currently Tested

The following **primitive CRDT components** have unit tests:

| Component | Test File | Coverage |
|-----------|-----------|----------|
| ElementID ordering | `storage/element_id_test.go` | Less(), Compare(), ReplicaID tiebreaking |
| ElementID encoding | `storage/element_id_test.go` | Key encoding with bitwise NOT, decode round-trip |
| ElementID sort order | `storage/element_id_test.go` | Encoded bytes sort correctly (highest first) |
| LamportClock Next() | `storage/lamport_clock_test.go` | Monotonically increasing |
| LamportClock Receive() | `storage/lamport_clock_test.go` | L = max(L, L_remote) + 1 |
| LamportClock concurrency | `storage/lamport_clock_test.go` | Safe under concurrent access |

### What is NOT Yet Tested (CRDT Semantic Gaps)

The following **true CRDT behaviors** are NOT tested yet:

| Semantic | Description | Required Test |
|----------|-------------|---------------|
| **Multi-replica merge** | Two databases with different ReplicaIDs write concurrently, then merge | `TestMultiReplicaMerge` |
| **LWW resolution at storage level** | Queries return highest (Lamport, ReplicaID) value | `TestLWWQueryResolution` |
| **ReplicaID tiebreaker at query level** | When Lamports are equal, higher ReplicaID wins | `TestReplicaIDTiebreaker` |
| **Add-wins for cardinality-many** | Concurrent Add + Remove with same Lamport → Add wins | `TestAddWinsSemantics` |
| **RGA for cardinality-vector** | Concurrent appends produce deterministic order | `TestRGADeterministicOrder` |
| **Cross-replica query correctness** | After merge, both replicas return identical results | `TestCrossReplicaQueryConvergence` |
| **Clock restoration after restart** | Lamport clock ≥ max existing after DB reopen | `TestClockRestorationAfterRestart` |

### Why This Matters

The primitive tests verify that the building blocks work correctly in isolation. However, CRDT correctness depends on how these primitives **compose** in real storage and query scenarios:

1. **ElementID ordering is tested** → but LWW resolution using that ordering in queries is not
2. **LamportClock.Receive() is tested** → but actual merge workflows using Receive() are not
3. **Bitwise NOT encoding is tested** → but O(1) "highest first" scans for current value are not

### Recommended Test Files to Add

```
datalog/storage/crdt_merge_test.go        # Multi-replica merge scenarios
datalog/storage/crdt_lww_test.go          # LWW resolution at storage/query level
datalog/storage/crdt_addwins_test.go      # Add-wins for cardinality-many
datalog/storage/crdt_rga_test.go          # RGA for cardinality-vector
datalog/storage/crdt_convergence_test.go  # Cross-replica query convergence
```

---

## Testing Strategy

### Unit Test Requirements (Per Component)

From CLAUDE_TESTING.md:

| Component | Required Tests |
|-----------|----------------|
| ElementID | Ordering, encode/decode round-trip, sort order verification |
| LamportClock | Next (monotonic), Receive (max+1), Restore, concurrency |
| KeyEncoder | All 6 indices, sort order verification (CRITICAL) |
| Index Selection | Cardinality-aware selection, schemaless fallback, membership optimization |
| SetEntry | Encode/decode, add-wins resolution |
| RGAElement | Encode/decode with tombstones, reconstruction |
| RGA Reconstruction | Concurrent inserts, tombstone filtering, deterministic order |
| Cache | Freshness, invalidation, rebuild, attribute-level freshness, vector position index |
| Vector Functions | All 8 functions, edge cases, type handling |
| History Predicates | Parser, execution, as-of filtering |

### Integration Test Requirements

From CLAUDE_TESTING.md patterns:

```go
func TestCRDTStorageE2E(t *testing.T) {
    // 1. Create database with CRDT enabled
    db := createCRDTDatabase(t)

    // 2. Define schema with all cardinalities
    s := defineTestSchema()
    db.SetSchema(s)

    // 3. Write data using new API
    tx := db.NewTransaction()
    tx.Set(entity, ":name", "Alice")     // One
    tx.Add(entity, ":tags", "warrior")   // Many
    tx.Append(entity, ":skills", "bow")  // Vector
    _, err := tx.Commit()
    require.NoError(t, err)

    // 4. Query and verify
    result, err := db.ExecuteQuery(`[:find ?name :where [?e :name ?name]]`)
    require.NoError(t, err)
    assert.Equal(t, "Alice", result[0][0])
}
```

### Concurrent Write Tests

Critical for CRDT correctness:

```go
func TestConcurrentCardinalityOneWrites(t *testing.T) {
    db := createCRDTDatabase(t)
    db.SetSchema(schema)

    // Simulate concurrent writes
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            tx := db.NewTransaction()
            tx.Set(entity, ":name", fmt.Sprintf("Name%d", i))
            tx.Commit()
        }(i)
    }
    wg.Wait()

    // Verify single current value (highest ReplicaID at max Lamport wins)
    result, _ := db.ExecuteQuery(`[:find ?name :where [?e :name ?name]]`)
    assert.Len(t, result, 1)  // Only one current value

    // Verify all values in history
    history, _ := db.ExecuteQuery(`[:find ?name :where [?e :name ?name] [(history)]]`)
    assert.Len(t, history, 10)  // All 10 writes preserved
}
```

### Performance Benchmarks

```go
func BenchmarkCRDTvsLegacy(b *testing.B) {
    b.Run("Legacy/CardinalityOne", func(b *testing.B) {
        // Read-modify-write pattern
    })

    b.Run("CRDT/CardinalityOne", func(b *testing.B) {
        // Append-only pattern (should be faster)
    })
}
```

---

## Implementation Order

### Critical Path: Clock Restoration

**⚠️ Clock restoration is broken until Phase 2 is complete.**

The Lamport clock currently restarts at 0 on every database open because:
- Clock restoration requires scanning TAEV to find max ElementID
- TAEV must encode ElementID (16 bytes) with descending sort order
- Current storage uses 20-byte Tx hashes, not ElementID

**Priority sequence to fix clock restoration:**
1. ✅ Phase 0.3: ReplicaID persistence (DONE)
2. ✅ Phase 1.1: ElementID type and encoding (DONE)
3. ✅ Phase 1.2: LamportClock implementation (DONE)
4. **→ Phase 2: Key encoding with 16-byte ElementID** (NEXT - enables clock restoration)
5. Phase 1.3: Clock restoration on database open (blocked on Phase 2)

### Recommended Sequence

**Phase Group 1: Core Primitives (Phases 0-2)**
- ElementID, LamportClock types
- **Key encoder with 16-byte ElementID (PRIORITY - unblocks clock restoration)**
- Extensive sort-order testing

**Phase Group 2: Cardinality Semantics (Phases 3-5)**
- Cardinality-One (simplest)
- Cardinality-Many with add-wins
- Cardinality-Vector with RGA

**Phase Group 3: Integration (Phases 6-8)**
- Cache implementation
- Transaction API
- Query integration and vector functions

**Phase Group 4: Finalization (Phase 9)**
- Remove legacy code
- Clean up removed code
- Full test suite validation

---

## File Summary

### New Files

| File | Phase | Purpose |
|------|-------|---------|
| `storage/element_id.go` | 1 | ElementID type and encoding |
| `storage/lamport_clock.go` | 1 | Lamport clock for CRDT ordering |
| `storage/set_entry.go` | 4 | Set entry type for cardinality-many |
| `storage/set_resolution.go` | 4 | Add-wins conflict resolution |
| `storage/rga_element.go` | 5 | RGA element type for vectors |
| `storage/rga_reconstruct.go` | 5 | RGA reconstruction algorithm (includes `reconstructRGAWithIDs` for position index) |
| `storage/matcher_vector.go` | 5 | Vector pattern matching |
| `storage/matcher_history.go` | 3, 8.4 | History query support |
| `storage/cache.go` | 6 | Unified CRDT resolution cache with attribute-level freshness and vector position index |
| `storage/index_selection.go` | 2.6 | Cardinality-aware index selection logic |
| `storage/export.go` | EDN | Export/Import using Merge() for cross-replica sync |
| `storage/migration.go` | Schema | Cardinality migration (One↔Many↔Vector) |
| `executor/vector_functions.go` | 8.3 | All 8 vector functions (enumerate, nth, contains?, length, first, last, index-of, subvec) |
| `query/history.go` | 8.4 | History predicate types (HistoryAll, HistoryAsOf) |
| `parser/predicate_parser.go` | 8.4 | Parser for [(history)] and [(as-of ?tx N)] predicates |
| `planner/tx_range_rewriter.go` | 8.5 | Tx range bound inversion for queries |

### Modified Files

| File | Phases | Changes |
|------|--------|---------|
| `types.go` | 1,2 | Add ElementID field to Datom; register ElementID as value type |
| `storage/types.go` | 2 | Add ElementID to StorageDatom |
| `storage/database.go` | 1,6,6.2,9.4 | Add LamportClock, Cache, WarmCache(), StorageStats() |
| `storage/transaction.go` | 3,4,5,7 | Replace Add/Retract with Set/Add/Remove/Append |
| `storage/matcher.go` | 3,4,5,8 | Cardinality-aware CRDT dispatch |
| `storage/index_types.go` | 2 | Replace EAVT etc. with EATV etc. |
| `storage/key_encoder_binary.go` | 2 | Update key layouts for ElementID |
| `storage/value_encoder.go` | 1 | Add ElementID value encoding (TypeElementID = 0x09) |
| `storage/badger_store.go` | 2 | Update for new index structure |
| `schema/types.go` | 0 | Add CardinalityVector |
| `schema/builder.go` | 0 | Add .Vector() method |
| `reflect/writer.go` | 7 | Use CRDT primitives |
| `edn/reader.go` | 1 | Add #eid tagged reader |
| `edn/writer.go` | 1 | Emit #eid for ElementID values |
| `executor/compare.go` | 1 | Add ElementID to compareValues |

### Deleted Code (Phase 9)

| What | Reason |
|------|--------|
| `Transaction.Retract()` | Replaced by tombstones |
| `RetractHistory` mode | History now built-in |
| `_HISTORY` index handling | No longer needed |

---

## Success Criteria

Before declaring complete:

### Core CRDT Semantics
1. **Lamport clock correct** - `Receive()` implements `L = max(L, L_remote) + 1`
2. **ElementID ordering correct** - `Less()` comparison is total order
3. **Tx encoding correct** - `^(Lamport, ReplicaID)` so first-in-scan = current
4. **Index selection correct** - EAVT for Many (group by V), EATV for One/Vector

### Cardinality-Specific
5. **Cardinality-One** - Highest ElementID wins, no read before write
6. **Cardinality-Many** - Add-wins at same Lamport, tombstones work
7. **Cardinality-Vector** - RGA reconstruction deterministic, concurrent appends merge

### Query Integration
8. **All 8 vector functions work** - enumerate, nth, contains?, length, first, last, index-of, subvec
9. **History predicates work** - `[(history)]` returns all versions, `[(as-of ?tx N)]` filters correctly
10. **Cache works** - Freshness check, invalidation, attribute-level freshness

### Performance
11. **Cache provides O(1) vector access** - Cache hit is O(1), miss triggers O(n) reconstruct
12. **Writes are O(1)** - No read-before-write for any operation
13. **Reads have no side effects** - No database writes from read operations
14. **Benchmarks acceptable** - No regression vs legacy for common operations

### Documentation
14. **API limitations documented** - Vector remove requires full replacement
15. **Append semantics documented** - HEAD + local tracking, not append-to-end
16. **Storage size documented** - Keys shrink by ~5% (Tx: 20→16 bytes)

### Testing
17. **All new tests pass** - Full coverage for CRDT semantics
18. **Concurrent write tests pass** - Multi-goroutine, multi-node simulation
19. **Sort order tests pass** - Key encoding preserves correct order (CRITICAL)