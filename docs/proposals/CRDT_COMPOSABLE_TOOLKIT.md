# CRDT Composable Toolkit Proposal

**Status:** Draft
**Created:** 2026-02-01
**Author:** Wes Brown, Claude
**Depends On:** CRDT_VECTOR_STORAGE_IMPLEMENTATION_PLAN.md (Phases 0-8 complete)

---

## Executive Summary

This proposal refactors the CRDT storage layer from fixed cardinality types (`One`, `Many`, `Vector`) to composable primitives (`Ordering`, `Unique`, `Conflict`). This enables a rich set of CRDT types through composition rather than hardcoding, while maintaining backward compatibility through convenience methods.

**Key insight:** Current cardinalities are just specific combinations:
- `One` = `Ordering:None + Conflict:LWW`
- `Many` = `Ordering:None + Unique:true + Conflict:AddWins`
- `Vector` = `Ordering:RGA + Unique:false + Conflict:AddWins`

By exposing the primitives, users can compose types that don't exist in Datomic or any single CRDT library.

---

## Motivation

### Beyond Datomic

| Feature | Datomic | Current janus | Proposed |
|---------|---------|---------------|----------|
| Temporal queries | ✅ | ✅ | ✅ |
| Cardinality one/many | ✅ | ✅ | ✅ |
| Ordered collections | ❌ | ✅ (RGA) | ✅ (RGA, LSeq) |
| Multi-replica merge | ❌ | ✅ | ✅ |
| Conflict resolution options | ❌ | Hardcoded | ✅ Composable |
| MV-Register (show conflicts) | ❌ | ❌ | ✅ |
| Remove-wins semantics | ❌ | ❌ | ✅ |
| Ordered sets | ❌ | ❌ | ✅ |

### Use Cases Enabled

1. **MV-Register** - Collaborative editing with explicit conflict UI
2. **RW-Set** - Blacklists where removal is authoritative
3. **OrderedSet** - Preference lists, priority queues, unique ordered items
4. **Event Log** - Audit trails with natural temporal ordering
5. **LSeq-List** - Kanban boards with efficient move operations

---

## Architecture

### Current Model

```go
type Cardinality int

const (
    CardinalityOne Cardinality = iota
    CardinalityMany
    CardinalityVector
)

type AttributeDefinition struct {
    Ident       Keyword
    ValueType   ValueType
    Cardinality Cardinality  // Single enum
    Unique      bool         // Only for identity uniqueness
}
```

Storage layer switches on `Cardinality`:
```go
switch card {
case CardinalityOne:    // LWW logic
case CardinalityMany:   // Add-wins set logic
case CardinalityVector: // RGA logic
}
```

### Proposed Model

```go
// Ordering determines how elements are arranged
type Ordering int

const (
    OrderingNone       Ordering = iota  // Unordered (sets, registers)
    OrderingRGA                         // Chained positions (current Vector)
    OrderingLSeq                        // Independent positions (new)
    OrderingTimestamped                 // Sort by ElementID
)

// Conflict determines how concurrent writes are resolved
type Conflict int

const (
    ConflictLWW        Conflict = iota  // Last Writer Wins (highest ElementID)
    ConflictAddWins                     // Add beats concurrent remove
    ConflictRemoveWins                  // Remove beats concurrent add
    ConflictMV                          // Multi-Value (keep all, app resolves)
)

type AttributeDefinition struct {
    Ident     Keyword
    ValueType ValueType

    // CRDT composition (replaces Cardinality)
    Ordering  Ordering
    Unique    bool      // Set semantics (no duplicates)
    Conflict  Conflict

    // Existing fields
    Identity  bool      // db.unique/identity
    Component bool      // Component entity (cascade delete)
}
```

Storage layer dispatches on composition:
```go
func (m *Matcher) dispatchWrite(schema AttributeDefinition, op WriteOp) {
    switch {
    case schema.Ordering == OrderingNone && !schema.Unique:
        // Register (One)
        m.writeRegister(op, schema.Conflict)
    case schema.Ordering == OrderingNone && schema.Unique:
        // Set (Many)
        m.writeSet(op, schema.Conflict)
    case schema.Ordering == OrderingRGA:
        // RGA List/Set
        m.writeRGA(op, schema.Unique, schema.Conflict)
    case schema.Ordering == OrderingLSeq:
        // LSeq List/Set
        m.writeLSeq(op, schema.Unique, schema.Conflict)
    case schema.Ordering == OrderingTimestamped:
        // Event log
        m.writeTimestamped(op, schema.Unique, schema.Conflict)
    }
}
```

---

## Primitive Definitions

### Ordering

| Value | Storage | Key Layout | Use |
|-------|---------|------------|-----|
| `None` | No position | `[E][A][V][Tx↓][Op]` | Registers, sets |
| `RGA` | AfterRef chain | `[E][A][V][Tx↓][Op][AfterRef]` | Ordered lists (current) |
| `LSeq` | Position identifier | `[E][A][Pos][Tx↓][Op][V]` | Ordered with move |
| `Timestamped` | ElementID order | `[E][A][Tx↓][Op][V]` | Event logs |

**Note:** LSeq moves V into the value portion since Position is the primary sort key.

### Unique

| Value | Behavior | Enforcement |
|-------|----------|-------------|
| `true` | Each value appears at most once | AVET lookup before write |
| `false` | Duplicates allowed | No check |

For ordered types with `Unique:true`:
- On add of existing value: No-op (value already present)
- Position of first occurrence is preserved
- Optional: `Move()` operation to change position

### Conflict

| Value | Resolution Rule | Implementation |
|-------|-----------------|----------------|
| `LWW` | Highest `(Lamport, ReplicaID)` wins | Compare full ElementID |
| `AddWins` | Add beats remove at same Lamport | Compare Lamport only |
| `RemoveWins` | Remove beats add at same Lamport | Compare Lamport only |
| `MV` | Keep all concurrent values | Track all at same Lamport |

---

## Composition Matrix

### Valid Combinations

| Ordering | Unique | Conflict | Name | Convenience |
|----------|--------|----------|------|-------------|
| None | N/A | LWW | Register | `.One()` |
| None | N/A | MV | MV-Register | `.MVRegister()` |
| None | true | AddWins | AW-Set | `.Many()` |
| None | true | RemoveWins | RW-Set | `.RWSet()` |
| None | true | LWW | LWW-Set | `.LWWSet()` |
| None | true | MV | MV-Set | `.MVSet()` |
| None | false | AddWins | AW-Bag | `.Bag()` |
| RGA | false | AddWins | RGA-List | `.Vector()` |
| RGA | true | AddWins | RGA-OrderedSet | - |
| LSeq | false | AddWins | LSeq-List | `.List()` |
| LSeq | true | AddWins | LSeq-OrderedSet | `.OrderedSet()` |
| Timestamped | false | AddWins | Event Log | `.EventLog()` |
| Timestamped | true | AddWins | Deduplicated Log | `.UniqueLog()` |

### Invalid Combinations

| Combination | Reason |
|-------------|--------|
| `Unique:false + Conflict:MV` | MV multiset semantics unclear |
| `Ordering:Timestamped + Conflict:RemoveWins` | Logs shouldn't support removal |
| `Ordering:None + Unique:false + Conflict:LWW` | Bag with LWW is meaningless |

Schema builder validates and rejects invalid combinations.

---

## Schema Builder API

### Convenience Methods (Backward Compatible)

```go
// Current API preserved
schema.Attribute(":person/name").Type(TypeString).One().Add()
schema.Attribute(":person/tags").Type(TypeString).Many().Add()
schema.Attribute(":person/skills").Type(TypeString).Vector().Add()

// New convenience methods
schema.Attribute(":person/preferences").Type(TypeString).OrderedSet().Add()
schema.Attribute(":audit/events").Type(TypeString).EventLog().Add()
schema.Attribute(":doc/content").Type(TypeString).MVRegister().Add()
schema.Attribute(":user/banned").Type(TypeString).RWSet().Add()
```

### Primitive Composition (Power Users)

```go
// Explicit composition
schema.Attribute(":custom/field").
    Type(TypeString).
    Ordered(OrderingLSeq).
    Unique(true).
    OnConflict(ConflictAddWins).
    Add()

// Partial override of convenience method
schema.Attribute(":special/set").
    Type(TypeString).
    Many().                      // Start with Many (AW-Set)
    OnConflict(ConflictRemoveWins). // Override to RW-Set
    Add()
```

### Implementation

```go
func (b *AttrBuilder) One() *AttrBuilder {
    b.ordering = OrderingNone
    b.unique = false
    b.conflict = ConflictLWW
    return b
}

func (b *AttrBuilder) Many() *AttrBuilder {
    b.ordering = OrderingNone
    b.unique = true
    b.conflict = ConflictAddWins
    return b
}

func (b *AttrBuilder) Vector() *AttrBuilder {
    b.ordering = OrderingRGA
    b.unique = false
    b.conflict = ConflictAddWins
    return b
}

func (b *AttrBuilder) OrderedSet() *AttrBuilder {
    b.ordering = OrderingLSeq
    b.unique = true
    b.conflict = ConflictAddWins
    return b
}

func (b *AttrBuilder) EventLog() *AttrBuilder {
    b.ordering = OrderingTimestamped
    b.unique = false
    b.conflict = ConflictAddWins
    return b
}

func (b *AttrBuilder) MVRegister() *AttrBuilder {
    b.ordering = OrderingNone
    b.unique = false
    b.conflict = ConflictMV
    return b
}

// Primitive setters
func (b *AttrBuilder) Ordered(o Ordering) *AttrBuilder {
    b.ordering = o
    return b
}

func (b *AttrBuilder) Unique(u bool) *AttrBuilder {
    b.unique = u
    return b
}

func (b *AttrBuilder) OnConflict(c Conflict) *AttrBuilder {
    b.conflict = c
    return b
}
```

---

## Reflect Package Types

### New Generic Types

```go
package datalog

// Set[T] maps to Many() - unordered, unique, add-wins
type Set[T any] struct {
    items map[T]struct{}
}

func (s *Set[T]) Add(v T)
func (s *Set[T]) Remove(v T)
func (s *Set[T]) Contains(v T) bool
func (s *Set[T]) Slice() []T
func (s *Set[T]) Len() int

// Vector[T] maps to Vector() - RGA ordered, duplicates allowed
type Vector[T any] struct {
    items []T
}

func (v *Vector[T]) Append(val T)
func (v *Vector[T]) Get(i int) T
func (v *Vector[T]) Slice() []T
func (v *Vector[T]) Len() int

// OrderedSet[T] maps to OrderedSet() - LSeq ordered, unique
type OrderedSet[T any] struct {
    items []T
    seen  map[T]struct{}
}

func (s *OrderedSet[T]) Append(v T)      // Add at end
func (s *OrderedSet[T]) InsertAt(i int, v T)
func (s *OrderedSet[T]) Remove(v T)
func (s *OrderedSet[T]) Contains(v T) bool
func (s *OrderedSet[T]) Slice() []T
func (s *OrderedSet[T]) Len() int
```

### Struct Tag Inference

```go
type Character struct {
    ID     datalog.Identity          `datalog:"-,id"`
    Name   string                    `datalog:"name"`       // One (LWW)
    Tags   []string                  `datalog:"tags"`       // Many (inferred)
    Skills datalog.Vector[string]    `datalog:"skills"`     // Vector (explicit)
    Prefs  datalog.OrderedSet[string] `datalog:"prefs"`     // OrderedSet (explicit)
    Roles  datalog.Set[string]       `datalog:"roles"`      // Many (explicit)
}
```

| Go Type | Inferred Schema |
|---------|-----------------|
| `T` | `One()` |
| `*T` | `One()` (optional) |
| `[]T` | `Many()` |
| `datalog.Set[T]` | `Many()` |
| `datalog.Vector[T]` | `Vector()` |
| `datalog.OrderedSet[T]` | `OrderedSet()` |

---

## LSeq Position Encoding

### Design Goals

1. **Sortable** - Lexicographic byte comparison = position order
2. **Subdivisible** - Can always insert between any two positions
3. **Compact** - Minimal storage overhead
4. **Deterministic** - Same position generated on all replicas for same operation

### Position Structure

```go
type LSeqPosition struct {
    Path []byte  // Variable-length path through tree
}
```

**Encoding:** Each byte represents a choice at one level of a virtual tree.

```
Position space (conceptual):

Level 0:  [0]───────────────────────[255]
           │                          │
Level 1:  [0,0]──[0,128]──[0,255]   [255,0]──[255,255]
           │       │        │         │         │
Level 2:  ...     ...      ...       ...       ...
```

### Allocation Algorithm

```go
func Between(left, right LSeqPosition) LSeqPosition {
    // Find first differing byte
    for i := 0; i < max(len(left), len(right)); i++ {
        l := getByte(left, i)   // 0 if past end
        r := getByte(right, i)  // 255 if past end

        if r - l > 1 {
            // Space between: pick midpoint
            return append(left[:i], (l + r) / 2)
        } else if r - l == 1 {
            // Adjacent: extend left path
            return append(left[:i+1], 128)
        }
        // Equal: continue to next level
    }
    // Shouldn't reach here if left < right
}
```

**Examples:**

| Left | Right | Result | Explanation |
|------|-------|--------|-------------|
| `[64]` | `[192]` | `[128]` | Midpoint |
| `[100]` | `[101]` | `[100, 128]` | Extend left |
| `[100, 200]` | `[100, 201]` | `[100, 200, 128]` | Extend at depth |
| `[]` (HEAD) | `[128]` | `[64]` | Insert at front |
| `[128]` | `[]` (TAIL) | `[192]` | Insert at back |

### Key Layout

```
LSeq Key: [E:20][A:32][Pos:var][Tx↓:16][Op:1]
Value:    [V:var]
```

Position is variable-length, requiring a length prefix or delimiter. Options:

1. **Length prefix**: `[len:2][pos:len]` - Simple but 2 extra bytes
2. **Null terminator**: Not possible (0x00 is valid in position)
3. **Fixed max length**: `[pos:32]` - Wastes space, limits depth

**Recommendation:** Length prefix. Typical positions are 1-8 bytes; 32-byte limit handles pathological cases.

---

## Implementation Phases

### Phase A: Schema Refactor (Foundation)

**Goal:** Replace `Cardinality` enum with composable primitives in schema.

**Changes:**
- `schema/types.go`: Add `Ordering`, `Conflict` types; update `AttributeDefinition`
- `schema/builder.go`: Add primitive setters, keep convenience methods
- `schema/validation.go`: Validate compositions, reject invalid

**Backward Compatibility:**
- `CardinalityOne/Many/Vector` constants remain but map to compositions
- `GetCardinality()` returns computed cardinality for old code

**Tests:** All existing schema tests must pass.

### Phase B: Storage Dispatch Refactor

**Goal:** Storage layer dispatches on composition, not cardinality enum.

**Changes:**
- `storage/matcher.go`: Refactor dispatch logic
- `storage/transaction.go`: Refactor write logic
- `storage/cache.go`: Refactor resolution logic

**Approach:**
```go
// Helper to map composition to behavior
func (s AttributeDefinition) IsRegister() bool {
    return s.Ordering == OrderingNone && !s.Unique
}

func (s AttributeDefinition) IsSet() bool {
    return s.Ordering == OrderingNone && s.Unique
}

func (s AttributeDefinition) IsOrdered() bool {
    return s.Ordering != OrderingNone
}
```

**Tests:** All existing storage tests must pass (behavior unchanged).

### Phase C: LSeq Implementation

**Goal:** Add LSeq ordering support.

**New Files:**
- `storage/lseq_position.go`: Position type and allocation
- `storage/lseq_resolution.go`: Position-based ordering

**Changes:**
- `storage/key_encoder_binary.go`: LSeq key layout
- `storage/matcher.go`: LSeq dispatch
- `storage/transaction.go`: LSeq write operations
- `storage/cache.go`: LSeq cache entry

**Transaction API:**
```go
tx.Append(e, a, v)           // Add at end
tx.InsertAfter(e, a, v, pos) // Insert after position
tx.InsertBefore(e, a, v, pos) // Insert before position
tx.Move(e, a, v, newPos)     // Atomic move (for OrderedSet)
```

### Phase D: Uniqueness Enforcement

**Goal:** Enforce `Unique:true` across all orderings.

**Changes:**
- `storage/transaction.go`: AVET check before write for unique types
- `storage/matcher.go`: Handle duplicate detection

**Behavior:**
- `Unique:true` + `Add()` of existing value → No-op
- `Unique:true` + `InsertAt()` of existing value → No-op or error (TBD)
- `Unique:true` + `Move()` → Update position, preserve value

### Phase E: Conflict Resolution Options

**Goal:** Support `RemoveWins` and `MV` conflict strategies.

**Changes:**
- `storage/set_resolution.go`: Add RemoveWins logic
- `storage/mv_resolution.go`: New file for MV handling
- `storage/cache.go`: MV cache entries (list of values)

**MV-Register Query Result:**
```go
// Query returns []any for MV attributes with conflicts
result := db.Query(`[:find ?content :where [?e :doc/content ?content]]`)
// If no conflict: [["hello"]]
// If conflict:    [["hello", "world"]]  // Both values returned
```

### Phase F: Reflect Types

**Goal:** Add `datalog.Set`, `datalog.Vector`, `datalog.OrderedSet` types.

**New Files:**
- `datalog/set.go`: Set[T] implementation
- `datalog/vector.go`: Vector[T] implementation
- `datalog/ordered_set.go`: OrderedSet[T] implementation

**Changes:**
- `reflect/schema.go`: Infer schema from new types
- `reflect/writer.go`: Handle new types in SaveStruct
- `reflect/reader.go`: Populate new types in PullInto

### Phase G: Timestamped Ordering

**Goal:** Add `OrderingTimestamped` for event logs.

**Changes:**
- `storage/key_encoder_binary.go`: Timestamped key layout
- `storage/timestamped_resolution.go`: Simple ElementID ordering

**This is simpler than LSeq** - just sort by existing Tx field.

---

## Migration Path

### Existing Databases

Existing databases using `CardinalityOne/Many/Vector` continue to work:

1. On schema load, map old cardinality to composition:
   - `CardinalityOne` → `{Ordering:None, Unique:false, Conflict:LWW}`
   - `CardinalityMany` → `{Ordering:None, Unique:true, Conflict:AddWins}`
   - `CardinalityVector` → `{Ordering:RGA, Unique:false, Conflict:AddWins}`

2. No data migration required - storage format unchanged for existing types

3. New composition types require new attributes (can't change existing)

### API Compatibility

```go
// Old API still works
def := schema.GetAttribute(":person/tags")
if def.Cardinality == schema.CardinalityMany { ... }

// New API available
if def.IsSet() { ... }
if def.Ordering == schema.OrderingNone && def.Unique { ... }
```

---

## File Summary

### New Files

| File | Phase | Purpose |
|------|-------|---------|
| `storage/lseq_position.go` | C | LSeq position type and allocation |
| `storage/lseq_resolution.go` | C | LSeq ordering resolution |
| `storage/mv_resolution.go` | E | Multi-value conflict handling |
| `storage/timestamped_resolution.go` | G | Timestamped ordering |
| `datalog/set.go` | F | Set[T] generic type |
| `datalog/vector.go` | F | Vector[T] generic type |
| `datalog/ordered_set.go` | F | OrderedSet[T] generic type |

### Modified Files

| File | Phases | Changes |
|------|--------|---------|
| `schema/types.go` | A | Add Ordering, Conflict; update AttributeDefinition |
| `schema/builder.go` | A | Primitive setters, convenience methods |
| `schema/validation.go` | A | Composition validation |
| `storage/matcher.go` | B, C, D | Composition-based dispatch |
| `storage/transaction.go` | B, C, D | Composition-based writes |
| `storage/cache.go` | B, C, E | Composition-based resolution |
| `storage/key_encoder_binary.go` | C, G | LSeq and Timestamped key layouts |
| `storage/set_resolution.go` | E | RemoveWins logic |
| `reflect/schema.go` | F | Infer from new types |
| `reflect/writer.go` | F | Handle new types |
| `reflect/reader.go` | F | Populate new types |

---

## Success Criteria

### Phase A Complete
- [ ] `Ordering`, `Conflict` types defined
- [ ] `AttributeDefinition` uses composition
- [ ] Convenience methods produce correct compositions
- [ ] All existing tests pass

### Phase B Complete
- [ ] Storage dispatches on composition
- [ ] No behavior change for existing types
- [ ] All existing tests pass

### Phase C Complete
- [ ] LSeq positions encode/decode correctly
- [ ] Position allocation produces valid ordering
- [ ] LSeq-OrderedSet works end-to-end
- [ ] Insert, move, remove operations work

### Phase D Complete
- [ ] Uniqueness enforced for all `Unique:true` types
- [ ] Duplicate adds are no-ops
- [ ] Tests for duplicate handling

### Phase E Complete
- [ ] RemoveWins resolution works
- [ ] MV-Register returns all concurrent values
- [ ] Tests for conflict scenarios

### Phase F Complete
- [ ] `Set[T]`, `Vector[T]`, `OrderedSet[T]` types work
- [ ] Reflect infers correct schema
- [ ] SaveStruct/PullInto handle new types

### Phase G Complete
- [ ] Timestamped ordering works
- [ ] EventLog convenience method works
- [ ] Natural ordering by ElementID

---

## Open Questions

1. **MV-Register API** - How should queries return multiple values?
   - Option A: Return `[]any` for MV attributes
   - Option B: Special `Siblings` type with metadata
   - Option C: Separate `QueryMV()` method

2. **OrderedSet Move semantics** - What if target position conflicts?
   - Option A: Error
   - Option B: Insert at nearest valid position
   - Option C: Merge with existing (no-op if same value)

3. **LSeq position growth** - How to handle pathological interleaving?
   - Option A: Accept growth (positions are small)
   - Option B: Periodic rebalancing (complex)
   - Option C: Adaptive allocation (LSEQ paper)

4. **Reflect type inference** - Should `[]T` infer `Many()` or require explicit type?
   - Current: `[]T` → `Many()` (set)
   - Alternative: `[]T` → `Vector()` (ordered), require `Set[T]` for sets

---

## References

- [RGA Paper](https://hal.inria.fr/inria-00555588/document) - Replicated Growable Array
- [LSEQ Paper](https://hal.archives-ouvertes.fr/hal-00921633/document) - Adaptive allocation
- [CRDT Survey](https://hal.inria.fr/inria-00609399v2/document) - Shapiro et al.
- [Automerge](https://automerge.org/) - CRDT library using RGA
- [Yjs](https://yjs.dev/) - CRDT library using YATA (RGA variant)