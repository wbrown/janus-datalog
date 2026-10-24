# CRDT Composable Toolkit

## Summary

This proposal refactors the CRDT storage layer from fixed cardinality types (`One`, `Many`, `Vector`) to composable primitives (`Ordering`, `Unique`, `Conflict`). This enables a rich set of CRDT types through composition rather than hardcoding, while maintaining backward compatibility through convenience methods.

**Key insight:** Current cardinalities are just specific combinations:

- `One` = `Ordering:None + Conflict:LWW`
- `Many` = `Ordering:None + Unique:true + Conflict:AddWins`
- `Vector` = `Ordering:RGA + Unique:false + Conflict:AddWins`

By exposing the primitives, users can compose types that don't exist in Datomic or any single CRDT library.

---

## What exists

The composition vocabulary exists as a view derived from the cardinality, and one composed type is built.

- `datalog/schema` declares `Ordering` with `OrderingNone` and `OrderingRGA`, and `Conflict` with `ConflictLWW` and `ConflictAddWins`. Their doc comments state that both are derived from `Cardinality` today and become primary under this toolkit.
- `AttributeDefinition` keeps `Cardinality` as its keyword field, beside `UniqueElements bool`. Its `GetOrdering()` and `GetConflict()` derive the primitives from the cardinality, and `IsRegister()`, `IsSet()`, `IsOrdered()`, `IsOrderedSet()` and `IsUniqueElements()` answer the composition questions the storage dispatch asks.
- The builder has `One()`, `Many()`, `Vector()`, `UniqueElements(bool)` and `OrderedSet()`. `OrderedSet()` is `Vector().UniqueElements(true)`: the RGA ordered set.
- `datalog.OrderedSet[T comparable]` is the Go type for it, with `NewOrderedSet` and `OrderedSetFrom`, and `datalog/reflect` writes and reads it (`writeOrderedSetField`, `updateOrderedSetField`, `setOrderedSetValue`).

`OrderingLSeq`, `OrderingTimestamped`, `ConflictRemoveWins`, `ConflictMV`, the primitive setters, the other convenience methods, and the `Set` and `Vector` Go types are the proposal.

---

## Motivation

### Beyond Datomic

| Feature                      | Datomic | Current janus | Proposed       |
| ---------------------------- | ------- | ------------- | -------------- |
| Temporal queries             | ✅      | ✅            | ✅             |
| Cardinality one/many         | ✅      | ✅            | ✅             |
| Ordered collections          | ❌      | ✅ (RGA)      | ✅ (RGA, LSeq) |
| Multi-replica merge          | ❌      | ✅            | ✅             |
| Conflict resolution options  | ❌      | Hardcoded     | ✅ Composable  |
| MV-Register (show conflicts) | ❌      | ❌            | ✅             |
| Remove-wins semantics        | ❌      | ❌            | ✅             |
| Ordered sets                 | ❌      | ✅ (RGA)      | ✅ (RGA, LSeq) |

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
type AttributeDefinition struct {
    Ident          datalog.Keyword // The attribute keyword (e.g., :person/name), interned
    ValueType      datalog.Keyword // Required for type validation
    Cardinality    datalog.Keyword // Required for Pull API (default: CardinalityOne)
    Unique         datalog.Keyword // Optional uniqueness constraint; nil is no constraint
    UniqueElements bool            // If true, collection has set semantics (no duplicate values)
    NeverZeroValue bool            // If true, the ValueType's zero value is not a value; requires ValueType
    Doc            string          // Optional documentation
}
```

`Cardinality` draws from the closed set `CardinalityOne`, `CardinalityMany`, `CardinalityVector`. `GetOrdering()` and `GetConflict()` derive the composition from it, and the storage layer switches on the cardinality:

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
| --- | --- | --- | --- |
| `None` | No position | `[E][A][V][Tx↓][Op]` | Registers, sets |
| `RGA` | AfterRef chain | `[E][A][V][Tx↓][Op][AfterRef]` | Ordered lists (current) |
| `LSeq` | Position identifier | `[E][A][Pos][Tx↓][Op][V]` | Ordered with move |
| `Timestamped` | ElementID order | `[E][A][Tx↓][Op][V]` | Event logs |

**Note:** LSeq moves V into the value portion since Position is the primary sort key. Timestamped ordering sorts by the existing Tx field and needs no position of its own.

### Unique

| Value   | Behavior                        | Enforcement              |
| ------- | ------------------------------- | ------------------------ |
| `true`  | Each value appears at most once | AVET lookup before write |
| `false` | Duplicates allowed              | No check                 |

For ordered types with `Unique:true`:

- On add of existing value: No-op (value already present)
- Position of first occurrence is preserved
- Optional: `Move()` operation to change position, which updates the position and preserves the value

### Conflict

| Value | Resolution Rule | Implementation |
| --- | --- | --- |
| `LWW` | Highest `(Lamport, ReplicaID)` wins | Compare full ElementID |
| `AddWins` | Add beats remove at same Lamport | Compare Lamport only |
| `RemoveWins` | Remove beats add at same Lamport | Compare Lamport only |
| `MV` | Keep all concurrent values | Track all at same Lamport |

---

## Composition Matrix

### Valid Combinations

| Ordering    | Unique | Conflict   | Name             | Convenience     |
| ----------- | ------ | ---------- | ---------------- | --------------- |
| None        | N/A    | LWW        | Register         | `.One()`        |
| None        | N/A    | MV         | MV-Register      | `.MVRegister()` |
| None        | true   | AddWins    | AW-Set           | `.Many()`       |
| None        | true   | RemoveWins | RW-Set           | `.RWSet()`      |
| None        | true   | LWW        | LWW-Set          | `.LWWSet()`     |
| None        | true   | MV         | MV-Set           | `.MVSet()`      |
| None        | false  | AddWins    | AW-Bag           | `.Bag()`        |
| RGA         | false  | AddWins    | RGA-List         | `.Vector()`     |
| RGA         | true   | AddWins    | RGA-OrderedSet   | `.OrderedSet()` |
| LSeq        | false  | AddWins    | LSeq-List        | `.List()`       |
| LSeq        | true   | AddWins    | LSeq-OrderedSet  | -               |
| Timestamped | false  | AddWins    | Event Log        | `.EventLog()`   |
| Timestamped | true   | AddWins    | Deduplicated Log | `.UniqueLog()`  |

### Invalid Combinations

| Combination | Reason |
| --- | --- |
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
schema.Attribute(":person/preferences").Type(TypeString).OrderedSet().Add()

// New convenience methods
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
    b.ordering = OrderingRGA
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

### The ordered set, built

`datalog.OrderedSet[T comparable]` is the Go type of an `OrderedSet()` attribute. `NewOrderedSet[T]()` and `OrderedSetFrom[T](values)` construct it. It carries `Append`, `Remove`, `Contains`, `Get`, `Slice`, `Len`, `Clear`, `Union`, `Intersection`, `Difference`, `SymmetricDifference`, `IsSubset`, `IsSuperset` and `Equals`. `datalog/reflect` infers its schema from the struct field, writes it in `SaveStruct` and reads it in `PullInto`.

### Proposed Generic Types

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

| Go Type                 | Inferred Schema    |
| ----------------------- | ------------------ |
| `T`                     | `One()`            |
| `*T`                    | `One()` (optional) |
| `[]T`                   | `Many()`           |
| `datalog.Set[T]`        | `Many()`           |
| `datalog.Vector[T]`     | `Vector()`         |
| `datalog.OrderedSet[T]` | `OrderedSet()`     |

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

| Left         | Right        | Result            | Explanation     |
| ------------ | ------------ | ----------------- | --------------- |
| `[64]`       | `[192]`      | `[128]`           | Midpoint        |
| `[100]`      | `[101]`      | `[100, 128]`      | Extend left     |
| `[100, 200]` | `[100, 201]` | `[100, 200, 128]` | Extend at depth |
| `[]` (HEAD)  | `[128]`      | `[64]`            | Insert at front |
| `[128]`      | `[]` (TAIL)  | `[192]`           | Insert at back  |

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

### Transaction operations for ordered types

```go
tx.Append(e, a, v)           // Add at end
tx.InsertAfter(e, a, v, pos) // Insert after position
tx.InsertBefore(e, a, v, pos) // Insert before position
tx.Move(e, a, v, newPos)     // Atomic move (for OrderedSet)
```

---

## Multi-value reads

An MV attribute with concurrent values returns every one of them. The query result's shape for that case is one of the choices below. Under the first alternative:

```go
// Query returns []any for MV attributes with conflicts
result := db.Query(`[:find ?content :where [?e :doc/content ?content]]`)
// If no conflict: [["hello"]]
// If conflict:    [["hello", "world"]]  // Both values returned
```

---

## Compatibility

- The `CardinalityOne`, `CardinalityMany` and `CardinalityVector` constants stay, each mapping to its composition: `{Ordering:None, Unique:false, Conflict:LWW}`, `{Ordering:None, Unique:true, Conflict:AddWins}`, `{Ordering:RGA, Unique:false, Conflict:AddWins}`.
- The storage format of the existing types does not change, so a database written under the cardinalities reads the same under the compositions.
- A new composition is declared on a new attribute. An existing attribute keeps its composition.
- `GetOrdering()`, `GetConflict()`, `IsRegister()`, `IsSet()` and `IsOrdered()` keep serving code written against the cardinalities.

```go
// Old API still works
def := schema.GetAttribute(":person/tags")
if def.Cardinality == schema.CardinalityMany { ... }

// New API available
if def.IsSet() { ... }
if def.Ordering == schema.OrderingNone && def.Unique { ... }
```

---

## The choices

- **The MV-Register read.** One alternative returns `[]any` for an MV attribute, as the example above shows. Another returns a `Siblings` type that carries each value with its metadata. A third adds a `QueryMV()` method and keeps `Query` single-valued.
- **An `OrderedSet` move whose target position conflicts.** One alternative errors. Another inserts at the nearest valid position. A third merges with the existing element, a no-op when the value is the same.
- **An insert of an existing value under `Unique:true`.** One alternative makes `InsertAt()` a no-op, as `Add()` is. The other makes it an error.
- **LSeq position growth under pathological interleaving.** One alternative accepts the growth, since positions are small. Another rebalances periodically, at the cost of the rebalance's complexity. A third allocates adaptively, as the LSEQ paper does.
- **Reflect type inference for `[]T`.** One alternative infers `Many()`, a set. The other infers `Vector()`, an ordered collection, and requires `Set[T]` for a set.

---

## References

- [RGA Paper](https://hal.inria.fr/inria-00555588/document) - Replicated Growable Array
- [LSEQ Paper](https://hal.archives-ouvertes.fr/hal-00921633/document) - Adaptive allocation
- [CRDT Survey](https://hal.inria.fr/inria-00609399v2/document) - Shapiro et al.
- [Automerge](https://automerge.org/) - CRDT library using RGA
- [Yjs](https://yjs.dev/) - CRDT library using YATA (RGA variant)
