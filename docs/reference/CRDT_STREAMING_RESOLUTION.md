# CRDT Streaming Resolution Design

## When to Use CRDTResolvingIterator

**Only wrap with CRDTResolvingIterator when E is unbound.**

| Pattern | E | A | Wrap? | Reason |
|---------|---|---|-------|--------|
| `[entity :attr ?v]` | bound | bound | No | Single (E, A) group - handled by cache or single read |
| `[?e :attr ?v]` | unbound | bound | Yes | Multiple (E, A) groups need resolution |
| `[?e ?a ?v]` | unbound | unbound | Yes | Multiple (E, A) groups need resolution |

When E is bound, there's only one (E, A) group. For CardinalityOne, just read the first datom. For CardinalityMany/Vector, the EA cache handles resolution.

## Key Insight: Index Ordering IS Resolution

EATV index stores Tx with bitwise NOT for **descending order** (highest Tx first).

This means:
- First entry for each (E, A) has the highest Tx
- For LWW (CardinalityOne): first entry IS the winner
- For add-wins (CardinalityMany): first ADD for a value wins unless preceded by a REMOVE with higher Tx

**The index ordering does the resolution. We just filter.**

## CardinalityOne: Emit First, Skip Rest

```
For each datom from source (Tx descending):
    if (E, A) is new:
        emit immediately (this IS the LWW winner)
        track current (E, A)
    else if same (E, A):
        skip (already emitted the winner)
```

**State required:** Just current E and current A (to detect duplicates)

**No buffering. No datom copies. Pure filtering.**

## CardinalityMany: Stream with Minimal State

Because we iterate Tx descending:
- First ADD for a value = highest Tx ADD → winner (unless tombstoned)
- REMOVE before ADD (in iteration) = REMOVE has higher Tx → value removed
- REMOVE after ADD (in iteration) = ADD had higher Tx → already emitted

```
State:
    emitted    map[any]bool     // values we've emitted
    tombstones map[any]uint64   // value → remove Lamport (for add-wins-at-same-Tx)

For each datom from source (Tx descending):
    if (E, A) changed:
        clear emitted and tombstones (new group)

    if Op == ADD:
        if value in emitted:
            skip (already emitted higher-Tx add)
        else if value in tombstones AND add.Lamport < tombstone.Lamport:
            skip (remove wins)
        else:
            emit immediately
            mark value as emitted

    if Op == REMOVE:
        if value not in tombstones:
            tombstones[value] = Lamport (first remove = highest Tx)
```

**State required:** Two maps keyed by value (not string! values ARE comparable)

**No buffering datoms. Emit immediately. Stream.**

## CardinalityVector (RGA): Accumulate State, Not Datoms

RGA requires seeing all elements before emitting because output order depends on tree structure (afterRef relationships).

### Why RGA Cannot Be Streamed: Formal Proof

**Theorem**: RGA resolution cannot be streamed with standard iterator semantics.

**Proof by contradiction**:

1. **RGA ordering requirement**: Output must be DFS order of the tree (parent before children, siblings in ascending ElementID order)

2. **Iterator constraint**: We iterate Tx in descending order (highest Tx first due to EATV index)

3. **Sibling ordering conflict**: Consider siblings A (id=100) and B (id=200) both after parent P:
   - DFS order requires: P, A, B (ascending id)
   - Iterator sees: B first, then A (descending Tx = descending id for inserts)
   - To emit in correct order, we must see A before emitting B
   - But A comes AFTER B in the iteration

4. **Cannot emit until group complete**: We cannot know if a lower-id sibling exists until we've seen all elements in the (E, A) group.

**What about reverse (ascending) scan?**

With ascending Tx order:
- Parents arrive before children (good for tree building)
- Lower-id siblings arrive before higher-id siblings (good for sibling order)

But DFS still requires complete subtrees:
```
         HEAD
        /    \
       A      B
      / \
     C   D
```

DFS order: A, C, D, B

With ascending scan, we see: A, C, D, B (if that's the Tx order)

But if B was inserted before C and D:
- Ascending sees: A, B, C, D
- DFS requires: A, C, D, B

**We cannot emit B until we know A's subtree is complete. We cannot know A's subtree is complete until we've seen all elements.**

**Conclusion**: RGA streaming is mathematically impossible. We must buffer state for the entire (E, A) group.

### Implementation: Buffer State, Not Datoms

We don't store datom pointers. We store minimal state:

```go
type rgaElement struct {
    id          ElementID  // from datom.Tx
    afterRef    ElementID  // from datom.AfterRef
    value       any        // from datom.V
    tombstoneID *ElementID // if deleted
}
```

**Why this is safe**:
- No datom pointers → no corruption from iterator reuse
- Minimal state → memory bounded by element count, not datom size
- Reconstruct at emit → datoms created fresh with correct E, A from group

At (E, A) boundary, reconstruct datoms:
```go
&datalog.Datom{
    E:        it.currentE,  // same for entire group
    A:        it.currentA,  // same for entire group
    V:        element.value,
    Tx:       element.id,
    Op:       datalog.OpRGAInsert,
    AfterRef: element.afterRef,
}
```

### Resolution Algorithm

1. **Accumulate**: As datoms arrive, store `rgaElement` (id, afterRef, value, tombstoneID)
2. **At boundary**: When (E, A) changes or source exhausts:
   - Deduplicate by id (tombstone info takes precedence)
   - Build children map: `map[ElementID][]*rgaElement`
   - Sort children by ascending id
   - DFS walk from HEAD, emit non-tombstoned elements
3. **Reconstruct**: Create fresh `*Datom` from stored state during DFS

**No datom pointer storage. No corruption from iterator reuse. Reconstruct at emit.**

## Values as Map Keys — all but one, and the one matters

Most of the closed value domain is directly hashable, so `map[any]` works without a
`valueKey()` layer:

| Value type | Hashable as a Go map key? | Why |
|---|---|---|
| `string`, `int64`, `float64`, `bool` | yes | comparable scalars |
| `time.Time` | yes | comparable struct |
| `datalog.ElementID` | yes | struct of two `uint64` |
| `Identity`, `Keyword`, `Symbol` | yes | **interned pointers** — one pointer per content, so pointer equality is exact |
| `[]byte` | **no** | slices are not hashable; keying one panics with *hash of unhashable type []uint8* |
| vectors | **no** | same reason — but see below, they cannot reach here |

**`processAddWins` converts `[]byte` to `string` before keying**, and must:

```go
key := datom.V
if b, ok := key.([]byte); ok {
    key = string(b)   // the emitted datom still carries the original []byte
}
```

This is not an optimization to remove. A cardinality-many attribute holding byte
values panics without it — a defect that was found and fixed once already.

**Vectors cannot reach this path.** A whole vector never reaches storage — a
vector literal is a `query.VectorConstant`, not a `Constant` — and a
cardinality-vector attribute stores *elements*, which route to `accumulateRGA`.

## Call Site Conditions

The wrapping condition should be consistent across all call sites:

```go
// Only wrap when E is unbound (scanning multiple entities)
if m.schema != nil && e == nil {
    iter = NewCRDTResolvingIterator(rawIter, m.schema, m.txID, matcher, report)
}
```

The constructor takes five arguments, not three. `matcher` enables the unique
walk — a unique CardinalityOne group resolves by walking the entity rather than
by first-entry-wins, and passing nil disables that and falls back. `report` is the
scan accounting the walk's AVET supersession reads accrue into, since those are
index reads this iterator causes that the source knows nothing about.

## Summary

| Cardinality | Strategy | State | Buffering | Streamable |
|-------------|----------|-------|-----------|------------|
| One | Emit first, skip rest | current (E, A) | None | ✓ Yes |
| Many | Emit qualifying adds | emitted + tombstones maps | None | ✓ Yes |
| Vector | Accumulate, resolve, emit | element list | State only, not datoms | ✗ No (proven impossible) |

**Trust the architecture:**
- Index ordering provides resolution (EATV = Tx descending = first entry wins)
- Values are comparable map keys, with `[]byte` converted to `string` first
- CardinalityOne/Many: streaming means filter and emit, not buffer and resolve
- CardinalityVector: mathematically impossible to stream due to DFS ordering requirements

## Implementation Status

The `CRDTResolvingIterator` in `crdt_resolving_iterator.go` implements all three strategies:

- **CardinalityOne**: Zero state beyond current (E, A). First entry emitted, rest skipped.
- **CardinalityMany**: `map[any]bool` for emitted values, `map[any]uint64` for tombstones, keyed by the value with `[]byte` converted to `string`. Emit immediately when qualifying.
- **CardinalityOne + unique**: resolves by walking the entity (`uniqueMode`), not by first-entry-wins, when the schema declares the attribute unique and a matcher is supplied.
- **CardinalityVector**: `[]rgaElement` accumulates minimal state. Resolve and reconstruct datoms at (E, A) boundary.

All 69 CRDT tests pass (5 CardinalityOne, 17 CardinalityMany, 47 CardinalityVector).
