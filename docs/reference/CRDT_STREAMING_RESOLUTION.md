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

## Why Values Work as Map Keys

Values in datoms are typed:
- `string` - comparable
- `int64` - comparable
- `float64` - comparable
- `bool` - comparable
- `time.Time` - comparable (struct)
- `datalog.Keyword` - comparable (string type)
- `datalog.Identity` - comparable (interface with comparable underlying)

Use `map[any]bool` directly. **No stringify. No `valueKey()` function. No allocations on hot path.**

## Call Site Conditions

The wrapping condition should be consistent across all call sites:

```go
// Only wrap when E is unbound (scanning multiple entities)
if m.schema != nil && e == nil {
    iter = NewCRDTResolvingIterator(rawIter, m.schema, m.txID)
}
```

Current state:
- `matcher_relations.go` - Correct: `e == nil && a != nil`
- Other call sites - Need audit: some wrap unconditionally

## Summary

| Cardinality | Strategy | State | Buffering | Streamable |
|-------------|----------|-------|-----------|------------|
| One | Emit first, skip rest | current (E, A) | None | ✓ Yes |
| Many | Emit qualifying adds | emitted + tombstones maps | None | ✓ Yes |
| Vector | Accumulate, resolve, emit | element list | State only, not datoms | ✗ No (proven impossible) |

**Trust the architecture:**
- Index ordering provides resolution (EATV = Tx descending = first entry wins)
- Values are comparable map keys (no `valueKey()` stringify!)
- CardinalityOne/Many: streaming means filter and emit, not buffer and resolve
- CardinalityVector: mathematically impossible to stream due to DFS ordering requirements

## Implementation Status

The `CRDTResolvingIterator` in `crdt_resolving_iterator.go` implements all three strategies:

- **CardinalityOne**: Zero state beyond current (E, A). First entry emitted, rest skipped.
- **CardinalityMany**: `map[any]bool` for emitted values, `map[any]uint64` for tombstones. Emit immediately when qualifying.
- **CardinalityVector**: `[]rgaElement` accumulates minimal state. Resolve and reconstruct datoms at (E, A) boundary.

All 69 CRDT tests pass (5 CardinalityOne, 17 CardinalityMany, 47 CardinalityVector).
