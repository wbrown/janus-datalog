# "Retract" named both physical deletion and a CRDT Remove tombstone

**Status**: Resolved.

Not a wrong-answer bug. Both meanings were implemented correctly. The
defect was the naming, which invited the mutable-storage mental model, the
one that produced `BUG_CACHE_CARDINALIY_ONE_TOMBSTONE`.

## The two meanings

**Physical deletion.** `Store.Retract` removes datoms from storage.
`MemoryStore.Retract` runs `retractMemoryDatom`, which finds the matching
EAVT keys and calls `deleteMemoryEntry` for every one of the eight indices.
Nothing is left behind; the history is gone. This is a maintenance
primitive: `TRANSACTION_ENVELOPES.md` plans to remove the public
`Transaction.Retract` entirely, so that "applications use schema-aware CRDT
`Remove`."

**A CRDT Remove tombstone.** The unique walk's state holds, keyed by value,
the highest `OpCRDTRemove` Tx seen in an entity's (E, A) history, so a later
`Set` at a lower Tx is known to be cancelled, and the streaming path's copy
of that state is assigned straight into it. Nothing is deleted by any of
it. The datom is still there, still scanned, still part of History and
AsOf.

**One erases history. The other *is* history.** Same word, same package:
the walk state's map was named `retracted`, the walk's decision for a
Remove entry `walkEntryRetract`, and the iterator's copy `uniqueRetracted`.

## Why it was dangerous rather than untidy

The engine's central rule is that datoms are operation records and
resolution interprets the operation. `BUG_CACHE_CARDINALIY_ONE_TOMBSTONE`
happened because a resolver returned the highest-Tx entry's value without
checking its `Op`, the "overwrite" mental model, in which a write replaces
rather than appends.

A reader who met `state.retracted` and carried over the `Store.Retract`
meaning had imported exactly that model: they then believed something was
taken out of the store. Every question they asked afterward, why the datom
is still scanned, why History still shows it, why the cache path needs an
`Op` check, started from a false premise.

The prose already knew better. The comment above the field said "Remove Tx"
while the identifier said retracted.

## The fix

"Retract" is reserved for physical deletion, where the public API uses it
and where `TRANSACTION_ENVELOPES.md` is deprecating it. The tombstone sense
is named for what it records:

- `uniqueWalkState.removedAt` (`datalog/storage/unique_resolve.go`) maps a
  value to the Tx it was removed at, which is what the walk asks.
- `walkEntryRemoved` is the walk's decision for an entry that is a Remove.
- `CRDTResolvingIterator.uniqueRemovedAt`
  (`datalog/storage/crdt_resolving_iterator.go`) is the streaming path's
  copy of that state.

The comments beside them say a value with a Remove tombstone is removed,
and the field docs state the invariant: nothing is deleted, the datom a
Remove cancels stays in the store and in History. The tests whose subject
is a Remove are named for it: `TestEntityView_RemoveCancelsSet` and
`TestEntityView_RemoveThenReassert` (`datalog/storage/unique_fallback_test.go`)
and `TestVOnlyBound_CardinalityMany_Removed`
(`datalog/storage/crdt_cache_matrix_test.go`).

The identifiers in the physical-deletion sense keep their name:
`Store.Retract`, `MemoryStore.Retract`, `memoryStoreTx.Retract`,
`retractMemoryDatom`, `Transaction.Retract`, and the two test comments that
contrast a `Remove` with "Retract which deletes" keep the contrast.
