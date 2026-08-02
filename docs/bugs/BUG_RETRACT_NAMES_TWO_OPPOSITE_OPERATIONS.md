# "Retract" names both physical deletion and a CRDT Remove tombstone

**Status**: Open, recorded 2026-07-31. Found while removing value encoding from
the matcher during the typed-memory-index work (`MEMORY_DATOM_INDEXES.md` PR B),
when a field named `retracted` turned out to hold Remove tombstones rather than
anything retracted.

Not a wrong-answer bug. Both meanings are implemented correctly. What it is, is
the naming that invites the mutable-storage mental model — the one that already
produced `BUG_CACHE_CARDINALIY_ONE_TOMBSTONE`.

## The two meanings

**Physical deletion.** `Store.Retract` removes datoms from storage.
`MemoryStore.Retract` runs `retractMemoryDatom`, which finds the matching EAVT
keys and calls `deleteMemoryEntry` for every one of the eight indices. Nothing
is left behind; the history is gone. This is a maintenance primitive —
`TRANSACTION_ENVELOPES.md` plans to remove the public `Transaction.Retract`
entirely, so that "applications use schema-aware CRDT `Remove`."

**A CRDT Remove tombstone.** `uniqueWalkState.retracted` is keyed by value and
holds the highest `OpCRDTRemove` Tx seen in an entity's (E, A) history, so a
later `Set` at a lower Tx is known to be cancelled. `walkEntryRetract` is the
walk's decision for "this entry is a Remove." `CRDTResolvingIterator`'s
`uniqueRetracted` is the streaming path's copy of that state, assigned straight
into `uniqueWalkState`. Nothing is deleted by any of it. The datom is still
there, still scanned, still part of History and AsOf.

**One erases history. The other *is* history.** Same word, same package.

## Why it is dangerous rather than untidy

The engine's central rule is that datoms are operation records and resolution
interprets the operation. `BUG_CACHE_CARDINALIY_ONE_TOMBSTONE` happened because
a resolver returned the highest-Tx entry's value without checking its `Op` —
the "overwrite" mental model, in which a write replaces rather than appends.

A reader who meets `state.retracted` and carries over the `Store.Retract`
meaning has imported exactly that model: they now believe something was taken
out of the store. Every question they ask afterward — why is the datom still
scanned, why does History still show it, why does the cache path need an `Op`
check — starts from a false premise.

The prose already knows better. The comment above the field says "Remove Tx"
while the identifier says retracted.

## Sites

Identifiers, all in the CRDT-tombstone sense:

- `uniqueWalkState.retracted` and its uses in `walkApplyEntry`
- `walkEntryRetract`
- `CRDTResolvingIterator.uniqueRetracted`

Plus comments and test names across `datalog/storage` using "retracted" for a
value with a Remove tombstone — `unique_fallback_test.go`,
`crdt_cache_matrix_test.go`, `unique_lookup_test.go`, `cache_resolver.go`,
`crdt_resolving_iterator.go`, and others.

Identifiers in the physical-deletion sense, which keep their name:
`Store.Retract`, `MemoryStore.Retract`, `memoryStoreTx.Retract`,
`retractMemoryDatom`, `Transaction.Retract`.

## The fix

Reserve "retract" for physical deletion, which is where the public API already
uses it and where `TRANSACTION_ENVELOPES.md` is deprecating it. Name the
tombstone sense for what it records:

- `uniqueWalkState.retracted` → `removedAt` (it maps a value to *when* it was
  removed, which is what the walk actually asks)
- `walkEntryRetract` → `walkEntryRemoved`
- `CRDTResolvingIterator.uniqueRetracted` → `uniqueRemovedAt`

Comments and test names follow: a value with a Remove tombstone is *removed*,
not retracted.

## Scope

Not inside PR B. Renaming only the field the swap happens to touch would
desynchronise it from `uniqueRetracted`, which is assigned directly into it, and
sweeping all of it inside a branch that is already rewriting the memory backend
makes both changes uninspectable — the diff stops showing what the swap did.

It is a mechanical rename with no behavioural component, so it wants to be its
own change, legible on its own, after the swap lands.
