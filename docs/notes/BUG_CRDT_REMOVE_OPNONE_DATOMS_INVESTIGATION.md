# Investigation: CRDT Remove / OpNone (2026-03-26)

## Premise

Bug report claims `tx.Remove()` fails on cardinality-many values imported from EDN because `Import()` writes datoms with `Op: OpNone`, and the CRDT resolver doesn't treat `OpNone` as a cancellable add.

## Finding: The premise is wrong

Checked actual EDN data from a downstream project. The `:entity/state`
datom (CardinalityMany) has `:op/add`, not `:op/none`.

The EDN file has ~76k `:op/none` datoms and ~135 `:op/add` datoms. The
`:op/none` datoms are all cardinality-one attributes — correct behavior.

## Why the Ops are correct

The pipeline preserves Ops end-to-end:

1. **Write path**: `Transaction.Add()` (`database.go:1270`) sets `Op: OpCRDTAdd` for CardinalityMany when schema is present.

2. **Export**: `Export()` scans raw EAVT storage and calls `FormatDatomEDN()` (`export.go:170`), which serializes `d.Op` directly. `OpCRDTAdd` → `:op/add`.

3. **Import**: `Import()` calls `ParseDatomEDN()` (`export.go:355-363`), which parses the Op field from EDN. `:op/add` → `OpCRDTAdd`. The Op field is the 5th element, optional — defaults to `OpNone` only when absent.

4. **Storage**: `Import()` calls `d.store.Assert()` (`BadgerStore.Assert`), which writes raw datoms with whatever Op was parsed. No transformation.

## The reproduction test is artificial

The bug report's test manually calls `Store().Assert()` with `Op: datalog.OpNone` on a CardinalityMany attribute. This does not occur in the actual pipeline — `Transaction.Add()` with schema always writes `OpCRDTAdd`.

## If there is still a real Remove failure downstream

The issue is not OpNone from import. Investigate:

- **Lamport ordering**: EDN import preserves original Lamport values. If the importing database's clock hasn't advanced past the imported Lamports, a subsequent `Remove()` could get a *lower* Lamport than the imported `Add()`. In add-wins semantics, the add would win. Check that the clock is advanced after import.

- **Cache invalidation**: Does `Remove()` → `Commit()` invalidate the EA cache entry for the affected (E, A) pair?

- **Stale matcher/snapshot**: Is the query after `Remove()` going through a matcher created before the commit?
