# BUG: A Single Entity Can Exceed One Badger Transaction, So Export Writes Dumps Import Cannot Read

**Date**: 2026-07-26 **Severity**: High - the JDZL round trip is not closed **Status**: Resolved 2026-07-26 **Affected**: `storage.Database.ImportBinary`, `storage.Database.Import`, `storage.BadgerStore.Assert`

## Summary

`ExportBinary` could write a JDZL file that `ImportBinary` then refused to read. The failure was reported against a large dump and reproduced identically by two independent tools reading the same file, at the same byte offset:

```
binary import assert chunk at 29697768: failed to write to 5 index: Txn is too big to fit into one request
```

Index 5 is AVET, the sixth of the eight index writes each datom performs. Nothing is special about AVET — it is simply where the transaction happened to fill.

The stripped export of the same database imported fine. The difference was not the file size but which entities it contained: the stripped variant omitted the entity class whose accumulated datoms overran the ceiling.

## The chunk floor is one entity

`ExportBinary` closes a chunk on a soft budget — 256 KiB by default — but only *arms* at the budget and closes at the next entity boundary, so an entity's datoms stay inside one LZJ compression window:

```go
// Soft budget arms closeSoon; the chunk actually closes on the next
// entity boundary so an entity's datoms stay in one LZJ window.
if haveOpenE && eBytes != openE && closeSoon {
    if err := flush(); err != nil {
        return err
    }
}
```

That is deliberate and worth keeping, but it means **an entity is the chunk floor**, and an entity has no bound: a cardinality-many or cardinality-vector attribute accumulates a datom per write, for the life of the entity. There was no cap above that floor, and `binaryUint32Len` only rejects chunks past 4 GiB.

`ImportBinary` then asserted each decoded chunk in exactly one call:

```go
if err := d.store.Assert(datoms); err != nil {
    reportErr(fmt.Errorf("binary import assert chunk at %d: %w", entry.offset, err))
}
```

and `BadgerStore.Assert` was one `db.Update` over the whole slice.

## The ceiling, and how low it is

`NewBadgerStore` sets `MemTableSize = 128 << 20`. Badger derives both transaction ceilings from it:

```go
opt.maxBatchSize = (15 * opt.MemTableSize) / 100
opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)
```

With a 128 MiB memtable and `skl.MaxNodeSize` of 96 bytes that is **19.2 MiB or 209,715 entries**, whichever `Txn.checkSize` reaches first. Every datom writes eight index keys, plus one blob entry when its value is stored out of line, so the count ceiling arrives at roughly **26,200 datoms in a single `Assert` call** — and much sooner when values are large enough to matter but still under the 512-byte compression threshold, since every index orders V and therefore carries the value inline in all eight keys.

Tens of thousands of datoms on one entity is ordinary for an append-heavy attribute. The ceiling was reachable by data, not by abuse.

## Why raising `MemTableSize` is not the fix

There is no knob for the transaction limit itself: `maxBatchSize` and `maxBatchCount` are unexported and derived only from `MemTableSize`, so batch headroom cannot be bought without buying memtables. Memtables are not a cap but a resident allocation — `newArena` is `make([]byte, n)` sized `MemTableSize + maxBatchSize + maxBatchCount*MaxNodeSize`, about 1.7× `MemTableSize` per memtable, with `NumMemtables` of them plus the active one under write pressure.

Because the ceiling is 15% of `MemTableSize`, buying *X* bytes of headroom costs about `6.7X` of `MemTableSize` and `11X` of resident arena, on every database open, for every consumer including memory-constrained wasm targets. And it would not close the hole: entity size is unbounded, so any chosen ceiling is a deadline rather than a fix.

## Why this is an import-only defect

`Store.Assert` has three production call sites and all three are import — `Database.Import` twice (EDN, batched at 1000 datoms) and `Database.ImportBinary` once (JDZL, unbatched). Ordinary writes do not go through it.

That is a consequence of [BUG_TRANSACTION_COMMIT_SPLIT_BADGER_TX](BUG_TRANSACTION_COMMIT_SPLIT_BADGER_TX.md). Before 2026-04-16 `Transaction.Commit` called `store.Assert`; that fix moved the whole logical commit onto a single `BeginTx`/`StoreTx` so retractions, assertions, and `:db/txInstant` commit or roll back together. Routing the commit off `Store.Assert` is precisely what makes it safe to split `Store.Assert` today — under the pre-2026-04-16 shape, splitting it would have broken commit atomicity.

## Lesson

**An entity is the unit of several floors in this system, and an entity is unbounded.**

The export chunk is floored at one entity by the LZJ window rule. The import transaction was floored at one chunk, and therefore at one entity too. Neither floor was wrong on its own; the defect is that an unbounded quantity sat underneath a fixed ceiling with nothing in between.

That shape generalizes beyond this bug. Anywhere the code groups by entity — a buffer, a transaction, a window, a batch — it has adopted an input whose size is set by how many times an attribute was written over the entity's life, which no caller declares and no schema constrains. Small-entity test data hides it completely: this fixture needed 8,000 datoms on one entity before anything went wrong, and every existing round-trip test used a handful.

The corollary for fixes: when a fixed ceiling meets an unbounded input, raising the ceiling is never the repair. It converts a reproducible failure into a latent one that returns with the next larger dataset.

## Resolution

**Resolved**: 2026-07-26

`BadgerStore.Assert` writes through a Badger `WriteBatch` rather than a single `db.Update`:

```go
// Before — one transaction, however many datoms the caller passed.
func (s *BadgerStore) Assert(datoms []datalog.Datom) error {
    return s.db.Update(func(txn *badger.Txn) error {
        for _, d := range datoms {
            if err := s.assertDatom(txn, &d); err != nil {
                return err
            }
        }
        return nil
    })
}

// After — the batch splits at Badger's own ceiling.
func (s *BadgerStore) Assert(datoms []datalog.Datom) error {
    wb := s.db.NewWriteBatch()
    defer wb.Cancel()
    for _, d := range datoms {
        if err := s.assertDatom(wb.Set, &d); err != nil {
            return err
        }
    }
    return wb.Flush()
}
``` `WriteBatch.handleEntry` commits and retries on Badger's own `ErrTxnTooBig`, so the ceiling arithmetic stays entirely on Badger's side and cannot drift from it. Computing the cost ourselves against the exported `MaxBatchCount()`/`MaxBatchSize()` was rejected for that reason: it would duplicate `Txn.checkSize`, including its per-entry overhead and value-threshold logic, in code that Badger has no obligation to keep in step with.

`WriteBatch` is applicable because the assert path is write-only — every index write is `txn.Set(key, nil)` and `putBlob` is a bare `Set`, with no read inside the transaction.

To let both destinations share one implementation, `assertDatom` takes a `set func(key, value []byte) error` instead of a `*badger.Txn`; `txn.Set` and `wb.Set` already have that signature, so the key-building logic did not move. `putBlob` takes the same parameter.

Both importers inherit the fix and neither changed. `Import`'s `batchSize = 1000` is kept: it bounds the importer's own slice allocation, which is a separate concern from the transaction ceiling — and it was never a sufficient bound on its own, since 1000 datoms carrying large values can cross the size ceiling while sitting far under the count ceiling.

### The cost, and where it stops

`Store.Assert` is no longer atomic: a mid-way failure leaves already-committed datoms in place. That costs nothing here — its only callers are the importers, and `ImportBinary` already documents that import is not transactional across chunks and that retrying into the same database is not safe recovery.

`BadgerTx.Assert` is deliberately **not** split. Its caller owns the transaction boundary, so an oversized commit still surfaces `ErrTxnTooBig` for that caller to handle, and the atomicity established by BUG_TRANSACTION_COMMIT_SPLIT_BADGER_TX is untouched.

### Regression test

`datalog/storage/import_transaction_ceiling_test.go::TestBinaryImportEntityLargerThanOneTransaction` builds one entity of 8,000 datoms with sub-threshold values, exports it, imports into a fresh database, and compares a re-export byte for byte.

The test asserts its own premise first: the same datoms are driven through `StoreTx.Assert` — the path that does not split — and required to return `badger.ErrTxnTooBig`. That is the shape `BadgerStore.Assert` had before this fix, so the test demonstrates the old path failing inside the same run that shows the new one succeeding, and it stays honest about itself: if `MemTableSize` is ever raised, the premise fails and says the fixture needs growing rather than the test quietly ceasing to reproduce anything.

Native-tagged, because it asserts a Badger-specific ceiling; `MemoryStore` has no transaction limit and nothing to reproduce.

### What this does not include

`BadgerStore.Retract` still uses one `db.Update`. `retractDatom` scans EAVT inside its transaction to find the stored datoms, so it is not write-only and `WriteBatch` does not apply. Import never retracts, so it is out of the path this bug describes.

`ExportBinary` was not capped. Doing so would restore the round trip only for dumps written afterward — it cannot read a file that already exists, which was the problem in hand — and it would split entities across LZJ windows, giving up the compression property the entity-aligned soft close exists to protect.

### Measurements

`BenchmarkBadgerAssertBulk` against the pre-fix implementation, Apple M5, darwin/arm64:

| size | before | after |
|---|---|---|
| 1000 | 8.73 ms/op, 5,133,920 B/op, 114,116 allocs/op | 9.19 ms/op, 5,105,416 B/op, 114,127 allocs/op |
| 4000 | 18.19 ms/op, 20,574,826 B/op, 456,311 allocs/op | 18.08 ms/op, 20,238,459 B/op, 456,320 allocs/op |

`WriteBatch` adds a constant ~10 allocations per `Assert` call rather than per datom, and byte totals came out marginally lower at both sizes. The two timings moved in opposite directions, which is run-to-run noise on a benchmark that constructs a Badger database per iteration.
