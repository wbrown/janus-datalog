# Plan: ElementID Throughout — Commit(), AsOf(), and Internal txID

## Context

`Commit()` returns `uint64` (just Lamport). `BadgerMatcher.txID` is `uint64`. `AsOf()` and `MatchAsOf()` take `uint64`. All as-of filtering compares `datom.Tx.Lamport > txID` — only half the ElementID. Two replicas can share a Lamport value but have different ReplicaIDs. Filtering on Lamport alone is incorrect in a multi-replica scenario.

Now that ElementID is first-class, the entire as-of path should use full ElementID comparison.

## Changes

### 1. `datalog/storage/database.go` — Commit() returns ElementID

- `func (t *Transaction) Commit() (uint64, error)` → `(datalog.ElementID, error)`
- Error returns: `0, err` → `datalog.ElementID{}, err`
- Final return: `metadataElemID.Lamport, nil` → `metadataElemID, nil`

**Already done in working tree.**

### 2. `datalog/storage/database.go` — Database.AsOf() takes ElementID

- `func (d *Database) AsOf(txID uint64)` → `AsOf(txID datalog.ElementID)`
- Line 429: `matcher.AsOf(txID)` — passes through

### 3. `datalog/storage/matcher.go` — BadgerMatcher.txID becomes ElementID

- **Struct field** line 18: `txID uint64` → `txID datalog.ElementID`
- **AsOf()** line 50: signature `uint64` → `ElementID`, field assignment direct
- **MatchAsOf()** line 326: signature `uint64` → `ElementID`
- **All `m.txID > 0` checks** → `m.txID != (datalog.ElementID{})` (zero-value check)
- **All `datom.Tx.Lamport > m.txID`** → `datom.Tx.Lamport > m.txID.Lamport` (Lamport ordering is still correct for as-of — we want "datoms created before this point in causal time")

Wait — actually the comparison semantics matter here. For as-of filtering, we want "include datoms whose Tx is causally before or equal to the target." Lamport ordering IS the right comparison for causal ordering within a single replica. Across replicas, same-Lamport datoms from different replicas should ALL be included if `Lamport <= target.Lamport`. The ReplicaID doesn't affect causal ordering — it's a tiebreaker for CRDT resolution, not for as-of filtering.

So the filter `datom.Tx.Lamport > m.txID.Lamport` is actually correct. The field type changes to `ElementID` so the full ID flows through the API, but the filter comparison still uses `.Lamport` because that's the causal clock.

### 4. `datalog/storage/crdt_resolving_iterator.go` — txID becomes ElementID

- **Field** line 18: `txID uint64` → `txID datalog.ElementID`
- **Constructor**: `NewCRDTResolvingIterator(source, schema, txID uint64)` → `ElementID`
- **Filter** line 112: `it.txID > 0 && datom.Tx.Lamport > it.txID` → `it.txID != (datalog.ElementID{}) && datom.Tx.Lamport > it.txID.Lamport`

### 5. `datalog/storage/iterator_helpers.go` — validateDatomWithConstraints

- Line 37: `txID uint64` → `txID datalog.ElementID`
- Line 41: `txID > 0 && datom.Tx.Lamport > txID` → `txID != (datalog.ElementID{}) && datom.Tx.Lamport > txID.Lamport`

### 6. All `m.txID > 0` and `datom.Tx.Lamport > m.txID` sites

These are mechanical — change the zero check and extract `.Lamport` for the comparison. Sites from the grep (all in `datalog/storage/`):

| File | Lines | Pattern |
|------|-------|---------|
| `matcher.go` | 247, 310, 368, 431, 817, 871, 915, 958, 1014 | `m.txID > 0 && datom.Tx.Lamport > m.txID` |
| `matcher_relations.go` | 107, 270 | `m.txID == 0` (cache eligibility) |
| `hash_join_matcher.go` | 757, 845 | `it.matcher.txID > 0 && datom.Tx.Lamport > it.matcher.txID` |
| `batch_iterator.go` | 286 | same pattern |
| `simple_batch_scanner.go` | 283 | same pattern |

### 7. Callers of AsOf/MatchAsOf

| File | Line | Fix |
|------|------|-----|
| `cache_integration_test.go` | 238 | `db.AsOf(tx1ID)` — tx1ID is now ElementID from Commit(), just works |
| `matcher_concurrency_test.go` | 68 | `baseMatcher.AsOf(100)` → `baseMatcher.AsOf(datalog.ElementID{Lamport: 100})` |
| `crdt_one_test.go` | 173, 181, 197 | `var lamports []uint64` → `var txIDs []datalog.ElementID`; `MatchAsOf(pattern, txIDs[i])` |
| `crdt_one_test.go` | 215 | `MatchAsOf(pattern, 0)` → `MatchAsOf(pattern, datalog.ElementID{})` |

### 8. Commit() return value callers

| File | Line | Fix |
|------|------|-----|
| `cmd/datalog/main.go` | 168, 349 | `%d` → `%v` |
| `datalog/storage/export_test.go` | 793, 796 | `datom.Tx.Lamport == txID1` → `datom.Tx == txID1` |
| `datalog/storage/crdt_cache_matrix_test.go` | 405 | No change (`%v`) |
| `examples/*.go` | various | `%d` → `%v` |

### 9. Bench test helper

| File | Line | Fix |
|------|------|-----|
| `iterator_refactoring_bench_test.go` | 15 | `txID uint64` → `txID datalog.ElementID` |
| `iterator_refactoring_bench_test.go` | 19 | Same pattern as iterator_helpers |
| `iterator_refactoring_bench_test.go` | 101 | `scenario.txID` — struct field needs ElementID |

## Verification

```bash
go build ./...
go test ./datalog/storage/ -run "TestCardinalityOneAsOf|TestExport|TestCRDTCacheMatrix|TestConcurrent" -v -count=1
go test ./...
go test ./datalog/storage/ -count=2
```

---

## Implementation Record

**Status: COMPLETE** — All items implemented and verified (`go test ./... -count=2` passes).

### Deviation from plan: full ElementID comparison, not Lamport extraction

The plan above recommended `datom.Tx.Lamport > m.txID.Lamport` for as-of filtering — comparing only the Lamport component. This was wrong. Comparing only Lamport is comparing half the ID. Two replicas can share a Lamport value but have different ReplicaIDs.

**What was actually implemented:** All as-of filter sites use `m.txID.Less(datom.Tx)` — the full `ElementID.Less()` comparison which compares Lamport first, then ReplicaID as tiebreaker. This is correct: if two datoms share a Lamport value but come from different replicas, `Less()` distinguishes them. The same pattern was applied everywhere:

- `matcher.go`: `m.txID != (datalog.ElementID{}) && m.txID.Less(datom.Tx)`
- `crdt_resolving_iterator.go`: `it.txID != (datalog.ElementID{}) && it.txID.Less(datom.Tx)`
- `iterator_helpers.go`: `txID != (datalog.ElementID{}) && txID.Less(datom.Tx)`
- `batch_iterator.go`, `hash_join_matcher.go`, `simple_batch_scanner.go`: same pattern via `it.matcher.txID` / `s.matcher.txID`
- `MatchAsOf`: `targetTx.Less(datom.Tx)` for the target parameter

### Additional changes not in original plan

- **`datalog/storage/queries.go`**: `GetTimeRange` and `GetEntityTimeRange` changed from `uint64` to `datalog.ElementID`. `GetEntityTimeRange` filter changed from Lamport comparison to `d.Tx.Less(endTx)` / `!d.Tx.Less(startTx)`.
- **`datalog/storage/matcher_relations.go` line 868**: `NewCRDTResolvingIterator(rawIter, m.schema, 0)` → `datalog.ElementID{}`.
- **`examples/*.go`**: Left unchanged — these have `//go:build example` tags and need a larger rewrite to use the Transaction API properly instead of fabricating ElementIDs from timestamps. Tracked as separate work.

### Next steps (not yet implemented)

- **`BadgerMatcher.txID` should become `*datalog.ElementID`**: Currently `ElementID{}` (zero value) means "no as-of filter, latest resolved state." We want to distinguish three modes:
  - `nil` → no AsOf called, latest CRDT-resolved state (current default)
  - `&ElementID{}` → history mode, no CRDT resolution (raw datoms)
  - `&ElementID{Lamport: N, ReplicaID: R}` → CRDT-resolved as-of that point
- **`[(as-of ?tx N)]` predicate is parsed but not wired up** to the executor. The Datomic approach is to apply as-of at the database level, not in the query. A `:as-of` parameter at the EDN query root level would be the right design.
- **`HistoryPredicate.TargetLamport` is still `uint64`** — needs to become `ElementID` if the predicate is kept.
- **`TxRangePredicate.Low`/`.High` are still `uint64`** — same issue.
