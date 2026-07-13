# Bug: HashJoin Treats Ordinary `?t` Variables as Transaction Attributes

## Summary

`executor.HashJoin` has a name-based special case for transaction attributes. If the build-side relation contains a symbol named `?tx`, `?t`, `?txid`, or `?transaction`, and the first tuple at that position is numeric or an `ElementID`, the join enters a "latest transaction wins" dedup path.

That is unsafe. `?t` is a normal user variable name, commonly used for "task", "ticker", "time", "type", or arbitrary tuple data. A natural join should preserve all matching rows unless relational set semantics deduplicate identical output tuples. This path can silently collapse distinct build-side rows that share the join key.

## Trigger

Any hash join where:

1. The build-side relation has an attribute named `?t`, `?tx`, `?txid`, or `?transaction`.
2. The value at that tuple position is `uint64`, `int64`, `int`, `datalog.ElementID`, or `*datalog.ElementID`.
3. Multiple build tuples share the join key but differ in other attributes.

Example shape:

```clojure
[:find ?e ?t ?label
 :where [?e :entity/group ?g]
        ;; ?t is ordinary data, not a transaction ID
        [(ground [[1 "a"] [2 "b"]]) [[?t ?label]]]
        [?other :entity/group ?g]]
```

If the relation containing `?t` is chosen as the hash-join build side and joins on `?g` or another key, rows can be collapsed by the largest `?t`.

## Code Evidence

`HashJoinWithOptions` infers transaction semantics from symbol names:

```go
txIndex := -1
for i, sym := range buildRel.Symbols() {
	if sym == datalog.NewSymbol("?tx") || sym == datalog.NewSymbol("?t") ||
		sym == datalog.NewSymbol("?txid") || sym == datalog.NewSymbol("?transaction") {
		txIndex = i
		break
	}
}
```

It then verifies only the Go value type, not the query semantics:

```go
switch firstTuple[txIndex].(type) {
case uint64, int64, int, datalog.ElementID, *datalog.ElementID:
	hasTxAttribute = true
}
```

Once enabled, the path keeps only the tuple with the largest transaction-like value for each join key:

```go
key := NewTupleKey(tuple, buildIndices)
if existingTxVal, exists := latestTx.Get(key); !exists || txID > existingTxVal.(uint64) {
	latestTuples.Put(key, maybeCopy(tuple))
	latestTx.Put(key, txID)
}
```

This is not a property of natural joins. It is a storage/temporal resolution rule leaking into a generic join operator.

## Impact

- Silent wrong query results.
- Data loss depends on join order and build-side selection, so the same logical query may pass or fail depending on planner choices and relation sizes.
- The variable name `?t` is especially dangerous because it is short and idiomatic.
- Users cannot know that a harmless variable name changes join semantics.

## Expected Behavior

`HashJoin` should perform a pure relational join. It should not infer CRDT or temporal resolution from relation-attribute names.

If transaction deduplication is required anywhere, it should be represented explicitly in the relation metadata, storage matcher output, or a dedicated operator with a clear semantic contract.

## Suggested Fix

Remove the name-based transaction dedup path from generic `HashJoin`.

If a caller truly needs latest-by-transaction behavior:

1. Add explicit metadata to the relation or query plan indicating the transaction attribute and dedup key.
2. Apply the dedup as a separate operator before or after the join.
3. Require the relation attribute to be proven to occupy a transaction position in a four-element data pattern, not guessed from the symbol name.

## Tests Needed

- A unit test where a relation with symbol `?t` and integer values joins on another symbol and preserves multiple distinct rows.
- The same test with both relations swapped in size so the `?t` relation is selected as the build side.
- Tests for `?tx`, `?txid`, and `?transaction` as ordinary user variables.
- A regression test proving storage history/as-of queries still get correct temporal behavior after removing this path.

---

## Resolution (2026-05-25)

**Resolved.** The name-based transaction-dedup path was removed from `HashJoin`
outright, with no replacement. `HashJoinWithOptions` now runs a single pure
relational build loop that preserves every row.

- `datalog/executor/join.go` — deleted the `?tx`/`?t`/`?txid`/`?transaction`
  symbol-name detection, the first-tuple type sniff, and the ~150-line
  "latest-transaction-wins" branch (≈190 lines out, ≈28 in). Dropped the
  now-unused `datalog` import.
- `datalog/executor/join_tx_name_dedup_test.go` —
  `TestHashJoin_TxNameVariablesDoNotDropRows` exercises all four names as
  ordinary build-side integer attributes. It dropped a row before the fix and
  preserves both rows after.

### Corrections to this report's assumptions

This report's Suggested Fix and Tests Needed assumed latest-by-transaction
behavior is "required somewhere" and should be preserved via explicit metadata,
a dedicated operator, or a proven four-element tx position. That assumption is
wrong on three counts:

1. **No caller needs join-level transaction dedup.** With the path simply
   deleted, the entire suite (`go test -count=1 ./...`) is green — including
   every history/as-of/CRDT test in `storage`, `db`, `query`, and `parser`.
   There are zero consumers whose behavior must be preserved. The proposed
   metadata/operator alternatives would build machinery for nobody; they are a
   tidier restatement of the same category error, so none were implemented. The
   report's last "Tests Needed" item is satisfied by the existing temporal
   tests, which already stay green without the path.

2. **CRDT/temporal resolution already happens in the storage layer, not the
   join.** The EATV index stores Tx in descending order, so the matcher /
   `CRDTResolvingIterator` resolve "latest wins" (and add-wins / RGA) before any
   datom reaches the executor. By the time relations are joined, resolution is
   already complete. This path was therefore not a misplaced-but-needed feature
   — it was pure redundancy for storage data and actively wrong for non-storage
   relations (ground values, computed `?t`, in-memory sources).

3. **Even a "principled" version of the heuristic is wrong.** Suggested Fix #3
   (prove the relation attribute is a tx position from a four-element pattern, then dedup)
   still embeds resolution semantics inside a generic relational operator. A
   join must not resolve CRDTs under any circumstances — proven position or not.
   The correct contract is simply: the join preserves rows; storage owns
   resolution.

### Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` all green; the new
regression test fails on the pre-fix code and passes after.
