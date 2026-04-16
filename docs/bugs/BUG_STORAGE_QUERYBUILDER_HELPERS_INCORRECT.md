# BUG: `storage.QueryBuilder` Helper Methods Do Not Implement Their Advertised Semantics

**Date**: 2026-04-16
**Severity**: Medium - auxiliary API returns wrong results and accidental full scans
**Status**: Open (direct code-path issue; no dedicated tests currently cover it)
**Affected**: `datalog/storage/queries.go`

## Summary

The exported helper API in `datalog/storage/queries.go` contains methods whose
signatures promise targeted lookups but whose implementations ignore key inputs
or build invalid scan ranges.

The clearest cases:

1. `GetAttributeValue(attr, value)` ignores `value`
2. `GetReferences(entity)` ignores `entity`
3. `GetTimeRange(startTx, endTx)` does not use the storage layer's Tx encoding
   and only encodes 8-byte Lamports, even though TAEV keys use 16-byte
   `ElementID` / Tx ordering

These methods appear to be exported API surface, but there are no obvious tests
covering them and the implementations do not match their names.

## Discovery

Found during external code review of the helper/query convenience layer.

Additional clue: a search across `*_test.go` files found no direct coverage of:

- `NewQueryBuilder(...)`
- `GetAttributeValue(...)`
- `GetTimeRange(...)`
- `GetReferences(...)`

So these methods are both under-tested and visibly incomplete in code.

## Code Evidence

### 1. `GetAttributeValue(attr, value)` ignores `value`

From `datalog/storage/queries.go`:

```go
func (q *QueryBuilder) GetAttributeValue(attr datalog.Keyword, value interface{}) ([]*datalog.Datom, error) {
    aBytes := attr.Bytes()
    // TODO: Need to encode value properly
    start, end := q.encoder.EncodePrefixRange(AVET, aBytes[:])
    return q.scan(AVET, start, end)
}
```

The method name says "attribute=value". The implementation scans only by
attribute prefix and ignores `value` entirely.

### 2. `GetReferences(entity)` ignores `entity`

From `datalog/storage/queries.go`:

```go
func (q *QueryBuilder) GetReferences(entity datalog.Identity) ([]*datalog.Datom, error) {
    // For entity references, we need to look for the entity as a value
    // This would require encoding the entity reference as a value
    // For now, do a full scan on VAET
    // TODO: Implement proper reference value encoding
    return q.scan(VAET, nil, nil)
}
```

This method performs a full VAET scan and returns every reference datom, not
just datoms referencing the supplied entity.

### 3. `GetTimeRange(startTx, endTx)` hand-builds the wrong key shape

From `datalog/storage/queries.go`:

```go
func (q *QueryBuilder) GetTimeRange(startTx, endTx datalog.ElementID) ([]*datalog.Datom, error) {
    // Convert Lamport to bytes for TAEV index prefix
    var startBytes, endBytes [8]byte
    binary.BigEndian.PutUint64(startBytes[:], startTx.Lamport)
    binary.BigEndian.PutUint64(endBytes[:], endTx.Lamport)

    start := q.encoder.EncodePrefix(TAEV, startBytes[:])
    end := q.encoder.EncodePrefix(TAEV, endBytes[:])
    return q.scan(TAEV, start, end)
}
```

Problems:

1. TAEV keys are ordered by the storage `Tx` representation, not raw 8-byte
   Lamport bytes.
2. The storage layer uses 16-byte transaction identifiers (`ElementID` / `Tx`),
   not just the Lamport half.
3. Tx ordering is descending in key space via specialized encoding (`EncodeTxForPrefix`,
   `txToDescending`), but this helper does not use it.

Even if the method returns something, it is not obviously scanning the intended
time range.

## Why This Matters

These methods look like developer-facing convenience API:

- the type is exported
- the names are straightforward
- they suggest direct storage helper semantics

A downstream caller has no reason to suspect that:

- `GetAttributeValue` returns all values for an attribute
- `GetReferences` returns all references in the entire database
- `GetTimeRange` is building a likely-invalid or inverted key range

This is exactly the kind of helper API that can produce "it seems to work on my
small dataset" bugs and hidden full scans in production.

## Reproduction Sketches

### 1. `GetAttributeValue` returns false positives

```go
// Setup:
//   e1 :user/name "Alice"
//   e2 :user/name "Bob"
//
// Expect:
//   GetAttributeValue(:user/name, "Alice") returns only Alice datoms
//
// Suspected actual:
//   returns both Alice and Bob because it scans only by attribute prefix
```

### 2. `GetReferences` returns unrelated rows

```go
// Setup:
//   a :friend/ref x
//   b :friend/ref y
//
// Expect:
//   GetReferences(x) returns only a -> x
//
// Suspected actual:
//   returns both a -> x and b -> y because it full-scans VAET
```

### 3. `GetTimeRange` returns empty, partial, or inverted results

```go
// Setup:
//   datoms at three known ElementIDs / transactions
//
// Expect:
//   GetTimeRange(mid, end) returns only later datoms
//
// Suspected actual:
//   range boundaries are encoded incorrectly for TAEV and results are wrong
```

## Secondary Concern: API Surface Drift

These helpers appear to be a parallel "storage query builder" API distinct from
the main query system in `datalog/qb`.

That by itself is not a bug, but it increases the risk of drift:

- `qb` is heavily tested and clearly in active use
- `storage.QueryBuilder` appears lightly used and under-tested

If this API is intended only for experiments/debugging, it should say so
explicitly.

## Impact

### 1. Wrong results

The helpers can return broader result sets than their names imply.

### 2. Accidental full scans

`GetReferences()` is explicitly a full scan today. Callers may not realize that.

### 3. Hidden correctness bugs in downstream tools

Because these are convenience helpers, downstream callers may trust them more
than they should and build application logic on top of incorrect assumptions.

## Possible Fix Directions

### Option 1: Make the helpers correct

- `GetAttributeValue`: encode the value correctly for AVET and include it in the prefix
- `GetReferences`: encode the entity reference as a value and restrict VAET range
- `GetTimeRange`: use the storage layer's Tx encoding helpers rather than raw
  Lamport bytes

### Option 2: Deprecate/remove the helper API

If these methods are no longer part of the supported direction, deprecate them
in favor of:

- `d.Query(...)`
- `d.Pull(...)`
- `d.Matcher()`
- `datalog/qb`

### Option 3: Mark as debug-only / experimental

If the helpers are intentionally incomplete, document that directly in code and
docs so downstream callers do not assume production semantics.

## Test Plan

1. Add direct tests for `GetAttributeValue`, `GetReferences`, and `GetTimeRange`.
2. Differential-test helper results against equivalent Datalog queries.
3. Add tests for reference values and ElementID ranges specifically.
4. Decide whether this API is supported, deprecated, or experimental.
