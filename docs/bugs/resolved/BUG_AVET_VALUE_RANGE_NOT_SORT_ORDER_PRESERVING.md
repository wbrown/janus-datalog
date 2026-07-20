# BUG: Value Encoding in Index Keys Is Not Sort-Order-Preserving (Latent — Blocks Correct Value-Range Scans)

**Date**: 2026-05-30 **Severity**: Latent / Correctness-Trap (Low today, High if value-range pushdown is added) **Status**: RESOLVED (2026-05-30) — value bytes in index keys are now order-preserving for `int64`/`float64`/`time.Time` (bijective transforms in `ValueBytes`/`ValueFromBytes`, no migration), and the dead `scanTimeRanges` is removed. See [Resolution](#resolution). **Affected**: `datalog/value_encoding.go` (`ValueBytes`), AVET/VAET key layout, `datalog/storage/matcher.go` (`chooseIndex`, dead `scanTimeRanges`). No incorrect results today, because the live query path never does value-range scans — this documents a trap that a future optimization will fall into.

## Summary

Numeric and temporal values are encoded into index keys in a form that is **not lexicographically sort-order-preserving for negative values**:

- `int64` → big-endian `uint64` (two's-complement bits). Negative integers have the high bit set, so they sort **after** all non-negative integers.
- `float64` → big-endian IEEE-754 bits. Negative floats have the sign bit set, so they sort after positives, and within the negatives the magnitude order is **reversed** (more-negative sorts as larger).
- `time.Time` → big-endian `UnixNano` as `int64`. Pre-1970 instants are negative and inherit the `int64` problem.

Today this is harmless: the live matcher (`chooseIndex`) only ever issues **exact-prefix** scans for bound values; range predicates (`<`, `>`, `<=`, `>=`) are evaluated as **in-memory filters** after the scan, never as a narrowed key range. So nothing currently relies on values being sorted correctly in the key.

The risk is that this is exactly the kind of "obvious" optimization someone will add later — push a value-range predicate into an AVET key range to avoid scanning the whole attribute. The moment that happens, any query whose range spans (or lands in) negative integers, negative floats, or pre-1970 times will **silently return wrong results**: the scan range `[low, high)` computed from the encoded bytes will not contain the rows it should.

A dead, never-called function already encodes this mistaken assumption (see below), so the trap is one step from being live.

## Root Cause

`ValueBytes` (`datalog/value_encoding.go`) encodes scalars for storage:

```go
case int64:
    binary.BigEndian.PutUint64(buf, uint64(val))     // negatives sort AFTER positives
case float64:
    binary.BigEndian.PutUint64(buf, math.Float64bits(val)) // sign bit + reversed negatives
case time.Time:
    binary.BigEndian.PutUint64(buf, uint64(val.UnixNano())) // pre-1970 negative -> int64 problem
```

In the AVET key layout `[prefix][A][type][value][E][Tx↓][...]`, the `value` bytes participate in key ordering. For ordering to support range scans, equal types must compare by value the same way their bytes compare lexicographically. That holds for strings and for non-negative ints/floats/times, but breaks for negatives:

- `int64(-1)` encodes to `0xFFFFFFFFFFFFFFFF`, which sorts **after** `int64(5)` (`0x0000000000000005`). A range `[-10, 10)` does not form a contiguous byte range.
- `float64(-1.0)` sorts after `float64(1.0)`, and `-2.0` sorts after `-1.0` (reversed), so even an all-negative range is misordered.

By contrast, the fixed-width key components that *are* meant to support range scans — `E`, `Tx`, and `RefValue`s — are L85-encoded specifically because L85 is sort-order-preserving (see `ARCHITECTURE.md` / `codec/l85.go`). Scalar values are the exception: they are stored as raw type-tagged bytes, which is fine for equality prefix matching but not for ordering.

### A dead function already assumes value sort order

`BadgerMatcher.scanTimeRanges` (`datalog/storage/matcher.go:451`) builds AVET scan bounds directly from encoded time values:

```go
startValue := datalog.ValueBytes(timeRange.Start)
endValue   := datalog.ValueBytes(timeRange.End)
start := encoder.EncodePrefix(AVET, aStorage[:], startValue)
end   := encoder.EncodePrefix(AVET, aStorage[:], endValue)
```

This is **never called** (no call sites; `WithTimeRanges` is plumbed but unused), so it causes no bug today. But it (a) assumes encoded time values are sort-ordered — true only for post-1970 instants — and (b) omits the value's type-tag byte that real keys carry (`[type][value]`), so the bounds wouldn't even align with stored keys. It is a ready-made example of the trap and should either be removed or fixed alongside any real range-scan work.

## Expected Behavior

Either:
- value bytes used in index keys are sort-order-preserving for all supported types (so range scans are correct by construction), or
- value-range scanning over the index is explicitly unsupported and guarded, so range predicates stay in-memory filters and no future change can quietly start trusting key order for values.

## Actual Behavior

- No incorrect results today (range predicates are in-memory filters; only exact-value prefix scans hit the index).
- The encoding silently mis-sorts negative ints/floats and pre-1970 times in key order, so any future value-range scan over those domains would return wrong results, and the existing (dead) `scanTimeRanges` already bakes in the bad assumption.

## Why This Is Subtle

- It is invisible until someone "optimizes" value-range predicates into index scans — a natural, well-intentioned change.
- It works perfectly for the common cases used in tests/examples (non-negative ints, positive prices, post-1970 timestamps), so a naive range-pushdown change would pass its own tests and ship.
- The rest of the key layout *is* sort-order-preserving (L85 for E/Tx/refs), so a developer reasonably assumes "keys sort correctly" applies to values too.
- The dead `scanTimeRanges` makes it look like value-range scanning is already a supported, working pattern to copy.

## Reproduction Sketch

No live reproduction (the path doesn't exist yet). The encoding defect is directly observable:

```go
import "bytes"

neg := datalog.ValueBytes(int64(-1)) // 0xFFFFFFFFFFFFFFFF
pos := datalog.ValueBytes(int64(5))  // 0x0000000000000005
// bytes.Compare(neg, pos) > 0  -> -1 sorts AFTER 5, so a key range
// [encode(-10), encode(10)) does NOT contain the rows for -1..9.
```

A future regression test for any added range-pushdown must include negative ints, negative floats, and pre-1970 times.

## Fix Direction

Two viable policies:

1. **Make value bytes order-preserving (enables correct range scans).** Use an order-preserving numeric encoding for the key `value` field:
   - `int64`: flip the sign bit (`uint64(v) ^ (1<<63)`) so negatives sort before
     positives in unsigned byte order.
   - `float64`: the standard total-order transform — flip the sign bit for
     positives, flip all bits for negatives — then big-endian.
   - `time.Time`: encode via the same `int64` transform on `UnixNano`. This must be a **separate key-encoding concern** from `ValueBytes` (which is also used for the value-log/`StorageDatom.Bytes` round-trip and must stay decodable); changing the key form is a storage-format change and needs a migration/version bump. Do not silently repurpose `ValueBytes`.

2. **Keep range predicates as in-memory filters and guard against pushdown.** Document at `ValueBytes` and at `chooseIndex` that scalar value bytes are **equality-only** in keys and are not sort-order-preserving, and delete or clearly quarantine `scanTimeRanges` so it isn't copied. Cheapest, preserves current correctness, forgoes the range-scan optimization.

This is an architectural decision (storage-format change vs. permanent in-memory range filtering) for the owner.

## Verification Plan

- `TestValueBytes_NotOrderPreserving_Documented` (or, if option 1 is taken, `TestValueKeyEncoding_OrderPreserving`) — assert byte order matches numeric order across negatives/positives/zero for `int64`, `float64`, and pre/post-1970 `time.Time`.
- If range pushdown is ever added: end-to-end tests with negative-spanning ranges on `int64`, `float64`, and pre-1970 instants, asserting results equal the in-memory-filter baseline.
- Decide and record the fate of `scanTimeRanges` (remove, or fix type-tag + ordering) so the dead path can't mislead.

## Resolution

Chosen: **make value bytes order-preserving** (the report's option 1), with **no migration**. The invariant has to hold — the value lives in the AVET/VAET key, which shares the single byte-sorted keyspace, so any future value-range scan / index min-max / ordered iteration is only correct if `bytes.Compare(enc(a), enc(b)) == cmp(a, b)`. Three scalar types violated it; the rest (string, []byte, bool, L85 refs, keyword/symbol, ElementID/uint64) already sort.

No migration is needed because the transforms are **bijections of the same 8 bytes** — `ValueFromBytes` inverts exactly what `ValueBytes` produced, so there is no old format to keep reading and no version negotiation. (`int64`: flip the sign bit; `float64`: the standard IEEE total-order transform — negatives flip all bits, non-negatives flip the sign bit; `time.Time`: `UnixNano` through the int64 transform.)

There is a single encode site and a single decode funnel, so one pair of changes covers everything:

- Encode: every value-in-key goes through `datalog.ValueBytes` (via `BinaryKeyEncoder.EncodeValueBytes`; `BinaryStrategy` is the only encoder the store uses), and the same bytes back the value portion.
- Decode: every reconstruction funnels through `datalog.ValueFromBytes` (datom_decoder, rga_element, types, set_entry). Nothing decodes value bytes with a separate int/float reader.

The entire never-wired time-range AVET-scan feature is deleted, not just the trap. `scanTimeRanges` was the only consumer of the `TimeRange` values that `extractTimeRanges` produced, and nothing ever called `WithTimeRanges` to push them into a matcher — the chain was dead end to end. Removed: `scanTimeRanges`, the `timeRanges` field, and `BadgerMatcher.WithTimeRanges` (storage); the `executor/time_range.go` file (`TimeRange`, `extractTimeRanges`), the `TimeRangeAware` interface, and `AnnotatedMatcher.WithTimeRanges` (executor); and the two test files dedicated to it (`time_range_optimization_test.go`, `time_range_bench_test.go`) — they only exercised the dead function, so they were not independent coverage. The live in-memory `TimeRangeConstraint` filter and the integration tests that run time-range *queries* are unrelated and untouched.

Tests added:

- `datalog`: `TestValueBytes_Int64_OrderPreserving`, `TestValueBytes_Float64_OrderPreserving`, `TestValueBytes_Time_OrderPreserving` — assert `bytes.Compare(enc(a), enc(b))` matches `CompareValues` across `MinInt64`…`MaxInt64`, `±MaxFloat64`, and pre/post-1970 instants, plus exact round-trip.
- `datalog/storage`: `TestNegativeAndPre1970ValuesRoundTripThroughStorage` — writes a negative int, negative float, and a 1955 time, reads them back through a query (real index keys + value portion) equal.

Full suite green (`go test -count=1 ./...`).
