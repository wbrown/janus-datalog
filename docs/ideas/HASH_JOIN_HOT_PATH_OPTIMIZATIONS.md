# Hash-Join Hot-Path Optimizations

**Status**: Proposed
**Date**: 2026-05-27
**Priority**: Medium (Performance)

## Context

Hash-join inner loops dominate CPU profiles for join-heavy queries. This
document catalogs concrete inefficiencies discovered by reading
`datalog/executor/join.go`, `datalog/executor/tuple_key.go`, and
`datalog/compare.go`. Each finding cites the file:line and proposes a
specific fix.

No benchmarks have been run for this analysis yet — these are findings
from static reading. Profile-and-measure before/after for each change.

## Findings

### 1. `combineTuples` allocates a map on every matched row

**Location**: `datalog/executor/join.go:642-673`

`combineTuples` is called for every (probe × build) match. Each call
builds a fresh `map[query.Symbol]bool` and walks `rightSyms` twice:

```go
func combineTuples(left, right Tuple, joinSyms, leftSyms, rightSyms []query.Symbol) Tuple {
    joinSet := make(map[query.Symbol]bool, len(joinSyms))  // alloc every call
    for _, sym := range joinSyms {
        joinSet[sym] = true
    }
    rightNonJoinCount := 0
    for _, sym := range rightSyms {                         // O(rightCols)
        if !joinSet[sym] {
            rightNonJoinCount++
        }
    }
    result := make(Tuple, len(left)+rightNonJoinCount)
    copy(result, left)
    offset := len(left)
    for i, sym := range rightSyms {                         // O(rightCols)
        if !joinSet[sym] {
            result[offset] = right[i]
            offset++
        }
    }
    return result
}
```

`joinSyms`, `leftSyms`, `rightSyms` are invariant for the lifetime of a
join. Computing the projection plan per-row is pure overhead.

**Proposed fix**: Precompute once in `HashJoinWithOptions`:

- `rightNonJoinIndices []int` — positions in the right tuple that aren't
  join columns.
- `resultWidth int` — `len(leftSyms) + len(rightNonJoinIndices)`.

Store both on the iterator. The combine becomes a pure indexed gather:

```go
result := make(Tuple, resultWidth)
copy(result, left)
for outIdx, rightIdx := range rightNonJoinIndices {
    result[len(left)+outIdx] = right[rightIdx]
}
```

Zero map allocations, no symbol scans in the inner loop. For a join
producing N result rows, this removes N map allocations.

### 2. `ValuesEqual` does unconditional `reflect.ValueOf` per equality check

**Location**: `datalog/compare.go:259-274`

```go
ra := reflect.ValueOf(a)    // UNCONDITIONAL reflection
rb := reflect.ValueOf(b)
if ra.Kind() == reflect.Slice || rb.Kind() == reflect.Slice {
    ...
}
// Quick pointer equality check for interned values
if a == b {
    return true
}
```

The reflect-based slice-equality path was added to handle typed slices
(`[]string`, `[]int64`). It happens *before* the cheap interface
pointer-equality check. For interned types (`Identity`, `Keyword`,
`Symbol`), `a == b` matches in one instruction — but the hot path
already paid for two `reflect.ValueOf` calls and two `Kind()` calls.

Join keys are dominantly `Identity` (entity references). Every hash key
collision check pays reflect cost.

**Proposed fix**: Move pointer-equality to the very top. Reflect becomes
the slow path:

```go
func ValuesEqual(a, b interface{}) bool {
    if a == b {                  // pointer-equal interned values, identical
        return true               //   primitives, both nil — all handled here
    }
    if a == nil || b == nil {
        return false
    }
    // []byte / []uint8 fast paths (existing)
    // Interned-type fast paths (existing — Identity/Keyword/Symbol)
    // Numeric / time.Time (existing)
    //
    // reflect.ValueOf only for the slice case, which is rare in hash keys.
    ...
}
```

The slice fall-through can be moved into a separate branch guarded by a
cheap type-assertion attempt against the common slice types, with
reflect only as the final fallback.

### 3. Identity values re-hash 20 bytes when SHA1 is already uniform

**Location**: `datalog/executor/tuple_key.go:73-78` (interned case) and
`98-102` (non-pointer case)

```go
case datalog.Identity:
    bytes := ptr.Hash()
    return hashBytes(bytes[:])
```

`hashBytes` runs an FNV-1a loop over 20 bytes (20 multiplies, 20 xors).
But the 20 bytes are a SHA1 hash — already uniformly distributed. We
need 8 bytes interpreted as `uint64`:

```go
case datalog.Identity:
    if ptr == nil {
        return 0
    }
    bytes := ptr.Hash()
    return binary.BigEndian.Uint64(bytes[:8])
```

One load instead of 20 multiplies. Join keys are dominantly `Identity`,
so this hits the hot path of the hot path.

### 4. Defensive tuple copies that aren't needed

**Location**: `datalog/executor/join.go:67-70` (joined tuple) and `90-93`
(probe tuple).

**Joined-tuple copy** (line 67-70):

```go
// BUG FIX: Make a copy since combineTuples might return a slice that gets reused
joinedCopy := make(Tuple, len(joined))
copy(joinedCopy, joined)
```

The comment is wrong. `combineTuples` always does
`result := make(Tuple, ...)` (line 658) — it never returns a reused
slice. One alloc per result row that produces nothing.

**Proposed fix**: Remove the copy. `combineTuples`'s contract is "returns
a fresh slice"; document and rely on it.

**Probe-tuple copy** (line 90-93):

```go
// BUG FIX: Make a copy of the tuple since the probe iterator might reuse the slice
tupleCopy := make(Tuple, len(it.currentProbeTuple))
copy(tupleCopy, it.currentProbeTuple)
it.currentProbeTuple = tupleCopy
```

This is currently unconditional in streaming mode. It's only needed when
the probe iterator's `Tuple()` returns a workspace that the next
`Next()` will overwrite. The relation's `RequiresCopy()` reports this.

Note the materialized mode (line 470) doesn't copy, because it knows
materialized tuples are stable. The streaming-mode iterator should do
the same check:

```go
// At setup:
probeNeedsCopy := probeRel.RequiresCopy()

// In Next():
it.currentProbeTuple = it.probeIt.Tuple()
if it.probeNeedsCopy {
    tupleCopy := make(Tuple, len(it.currentProbeTuple))
    copy(tupleCopy, it.currentProbeTuple)
    it.currentProbeTuple = tupleCopy
}
```

When probe is a `MaterializedRelation`, this removes one alloc per probe
row.

### 5. `seen` does two map lookups per matched row

**Location**: `datalog/executor/join.go:64-66` (streaming) and `494-498`
(materialized):

```go
dedupKey := NewTupleKeyFull(joined)
if !it.seen.Exists(dedupKey) {
    it.seen.Put(dedupKey, true)
    ...
}
```

`Exists` walks the hash bucket's `[]mapEntry` calling
`tupleValuesEqual` against every entry. Then `Put` walks the **same
bucket again** doing the **same comparisons**, then appends if absent.
Combined with finding #2 (every `tupleValuesEqual` calls `ValuesEqual`
which calls `reflect.ValueOf`), this doubles the dedup cost.

**Proposed fix**: Add `PutIfAbsent(key) (existed bool)` to
`TupleKeyMap`:

```go
func (m *TupleKeyMap) PutIfAbsent(key TupleKey) bool {
    entries := m.m[key.hash]
    for i := range entries {
        if tupleValuesEqual(entries[i].values, key.values) {
            return true
        }
    }
    m.m[key.hash] = append(entries, mapEntry{values: key.values, value: true})
    return false
}
```

Caller:

```go
if !it.seen.PutIfAbsent(dedupKey) {
    // newly inserted, emit
}
```

One bucket walk instead of two.

### 6. Debug and annotation fields incur per-row overhead

**Location**: `datalog/executor/join.go:38-41` and `:318-326`.

Three categories of bookkeeping cost different amounts at runtime.

#### Pure debug bookkeeping (cost: per-row even when off)

```go
// hashJoinIterator
probeCount  int
matchCount  int
resultCount int
```

- `probeCount++` at line 87 (every probe `Next()`)
- `matchCount++` at line 105 (every hash bucket hit)
- `resultCount++` at line 71 (every emitted result)

All three are read only inside `if it.options.EnableDebugLogging`
blocks (lines 80-83, 97-99). With debug off they cost three int
writes per result row into the iterator's struct, dirtying cache
lines the hot path doesn't otherwise need.

The materialized path has the same pattern with local
`probeCount` / `matchCount` (lines 467-468, incremented at 472/479,
read only at 503).

#### Annotation bookkeeping (cost: per-row unless collector is set)

```go
maybeCopy := func(t Tuple) Tuple {
    if needsCopy {
        copyCount++
        return copyTuple(t)
    }
    passthruCount++
    return t
}
```

`copyCount` / `passthruCount` are consumed only by the annotation
emit at lines 369-379:

```go
if opts.Collector != nil && (copyCount > 0 || passthruCount > 0) {
    opts.Collector.Add(annotations.Event{...})
}
```

Two issues:

1. The increments happen on every build tuple regardless of whether
   a collector exists.
2. `maybeCopy` is a closure with three captured locals, so each call
   reaches them through the closure frame.

#### Debug-guarded at write time (cost: branch, well predicted)

These are fine — they cost only a predicted branch:

- `firstBuildKey` / `firstBuildTuple` (lines 333-334) assigned only
  inside `if buildCount == 0 && opts.EnableDebugLogging`.
- The `fmt.Printf` blocks at lines 174-185, 290-292, 350-355,
  404-407, 474-476, 480-482, 503-505.

#### Proposed fix

Gate the per-row increments on the same condition that gates their
read:

```go
type hashJoinIterator struct {
    ...
    // debug is non-nil only when EnableDebugLogging is set.
    debug *hashJoinDebug
}

type hashJoinDebug struct {
    probeCount, matchCount, resultCount int
}
```

In `Next()`:

```go
if it.debug != nil {
    it.debug.probeCount++
}
```

With `EnableDebugLogging` false, `it.debug` stays `nil` and the
counter writes never happen — the branch is well-predicted and the
cache lines stay clean.

For `maybeCopy`, inline the build loop's copy decision instead of
using a closure, and gate the counters on `opts.Collector != nil`:

```go
trackCopy := opts.Collector != nil
for buildIt.Next() {
    tuple := buildIt.Tuple()
    if needsCopy {
        tuple = copyTuple(tuple)
        if trackCopy {
            copyCount++
        }
    } else if trackCopy {
        passthruCount++
    }
    ...
}
```

The closure goes away (one less heap allocation per join), and the
counter increments only happen when something will read them.

Impact is small — int increments are cheap — but the change is
local and removes per-row overhead that benefits nothing in the
common case (debug off, no collector).

## Secondary Observations

These are lower-impact and can be addressed once the above are measured.

### `NewTupleKey` allocates `[]interface{}` for multi-column join keys

**Location**: `datalog/executor/tuple_key.go:30`

```go
values := make([]interface{}, len(indices))
```

The single-column case (line 22-28) avoids this allocation. For 2-3
column joins (common), one alloc per probe and build row.

**Option A**: Pool the `values` slices and recycle them. Lifecycle:
slice lives as long as the key is in the map, so pooling requires
either copy-on-store or a clear release point.

**Option B**: A small-array variant (e.g., inline `[4]interface{}`)
tagged by a length, sized to cover common cases.

Both are structural changes. Defer until needed.

### `TupleKeyMap` stacks collision handling

**Location**: `datalog/executor/tuple_key.go:233-239`

```go
type TupleKeyMap struct {
    m map[uint64][]mapEntry
}
type mapEntry struct {
    values []interface{}
    value  interface{}
}
```

Go's runtime map already handles hash collisions. Layering a
`[]mapEntry` bucket on top is redundant. The `append` on every `Put`
also grows the inner slice by 1 each time (no capacity hint).

**Option**: Pre-allocate inner slices with `cap(2)` or `cap(4)` so the
common case (1-2 entries per bucket) avoids reallocation.

A larger fix is to change the keying strategy entirely — e.g., a single
hash → single value map where collisions are handled by open addressing
in the same flat array. Out of scope for an incremental change.

### `hashString` is per-byte

**Location**: `datalog/executor/tuple_key.go:195-205`

FNV-1a per-byte loop. String keys are rare (most join keys are
`Identity` or `Keyword`); low priority. If profiling shows string-heavy
joins, replace with `xxhash` or a 64-bit-at-a-time FNV variant.

### Iterator stores symbols that become unused after #1

**Location**: `datalog/executor/join.go:25-28`

```go
joinSyms     []query.Symbol
leftSyms     []query.Symbol
rightSyms    []query.Symbol
```

Once `combineTuples` is hoisted (finding #1), these are no longer
needed on the iterator. The iterator instead carries the precomputed
`rightNonJoinIndices` and a few ints. Removes a few cache-cold slice
headers.

## Baseline Measurements (2026-05-27)

Captured before any code changes. Machine: Apple M5, go1.26.3
darwin/arm64, commit `8e336f2`. Artifacts under `docs/perf/`:

- `hash_join_baseline_2026-05-27.txt` — 25 sub-benchmarks at n=10
  (`InputTypes`, `MaterializedVsStreaming`, `SingleIteration`,
  `LargeResult`, `Streaming`)
- `hash_join_buildsize_baseline_2026-05-27.txt` — 48-cell BuildSize
  sweep at n=3 (kept as reference; not a hot-path target)
- `hash_join_baseline_{mat_x_mat,stream_x_mat,large}_*.{prof,top.txt}` —
  three CPU profiles (10s each) + top renderings

### Per-op headline numbers

| Case | sec/op | B/op | allocs/op |
|------|-------:|-----:|----------:|
| InputTypes/mat_x_mat/size_5000 | 1.12 ms | 2.45 MB | 40,060 |
| InputTypes/stream_x_mat/size_5000 | 1.59 ms | 3.28 MB | 55,083 |
| LargeResult/size_50000 | 13.0 ms | 24.4 MB | 400,293 |

The streaming-probe case adds ~15k allocs vs both-materialized at the
same data size (≈3 alloc/row of extra cost) — consistent with the
unconditional `tupleCopy` at `join.go:91-93` (finding #4) plus
streaming wrapping overhead. Allocations scale linearly with result
size: ≈8/row in the 50k case.

### Profile signatures

All three profiles share the same dominant symbols — GC and the
scheduler primitives behind it — confirming that allocation pressure
from the inner loop is the dominant cost:

| Symbol | mat×mat 5K | stream×mat 5K | large 50K |
|--------|-----------:|--------------:|----------:|
| `runtime.madvise` (GC pages) | 28.48% flat | 23.21% flat | 16.94% flat |
| `runtime.pthread_cond_wait` | 14.62% flat | 13.94% flat | 14.40% flat |
| `runtime.kevent` (netpoll) | 21.07% flat | 28.19% flat |  8.35% flat |
| `runtime.usleep` |  6.11% flat |  6.10% flat | – |
| `HashJoinWithOptions` (cum) |  8.85% | – | 28.43% |
| `combineTuples` (cum) | – | – |  6.92% |
| `TupleKeyMap.Put` (cum) | – | – |  5.68% |

`combineTuples` and `TupleKeyMap.Put` are the largest
application-level symbols in the 50k profile, after GC system work.
They are exactly the targets of findings #1 and #5. They don't appear
in the smaller-size profiles because scheduler noise overwhelms the
signal — but the same allocation pressure is what's driving the
scheduler symbols, so the connection still holds.

### Identity-keyed benchmark (added 2026-05-27)

Added `BenchmarkHashJoinIdentityKeys` and
`BenchmarkHashJoinIdentityLargeResult` in
`datalog/executor/hash_join_identity_bench_test.go` to surface the
`datalog.Identity` code paths the int64-keyed benchmarks don't
exercise.

Headline numbers and Identity-vs-int64 deltas:

| Case | int64 | Identity | Δ |
|------|------:|---------:|--:|
| InputTypes/mat_x_mat/size_5000 | 1.12 ms | 1.33 ms | +19% |
| LargeResult/size_50000 | 13.0 ms | 14.95 ms | +15% |

Allocation counts are identical at 400,293/op for the 50K case — the
15-19% wall-time delta is entirely CPU on Identity-specific hash and
equality paths.

Application-level symbols that surface in the Identity 50K profile
but not the int64 50K profile (cum %):

| Symbol | Identity | int64 | Finding |
|--------|---------:|------:|---------|
| `executor.combineTuples` | 7.21% | 6.92% | #1 |
| `executor.NewTupleKey` | 6.35% | not in top 40 | secondary (multi-col alloc) |
| `executor.hashValue` | 5.72% | not in top 40 | #3 |

`hashValue` at 5.72% cum is directly the FNV-1a-over-20-bytes loop
for `Identity` values that finding #3 proposes to replace with one
`binary.BigEndian.Uint64(bytes[:8])` load.

`NewTupleKey` at 6.35% cum is the multi-column `[]interface{}`
allocation called out in Secondary Observations — promoted to a
visible target now that the benchmark exercises it.

`ValuesEqual` / `reflect.ValueOf` (finding #2) does not surface in
top-40 for the `IdentityLargeResult/size_50000` profile. The cause
is workload shape, not absence of cost: `ValuesEqual` is called once
per `TupleKeyMap.Get/Put/Exists` when the target bucket already has
an entry. With unique entity IDs and unique payloads, every bucket
has one entry, so each lookup does one `ValuesEqual` — total ~200k
calls in the 50k benchmark, distributed across many call sites, none
of which clear the noise floor as a top symbol.

Two complementary benchmarks added to stress this:

#### BenchmarkHashJoinIdentityHighFanout — realistic cardinality-many

K distinct entities, each appearing M times on each side with
different payloads. Build accumulates M-tuple chains per key; probe
expands by M; result is K · M² rows. Models person × posts,
account × transactions, etc.

Shapes: `keys100/fanout10` (10k rows), `keys100/fanout50` (250k),
`keys500/fanout20` (200k).

Profile (`keys100/fanout50`, 50.9 ms/op):

| Symbol | cum % |
|--------|------:|
| `HashJoinWithOptions` | 15.36% |
| `TupleKeyMap.Put` | 4.71% |
| `combineTuples` | 4.64% |

The build-side `Put` activity is new — finding #1's combineTuples
is no longer the only big application symbol; bucket-walk during
same-key accumulation now shares the spotlight.

#### BenchmarkHashJoinIdentityDuplicates — engineered dedup pressure

K keys × R repetitions per side with shared payload per key. After
join, 99%+ of result rows are duplicates that `seen` rejects.
Inputs use `NewMaterializedRelationNoDedupeWithOptions` to preserve
duplicate input tuples.

Shapes: `keys10/reps100` (100k raw → 10 unique), `keys50/reps50`
(125k raw → 50 unique).

Profile (`keys10/reps100`, 8.7 ms/op):

| Symbol | cum % | Finding |
|--------|------:|---------|
| `HashJoinWithOptions` | 25.10% | – |
| `combineTuples` | 12.90% | #1 |
| `TupleKeyMap.Exists` | 5.98% | #5 (dedup path) |
| `NewTupleKeyFull` | 5.13% | – (allocates dedup key per match) |
| `hashValues` | 4.82% | – |
| `tupleValuesEqual` | 3.26% | – |
| **`ValuesEqual`** | **2.95%** | **#2** |
| `hashValue` | 2.56% | #3 |

**Finding #2 is now profile-visible at 2.95% cum.** `reflect.ValueOf`
is inlined into `ValuesEqual`; its cost rolls up to the caller and
constitutes the bulk of that 2.95%. After the fix (move
pointer-equality short-circuit to the top of `ValuesEqual`), this
should drop sharply on this workload.

#### Note on engineered hash collisions

True `uint64` hash collisions in `TupleKeyMap` buckets (where
distinct key values share a hash) cannot be engineered — FNV-1a
over SHA1 bytes is collision-resistant enough that synthetic
preimages are computationally infeasible. The cost #2 targets is
the unconditional `reflect.ValueOf` *per `ValuesEqual` call*, which
fires on any non-empty bucket regardless of whether the bucket
holds one entry or many. The two benchmarks above maximize call
count via duplicates and cardinality-many, which is sufficient.

### Predicted impact per finding

Given the profile, expected directions:

| Finding | Expected effect on existing benchmarks |
|---------|----------------------------------------|
| #1 `combineTuples` map | Drop 1 alloc per result row → cut allocs/row from ~8 to ~7, reduce GC pressure visibly |
| #2 `ValuesEqual` reflect | Minimal here (int64 keys); add Identity-keyed bench |
| #3 Identity hash | Minimal here (int64 keys); add Identity-keyed bench |
| #4 Defensive copies | Drop ~1 alloc per probe row + 1 per result row → ~6 alloc/row in streaming probe cases |
| #5 `seen` double-lookup | Halve dedup map walks; mostly CPU win, not allocs |
| #6 Debug fields | Counter writes per row; very small wins; one closure heap alloc per join |

If we hit findings #1, #4, and #5 together, the 50k profile should
move from "~8 allocs/row, GC-dominated" toward "~5 allocs/row, less
GC-dominated" with a wall-time improvement proportional to the GC
share that disappears.

## Ranking by Expected Profile Impact

1. **#1 `combineTuples` map allocation** — one map alloc per result row,
   direct allocation reduction.
2. **#2 `ValuesEqual` reflect cost** — every probe + every dedup,
   multiplied by collision-chain depth.
3. **#3 Identity hash** — every key construction (probe + build).
4. **#4 Defensive copies** — one alloc per probe row + one per result
   row, removable in the common materialized-probe case.
5. **#5 `seen` double-lookup** — halves dedup map work, compounds with
   #2.
6. **#6 Debug/annotation fields** — int writes per row + one closure
   heap allocation per join; small but free of risk.

## Recommended Approach

For each finding, in order:

1. Establish a benchmark baseline using the existing
   `BenchmarkHashJoinMaterializedVsStreaming`,
   `BenchmarkHashJoinInputTypes`, and
   `BenchmarkHashJoinSingleIteration`. Save under `docs/perf/`.
2. Apply the fix in isolation.
3. Re-run with `benchstat`.
4. Verify with `go test -count=1 ./...`.
5. Commit if green and significant; revert if not.

Findings #1, #3, and #4 are local — they touch `join.go` and
`tuple_key.go` only. Findings #2 and #5 touch `compare.go` and the
`TupleKeyMap` API respectively; review broader call sites before
committing.

## Out of Scope

- Symmetric hash join changes
- Storage-side hash-join-matcher work
- Symbol-index optimization (linear search is fine at N ≤ 10 columns)
- Replacement of `TupleKeyMap` with a different data structure

These may become worth doing after the local fixes are in.
