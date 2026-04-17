# External Code Review — Bug Report

**Reviewer:** Claude (Opus 4.7), 1M context, reading code directly rather than relying on docs
**Date:** 2026-04-17 (updated after finishing full codebase read)
**Scope:** Full codebase read. All items include file:line; please verify against current HEAD
before acting.

## Summary

Correctness bugs first, then dead/duplicated code, then doc drift, then cleanup nits.

| # | Severity | File | Issue |
|---|----------|------|-------|
| 1 | Correctness | `storage/crdt_resolve.go:20-36` | `ResolveLWWFromDatoms` does not check `Op` — cache-populate path ignores tombstones |
| 2 | Correctness | `storage/matcher.go:844-869` | `LookupAttribute` storage fallback does not check `Op` — cache-miss path returns tombstoned values |
| 3 | Correctness | `storage/badger_store.go:571-584` | `extractElementIDFromKey` reads `key[len-16:]` but Op is now the last byte — garbled ElementIDs for EAVT/AEVT/AVET/VAET |
| 4 | Correctness | `storage/simple_batch_scanner.go:200-273` | Switch uses pre-expansion enum values — when invoked with EATV/AETV/AVET it builds wrong-shaped keys |
| 5 | Correctness | `executor/executor_utils.go:87-99` | `MaterializeResult` panics with "BUG DETECTED" on any relation where N tuples happen to be `==`-equal — false-positive heuristic in production path |
| 6 | Feature gap | `schema/parser.go:170-177` | EDN schema parser rejects `:db.cardinality/vector` |
| 7 | Self-rule violation | `executor/subquery.go:21, 173, 279` | `SubqueryWorkerCount` is a package-level mutable `var` — duplicates `ExecutorOptions.MaxSubqueryWorkers` and violates the "no global config state" rule in CLAUDE.md |
| 8 | Dead code | `executor/batch_iterator.go` (501 lines) | `batchScanIterator` never instantiated anywhere; contains the same obsolete-enum bug as #4 plus natural-order Tx encoding (no bitwise NOT) |
| 9 | Dead code | `executor/context_minimal.go` (9 lines) | `MinimalContext` with TODO "Replace with full implementation" — never referenced |
| 10 | Dead code | `executor/pattern_match.go:29-137` (~110 lines) | `MemoryPatternMatcher` struct + methods — `NewMemoryPatternMatcher` returns `NewIndexedMemoryMatcher` (line 38), struct literal never used anywhere |
| 11 | Dead code | `executor/datom_relation.go` | `NewDatomIterator`/`NewDatomRelation` only referenced from `datom_test.go` |
| 12 | Dead code | `storage/key_mask_iterator.go` + `storage/badger_store.go:410-413` | `ScanKeysOnlyWithMask` marked deprecated, now just returns plain `ScanKeysOnly`; ~400 lines of key-mask infrastructure still compiled with stale key-layout offsets |
| 13 | Cleanup | `query/tuple_builder.go:7-9`, `query/tuple_builder_optimized.go:8-10` | Both marked "UNUSED / CANDIDATE FOR REMOVAL" in their own comments |
| 14 | Cleanup | `storage/utils.go` | 18-line file named `utils.go` — violates stated naming rule |
| 15 | Cleanup | `codec/lz77.go:225-293` | `RepeatOffsets` defined but not wired into `FindMatches` or `encodeOffset` |
| 16 | Cleanup | `annotations/output.go:599-605` | `isTerminal` just returns `fd == 1 \|\| fd == 2` — not actual tty detection |
| 17 | Cleanup | `aggregation.go:241` | `fmt.Printf("AGGREGATE BUG: ...")` fires to stdout unconditionally on an edge case, outside any debug gate |
| 18 | Cleanup | `planner/types.go:14-20` | Planner has its own 5-index `IndexType` enum that doesn't include `EATV`/`AETV` — stale copy, used only in explain output |
| 19 | Cleanup | `storage/hash_join_matcher.go:374, 397` | Case-label comments say `// 3`/`// 4` for VAET/TAEV; actual enum values are 5/6. Code uses named constants so this is cosmetic, but the comments predate the EATV/AETV expansion |
| 20 | Cleanup | `executor/predicate_classifier.go:330` | `SplitPredicatesForPattern` mutates `phase.Predicates` via `classifier.phase.Predicates = predicates` — concerning if phase is shared |
| 21 | Doc drift | `executor/iterator_composition.go:7-8` | Comment references package-level `Enable*` variables that do not exist |
| 22 | Doc drift | `docs/STREAMING_ARCHITECTURE_DECISION.md` | Describes package globals as the current implementation; they were removed and replaced by `ExecutorOptions` threading |
| 23 | Doc drift | `README.md` | Claims both "~70%" and "~80%" Datomic parity in different places |
| 24 | Doc drift | `storage/key_mask_iterator.go:31-84` | Key-layout comments `[1 prefix][20 entity][32 attr][value][20 tx]` predate the Op-at-last migration; no longer accurate |

---

## 1. `ResolveLWWFromDatoms` ignores tombstones

**File:** `datalog/storage/crdt_resolve.go:20-36`

```go
func ResolveLWWFromDatoms(datoms []datalog.Datom) (any, datalog.ElementID) {
    if len(datoms) == 0 { return nil, datalog.ElementID{} }
    var maxID datalog.ElementID
    var currentValue any
    for _, d := range datoms {
        if d.Tx.Compare(maxID) > 0 {
            maxID = d.Tx
            currentValue = d.V          // ← no Op check
        }
    }
    return currentValue, maxID
}
```

The companion function `BadgerMatcher.ResolveLWW` (`cache_resolver.go:55`) correctly returns
`(nil, tx, nil)` when `datom.Op == OpCRDTRemove`. This pure version does not.

**Call path that exposes it:** `Cache.PopulateFromDatoms` (`cache.go:252, 277`) → invoked by
`PrefetchEntities` (`prefetch.go:81-84`). When entity prefetch runs on an (E, A) whose latest op is
`OpCRDTRemove`, the cache is populated with the tombstoned value's `V` as the current value.

**Suggested fix:** Add the same tombstone check from `cache_resolver.go:55`:

```go
if d.Tx.Compare(maxID) > 0 {
    maxID = d.Tx
    if d.Op == datalog.OpCRDTRemove {
        currentValue = nil
    } else {
        currentValue = d.V
    }
}
```

**Test gap:** Existing tombstone tests (per `BUG_CACHE_CARDINALIY_ONE_TOMBSTONE.md`) all bind E via
`:in` parameters, routing through the streaming `CRDTResolvingIterator`. None of them exercise
`PrefetchEntities` → `PopulateFromDatoms` → `ResolveLWWFromDatoms`. Suggested test: PullInto on an
entity where a CardinalityOne attribute has been Remove()d, with entity prefetch enabled.

---

## 2. `LookupAttribute` storage fallback ignores tombstones

**File:** `datalog/storage/matcher.go:844-869`

```go
if card == schema.CardinalityOne {
    start, end := encoder.EncodePrefixRange(EATV, eBytes[:], aStorage[:])
    iter, _ := m.store.ScanKeysOnly(EATV, start, end)
    defer iter.Close()
    for iter.Next() {
        datom, _ := iter.Datom()
        if m.shouldFilterTx(datom.Tx) { continue }
        // First entry with valid Tx is the current value (LWW)
        return datom.V, true            // ← no Op check
    }
    return nil, false
}
```

Same bug, different code path. This is the fallback when the cache is nil or when a temporal view
(`AsOf`/`History`) bypasses the cache. Visible via `get-else`, `missing?`, `get-some`, and any
direct call to `LookupAttribute`.

**Suggested fix:** mirror `cache_resolver.go:55`:

```go
if datom.Op == datalog.OpCRDTRemove {
    return nil, false
}
return datom.V, true
```

**Test gap:** as-of queries (cache bypassed) on a CardinalityOne attribute that was tombstoned
between the target Tx and now.

---

## 3. `extractElementIDFromKey` reads wrong bytes under current key layout

**File:** `datalog/storage/badger_store.go:533-584`

Key layout after the OP_POSITION migration is `[prefix][...][Tx↓][AfterRef?][Op]` where `Op` is
always the last byte (see `key_encoder_binary.go:40-51`). For the four indices where Tx is at the
tail, this function reads:

```go
case EAVT, AEVT, AVET, VAET:
    if len(key) < txSize { return datalog.ElementID{} }
    txBytes = key[len(key)-txSize:]   // ← includes the Op byte (and AfterRef when present)
```

For a key without AfterRef, the extracted "Tx" is actually the last 15 bytes of the real Tx plus
the Op byte. For a key with AfterRef, it's the last 16 bytes of AfterRef. Either way, the result is
wrong.

**Method: `BadgerIterator.ElementID()` (line 523).** Every caller of the fast-path `ElementID()` on
EAVT/AEVT/AVET/VAET iterators gets garbage. Grep for callers to assess blast radius — at minimum,
any cache-freshness path that uses `Iterator.ElementID()` directly (rather than `DecodeKey`).

**Suggested fix:** use the key encoder's `DecodeKey` (same as `MaxElementIDForAttribute` at
`badger_store.go:312`), or hand-roll the correct offset:

```go
case EAVT, AEVT, AVET, VAET:
    // Op is last byte; AfterRef is 16 bytes before Op when Op.HasAfterRef()
    op := key[len(key)-1]
    tailSize := 1
    if datalog.CRDTOp(op).HasAfterRef() {
        tailSize = 17
    }
    txBytes = key[len(key)-tailSize-txSize : len(key)-tailSize]
```

---

## 4. `simpleBatchScanner.buildKey` uses pre-expansion index enum

**File:** `datalog/storage/simple_batch_scanner.go:200-273`

```go
switch s.index {
case 0:   // EAVT
case 1:   // AEVT          ← comment is wrong; 1 = EATV
case 3:   // VAET          ← comment is wrong; 3 = AETV
case 4:   // TAEV          ← comment is wrong; 4 = AVET
}
```

Current enum (`store.go:10-18`):
```
EAVT=0, EATV=1, AEVT=2, AETV=3, AVET=4, VAET=5, TAEV=6
```

`matcher_strategy.go` routinely returns `AETV` (=3) as the best index for A-bound CardinalityOne
patterns. When that strategy flows into `matchWithSimpleBatchScanning`
(`matcher_relations.go:1027`) with AETV, `buildKey` enters the `case 3` branch and produces
VAET-shaped keys. The scan range will not match real stored data — the result is silent
under-counting or zero results.

The bug bites specifically at `bindingRel.Size() > 100` where batch scanning kicks in.

**Suggested fix:** replace integer literals with the named constants and add explicit cases for the
currently-missing indices (or fall through to an error rather than silently returning nil):

```go
switch s.index {
case EAVT:
    ...
case EATV, AETV:
    // Add handling or panic with clear message
case AEVT:
    ...
```

**Also:** grep for `case 0|case 1|case 2|case 3|case 4|case 5|case 6` under `storage/` for similar
latent bugs that rode along on the old enum.

---

## 5. EDN schema parser rejects `:db.cardinality/vector`

**File:** `datalog/schema/parser.go:170-177`

```go
func parseCardinality(node *edn.Node) (Cardinality, error) {
    ...
    switch val {
    case "db.cardinality/one":  return CardinalityOne, nil
    case "db.cardinality/many": return CardinalityMany, nil
    default:
        return "", fmt.Errorf("unknown cardinality: %s", node.Value)
    }
}
```

`schema/types.go:29` defines `CardinalityVector = "db.cardinality/vector"` and
`schema/builder.go:85-96` exposes it via `.Vector()` / `.OrderedSet()`. The EDN path rejects it.

**Suggested fix:** add a `"db.cardinality/vector"` case returning `CardinalityVector`. Also consider
parsing `:db/unique-elements` (or equivalent) for the `UniqueElements` flag.

---

## 5. `MaterializeResult` production panic on identical tuples

**File:** `datalog/executor/executor_utils.go:82-105`

```go
func MaterializeResult(rel Relation, symbols []query.Symbol) Relation {
    var tuples []Tuple
    collectTuplesInto(&tuples, rel)

    // DEBUG: Check for tuple copying bug
    if len(tuples) > 1 {
        first := tuples[0]
        last := tuples[len(tuples)-1]
        allSame := true
        for i := range first {
            if first[i] != last[i] {
                allSame = false
                break
            }
        }
        if allSame {
            panic(fmt.Sprintf("BUG DETECTED in MaterializeResult: All %d tuples identical! ...", ...))
        }
    }
    ...
}
```

This is a runtime debug check that panics in production if any two tuples (first and last) happen
to have interface-equal elements. The comment says "tuple copying bug" — suggesting this caught a
real iterator-workspace-reuse bug at some point. If that bug has been fixed (which the interning
invariants, `BufferedIterator`, and `RequiresCopy()` plumbing suggest), this is now a
false-positive waiting to fire on any legitimate query that returns interned-pointer tuples that
happen to match.

**Suggested fix:** remove the check or gate it behind a test-only build tag. If it's still valuable
as a debug assert, make it a logged warning rather than a panic.

---

## 6. EDN schema parser rejects `:db.cardinality/vector`

(Was #5 in initial report — moved for numbering consistency with the table above.)

**File:** `datalog/schema/parser.go:170-177` — unchanged, see previous section on Vector support.

---

## 7. `SubqueryWorkerCount` is a mutable package-level global

**File:** `datalog/executor/subquery.go:19-21`

```go
// SubqueryWorkerCount is the number of goroutines to use for parallel subquery execution
// Default is runtime.NumCPU() for optimal CPU utilization
var SubqueryWorkerCount = runtime.NumCPU()
```

Read at `subquery.go:173` and `279`, mutated by tests at
`parallel_subquery_test.go:201, 207`:

```go
oldCount := SubqueryWorkerCount
defer func() { SubqueryWorkerCount = oldCount }()
SubqueryWorkerCount = count
```

This duplicates `ExecutorOptions.MaxSubqueryWorkers` (which exists on the struct at
`options.go:19`) and directly violates the "No Global Configuration State" rule in CLAUDE.md
(lines 351-358). It also makes concurrent tests of subquery parallelism race with each other.

**Suggested fix:** delete `SubqueryWorkerCount`. Pass workers via `ExecutorOptions.MaxSubqueryWorkers`,
which is already propagated correctly elsewhere (`executor.go:73`). Update the two read sites and
the test.

**Author's note:** earlier in this review I incorrectly claimed all streaming-related globals had
been refactored away. I only checked `EnableIteratorComposition`/`EnableTrueStreaming`/`EnableSymmetricHashJoin`
and assumed the pattern was universal. This one survived the refactor and is real.

---

## 8. `batchScanIterator` — 500 lines of dead code with the same enum bug

**File:** `datalog/executor/batch_iterator.go` (entire file, 501 lines)

`newBatchScanIterator` and `batchScanIterator` type are referenced only within the file itself
(verified by grep — no other callers in production or tests). The file contains:

1. The **same obsolete-enum bug** as `simpleBatchScanner` (case 0=EAVT, 1=AEVT, 3=VAET, 4=TAEV) in
   `calculateKey` (lines 200-228).
2. **Natural-order Tx encoding** in `encodeUint64` (line 494) — no bitwise NOT, so Tx scans would
   iterate in wrong direction.
3. A declared-but-unused `tupleBuilder *query.OptimizedTupleBuilder` field (line 40).
4. Uses the old `query.DatomToTuple` function (329) instead of the production `InternedTupleBuilder`.

**Suggested fix:** delete the file. If any of the sub-algorithms are valuable (range grouping logic?),
lift them into `simple_batch_scanner.go` with proper enum constants and correct Tx encoding.

---

## 9. `MinimalContext` — 9-line TODO stub

**File:** `datalog/executor/context_minimal.go`

```go
// MinimalContext provides a no-op implementation for immediate compilation fix.
// TODO: Replace with full implementation from context.go
type MinimalContext struct{}

func NewMinimalContext() *MinimalContext {
    return &MinimalContext{}
}
```

Grep confirms no callers anywhere. `BaseContext` in `context.go` already provides the no-op
implementation the TODO is pointing to.

**Suggested fix:** delete the file.

---

## 10. `MemoryPatternMatcher` struct is dead — constructor swaps to a different type

**File:** `datalog/executor/pattern_match.go:29-137`

```go
type MemoryPatternMatcher struct {
    datoms []datalog.Datom
}

func NewMemoryPatternMatcher(datoms []datalog.Datom) PatternMatcher {
    return NewIndexedMemoryMatcher(datoms)   // ← returns a DIFFERENT type
}

func (m *MemoryPatternMatcher) Match(...) { ... }   // ← never called
func (m *MemoryPatternMatcher) MatchWithConstraints(...) { ... }   // ← never called
```

The constructor returns `*IndexedMemoryMatcher`, not `*MemoryPatternMatcher`. The struct and its
methods (~100 lines, lines 42-137) are unreachable from any caller. Grep for
`&MemoryPatternMatcher{` or `MemoryPatternMatcher{` returns no matches.

The bottom half of the file (from `matchesDatomWithPattern` at line 175 down) contains helpers
that *are* still used elsewhere in the package, so don't delete the whole file.

**Suggested fix:** delete the struct definition and its methods (lines 29-137). Keep the helpers
below.

---

## 11. `NewDatomIterator`/`NewDatomRelation` only used in tests

**File:** `datalog/executor/datom_relation.go`

Grep confirms all non-test callers are absent. Used only by `datom_test.go`. 125 lines.

**Suggested fix:** move the file to `_test.go` or delete if tests can use `NewMaterializedRelation`
directly.

---

## 12. `ScanKeysOnlyWithMask` is disabled but key-mask infrastructure persists

**File:** `datalog/storage/badger_store.go:408-413`

```go
// ScanKeysOnlyWithMask - DEPRECATED: Key mask filtering was benchmarked slower
// Just use regular key-only scanning with filtering in the matcher
func (s *BadgerStore) ScanKeysOnlyWithMask(...) (Iterator, error) {
    return NewKeyOnlyIterator(s, index, start, end)
}
```

The ~425-line `key_mask_iterator.go` file still exists and is called from
`matcher_relations.go:461`, `unbound_mask_iterator` still exists and is instantiated. Since
`ScanKeysOnlyWithMask` now returns a plain iterator, `unboundMaskIterator` wrapping it does
nothing useful on the mask side — but it still runs the mask construction code, which has
**outdated key-layout comments** (key_mask_iterator.go lines 31-84) that predate the Op-at-last
migration.

**Suggested fix:** delete `key_mask_iterator.go`, `unboundMaskIterator` (matcher_iterator_unbound.go:92+),
the `TryConvertConstraintsToMasks` call site (matcher_relations.go:421-430), and the
`ScanKeysOnlyWithMask` method. All dead weight.

---

## 21 & 22. Stale streaming-architecture docs

**`executor/iterator_composition.go:7-8`:**

```go
// Note: These variables are now managed by ExecutorOptions but kept for backward compatibility
// Use ExecutorOptions instead of these global variables
```

No such variables exist in `iterator_composition.go`. Three streaming flags (`EnableIteratorComposition`,
`EnableTrueStreaming`, `EnableSymmetricHashJoin`) were fully moved to `ExecutorOptions`. But — see
item 7 above — `SubqueryWorkerCount` is still a global, so the rule isn't universally enforced.

**`docs/STREAMING_ARCHITECTURE_DECISION.md`:** the body describes a "pragmatic compromise" of
keeping package-level globals. For the three streaming flags, this no longer applies; the
refactor was completed. For `SubqueryWorkerCount`, the compromise is de-facto still in place,
just not documented.

**Suggested fix:** delete the stale comment in `iterator_composition.go`. Either delete
`STREAMING_ARCHITECTURE_DECISION.md` entirely or rewrite it to reflect current reality (three
flags threaded through options, `SubqueryWorkerCount` still a global).

---

## 8. README Datomic-parity percentage inconsistency

**`README.md`** uses "~70%" in one place and "~80%" in another (also DATOMIC_COMPATIBILITY.md
line 9 says "~80%"). Pick one.

---

## 9. Two tuple builders marked for removal

**`query/tuple_builder.go:7-9`:**
```go
// NOTE: TupleBuilder is UNUSED in production code. Only InternedTupleBuilder is used.
// This implementation exists solely for benchmark comparisons (see tuple_builder_bench_test.go).
// CANDIDATE FOR REMOVAL: Consider removing this file to reduce maintenance burden.
```

**`query/tuple_builder_optimized.go:8-10`:** identical comment about `OptimizedTupleBuilder`.

Either delete them (matches CLAUDE.md's "if you are certain something is unused, you can delete it
completely") or if benchmarks genuinely need them, move them to `_test.go` files and strip the
comment.

---

## 10. `storage/utils.go`

**File:** `datalog/storage/utils.go` — 18 lines containing one function, `concatBytes`. CLAUDE.md
forbids `utils.go`. Move `concatBytes` into `key_encoder_base.go` (also 18 lines) or inline it at
call sites — it's used exactly for key concatenation.

---

## 11. `RepeatOffsets` is dead code

**File:** `datalog/codec/lz77.go:225-293`

`RepeatOffsets`, `NewRepeatOffsets`, `EncodeOffset`, `DecodeOffset`, and the commentary about
"Offset encoding: 1 = repeat offset 0…" are all defined but never called. `FindMatches` emits raw
offsets; `sequences.go`'s `encodeOffset` does a pure log2 encoding with no repeat-offset ring.

Either wire the ring into the sequence encoder (which is the point — it's a real compression win
for repetitive data) or delete the unused code. Leaving it risks a future reader assuming
compression uses repeat-offset coding when it doesn't.

---

## 12. `isTerminal` is not a terminal check

**File:** `datalog/annotations/output.go:599-605`

```go
func isTerminal(fd uintptr) bool {
    return fd == uintptr(1) || fd == uintptr(2) // stdout or stderr
}
```

An `os.File` for a regular file can have fd 1 or 2 (e.g., `./prog > log.txt` redirects stdout to a
file with fd 1). This always returns true for any file backed by those descriptors, which means
ANSI color codes will land in files on redirection.

**Suggested fix:** use `golang.org/x/term.IsTerminal(int(fd))` or `github.com/mattn/go-isatty`.
The comment already admits this is "simplified."

---

## Items 13-20 (cleanup nits)

Covered in the summary table above and earlier sections 13-20. Brief recap:

- **13** Two tuple builders marked "CANDIDATE FOR REMOVAL" in their own comments
  (`query/tuple_builder.go:7-9`, `query/tuple_builder_optimized.go:8-10`). Either delete or
  move to `_test.go`.
- **14** `storage/utils.go` — 18 lines, violates naming rule. Move `concatBytes` into
  `key_encoder_base.go`.
- **15** `RepeatOffsets` in `codec/lz77.go:225-293` defined but not wired; genuine missed
  compression opportunity or dead code.
- **16** `isTerminal` in `annotations/output.go:599-605` is just `fd == 1 || fd == 2`; will
  emit ANSI codes into redirected files.
- **17** `aggregation.go:241` has unconditional `fmt.Printf("AGGREGATE BUG: ...")` outside any
  debug flag — pollutes production logs.
- **18** `planner/types.go:14-20` defines its own `IndexType` enum with only 5 values (missing
  `EATV`/`AETV`). Used only in explain output so mostly benign, but tells a misleading story.
- **19** `hash_join_matcher.go:374, 397` have case-label comments (`// 3`, `// 4`) that are
  numerically wrong after the EATV/AETV expansion. Code itself is correct (uses named
  constants).
- **20** `executor/predicate_classifier.go:330` — `SplitPredicatesForPattern` mutates its
  `phase` argument via `classifier.phase.Predicates = predicates`. Concerning under shared
  phase access.

---

## Meta-observations

These bugs cluster into three recognizable patterns:

**1. Scope-audit omission after a migration.** When a migration landed (Op-to-end, 7-index
expansion, globals-to-options) the "obvious" sites were updated but parallel code paths were
not:

- `extractElementIDFromKey` missed the Op-to-end migration
- `simpleBatchScanner.buildKey` missed the 7-index expansion
- `batch_iterator.go` missed both
- Both tombstone gaps (`ResolveLWWFromDatoms`, `LookupAttribute` fallback) missed the Op
  check
- `SubqueryWorkerCount` missed the globals-to-options migration
- `planner/types.go` has a stale copy of the old 5-index enum
- `key_mask_iterator.go` offset comments describe the old key layout

One fix, many uncaught siblings. This is exactly the pattern CLAUDE.md documents for the
original tombstone bug; it's now also true of its fix, and of every migration in the history.

**2. Unreachable code accumulating around migrations.** Multiple parallel implementations
persist after being replaced:

- `TupleBuilder` and `OptimizedTupleBuilder` (marked CANDIDATE FOR REMOVAL, still compiled)
- `batchScanIterator` (501 lines, zero callers)
- `MemoryPatternMatcher` struct (~100 lines, constructor swaps to `IndexedMemoryMatcher`)
- `MinimalContext` (TODO stub)
- `NewDatomIterator` (test-only)
- `ScanKeysOnlyWithMask` + `unboundMaskIterator` + `KeyMaskIterator` (disabled but
  instantiated)

This is real maintenance cost: the scope-audit omissions above happened in part because the
author (or an AI agent) had to remember to update N parallel implementations instead of 1.

**3. Doc drift under refactoring.** `STREAMING_ARCHITECTURE_DECISION.md`, the comment in
`iterator_composition.go`, the planner's `IndexType` enum, and the README percentage
discrepancy all describe intermediate states that got cleaned up in code but not in prose.

These interact: prior docs that describe "fixed" states cause future readers (and future AI
agents) to confidently state things that are no longer true. I did this myself earlier in the
review before directly reading the code — specifically, I claimed all streaming globals had
been removed, which is true for three of them and false for `SubqueryWorkerCount`.

---

## Mitigations worth considering

**Compiler-enforced invariants stay accurate.** The codebase already does this well in
places: the interning `panic` on pointer-equality-but-value-equality violations, the
`_ = (*Schema)(nil)` compile-time interface assertions, the panic on unknown index in the
key encoders. These never drift because they break the build when violated.

Prose invariants drift silently. Every doc file that describes current state is a liability;
every comment block next to code is a liability. The fewer invariants encoded in prose, the
less drift surface exists.

**Concrete suggestions:**

1. Replace stale index-layout comments in `key_mask_iterator.go` with a runtime `panic(...)`
   or `// LAYOUT: ...` block next to the constants in `key_encoder_binary.go` — one source of
   truth.
2. Kill the planner's local `IndexType` enum. Import storage's. One enum, one story.
3. Move `SubqueryWorkerCount` into `ExecutorOptions.MaxSubqueryWorkers`. Close the loop the
   streaming refactor started.
4. Delete the half-dozen dead-or-duplicated files/types. They're not paying for themselves
   in testing or documentation value; they're paying *against* themselves in scope-audit
   cost.
5. For any file with a "CANDIDATE FOR REMOVAL" or "DEPRECATED" comment: either remove or
   remove the comment. A comment saying "this is dead" is a worse outcome than removing the
   dead code, because the comment rot-guarantees the code.
