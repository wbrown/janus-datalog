# External Code Review — Bug Report

**Reviewer:** Claude (Opus 4.7), 1M context, reading code directly rather than relying on docs
**Date:** 2026-04-17
**Scope:** Most of `datalog/` root + `codec/` + `edn/` + `query/` + `parser/` + `annotations/` +
`constraints/` + `schema/` + most of `storage/`. **Not** reviewed: most of `executor/`, `algebra/`,
`planner/`, `qb/`, `reflect/`, `db/`, `cmd/`. Each item below includes file:line; please verify
against current HEAD before acting.

## Summary

| # | Severity | File | Issue |
|---|----------|------|-------|
| 1 | Correctness | `storage/crdt_resolve.go:20-36` | `ResolveLWWFromDatoms` does not check `Op` — cache-populate path ignores tombstones |
| 2 | Correctness | `storage/matcher.go:844-869` | `LookupAttribute` storage fallback does not check `Op` — cache-miss path returns tombstoned values |
| 3 | Correctness | `storage/badger_store.go:571-584` | `extractElementIDFromKey` reads `key[len-16:]` but Op is now the last byte — garbled ElementIDs for EAVT/AEVT/AVET/VAET |
| 4 | Correctness | `storage/simple_batch_scanner.go:200-273` | Switch uses pre-expansion enum values — when invoked with EATV/AETV/AVET it builds wrong-shaped keys |
| 5 | Feature gap | `schema/parser.go:170-177` | EDN schema parser rejects `:db.cardinality/vector` |
| 6 | Doc drift | `executor/iterator_composition.go:7-8` | Comment references package-level `Enable*` variables that do not exist |
| 7 | Doc drift | `docs/STREAMING_ARCHITECTURE_DECISION.md` | Describes package globals as the implementation; code actually threads `ExecutorOptions` |
| 8 | Doc drift | `README.md` | Claims both "~70%" and "~80%" Datomic parity in different places |
| 9 | Cleanup | `query/tuple_builder.go:7-9`, `query/tuple_builder_optimized.go:8-10` | Both marked "UNUSED / CANDIDATE FOR REMOVAL" in their own comments |
| 10 | Cleanup | `storage/utils.go` | 18-line file named `utils.go` — violates stated naming rule |
| 11 | Cleanup | `codec/lz77.go:230-293` | `RepeatOffsets` defined but not wired into `FindMatches` or `encodeOffset` |
| 12 | Cleanup | `annotations/output.go:599-605` | `isTerminal` just returns `fd == 1 \|\| fd == 2` — not actual tty detection |

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

## 6 & 7. Stale streaming-architecture docs

**`executor/iterator_composition.go:7-8`:**

```go
// Note: These variables are now managed by ExecutorOptions but kept for backward compatibility
// Use ExecutorOptions instead of these global variables
```

No such variables exist in the file or anywhere in `executor/`. Verified by grep: every occurrence
of `EnableIteratorComposition`/`EnableTrueStreaming`/`EnableSymmetricHashJoin` is either a field of
an `ExecutorOptions{}` literal or a field access on an `opts` parameter.

**`docs/STREAMING_ARCHITECTURE_DECISION.md`:** the body describes a "pragmatic compromise" of
keeping package-level globals `EnableIteratorComposition = true`, etc., as "READ-ONLY after
initialization." This is no longer the implementation; options are properly threaded via
`ExecutorOptions`.

**Suggested fix:** delete the stale comment in `iterator_composition.go`. Either delete
`STREAMING_ARCHITECTURE_DECISION.md` entirely or move it to `docs/archive/` with a header noting
it's historical.

**Meta:** CLAUDE.md explicitly forbids "backwards-compatibility shims" and the phrase that was in
that comment. A future Claude or reader seeing the comment would reasonably assume globals still
existed somewhere — this is exactly the kind of artifact the rule in CLAUDE.md exists to prevent.

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

## Meta-observations

These bugs cluster into two recognizable patterns:

**Scope-audit omission.** When a migration landed (Op-to-end, 7-index expansion, globals-to-options)
the "obvious" sites were updated but parallel code paths were not. `extractElementIDFromKey`,
`simpleBatchScanner.buildKey`, and both tombstone gaps all share this shape: a rule changed, one
implementation was updated, siblings were left stale. This is exactly the pattern CLAUDE.md
documents for the original tombstone bug; it's now also true of its fix.

**Doc drift under refactoring.** `STREAMING_ARCHITECTURE_DECISION.md`, the comment in
`iterator_composition.go`, and the README percentage discrepancy all describe intermediate states
that got cleaned up in code but not in prose.

The two patterns interact: prior docs that describe "fixed" states cause future readers (and future
AI agents) to confidently state things that are no longer true. I did this myself earlier in the
review before directly reading the code.

A partial mitigation visible in the codebase: invariants enforced by the compiler or by panics
(interning checks, `_ = (*Schema)(nil)` compile-time assertions, the Op-always-last convention
with panic on unknown index) stay accurate because they break the build when violated. Invariants
written only in prose drift. Moving as much documentation as possible into `const`, `var _ Interface
= (*Impl)(nil)`, tests named after the invariant, or runtime assertions would narrow the surface
where drift can hide.
