# BUG: Stochastic `found bad pointer in Go heap` crash in the wasm storage suite

**Status**: Open (2026-07-19). Observed twice, back to back, then not reproduced in seven consecutive runs including a fully green `make test`. Not root-caused; causality with the working tree's diff is not established in either direction. Logged so the signature is recognizable the next time it fires and the evidence is not lost to a green re-run.

## Observations

Environment: darwin/arm64 host, go 1.26.3, `GOOS=js GOARCH=wasm`, Node runner, `go test -count=1 ./datalog/storage`.

Three crashes across eleven runs:

| Run | Config | Result |
|-----|--------|--------|
| 1 | `make test` (wasm half) | **crash**, ~24s in |
| 2 | manual re-run, same package | **crash**, ~22s in |
| 3 | scoped `-run 'TestCorrelatedOr*'` | pass |
| 4 | full suite, `-v` | pass (25.5s) |
| 5–8 | full suite, non-verbose, ×4 | pass |
| 9 | `make test` (full gate) | pass |
| 10 | `make test` (full gate, later the same day) | **crash**, ~27s in |
| 11 | manual re-run, same package | pass |

All three crashes are the same fatal — the GC write barrier discovering a poisoned pointer slot:

- Run 1: `runtime: pointer 0x22300000 to unallocated span` → `fatal error: found bad pointer in Go heap (incorrect use of unsafe or cgo?)`. Discovered in `runtime.wbBufFlush1`; the running goroutine (3932) was in `runtime.wbMove` inside `executor.(*ScanSharingMatcher).Match` (`scan_sharing_matcher.go:81`), under `DefaultQueryExecutor.executePattern` (`query_executor.go:404`).
- Run 2: `runtime: pointer 0x22300000 to unused region of span` (span base `0x12f30000`) — same fatal, different stack: `bulkBarrierPreWriteSrcOnly` during `growslice`, an append in `storage.(*MemoryStore).scan.func1` (`memory_store.go:153`) inside a btree `AscendRange`, reached from `validatingVBoundIterator.openCRDTScan` (`matcher_relations.go:1077`) via `Next` ← `CountingIterator` ← `LazySeq` realize. Same goroutine number (3932).
- Run 10: `runtime: pointer 0x21970000 to unallocated span` — and this time the crashing goroutine's **stack itself is corrupted**: `runtime: g 4129: unexpected return pc for gcWriteBarrier called from 0x82ed80` (a stack address in a return-PC slot). The dumped stack memory around the frame contains storage key material — the ASCII bytes `entity/type` and hash-like byte runs — and the symbolizer mis-attributes a data word to `BenchmarkElementID.func3`, i.e. raw data is sitting where the runtime expects frame structure.

What the signature says:

- **The poison values are wasm code PCs, not heap addresses.** Both `0x22300000` (runs 1–2: `ScanSharingMatcher.Match`'s function base — run 1 shows the frame at `pc=0x22300046`) and `0x21970000` (run 10) are exact multiples of `0x10000`, the shape of wasm function-base PCs (`funcindex << 16`). Code addresses are landing in slots the GC scans as heap pointers — and run 10 shows the mirror image, a non-code value in a return-PC slot. Both directions of the same confusion: code addresses and data swapping places in stack/pointer slots.
- **The crash site is not the bug site.** The write barrier discovers the corrupted slot when something later writes near it; the three different stacks (a struct move in the executor, a slice grow in the store, a plain `gcWriteBarrier` on a corrupted frame) are discovery points, not origins.
- **It is heap/stack-layout and timing sensitive.** Identical poison across runs 1–2, a different one in run 10; eight passes interleaved, including the exact configurations that crashed. All three crashes hit ~22–27s into the suite — late, far from any single test's obvious footprint. All three crashing runs were full-suite runs launched inside longer pipelines (`make test` ×2, immediate manual re-run ×1); all standalone re-runs after a cold start passed.
- Runs 1–2's discovery stacks flow through the V-bound scan machinery over `MemoryStore` (the wasm default backend); run 10's corrupted frame contains storage key bytes, which encoder stacks legitimately hold — the abnormality is the smashed return PC, not the key bytes' presence.
- The return-PC corruption plus code-PCs-as-heap-pointers pattern is consistent with a goroutine **stack move/scan defect under js/wasm** (frames misread while a stack grows or shrinks during GC) — a Go runtime/compiler bug flavor — but an out-of-bounds write through `unsafe` in the repo's key-encoding paths is not eliminated.

## Context

First observed on the first wasm run after the correlated or-join round-trip fix landed in the working tree (see `resolved/BUG_CORRELATED_ORJOIN_GLOBAL_FALLBACK_DROPS_ROWS.md`). That diff contains no unsafe, no memory-representation code, and no storage/executor runtime changes; its new tests pass standalone under wasm; and the crashes occur late in the suite, long after those tests run. It did change which executor paths correlated or/or-join queries exercise and shifted suite composition/heap history. None of that establishes or excludes causality — recorded here as fact, not attribution.

## Next steps (in order of information per effort)

1. Raise incidence to make it bisectable: run the wasm storage suite under `GOGC=1` (and/or `-count` loops) — a barrier-discovery bug's incidence scales with GC frequency. Stochastic-at-25% is untriageable; stochastic-at-90% is a bisection target.
2. Audit the repo for `unsafe` usage reachable from the storage/executor wasm paths; the runtime's own diagnostic ("incorrect use of unsafe or cgo?") is the first hypothesis to eliminate, and there is no cgo under wasm.
3. If incidence can be raised: binary-search the test list to a minimal triggering set, then decide repo-bug vs. toolchain-bug.
4. Search golang/go issues for `found bad pointer in Go heap` on `js/wasm` around 1.26 — a code-PC-in-pointer-slot has the flavor of a funcval/closure handling defect in the wasm backend; if it matches a known issue, pin/upgrade the toolchain and verify incidence drops to zero.
5. Watch CI (linux/amd64 host, same wasm target) for the signature; a CI hit gives an independent reproduction environment.
