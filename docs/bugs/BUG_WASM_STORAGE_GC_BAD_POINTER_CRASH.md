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
| 12 | CI `make test-wasm`, linux/amd64, go1.25.12 (two runs of the same commit) | one pass, one **crash** ~30s into storage |
| 13 | 2026-07-21, full `make test` pipeline, darwin/arm64, go1.26.3, branch `fix/tuple-builder-string-key` (uncommitted key-encoder rewrite in tree) | **crash** ~35s into storage |

All crashes are the same fatal — the GC write barrier discovering a poisoned pointer slot:

- Run 1: `runtime: pointer 0x22300000 to unallocated span` → `fatal error: found bad pointer in Go heap (incorrect use of unsafe or cgo?)`. Discovered in `runtime.wbBufFlush1`; the running goroutine (3932) was in `runtime.wbMove` inside `executor.(*ScanSharingMatcher).Match` (`scan_sharing_matcher.go:81`), under `DefaultQueryExecutor.executePattern` (`query_executor.go:404`).
- Run 2: `runtime: pointer 0x22300000 to unused region of span` (span base `0x12f30000`) — same fatal, different stack: `bulkBarrierPreWriteSrcOnly` during `growslice`, an append in `storage.(*MemoryStore).scan.func1` (`memory_store.go:153`) inside a btree `AscendRange`, reached from `validatingVBoundIterator.openCRDTScan` (`matcher_relations.go:1077`) via `Next` ← `CountingIterator` ← `LazySeq` realize. Same goroutine number (3932).
- Run 10: `runtime: pointer 0x21970000 to unallocated span` — and this time the crashing goroutine's **stack itself is corrupted**: `runtime: g 4129: unexpected return pc for gcWriteBarrier called from 0x82ed80` (a stack address in a return-PC slot). The dumped stack memory around the frame contains storage key material — the ASCII bytes `entity/type` and hash-like byte runs — and the symbolizer mis-attributes a data word to `BenchmarkElementID.func3`, i.e. raw data is sitting where the runtime expects frame structure.
- Run 13: `runtime: pointer 0x22430000 to unallocated span` — same 0x10000-multiple poison shape, discovered in `runtime.wbBufFlush1` from `runtime.wbMove` inside `executor.(*DefaultQueryExecutor).tryFuseAttributeFetchBundle` (`query_executor.go:681`) under `DefaultQueryExecutor.Execute`, goroutine 4697, go1.26.3. Same late-suite timing (~35s), same full-pipeline context.

What the signature says:

- **The poison values are wasm code PCs, not heap addresses.** Both `0x22300000` (runs 1–2: `ScanSharingMatcher.Match`'s function base — run 1 shows the frame at `pc=0x22300046`) and `0x21970000` (run 10) are exact multiples of `0x10000`, the shape of wasm function-base PCs (`funcindex << 16`). Code addresses are landing in slots the GC scans as heap pointers — and run 10 shows the mirror image, a non-code value in a return-PC slot. Both directions of the same confusion: code addresses and data swapping places in stack/pointer slots.
- **The crash site is not the bug site.** The write barrier discovers the corrupted slot when something later writes near it; the three different stacks (a struct move in the executor, a slice grow in the store, a plain `gcWriteBarrier` on a corrupted frame) are discovery points, not origins.
- **It is heap/stack-layout and timing sensitive.** Identical poison across runs 1–2, a different one in run 10; eight passes interleaved, including the exact configurations that crashed. All three crashes hit ~22–27s into the suite — late, far from any single test's obvious footprint. All three crashing runs were full-suite runs launched inside longer pipelines (`make test` ×2, immediate manual re-run ×1); all standalone re-runs after a cold start passed.
- Runs 1–2's discovery stacks flow through the V-bound scan machinery over `MemoryStore` (the wasm default backend); run 10's corrupted frame contains storage key bytes, which encoder stacks legitimately hold — the abnormality is the smashed return PC, not the key bytes' presence.
- The return-PC corruption plus code-PCs-as-heap-pointers pattern is consistent with a goroutine **stack move/scan defect under js/wasm** (frames misread while a stack grows or shrinks during GC) — a Go runtime/compiler bug flavor — but an out-of-bounds write through `unsafe` in the repo's key-encoding paths is not eliminated.
- **The crash is environment-independent** (run 12): it reproduced on a GitHub linux/amd64 runner under Go **1.25.12** with the same fatal, the same `0xNNNN0000` poison shape (`0x21cc0000`), and the same "unexpected return pc" stack corruption (`runtime.rtype.pkgpath called from 0x300`), while the sibling CI run of the identical commit passed. Not darwin-specific, not toolchain-1.26-specific, not machine-specific — the defect lives in the wasm target generically (repo code or a long-standing runtime issue). Note CI ran the full wasm storage suite for the first time on this branch (the previous workflow ran a curated subset), so CI history before this point carries no signal about when the crash became reachable.

## Context

First observed on the first wasm run after the correlated or-join round-trip fix landed in the working tree (see `resolved/BUG_CORRELATED_ORJOIN_GLOBAL_FALLBACK_DROPS_ROWS.md`). That diff contains no unsafe, no memory-representation code, and no storage/executor runtime changes; its new tests pass standalone under wasm; and the crashes occur late in the suite, long after those tests run. It did change which executor paths correlated or/or-join queries exercise and shifted suite composition/heap history. None of that establishes or excludes causality — recorded here as fact, not attribution.

## Triage results (2026-07-19)

**Reliable reproducer**: `GOGC=1 GOOS=js GOARCH=wasm go test -count=1 ./datalog/storage` crashes deterministically (4/4 local runs), always detonating at `TestWildcardPullQueryUsesOneBatch` on the same goroutine number. Constant GC makes the write barrier validate constantly, converting the ~25% stochastic crash into a turnkey one.

What the instruments established:

1. **The poison value is a per-binary constant.** `0x21970000` in every run of the current binary across `-v`, `GODEBUG=clobberfree=1`, and `GODEBUG=gccheckmark=1`; earlier binaries produced `0x22300000` (pre-commit tree) and `0x21cc0000` (CI's Go 1.25.12 build). A link-layout-determined value with the `0xNNNN0000` shape of a wasm function-base PC, landing in the write-barrier buffer as a recorded "pointer".
2. **Not a read of freed heap memory**: `clobberfree=1` (which fills freed objects with a clobber pattern) leaves the poison unchanged.
3. **Not caught earlier by `gccheckmark=1`**: identical detonation, so the corruption enters through the barrier buffer, not a missed mark.
4. **Not repo `unsafe`**: an exhaustive audit found exactly one `unsafe` site in the tree (`executor/tuple_key.go` — pointer-address hash, converted to `uint64` in a single expression, stored only in non-pointer slots, and linked equally into the never-crashing executor test binary); dependency `unsafe` is either build-tag-excluded under js/wasm (go-spew, isatty, colorable) or a cold read-only alias (`ll`'s `[]byte`→`string`); no `go:linkname`/`nosplit`/`noescape` anywhere in the graph; interning is plain `sync.Map` with strong references (no stdlib `unique`/`weak`).
5. **Not test-bisectable — extreme layout sensitivity**: the detonating test passes in isolation and with a suffix window; selecting **all 767 preceding tests** via a `-run` regex (preserving execution order) does not crash, while the identical unfiltered run crashes 4/4. The `-run` filter's own allocations perturb the heap enough to mask the bug, so no test-subset instrument can minimize it.
6. **The "unexpected return pc for gcWriteBarrier called from X" lines show a different X every crash** (a stack address, `0x300`, a heap-like address) while the primary poison stays constant — consistent with the traceback machinery simply failing to unwind through the wasm write-barrier fast path once the throw begins, i.e. secondary noise, not independent corruption.

**Conclusion**: every discriminator points at the Go js/wasm write-barrier code path (compiler barrier codegen or `asm_wasm.s`'s `gcWriteBarrier`), reproducing across Go 1.25.12 and 1.26.3 and across darwin/arm64 and linux/amd64 hosts. No repo-side defect was found by any instrument, and no repo-side fix target exists. Not yet proven upstream: there is no minimal reproducer (layout sensitivity blocks minimization), but reproduction is turnkey on this public repo with the one-line command above.

**Crashes at tip** (2026-07-19): `go1.28-devel_9f236fbe` reproduces identically — same fatal, same detonating goroutine, a new per-binary poison constant (`0x22160000`), confirming the per-binary-constant observation across a third toolchain. The matrix is now Go 1.25.12, 1.26.3, and tip, on darwin/arm64 and linux/amd64 hosts: unfixed upstream.

**Filed upstream**: https://github.com/golang/go/issues/80472 (2026-07-19) — traces, the per-binary-constant poison observation, the crashes-at-tip result, and the turnkey reproduction pinned to `ce6baf7`.

## Remaining next steps

1. Track golang/go#80472; run instrumented builds or candidate fixes against the `GOGC=1` reproducer on request.
2. Until an upstream fix: the CI wasm job will flake at roughly the stochastic rate; reruns are sanctioned for this signature only, with this doc as the reference.
