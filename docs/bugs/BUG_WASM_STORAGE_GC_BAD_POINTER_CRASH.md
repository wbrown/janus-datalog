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
| 14 | 2026-07-21, full `make test` pipeline, darwin/arm64, go1.26.3, branch `fix/tuple-builder-string-key` (uncommitted LookupAttribute single-homing in tree) | **crash** ~29s into storage |
| 15 | 2026-07-21, manual `make test-wasm` re-run of the same tree as run 14 | **crash** ~28s into storage |
| 16 | 2026-07-21, second manual `make test-wasm` re-run, same tree | **crash** ~28s into storage |
| 17 | 2026-07-21, full `make test` pipeline, darwin/arm64, go1.26.3, uncommitted tuple-key split-overflow work in tree | **crash** ~29s into storage, poison `0x223f0000` (new per-binary constant; layout shifted by the tuple-key changes), gcWriteBarrier corrupted-return-pc discovery shape |
| 18 | 2026-07-21, manual `make test-wasm` re-run of the same tree | **crash** ~28s, same poison and goroutine — this binary's layout is in the near-deterministic window, as runs 14–16's was. Root cause is upstream-confirmed (itabInit, CL 803460); reruns stopped per the run-14–16 precedent |
| 19 | 2026-07-21, full `make test` pipeline, darwin/arm64, go1.26.3, uncommitted span-grouped branch cache in tree | **crash** ~28s into storage, poison `0x22430000` — the runs-13–16 value recurring in a third distinct binary — gcWriteBarrier corrupted-return-pc discovery shape (`called from 0xd1ea0`, frame dump shows `newobject`/`mallocgc`/`gcmarknewobject`), goroutine 4720. Sanctioned rerun green |
| 20 | 2026-07-21, full `make test` pipeline, darwin/arm64, go1.26.3, uncommitted or-fallback direct-emit work in tree | **crash** ~28s into storage, poison `0x22450000` (new per-binary constant for the shifted layout), `wbBufFlush1` discovery, goroutine 4720 |
| 21 | 2026-07-21, manual `make test-wasm` re-run of the same tree | **crash** ~28s, same poison and goroutine — this binary's layout is in the near-deterministic window, as runs 14–16 and 17–18 were. Reruns stopped per that precedent |
| 22 | 2026-07-21, full `make test` pipeline, darwin/arm64, go1.26.3, uncommitted grouped-join-build work in tree | **crash** ~28s into storage, poison `0x22450000` (the occurrences-20–21 value recurring in a distinct binary), `wbBufFlush1` discovery. Sanctioned rerun green |
| 23 | 2026-07-21, `make test-wasm` re-run of the run-22 tree (only an executor test file added; the storage wasm binary is unchanged from run 22) | **crash** ~28s into storage, same poison and discovery — the run-22 binary flaking at its stochastic rate after its green rerun. The run's purpose (new executor test under wasm) passed; storage's status for this tree stands on run 22's green rerun |
| 24 | 2026-07-21, full `make test` pipeline, darwin/arm64, go1.26.3, uncommitted per-query read-session work in tree | **crash** ~28s into storage, poison `0x22450000` (fourth distinct binary carrying this value), `wbBufFlush1` discovery. Every other wasm package green, including the session-wired storage reads under MemoryStore elsewhere in the suite. Sanctioned rerun green |
| 25 | 2026-07-21, full `make test` pipeline, darwin/arm64, go1.26.3, uncommitted session-bounded cache work in tree | **crash** ~28s into storage, poison `0x22450000` (fifth distinct binary), "unused region of span" discovery (run 2's shape). Every other wasm package green. Sanctioned rerun green |
| 26 | 2026-07-22, CI `make test-wasm`, linux/amd64, go1.25.12, commit `70474ea` (subquery projection + scan set semantics), workflow run 29949559233 | **crash** ~30s into storage, poison `0x21f60000` (new per-binary constant; layout shifted by the commit), `wbBufFlush1` discovery, goroutine 4731. The sibling CI run (29949563963) of the identical commit was fully green including wasm — the run-12 pattern exactly. Every other wasm package green in the crashing run |
| 27 | 2026-07-22, sanctioned rerun of the run-26 job (same commit, same toolchain, same binary) | **crash** ~35s into storage, same poison `0x21f60000`, same goroutine 4731, `gcWriteBarrier` corrupted-return-pc discovery (`called from 0xcf600`, `mallocgc`/`gcmarknewobject` frames in the dump). This binary's layout is in the near-deterministic window, as runs 14–16 and 20–21 were; reruns stopped per that precedent. The commit's wasm status stands on the sibling run's fully green wasm job and the local darwin/arm64 pre-push full gate |
| 28 | 2026-07-22, CI `make test-wasm`, linux/amd64, go1.25.12, commit `4480f9b` (typed-input-identity pin + comment + CLI bug report), workflow run 29965347444 | **crash** into storage, poison `0x21fc0000` (new per-binary constant; layout shifted by the commit), `wbBufFlush1` discovery plus `gcWriteBarrier` corrupted-return-pc (`called from 0xb4e20`), goroutine 4804. The sibling CI run (29965345093) of the identical commit was fully green including wasm — the run-12/26 pattern. Every other wasm package green in the crashing run; local darwin/arm64 pre-push full gate was green. Sanctioned rerun issued |
| 29 | 2026-07-22, sanctioned rerun of the run-28 job (same commit, same toolchain, same binary) | **crash** into storage, same poison `0x21fc0000`, `wbBufFlush1` discovery, "unused region of span" variant (run 2's shape); the rerun's race and test jobs green. This binary's layout is in the near-deterministic window, as runs 14–16, 20–21, and 26–27 were; reruns stopped per that precedent. The commit's wasm status stands on the sibling run's fully green wasm job and the local darwin/arm64 pre-push full gate |
| 30 | 2026-07-22, CI `make test-wasm`, linux/amd64, go1.25.12, commit `c1c06d4` (doc-only: skill + this occurrence log — the storage wasm binary is unchanged from `4480f9b`), workflow run 29970449260 | **crash** into storage, same poison `0x21fc0000`, same goroutine 4804, `wbBufFlush1` + corrupted-return-pc discovery — the runs-28–29 binary flaking at its rate, the run-23 precedent exactly (doc-only tree, unchanged binary). The sibling CI run (29970447503) was fully green including wasm. Reruns remain stopped for this binary; the commit's wasm status stands on the sibling run and the local pre-push full gate |
| 31 | 2026-07-22, full `make test` pipeline, darwin/arm64, go1.26.3, uncommitted CLI-conversion work in tree (TaggedLiteralValue export + relation-input interface unwrap) | **crash** into storage under wasm, poison `0x22600000` (new per-binary constant; layout shifted by the arc's code), `wbBufFlush1` via `bulkBarrierPreWriteSrcOnly` (run 2's discovery shape), goroutine 4804. The full native suite in the same pipeline was green, including the arc's three new pins; every other wasm package green. First crash on this binary — sanctioned rerun green |
| 32 | 2026-07-25, full `make test` pipeline, darwin/arm64, go1.26.3, uncommitted PR A typed-bound seam in tree (`ScanBound`, ~35 converted scan sites) | **crash** ~28s into storage, poison `0x22600000` — the occurrence-31 value recurring in a distinct binary — "unused region of span" discovery via `wbBufFlush1`, goroutine 4805. The full native suite in the same pipeline was green, including the seven new `ScanBound` pins; every other wasm package green. Sanctioned rerun green |
| 33 | 2026-07-25, second `make test-wasm` re-run of the run-32 tree (unchanged binary) | **crash** ~26s, same poison and goroutine, `gcWriteBarrier` corrupted-return-pc discovery (`called from 0x5bac0`). **The dump carries the direct symbolization**: the stack word at `0x029aa980` reads `22600046` and the symbolizer annotates it `executor.(*ScanSharingMatcher).Match+0x46` — the poison is that method's entry PC, visible in the frame the way run 1's was, confirming the upstream itab diagnosis on this binary without recourse to the name section. Unlike runs 14–16 and 20–21 this binary is *not* in the near-deterministic window: 3 of 5 runs on it passed, including the sanctioned rerun after this crash |
| 34 | 2026-07-25, full `make test` pipeline, darwin/arm64, go1.26.3, uncommitted typed-bound annotation work in tree (scan events report the bound, not the encoded range) | **crash** ~27s into storage, poison `0x226a0000` (new per-binary constant; layout shifted by the annotation code), `wbBufFlush1` discovery, goroutine 4805. The dumped frame again carries a direct symbolization of a nearby stack word — `storage.TestHashJoinSymbolIndexBug+0x8d48` against a data value — the mis-attribution shape run 10 first showed. The full native suite in the same pipeline was green, including the new annotations-package tests; every other wasm package green. Sanctioned rerun green |
| 35 | 2026-07-25, pre-commit `make test` on the same tree plus the restored scan latency and a one-line gofmt in `datalog/executor` | **crash** ~27s into storage, poison `0x226a0000` and goroutine 4805 again — the occurrence-34 value persisting across an executor-side edit, so this binary's layout for the poisoned itab is unchanged by it. `gcWriteBarrier` corrupted-return-pc discovery (`called from 0xb3280`). Full native suite green. Sanctioned rerun green |
| 36 | 2026-07-26, CI `make test-wasm`, linux/amd64, go1.25.12, PR #114 head `566e806`, workflow run 30185434418 | **crash** ~1m50s into storage — **a new discovery shape**: `fatal error: found pointer to free object` from `runtime.(*mspan).reportZombies` under `sweepone`, reached from an ordinary `mapassign` in `TupleKeyMap.PutIfAbsentPositions` ← `OrFallbackIterator.outerJoinKeys`, rather than the write-barrier `found bad pointer in Go heap` every prior occurrence carried. The zombie object is at `0x22060000` — the `funcindex<<16` poison shape — on goroutine 4805, the goroutine number occurrences 32, 34 and 35 also carried; the object's own bytes decode to keyword text (`…:task/token-count`), i.e. ordinary interned data, so it is the marked victim rather than a corruptor. **The link to this bug is inferred, not proven**: marking a free object is the expected consequence of the GC following a bogus recorded pointer, and this doc already holds that the crash site is not the bug site — but the fatal differs from the signature the rerun policy names, so this occurrence is recorded rather than self-certified. The sibling CI run of the identical commit (30185402692) was fully green including wasm, the run-12/26/28 pattern; the local darwin/arm64 `make test` on the same code was green both legs |

All crashes are the same fatal — the GC write barrier discovering a poisoned pointer slot:

- Run 1: `runtime: pointer 0x22300000 to unallocated span` → `fatal error: found bad pointer in Go heap (incorrect use of unsafe or cgo?)`. Discovered in `runtime.wbBufFlush1`; the running goroutine (3932) was in `runtime.wbMove` inside `executor.(*ScanSharingMatcher).Match` (`scan_sharing_matcher.go:81`), under `DefaultQueryExecutor.executePattern` (`query_executor.go:404`).
- Run 2: `runtime: pointer 0x22300000 to unused region of span` (span base `0x12f30000`) — same fatal, different stack: `bulkBarrierPreWriteSrcOnly` during `growslice`, an append in `storage.(*MemoryStore).scan.func1` (`memory_store.go:153`) inside a btree `AscendRange`, reached from `validatingVBoundIterator.openCRDTScan` (`matcher_relations.go:1077`) via `Next` ← `CountingIterator` ← `LazySeq` realize. Same goroutine number (3932).
- Run 10: `runtime: pointer 0x21970000 to unallocated span` — and this time the crashing goroutine's **stack itself is corrupted**: `runtime: g 4129: unexpected return pc for gcWriteBarrier called from 0x82ed80` (a stack address in a return-PC slot). The dumped stack memory around the frame contains storage key material — the ASCII bytes `entity/type` and hash-like byte runs — and the symbolizer mis-attributes a data word to `BenchmarkElementID.func3`, i.e. raw data is sitting where the runtime expects frame structure.
- Run 13: `runtime: pointer 0x22430000 to unallocated span` — same 0x10000-multiple poison shape, discovered in `runtime.wbBufFlush1` from `runtime.wbMove` inside `executor.(*DefaultQueryExecutor).tryFuseAttributeFetchBundle` (`query_executor.go:681`) under `DefaultQueryExecutor.Execute`, goroutine 4697, go1.26.3. Same late-suite timing (~35s), same full-pipeline context.
- Run 14: `runtime: pointer 0x22430000 to unallocated span` — same poison value as run 13, discovered in `runtime.wbBufFlush1` via `bulkBarrierPreWriteSrcOnly` during `growslice`, an append in `storage.(*MemoryStore).scan.func1` (`memory_store.go:155`) inside a btree iterate — run 2's discovery stack shape. Goroutine 4698, go1.26.3, ~29s into storage, full-pipeline context.
- Run 15: same poison (`0x22430000`), same goroutine number (4698), discovered at `gcWriteBarrier` with a corrupted return PC (`unexpected return pc for gcWriteBarrier called from 0x1856410`) and fmt frames in the dumped stack — run 10's discovery shape. Back-to-back with run 14, mirroring the run 1–2 pattern.
- Run 16: same poison, same goroutine 4698, discovered at `gcWriteBarrier` inside `fmt.(*fmt).padString` under a `deferreturn`. **Three consecutive crashes on one tree — a first** (previous maximum was two, runs 1–2); this binary's layout appears to sit in a near-deterministic crash window, consistent with the extreme layout sensitivity the GOGC=1 triage established. Runs 14–16 share one binary (only doc edits between them); run 13's binary was different yet produced the same `0x22430000` poison — the first observation of one poison value recurring across distinct binaries.

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
4. **Not repo `unsafe`**: an exhaustive audit found (as of 2026-07-19; `datalog/storage/hashtriemap.go` later added the stdlib HashTrieMap port's two struct-prefix node casts) exactly one `unsafe` site in the tree (`executor/tuple_key.go` — pointer-address hash, converted to `uint64` in a single expression, stored only in non-pointer slots, and linked equally into the never-crashing executor test binary); dependency `unsafe` is either build-tag-excluded under js/wasm (go-spew, isatty, colorable) or a cold read-only alias (`ll`'s `[]byte`→`string`); no `go:linkname`/`nosplit`/`noescape` anywhere in the graph; interning is plain `sync.Map` with strong references (no stdlib `unique`/`weak`).
5. **Not test-bisectable — extreme layout sensitivity**: the detonating test passes in isolation and with a suffix window; selecting **all 767 preceding tests** via a `-run` regex (preserving execution order) does not crash, while the identical unfiltered run crashes 4/4. The `-run` filter's own allocations perturb the heap enough to mask the bug, so no test-subset instrument can minimize it.
6. **The "unexpected return pc for gcWriteBarrier called from X" lines show a different X every crash** (a stack address, `0x300`, a heap-like address) while the primary poison stays constant — consistent with the traceback machinery simply failing to unwind through the wasm write-barrier fast path once the throw begins, i.e. secondary noise, not independent corruption.

**Conclusion**: every discriminator points at the Go js/wasm write-barrier code path (compiler barrier codegen or `asm_wasm.s`'s `gcWriteBarrier`), reproducing across Go 1.25.12 and 1.26.3 and across darwin/arm64 and linux/amd64 hosts. No repo-side defect was found by any instrument, and no repo-side fix target exists. Not yet proven upstream: there is no minimal reproducer (layout sensitivity blocks minimization), but reproduction is turnkey on this public repo with the one-line command above.

**Crashes at tip** (2026-07-19): `go1.28-devel_9f236fbe` reproduces identically — same fatal, same detonating goroutine, a new per-binary poison constant (`0x22160000`), confirming the per-binary-constant observation across a third toolchain. The matrix is now Go 1.25.12, 1.26.3, and tip, on darwin/arm64 and linux/amd64 hosts: unfixed upstream.

**Filed upstream**: https://github.com/golang/go/issues/80472 (2026-07-19) — traces, the per-binary-constant poison observation, the crashes-at-tip result, and the turnkey reproduction pinned to `ce6baf7`.

## Upstream diagnosis and fix verification (2026-07-21)

Upstream diagnosed the mechanism (Zxilly on golang/go#80472): `runtime.itabInit`
fills an itab's `Fun` array by storing code pointers through an
`unsafe.Pointer` slice, for which the compiler emits a write barrier. On wasm a
code PC is `funcIndex<<16` — small enough to land inside a live heap span, so
the GC treats the recorded value as a bad heap pointer. Fix: CL 803460
(`runtime: don't emit write barrier for code pointers in itabInit`) stores
through a `uintptr` slice and marks `itabInit` `//go:nowritebarrier`.

Local corroboration and verification, from the reproducer side:

1. **The poison is the entry PC of the same interface method in every crash.**
   Symbolizing each crashing binary's poison against its own wasm name section
   (Go `PC_F` = wasm function index + `funcValueOffset(0x1000)` − 22 imports;
   calibration cross-checked against three symbolized frames per binary):
   `0x22300000` (2026-07-19 binary, also directly visible as frame pc
   `0x22300046` in run 1), `0x22430000` (runs 13–16's binary), and `0x22ac0000`
   (tip binary, 2026-07-21) all resolve to
   `executor.(*ScanSharingMatcher).Match`. The "per-binary constant" was never
   arbitrary — it is this method's entry PC in each layout. `*ScanSharingMatcher`
   is first converted to its matcher interface mid-suite, when the heap has
   grown enough to contain the PC value — explaining both the late-suite timing
   and the extreme layout sensitivity (test selection shifts when that itab
   first initializes and how large the heap is at that moment). This answers
   the upstream request to identify the write barrier's caller: `itabInit`.
2. **CL 803460 (PS2) fixes the turnkey reproducer.** Same tree, same host
   (darwin/arm64, node runner), same command, back to back: gotip `9f236fbe`
   unpatched **crashes** (poison `0x22ac0000`); gotip + CL 803460 cherry-picked
   (`a583f0a9`) **passes 3/3** `GOGC=1` runs. The patched toolchain remains at
   `~/sdk/gotip` (detached HEAD `a583f0a9`); `gotip download` restores plain tip.

## Remaining next steps

1. **CL 803460 merged into Go master (2026-07-21)** — patch set 2, revision
   `5141d4e`, the exact revision verified 3/3 against this repo's `GOGC=1`
   reproducer. The crash class ends at the first Go release (or toolchain
   update) carrying the merge.
2. Until the gate's toolchain carries it: the wasm job flakes at a
   layout-determined rate — some binaries sit in a near-deterministic crash
   window (runs 14–16, 17–18) and stay red until the next code change shifts
   the layout. Reruns are sanctioned for this signature only, with this doc
   as the reference.
