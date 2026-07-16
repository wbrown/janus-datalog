# MemoryStore performance comparisons

## Native MemoryStore vs Badger (same machine)

Same shapes, count=3, Apple M5, go1.26.3, cache disabled where the Badger
benches disable it. Artifacts: `badger_native_darwin_arm64_count3.txt` vs
`memory_native_darwin_arm64_count3.txt`.

| Benchmark | Badger | Memory | Memory / Badger |
|-----------|-------:|-------:|----------------:|
| KeyOnlyScanning/1K | 129 µs | 373 µs | **2.9×** |
| KeyOnlyScanning/10K | 1.44 ms | 4.98 ms | **3.5×** |
| ResolveAllAttributesMany/230/batch | 395 µs | 561 µs | **1.4×** |
| ResolveAllAttributesMany/3899/batch | 7.48 ms | 13.9 ms | **1.9×** |
| StorageHashJoinCompiledMatching | 1.86 ms | 6.13 ms | **3.3×** |

Allocations are the larger story on scan/join: MemoryStore uses ~5–20× more
B/op on key-only scans and ~10× more allocs on the compiled hash-join match
(map-backed iteration + copies vs Badger’s iterator/workspace path). Pull batch
is closer (~1.4–1.9× time, ~2–3× bytes).

MemoryStore is the wasm/portability backend, not a Badger performance peer yet.

---

## Native MemoryStore vs js/wasm

Paired `BenchmarkMemory*` suite on the same `MemoryStore` backend so the
comparison is substrate (Go native vs Go→WASM→Node), not Badger vs memory.

| Artifact | Target |
|----------|--------|
| `memory_native_darwin_arm64_count3.txt` | `darwin/arm64`, Apple M5, go1.26.3 |
| `memory_wasm_js_wasm_count3.txt` | `js/wasm` via Node (`go_js_wasm_exec`) |

Benches live in `datalog/storage/memory_backend_bench_test.go` (portable; no
Badger). Count=3. Cache disabled.

## sec/op (benchstat medians)

| Benchmark | Native | WASM (Node) | WASM / native |
|-----------|-------:|------------:|--------------:|
| KeyOnlyScanning/Size1000 | 373.1 µs | 2.110 ms | **5.7×** |
| KeyOnlyScanning/Size10000 | 4.984 ms | 24.43 ms | **4.9×** |
| ResolveAllAttributesMany/230/batch | 561.2 µs | 3.288 ms | **5.9×** |
| ResolveAllAttributesMany/3899/batch | 13.86 ms | 67.29 ms | **4.9×** |
| StorageHashJoinCompiledMatching | 6.131 ms | 29.93 ms | **4.9×** |
| SimpleQuery | 32.63 ms | 176.4 ms | **5.4×** |
| **geomean** | 3.775 ms | 19.80 ms | **5.2×** |

## Allocations

B/op and allocs/op are essentially identical across targets (same Go code paths;
~0.2% noise). The gap is runtime cost of the WASM/Node substrate, not a
different algorithm or allocation shape.

## Notes

- Prior Badger-only baselines in this directory are **not** wasm-runnable and
  are not part of this pair.
- Cache-disabled per-entity `ResolveAllAttributes` on MemoryStore is
  pathological (minutes/op at 3899) and was excluded from the dual suite.
- Re-run:

```bash
go test -count=3 -bench='BenchmarkMemory' -benchmem ./datalog/storage \
  > docs/perf/wasm_memory_backend_2026-07-15/memory_native_darwin_arm64_count3.txt

PATH="$(go env GOROOT)/lib/wasm:$PATH" \
GOOS=js GOARCH=wasm go test -count=3 -bench='BenchmarkMemory' -benchmem ./datalog/storage \
  > docs/perf/wasm_memory_backend_2026-07-15/memory_wasm_js_wasm_count3.txt
```
