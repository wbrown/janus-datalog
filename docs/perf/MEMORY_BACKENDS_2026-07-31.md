# The two in-process backends, measured

`MemoryStore` holds the binary index keys a disk store would and pays
`BinaryKeyEncoder` on every assert and scan. `MemoryTreeStore` holds eight
sorted trees of typed datoms and encodes only at the JDZL and EDN boundaries.
This is the measurement of the difference that motivates
[`MEMORY_DATOM_INDEXES.md`](../proposals/MEMORY_DATOM_INDEXES.md).

Both satisfy `Store` and both build under `GOOS=js`, so the store-level
benchmarks in `memory_backend_bench_test.go` run every entry of
`inProcessStores()`. One run answers two axes: backend against backend, and
native against wasm for each.

Machine: Apple M5, go1.26.3. Native `darwin/arm64`; wasm `js/wasm` via the Node
runner. Default `benchtime`, `count=1`, `-benchmem`.

## Native (darwin/arm64)

| Benchmark | `memory` ns/op | `memory-trees` ns/op | | `memory` allocs | `memory-trees` allocs |
|---|---:|---:|---|---:|---:|
| KeyOnlyScanning/1000 | 81,309 | 6,215 | **13.1×** | 1,761 | **3** |
| KeyOnlyScanning/10000 | 900,042 | 62,201 | **14.5×** | 19,768 | **3** |
| AssertBulk/1000 | 2,251,681 | 886,830 | **2.54×** | 18,726 | 1,721 |
| AssertBulk/4000 | 10,456,826 | 4,075,501 | **2.57×** | 74,721 | 6,417 |
| Retract/1000 | 3,119 | 7,761 | 0.40× | 59 | 140 |
| Retract/10000 | 3,061 | 12,157 | 0.25× | 59 | 140 |

## js/wasm

| Benchmark | `memory` ns/op | `memory-trees` ns/op | | `memory` allocs | `memory-trees` allocs |
|---|---:|---:|---|---:|---:|
| KeyOnlyScanning/1000 | 508,119 | 40,319 | **12.6×** | 1,761 | **3** |
| KeyOnlyScanning/10000 | 5,345,347 | 286,720 | **18.6×** | 19,768 | **3** |
| AssertBulk/1000 | 11,319,722 | 9,635,596 | 1.17× | 18,725 | 1,721 |
| AssertBulk/4000 | 45,017,312 | 42,386,802 | 1.06× | 74,721 | 6,417 |
| Retract/1000 | 21,151 | 57,000 | 0.37× | 59 | 139 |
| Retract/10000 | 19,209 | 101,093 | 0.19× | 59 | 139 |

## Scan

**Three allocations, whatever the size.** 320 B/op at 1,000 datoms and 320 B/op
at 10,000, against 145,769 B and 1,855,272 B. The byte-key backend allocates
about two objects per datom because it decodes each key into a datom; the tree
hands out the datoms it already holds. That constant is the representation
argument in one number, and it is the operation queries spend their time in.

The wasm ratio at 10,000 (18.6×) exceeds the native one (14.5×), which is the
direction to expect when the eliminated work is allocation.

## Bulk assert

Allocations fall 10.9× (1,000) and 11.6× (4,000) on both platforms. Wall time
follows natively — 2.5× — but **barely moves under wasm**, 1.17× and 1.06×. The
same 11× allocation reduction buys 2.5× natively and nothing measurable under
wasm. This is not explained here; nothing in these runs says where wasm's assert
time goes, and a `-cpuprofile` on the wasm side is the next step for anyone who
needs the answer.

On a wasm heap that never returns pages, the allocation column has standing on
its own: 5.0 MB → 0.93 MB at N=1,000 and 21.5 MB → 3.7 MB at N=4,000 is
high-water-mark, not just churn.

## Retract, where the tree loses

`memory-trees` is **2.5× slower natively and up to 5.3× slower under wasm**, and
the gap widens with store size — 7,761 → 12,157 ns from 1,000 to 10,000 datoms,
where the map backend is flat at ~3,100. Allocated bytes grow the same way,
26,096 → 44,783.

This is the cost of persistent structure. The benchmark asserts one datom and
retracts it per iteration, which is the shape path copying is worst at: every
single-datom write copies a path through all eight trees and publishes a
version, where the map backend mutates eight entries in place. The proposal
anticipated the cost and answered it with the transient builder, which is what
the bulk numbers above measure — but a bulk answer does not make the
single-datom number go away, and the growth with size is a scaling property
rather than a constant factor.

## A whole query, all three backends

`BenchmarkComplexQueryBackends` runs the checkpoint query — phase planning,
joins, same-entity bundles, correlated and nested subqueries, conditional
aggregation, `get-else`, `or-default`, expressions, ordering, bounded Top-N —
over 75 scenarios × 100 tasks, through the public `Query` API. Badger is in it
because at query level it is the comparison that matters. n=10, native.

| | sec/op | B/op | allocs/op |
|---|---:|---:|---:|
| `memory` | 20.53m ± 1% | 41.74Mi ± 0% | 199.9k ± 0% |
| `memory-trees` | **17.30m ± 2%** | **27.74Mi ± 0%** | **124.3k ± 0%** |
| `badger` | 18.39m ± 3% | 27.74Mi ± 0% | 124.5k ± 0% |

**The typed store's allocation profile is Badger's** — 27.74Mi against 27.74Mi,
124.3k against 124.5k, a 0.16% difference on a deterministic counter. Whatever
this query allocates, it allocates above the store.

`MemoryStore` is the outlier, +75.4k allocations and +14.0Mi over both. That is
a measurement; the mechanism is not established here, and a profile diff against
either of the other two is what would establish it. It is also the wasm default,
so the difference lands on the build with a 4 GiB ceiling.

Raw: `complex_query_backends_2026-07-31.txt`.

## What this does not measure

`DeleteDatoms`, the other removal path. Retained heap, as opposed to allocated
bytes. Query execution under wasm — the table above is native only.
Datoms whose values are strings or vectors — every benchmark here uses `int64`
values and one attribute, which is the shape most favourable to a fixed-width
key and therefore the conservative direction for the scan result.
