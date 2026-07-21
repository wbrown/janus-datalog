# BUG: Identity.L85() Mutates Interned Identity Without Synchronization

**Date**: 2026-05-25 **Severity**: Concurrency / Correctness (Medium) **Status**: Open **Affected**: Concurrent calls to `Identity.L85()` or `Identity.String()` on identities created by `NewIdentity`

## Summary

`Identity` values are globally interned and shared across goroutines, but `Identity.L85()` lazily writes cached fields without synchronization. Two goroutines can call `L85()` or `String()` on the same identity at the same time and race on `l85` and `l85Computed`.

This is a data race in the Go memory model. The encoded value is deterministic, so the symptom is unlikely to be an incorrect string in normal execution, but race-detector failures and undefined concurrent behavior are enough to treat it as a real bug.

## Code Evidence

`Identity` stores mutable cached encoding fields:

```go
// identity.go
type identity struct {
    value       [20]byte
    l85         string
    str         string
    l85Computed bool
}
```

The constructor interns identities by hash, so callers share the same pointer:

```go
// identity.go
func NewIdentity(s string) Identity {
    hash := sha1.Sum([]byte(s))

    if val, ok := identityIntern.cache.Load(hash); ok {
        return val.(Identity)
    }

    id := &identity{
        value: hash,
        str:   s,
    }
    actual, _ := identityIntern.cache.LoadOrStore(hash, id)
    return actual.(Identity)
}
```

`L85()` mutates the shared interned object without a lock, atomic, or `sync.Once`:

```go
// identity.go
func (i Identity) L85() string {
    if i == nil {
        return ""
    }
    if !i.l85Computed {
        i.l85 = codec.EncodeL85(i.value[:])
        i.l85Computed = true
    }
    return i.l85
}
```

`String()` calls `L85()` for identities decoded from storage, and can therefore participate in the same race:

```go
// identity.go
func (i Identity) String() string {
    if i == nil {
        return ""
    }
    if i.str != "" {
        return i.str
    }
    return i.L85()
}
```

The storage decode path often creates identities with an eagerly populated L85 string:

```go
// intern.go
id := &identity{
    value:       hash,
    l85:         codec.EncodeL85(hash[:]),
    str:         "",
    l85Computed: true,
}
```

But identities created through `NewIdentity` leave `l85Computed` false until first use. Those are the affected objects.

## Failure Mode

The race is straightforward:

```text
goroutine A: sees l85Computed == false
goroutine B: sees l85Computed == false
goroutine A: writes i.l85
goroutine B: writes i.l85
goroutine A: writes i.l85Computed
goroutine B: writes i.l85Computed
```

The writes compute the same bytes, but unsynchronized read/write access to the same memory is still a Go data race.

## Reproduction Sketch

Run under the race detector:

```go
func TestIdentityL85ConcurrentAccessRace(t *testing.T) {
    id := datalog.NewIdentity("race-target")

    var wg sync.WaitGroup
    for g := 0; g < 16; g++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for i := 0; i < 1000; i++ {
                _ = id.L85()
                _ = id.String()
            }
        }()
    }
    wg.Wait()
}
```

Expected:

```bash
go test -race ./datalog -run TestIdentityL85ConcurrentAccessRace
```

The race detector should report concurrent reads/writes to the `identity`'s `l85` or `l85Computed` fields.

## Impact

- Any application that logs, formats, serializes, or compares identities from multiple goroutines can trigger a race.
- Query execution uses parallel subqueries by default, so concurrent formatting or annotation paths may encounter shared interned identities.
- Race detector failures make the package harder to validate in concurrent applications, even if normal results appear stable.

## Fix Direction

Prefer making `identity` immutable after construction.

Options:

1. Compute `l85` eagerly in `NewIdentity`, matching `InternIdentityFromHash`. Then remove `l85Computed` entirely or leave it always true.
2. Remove the cached fields and compute `codec.EncodeL85(i.value[:])` each time. This is simpler but may regress hot formatting paths.
3. Add `sync.Once` or atomic synchronization to the identity struct.

Option 1 is the cleanest fit for interned identities: all constructors return a fully initialized immutable pointer. The memory cost is one cached string per identity, which the current type already allows for once `L85()` is called.

## Verification Plan

Add a race-focused regression test in `datalog/identity_test.go` or a dedicated concurrency test file:

- `TestIdentityL85ConcurrentAccess`

Then run:

```bash
go test -race ./datalog -run TestIdentityL85ConcurrentAccess
go test -count=1 ./...
```

The race test should be skipped only if the project explicitly decides not to include race-detector-only tests in the normal suite. It should still be kept as documentation of the concurrency contract.

---

## Resolution (2026-05-25)

**Resolved** by removing the L85 cache entirely (compute on demand) and keying the per-datom dedup/index maps on the interned pointers instead of L85 strings. We got here by asking "do we actually need to store L85?" — and the answer was no.

### The cache existed to prop up string-keyed maps

The on-disk BadgerDB keys never use `identity.L85()` — the key encoders encode the hash bytes directly. The cached string's real consumers were a few *per-datom* `map[string]` keys: the in-memory matcher's entity/EA indices (`E.L85()`, `E.L85()+"|"+A.String()`), the Badger dedup `seen` set, and one identity comparison. Identities and keywords are interned (pointer equality ⟺ value equality), so those maps can key on the pointers directly — no encode, no string. Once they do, `L85()` has only cool callers (`String()` fallback and `export`), so the cache is pure liability.

### Changes

- `datalog/identity.go`, `datalog/intern.go`: dropped the `l85`/`l85Computed` fields. `identity` is now `{value [20]byte, str string}` — immutable. `L85()` computes on demand (`codec.EncodeL85(i.value[:])`). No cached field → no race and no permanent ~25-byte L85 string per interned identity. (Removed the now-unused `codec` import from `intern.go`.)
- `datalog/executor/indexed_memory_matcher.go`: `entityIndex` → `map[datalog.Identity][]int`; `eavIndex` → `map[eaIndexKey][]int` with `eaIndexKey{e datalog.Identity, a datalog.Keyword}` — a pair of interned pointers.
- `datalog/storage/matcher.go`: the entity-dedup `seen` set → `map[datalog.Identity]bool`.
- `datalog/executor/subquery.go`: `id1.L85() != id2.L85()` → `!id1.Equal(id2)`.

### Why not just make the cache safe (eager / sync.Once)

Eager compute pays a Base85 encode per unique identity and keeps a ~25-byte string resident for every interned identity forever — including the many used only for equality/joins. `sync.Once` keeps lazy compute but adds ~16B and a synchronized check per call. Both retain a cache that, once the map keys stop using it, serves almost nothing. Removing it is simpler and strictly cheaper.

### Benchmark (`index_key_bench_test.go`)

(E,A) index build + lookup, 1000 entities × 8 attrs. The string key uses precomputed L85 to model the old cache fairly (L85 was cached, so the per-datom cost was the concat + long hash, not the encode):

| key | ns/op | B/op | allocs/op |
|-----|-------|------|-----------|
| string (old) | 644,085 | 1,619,084 | 24,033 |
| interned pointer (new) | 240,159 | 851,076 | 8,033 |

~2.7× faster, ~47% less memory, 1/3 the allocations — before even counting the removed race and the per-identity string.

### Test

`datalog/identity_concurrency_test.go` — `TestIdentityL85ConcurrentAccess` hammers `L85()`/`String()` from 16 goroutines. Under `-race` it reports a data race on the lazy write before the fix and is clean after; it passes under the standard gate too (kept as the concurrency-contract regression).

### Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` all green; `go test -race ./datalog -run TestIdentityL85ConcurrentAccess` clean.
