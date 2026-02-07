# BUG: PullInto Panics on Nil Entity (LookupAttribute)

**Date**: 2026-02-07
**Severity**: Critical — unrecoverable panic, crashes the process
**Status**: FIXED

## Summary

`Database.PullInto()` panics with a nil entity ID. `LookupAttribute` converts
`entity.Bytes()` (nil for nil identity) to `Entity` (`[20]byte`) via a
slice-to-array conversion, which panics when the slice length is 0.

## Reproduction

```go
db, _ := storage.NewDatabase(dir)

tx := db.NewTransaction()
id := datalog.NewIdentity("test")
tx.Add(id, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":entity.type/room"))
tx.Add(id, datalog.NewKeyword(":entity/name"), "Test Room")
tx.Commit()

// This panics:
var result struct {
    Name string `datalog:"entity/name"`
}
db.PullInto(nil, &result)
```

```
panic: runtime error: cannot convert slice with length 0 to array or pointer
to array with length 20

goroutine 1 [running]:
github.com/wbrown/janus-datalog/datalog/storage.(*BadgerMatcher).LookupAttribute(...)
    matcher.go:806
github.com/wbrown/janus-datalog/datalog/executor.(*PullExecutor).lookupAttribute(...)
    pull.go:190
github.com/wbrown/janus-datalog/datalog/executor.(*PullExecutor).processResolvedSpec(...)
    pull.go:399
github.com/wbrown/janus-datalog/datalog/executor.(*PullExecutor).pullResolvedWithVisited(...)
    pull.go:374
github.com/wbrown/janus-datalog/datalog/executor.(*PullExecutor).PullResolved(...)
    pull.go:343
github.com/wbrown/janus-datalog/datalog/storage.(*Database).PullInto(...)
    database.go:2603
```

## Expected Behavior

`PullInto(nil, &result)` should return an error, not panic. A nil entity ID
is a recoverable condition — the caller may have a nil identity from a failed
lookup. Panicking crashes the entire process.

## Actual Behavior

Panic at `matcher.go:806`:

```go
func (m *BadgerMatcher) LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool) {
    eBytes := entity.Bytes()  // returns nil for nil identity
    // ...
    if m.cache != nil && m.txID == 0 {
        eEntity := Entity(eBytes)  // Entity is [20]byte — PANICS on nil slice
```

`Identity.Bytes()` correctly returns nil for nil identity (`identity.go:137-138`),
but `LookupAttribute` does not guard against this before the slice-to-array
conversion.

## Root Cause

`LookupAttribute` (`matcher.go:789`) has no nil check on the entity parameter.
The `Entity(eBytes)` conversion at line 806 assumes `eBytes` is always 20 bytes.
Go panics on `[N]T(slice)` when `len(slice) < N`.

This path is only reachable when the cache is non-nil (`m.cache != nil`), which
is why it may not have been caught before — tests without cache would take a
different code path.

## Discovery Context

Found in a downstream project after upgrading janus-datalog. A test panicked
during entity loading when a lookup function returned nil (entity not found)
and the caller passed the nil identity directly to `PullInto` without checking
the return value first.

While the caller should check for nil, `PullInto` must not panic on nil input.
Every public API function that takes an `Identity` should handle nil gracefully.

## Fix

### 1. Guard in `LookupAttribute`

**File**: `matcher.go`, `LookupAttribute` method (~line 789)

```go
func (m *BadgerMatcher) LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool) {
    if entity == nil {
        return nil, false
    }
    // ... rest unchanged
```

### 2. Guard in `PullInto` / `PullResolved`

**File**: `database.go`, `PullInto` method (~line 2603)

```go
func (d *Database) PullInto(entityID Identity, v interface{}) error {
    if entityID == nil {
        return fmt.Errorf("PullInto: entity ID is nil")
    }
    // ... rest unchanged
```

### 3. Audit other `Identity.Bytes()` → array conversion sites

Search for `Entity(` conversions in the storage package that don't guard
against nil. The same pattern likely exists in other methods.

```bash
grep -n 'Entity(' datalog/storage/matcher.go | grep -v '//'
```

## Resolution

All three fixes applied:

1. **`database.go:PullInto`** — nil check at API boundary, returns
   `fmt.Errorf("PullInto: entity ID is nil")`
2. **`matcher.go:LookupAttribute`** — nil check, returns `nil, false`
3. **`matcher.go:LookupAllAttributes`** — nil check, returns `nil`
   (found via audit; same `Entity(eBytes)` conversion at line 947)

Regression test: `TestPullInto_NilEntity_NoPanic` in `pullinto_crdt_test.go`.
