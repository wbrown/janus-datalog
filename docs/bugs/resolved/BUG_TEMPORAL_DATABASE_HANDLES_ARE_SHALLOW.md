# BUG: `AsOf()` / `History()` Return Shallow `Database` Handles With Full-API Footguns

**Date**: 2026-04-16 **Severity**: High - panic, accidental close of shared store, likely semantic drift on custom executor paths **Status**: Resolved (2026-05-25) — see Resolution below **Affected**: `storage.Database.AsOf()`, `storage.Database.History()`, `storage.Database.NewExecutorWithOptions()`

## Summary

`AsOf()` and `History()` return `*Database` values that look like normal database handles, but they are only shallow clones of the original `Database`.

This creates three distinct problems:

1. **Write footgun**: `NewTransaction()` on a temporal handle uses an uninitialized `activeTx` map and will panic.
2. **Close footgun**: `Close()` on a temporal handle closes the shared underlying store, invalidating the parent database handle.
3. **Custom executor footgun**: `NewExecutorWithOptions()` constructs a fresh matcher directly instead of using `d.Matcher()`, so temporal/schema/cache/ annotation state is likely bypassed on derived handles.

The returned type suggests "full database handle with a different read view." The implementation behaves more like "partial read-only view sharing some fields."

## Discovery

Found during external code review of temporal query support.

The review started from the public API claim that:

- `d.AsOf(...)` returns a new `*DB` handle
- `d.History()` returns a new `*DB` handle

and then checked whether those derived handles were actually safe to use as normal `Database` values.

## Code Evidence

### 1. `AsOf()` / `History()` return partial `Database` structs

From `datalog/storage/database.go`:

```go
func (d *Database) AsOf(txID datalog.ElementID) *Database {
    return &Database{
        store:             d.store,
        schema:            d.schema,
        annotationHandler: d.annotationHandler,
        planCache:         d.planCache,
        cache:             d.cache,
        clock:             d.clock,
        replicaID:         d.replicaID,
        temporalTxID:      &txID,
    }
}

func (d *Database) History() *Database {
    empty := datalog.ElementID{}
    return &Database{
        store:             d.store,
        schema:            d.schema,
        annotationHandler: d.annotationHandler,
        planCache:         d.planCache,
        cache:             d.cache,
        clock:             d.clock,
        replicaID:         d.replicaID,
        temporalTxID:      &empty,
    }
}
```

Notably absent:

- `activeTx`
- `parseCache`
- `plannerOptions`
- `txCounter`

and any indication that the returned handle is restricted to read-only use.

### 2. `NewTransaction()` assumes `activeTx` is initialized

From `datalog/storage/database.go`:

```go
func (d *Database) NewTransaction() *Transaction {
    d.mu.Lock()
    defer d.mu.Unlock()

    tx := &Transaction{
        db:                d,
        datoms:            make([]datalog.Datom, 0),
        retracts:          make([]datalog.Datom, 0),
        lastVectorElement: make(map[entityAttrKey]datalog.ElementID),
    }

    d.activeTx[tx] = true
    return tx
}
```

On a derived temporal handle, `d.activeTx` is nil. Writing into it will panic.

### 3. `Close()` closes the shared store

From `datalog/storage/database.go`:

```go
func (d *Database) Close() error {
    // ...
    return d.store.Close()
}
```

The derived temporal handle shares `d.store` with its parent. Closing the child handle closes the real database.

### 4. `NewExecutor()` and `NewExecutorWithOptions()` diverge

`NewExecutor()` uses `d.Matcher()`, which applies temporal mode and current database state:

```go
func (d *Database) NewExecutor() *executor.Executor {
    // ...
    return executor.NewExecutorWithOptions(d.Matcher(), d, opts)
}
```

But `NewExecutorWithOptions()` constructs a fresh matcher directly:

```go
func (d *Database) NewExecutorWithOptions(opts planner.PlannerOptions) *executor.Executor {
    // ...
    matcher := NewBadgerMatcherWithOptions(d.store, execOpts)
    return executor.NewExecutorWithOptions(matcher, d, opts)
}
```

That fresh matcher does **not** go through `d.Matcher()`, so it does not inherit:

- `temporalTxID`
- schema via `SetSchema`
- cache via `SetCache`
- annotation handler via `SetHandler`

On a derived temporal handle, that likely means the custom executor path ignores the temporal view and executes against current state instead.

## Failure Mode 1: Panic on Write Through Temporal Handle

### Sketch

```go
d2 := d.AsOf(txID)
tx := d2.NewTransaction() // writes to nil d2.activeTx map
```

Expected user intuition:

- "`AsOf()` returns another database handle; I can use normal methods on it."

Actual behavior:

- the write path is not initialized
- `NewTransaction()` is unsafe

## Failure Mode 2: Closing Derived Handle Closes Parent Database

### Sketch

```go
hist := d.History()
_ = hist.Close()  // closes shared underlying store

// Parent handle now likely broken
_, err := d.Query(`[:find ?e :where [?e ?a ?v]]`)
```

Expected user intuition:

- closing the derived read view should release only that view

Actual behavior:

- it closes the actual shared store

## Failure Mode 3: Custom Executor Path Likely Bypasses Temporal Semantics

### Sketch

```go
asOf := d.AsOf(txID)
exec := asOf.NewExecutorWithOptions(opts)
rel, err := exec.Execute(query)
```

Expected:

- query executes against the as-of snapshot

Suspected actual behavior:

- `NewExecutorWithOptions()` builds a fresh current-state matcher
- temporal mode from `asOf.temporalTxID` is bypassed

This needs a dedicated reproducer, but the code path strongly suggests it.

## Impact

### 1. API contract ambiguity

The public API returns a normal `*Database` handle, but the derived handle is not actually safe to use like one.

### 2. Easy-to-trigger panic

Any caller who treats `AsOf()` / `History()` as ordinary handles and calls `NewTransaction()` will hit a runtime panic.

### 3. Resource lifecycle surprises

Closing a child/derived handle can invalidate the original parent handle.

### 4. Potential correctness drift on custom execution paths

If `NewExecutorWithOptions()` bypasses temporal mode, a derived handle can return current-state results on the custom-executor path while `Query()` / `NewExecutor()` still return as-of/history results.

That is a very subtle class of bug because different read APIs on the same handle can disagree.

## Why This Is Subtle

The design is understandable: a derived temporal handle wants to share storage, cache, and plan cache with its parent.

The problem is not sharing. The problem is that the returned type still exposes the full `Database` API while only part of the struct is initialized for safe use.

This turns a "derived read view" into a footgun.

## Possible Fix Directions

### Option 1: Make temporal handles explicitly read-only wrappers

Return a dedicated read-only type for `AsOf()` / `History()` instead of another full `*Database`.

This is the clearest semantic model.

### Option 2: Fully initialize derived `Database` values

If the return type must remain `*Database`, copy/initialize all required fields:

- `activeTx`
- `parseCache`
- `plannerOptions`
- other lifecycle-related state

and ensure `Close()` on a derived handle is either safe or clearly prohibited.

### Option 3: Route all executor creation through `d.Matcher()`

`NewExecutorWithOptions()` should likely mirror `NewExecutor()` and start from `d.Matcher()` rather than constructing a raw `BadgerMatcher`.

That would at least unify:

- temporal mode
- schema
- cache
- annotation handler

## Test Plan

1. Add `TestAsOfHandle_NewTransactionPanicsOrErrors`.
2. Add `TestHistoryHandle_CloseDoesNotCloseParent` (or document that it does).
3. Add `TestAsOf_NewExecutorWithOptionsUsesTemporalState`.
4. Add `TestHistory_NewExecutorWithOptionsUsesHistoryState`.
5. Verify `Query()`, `NewExecutor()`, and `NewExecutorWithOptions()` agree on the same derived handle.

---

## Resolution (2026-05-25)

**Resolved** via Options 2 and 3 above: the return type stays `*Database` (a dedicated read-only type, Option 1, was not adopted), but all three footguns are closed and covered by `datalog/storage/temporal_handle_test.go` (all passing):

- **FM1 (write panic):** `NewTransaction()` now panics with a clear message — "NewTransaction called on a read-only temporal database handle (AsOf/History); use the parent handle for writes" (`database.go:365-367`) — instead of a nil-map crash. A temporal handle is explicitly write-prohibited. Covered by `TestTemporalHandle_NewTransaction_PanicsWithClearMessage` (AsOf and History).
- **FM2 (Close closes shared store):** `Close()` is a no-op on a temporal handle — "Read-only view; parent owns the store" (`database.go:588-590`) — so closing a derived handle no longer invalidates the parent. Covered by `TestTemporalHandle_Close_DoesNotCloseParent`.
- **FM3 (custom executor bypasses temporal):** `NewExecutorWithOptions()` routes through `matcherWithExecOptions()`, which applies schema, cache, annotation handler, and temporal mode (`matcher.AsOf(*d.temporalTxID)`) — `database.go:540, 550-575`. The custom-executor path now agrees with `Query()` / `NewExecutor()` on the derived view. Covered by `TestTemporalHandle_NewExecutorWithOptions_UsesTemporalMode`.
