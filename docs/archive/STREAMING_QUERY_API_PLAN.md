# Plan: Streaming `Query()` — Return `executor.Relation`

## Context

`Query()` should never have materialized. The engine streams throughout — hash joins, predicate pushdown, phase execution all operate on `Iterator` chains. Then `Query()` calls `relationToSlice()` which copies everything into `[][]any`. `QueryInto` is worse — double materialization.

`ExecuteQueryRelation()` already exists and returns `executor.Relation`. `Query()` is just `ExecuteQueryWithInputs()` + `relationToSlice()`. Remove the materialization.

## Changes

### 1. `storage/badger_store.go` — Finalizer safety net on `BadgerIterator`

Add `runtime.SetFinalizer` at creation, clear on explicit `Close()`. This covers both `Scan()` (line 155) and `KeyOnlyIterator` (embeds `*BadgerIterator`, `datom_decoder.go:57`).

**`BadgerIterator.Close()`** (line 471) — make idempotent + clear finalizer:
```go
func (i *BadgerIterator) Close() error {
    if i.txn == nil {
        return nil
    }
    runtime.SetFinalizer(i, nil)
    i.it.Close()
    i.txn.Discard()
    i.txn = nil
    return nil
}
```

**`Scan()`** (line 155) — set finalizer after creating `BadgerIterator`:
```go
iter := &BadgerIterator{...}
runtime.SetFinalizer(iter, (*BadgerIterator).Close)
return iter, nil
```

**`NewKeyOnlyIterator()`** (`datom_decoder.go:74`) — same, set finalizer on the embedded `*BadgerIterator`:
```go
bi := &BadgerIterator{...}
runtime.SetFinalizer(bi, (*BadgerIterator).Close)
return &KeyOnlyIterator{BadgerIterator: bi, ...}, nil
```

### 2. `storage/convenience.go` — `Query()` returns `executor.Relation`

```go
func (d *Database) Query(queryInput interface{}, inputs ...interface{}) (executor.Relation, error) {
    return d.ExecuteQueryRelation(queryInput, inputs...)
}
```

`Query()` becomes an alias for `ExecuteQueryRelation()`.

### 3. `storage/database.go` — `QueryInto` and `QueryOneInto` use streaming

Both currently call `ExecuteQueryWithInputs()` (materialize), then map. Change to `ExecuteQueryRelation()` + iterate with `MapTuple()` per tuple.

**`QueryInto` struct path** (line 962):
```go
rel, err := d.ExecuteQueryRelation(q, inputs...)
iter := rel.Iterator()
defer iter.Close()
for iter.Next() {
    elem := reflect.New(m.elemType).Elem()
    mapper.MapTuple(iter.Tuple(), elem)
    newSlice = reflect.Append(newSlice, elem)
}
```

**`QueryOneInto`** (line 1026): open iterator, read one tuple, close. No full materialization.

### 4. `db/interfaces.go` — Update `Querier` interface

```go
type Querier interface {
    Query(q any, inputs ...any) (executor.Relation, error)
    // rest unchanged
}
```

### 5. `db/db_test.go` — Update callers

Tests that did `results, err := d.Query(...)` become:
```go
rel, err := d.Query(...)
require.NoError(t, err)
iter := rel.Iterator()
defer iter.Close()
// iterate or materialize as needed
```

Add streaming-specific tests: iterate without materializing, verify `Close()` releases resources.

### 6. Other callers of `d.Query()`

Only 5 call sites (all in `db/db_test.go`). No `cmd/` or `tests/` callers — they use `ExecuteQueryWithInputs()`.

## What does NOT change

- `ExecuteQueryWithInputs()` — stays as materializing `[][]any` path for ~100 internal test files
- `ExecuteQueryRelation()` — stays as-is
- `executor.Relation`, `executor.Iterator` — no changes, no new types
- `relationToSlice()` — stays, used by `ExecuteQueryWithInputs()`
- Typed accessors (`GetString`, etc.) — signatures unchanged, internally use streaming `QueryOneInto`

## No new files, no new types

## Verification

```bash
go build ./...
go test -count=1 ./datalog/db/
go test -count=1 ./datalog/storage/
go test -count=1 ./...
```
