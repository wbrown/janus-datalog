# Panic: hash of unhashable type []uint8 in chooseBestMultiPositionStrategy

## Trigger

A query with collection inputs and variable attributes where bound values include `[]byte` (TypeBytes) datom values:

```clojure
[:find ?related :in $ ?self [?fwd ...] [?rev ...]
 :where
 (or (and [?self ?fwd ?target]
          (or [?related ?fwd ?target]
              [(identity ?target) ?related]))
     [?related ?rev ?self])]
```

## Error

```
panic: runtime error: hash of unhashable type []uint8
  at matcher_strategy.go:273
```

## Root cause

`chooseBestMultiPositionStrategy` counts distinct values per bound position using `map[interface{}]bool`. When the binding relation contains `[]byte` values (from datoms with `TypeBytes` values), Go panics because slices are not comparable and cannot be used as map keys.

The `[]byte` values originate from `ValueFromBytes` (`value_encoding.go:149-150`):
```go
case TypeBytes:
    return data, nil  // returns []byte — unhashable
```

Any datom with a `TypeBytes` value that participates in a multi-position binding will trigger this panic.

## Fix

In `chooseBestMultiPositionStrategy` (`matcher_strategy.go:273`), convert unhashable types to hashable equivalents before using as map keys:

```go
val := tuple[pi.symIdx]
// []byte is not hashable — convert to string for map key
if b, ok := val.([]byte); ok {
    val = string(b)
}
sets[i][val] = true
```

This affects only the cardinality counting for index strategy selection — the actual values in tuples remain unchanged.

## Key files

| File | Lines | Role |
|------|-------|------|
| `storage/matcher_strategy.go` | 273 | Panic site: `sets[i][tuple[pi.symIdx]]` |
| `datalog/value_encoding.go` | 149-150 | Source: `TypeBytes` returns `[]byte` |
