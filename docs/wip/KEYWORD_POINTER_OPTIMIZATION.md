# Keyword Pointer Optimization TODO

## Background

With the migration to `*Keyword` (interned pointers), several places in the codebase still use string conversion for comparison/hashing when pointer equality would be more efficient.

## Files to Refactor

### `datalog/storage/batch_iterator.go`

The `valueToString()` function converts values to strings for use as map keys:

```go
bindingValues := make(map[string]executor.Tuple)
// ...
key := valueToString(tuple[it.position])
```

**Optimization**: Change to `map[interface{}]executor.Tuple` and use native Go comparison:
- `*Keyword` → pointer equality (interned keywords are unique pointers)
- `Identity` → struct equality
- Primitives → value equality

This eliminates `fmt.Sprintf("%p", val)` overhead.

### Other potential locations

- `datalog/storage/simple_batch_scanner.go`
- `datalog/storage/hash_join_matcher.go`
- `datalog/storage/key_mask_iterator.go`

## Invariant

All `Keyword` values MUST be `*Keyword` (interned pointers). Non-pointer `datalog.Keyword` in type switches should panic with assertion failure.
