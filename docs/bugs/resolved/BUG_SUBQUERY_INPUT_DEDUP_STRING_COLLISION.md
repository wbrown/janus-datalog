# Bug: Subquery Input Dedup Uses String Keys That Collide

## Summary

`getUniqueInputCombinations` deduplicates correlated subquery input tuples using `fmt.Sprintf("%v", value)` joined with `"|"`. This encoding is not injective: distinct input combinations can produce the same string key and collapse into one subquery execution.

The executor already has `TupleKey` / `TupleKeyMap` for this exact problem, and other dedup paths have been migrated away from stringification. This subquery path still uses the old collision-prone approach.

## Trigger

Any correlated subquery whose input symbols can contain values that stringify ambiguously.

Examples:

| Tuple A | Tuple B | String key |
|---------|---------|------------|
| `["a|b", "c"]` | `["a", "b|c"]` | `"a|b|c"` |
| `[int64(5), "x"]` | `["5", "x"]` | `"5|x"` |
| `[true, "x"]` | `["true", "x"]` | `"true|x"` |
| `[float64(3.14), "x"]` | `["3.14", "x"]` | `"3.14|x"` |

If both combinations appear in the outer relation, the subquery executes for only one of them.

## Code Evidence

`getUniqueInputCombinations` builds string keys from formatted values:

```go
keyParts := make([]string, len(inputSymbols))

for i, sym := range inputSymbols {
	if sym.IsSource() {
		values[sym] = sym
		keyParts[i] = sym.String()
	} else {
		idx := indices[i]
		if idx < len(tuple) {
			values[sym] = tuple[idx]
			keyParts[i] = fmt.Sprintf("%v", tuple[idx])
		}
	}
}

key := strings.Join(keyParts, "|")
if !seen[key] {
	seen[key] = true
	combinations = append(combinations, values)
}
```

This has the same collision class documented and tested for older NOT/OR dedup paths. The correct primitive already exists:

```go
key := NewTupleKeyFull(combo)
if !seen.Exists(key) {
	seen.Put(key, struct{}{})
	combos = append(combos, combo)
}
```

## Impact

- Correlated subqueries can skip valid input combinations.
- Aggregates can be missing for some outer rows.
- Results depend on value text representation, not typed value identity.
- The failure is silent and data-dependent.

This is especially dangerous because subquery input extraction happens before sequential/parallel/batched execution selection, so all execution strategies inherit the bad dedup set.

## Expected Behavior

Two input combinations should be considered equal only when they have the same arity and every value compares equal under `datalog.ValuesEqual`.

String rendering should not participate in query semantics.

## Suggested Fix

Use `TupleKeyMap` in `getUniqueInputCombinations`:

1. Build a `Tuple` for the real dedup key in input-symbol order.
2. Include source symbols as typed symbol values or exclude them consistently if they are constant execution context.
3. Use `NewTupleKeyFull(combo)` and `TupleKeyMap.Exists/Put`.
4. Store the existing `map[query.Symbol]interface{}` as the combination payload.

Avoid `fmt.Sprintf` and delimiter joins for semantic keys.

## Tests Needed

- Unit test for `getUniqueInputCombinations` with the adversarial pairs already used in `tuple_key_collision_test.go`.
- End-to-end correlated subquery test where two outer rows have colliding string keys but require distinct subquery executions.
- Tests with `[]byte` inputs to ensure content equality works and pointer identity does not leak into dedup.
- Tests with source symbols in the input list to ensure `$` / `$foo` do not create accidental collisions.

---

## Resolution (2026-05-25)

**Resolved.** `getUniqueInputCombinations` now dedups with `TupleKeyMap` (`NewTupleKeyFull` + `Exists`/`Put`) instead of `fmt.Sprintf("%v")` joined with `"|"`. `TupleKeyMap` resolves hash collisions by comparing values with `datalog.ValuesEqual`, so two combinations are equal only under typed value identity — string rendering no longer participates in query semantics.

Source markers are constant execution context (identical on every tuple) and are excluded from the dedup key, while still being carried through in the returned combination payload. Excluding them also sidesteps `hashValue`'s address-based default for `query.Symbol`, which would otherwise defeat dedup whenever a source marker appears in the input list.

### Code

- `datalog/executor/subquery.go` — `getUniqueInputCombinations` rewritten; the now-unused `strings` import removed.

### Tests

- `datalog/executor/subquery_input_dedup_test.go` — `TestGetUniqueInputCombinations_NoStringKeyCollisions` covers the adversarial pairs (delimiter `a|b`+`c` vs `a`+`b|c`; `int64(5)` vs `"5"`; bool vs string; float vs string; distinct `[]byte`), and `TestGetUniqueInputCombinations_SourceMarkerDoesNotCollide` covers a `$` source marker in the input list. Each collapses to one combination on the old string-key code and stays distinct after the fix.

### Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` all green.
