# BUG: Empty-string (zero-length) value in a query pattern matches as a wildcard

**Date**: 2026-05-25 **Severity**: High — silent wrong results; an equality query returns unrelated rows **Status**: Resolved (2026-05-25) — see Resolution below **Affected**: Value-bound pattern matching where the value encodes to zero content bytes — the empty string `""` and the empty byte slice `[]byte{}`. Reproduced on the A-bound / E-unbound path (AVET); the same mechanism applies to other value-prefix scans (e.g. VAET for V-only-bound patterns).

## Summary

A pattern with an empty-string constant in the value position — `[?e :attr ""]` — matches every datom for that attribute whose value is a string, instead of only the datoms whose value is the empty string. The empty string behaves as a wildcard.

## Reproduction

`datalog/storage/empty_string_value_test.go` — `TestEmptyStringValueIsLiteralNotWildcard`. Three entities share `:person/nickname`: one value is `""`, the others are `"Bob"` and `"Carol"`.

- Control: `[:find ?e :where [?e :person/nickname "Bob"]]` → **1** match (correct).
- Bug: `[:find ?e :where [?e :person/nickname ""]]` → **3** matches (`e-empty`, `e-bob`, `e-carol`).

The control proves non-empty constant filtering works; the empty-string query returns the entire attribute.

## Root cause

The empty string reaches the index scan correctly bound — the bug is in how a zero-length value is turned into the scan range. Each hop was verified:

1. **Parse** — `[?e :attr ""]` parses the `""` node (`edn.NodeString`) to `query.Constant{Value: ""}` (`datalog/parser/parser.go:705-707`). Correct.
2. **Extract** — `BadgerMatcher.extractValue(Constant{""})` returns `""`, not `nil` (`datalog/storage/matcher.go:543-545`). Correct — so `chooseIndex` sees the value as bound, not as an unbound wildcard.
3. **Index selection** — A bound, E unbound, V bound → AVET **prefix-range** scan, using the encoded value as the prefix: `valueBytes := encodeValueForSearch(v, encoder); EncodePrefixRange(AVET, aStorage[:], valueBytes)` (`datalog/storage/matcher.go:631-635`).
4. **Encode** — `encodeValueForSearch("")` returns just the 1-byte type tag with no value bytes: it calls `datalog.EncodeValue("", threshold)`, whose `string` case returns `(TypeString, []byte{}, nil)` for the empty string (`datalog/value_encoding.go:221-224`), and prepends the type tag (`matcher.go:36`). Result: `[TypeString]`.

So the AVET scan range becomes `[attribute][type=string]` with no value component. A prefix of `[attribute][string-type-tag]` matches *every* string-valued datom for that attribute, so the scan returns all of them.

For a non-empty value like `"Bob"` the prefix is `[attribute][string]Bob`, which selects only values beginning with `Bob` — which is why the control returns a single row. The empty string collapses the value portion of the prefix to nothing, degenerating an equality scan into an "any value of this type" scan.

This generalizes to any value whose encoded form is zero-length — notably the empty byte slice `[]byte{}` (`value_encoding.go:228-230` returns `(TypeBytes, []byte{}, nil)`), giving the prefix `[attribute][bytes-type]`.

## Impact

- **Correctness**: any equality query against an empty string (or empty bytes) silently returns unrelated rows. `(not ...)`, `missing?`, and joins built on such a pattern inherit the wrong result set.
- **Silent**: no error is raised; the result looks like a successful query, so callers debug "too many rows" rather than seeing a clear failure.
- Empty string is a legitimate, common attribute value (e.g. a cleared text field), so this is reachable in ordinary use.

## Expected vs actual

- Expected: `[?e :attr ""]` matches only datoms whose value is exactly `""`.
- Actual: it matches every datom for `:attr` whose value is a string.

## Fix direction (for discussion — not yet decided)

The specific defect is narrow: a **zero-length** encoded value makes the AVET range cover the whole `[attribute][type]` span, so it matches every value of that type. The value field in the index key is variable-length and not self-delimiting, so a prefix range cannot isolate a zero-length value from longer values of the same type.

Importantly, the prefix-range scan is a useful capability in its own right — it is what enables range/prefix access over the index — so a fix should correct the zero-length degenerate case **without** removing intended prefix-scan behavior. Forcing exact-match on every value-bound scan would be an overcorrection. This is an architectural decision; the right approach depends on whether and where prefix scanning is meant to be exposed. Directions that target the degenerate case specifically include detecting a zero-length bound value and constraining that scan to exact equality, or making the value component self-delimiting in the relevant indices (a key-format change).

(Whether a non-empty constant like `"Bob"` currently prefix-matches `"Bobby"` was not investigated and is out of scope here — it may be intentional. This report is only about the empty/zero-length value matching as a wildcard.)

## Related

- `datalog/storage/matcher.go` — `encodeValueForSearch`, `chooseIndex`, `extractValue`.
- `datalog/value_encoding.go` — `EncodeValue` (zero-length encoding for empty string / empty bytes).

---

## Resolution (2026-05-25)

**Resolved.** Fixed by validating schemaless attributes the same way cardinality-one is validated, so a value constant means exact equality on the E-unbound path.

### Refined root cause (corrects the analysis above)

The Root cause section above is right that an empty bound value makes the AVET candidate scan degenerate to `[attribute][type]` and return every value of that type — but that over-scan is *by design*. The E-unbound V-bound path is a **candidate + validate** pattern: the V-prefix index scan narrows candidates, and a per-candidate EATV point lookup (`validateCandidate`) is meant to confirm the candidate's LWW-winner value exactly equals the bound value. An over-broad candidate set is expected and is supposed to be filtered back to exact.

The actual defect is that **the validation step is skipped for schemaless attributes.** In `validatingVBoundIterator.Next()` the gate was `card == CardinalityOne`, and `getCardinalityEnum` returns `CardinalityUnknown` for an attribute with no schema — so schemaless candidates were emitted unvalidated (the "Datascript-style emit-all" path). With a non-empty value the candidate scan is usually narrow enough that this is invisible (it degrades to prefix-matching, e.g. `"Bob"` would also match `"Bobby"`); with an empty value the candidate scan matches everything, so the bug is glaring.

So the bug is specific to the **E-unbound, A-bound, V-bound, schemaless** path. The other shapes were already exact and are unaffected:

- Schema-declared cardinality-one runs `validateCandidate`, which already compares the winner's value exactly (the empty string included).
- E-bound shapes (E supplied via `:in`, or bound by a prior join clause) decode the actual `(E, A)` value and compare it directly; they never rely on the candidate scan.

### Decision

A bare value constant in a pattern means **exact equality** (standard Datalog/Datomic semantics). The prefix-range scan is kept as the candidate-narrowing optimization — it is not removed — but what it emits must be exact. A consequence is that non-empty constants are now exact for schemaless attributes too: `[?e :attr "Bob"]` no longer also matches `"Bobby"`. (Earlier this report flagged that prefix behavior as possibly intentional; the decision is exact equality.)

The fix directions floated earlier (a generic exact-value post-filter, or self-delimiting value bytes in the key) turned out to be unnecessary once the candidate+validate design and the schemaless validation-skip were understood — neither a key-format change nor a new filter is needed.

### Fix

`datalog/storage/matcher_relations.go`, `validatingVBoundIterator.Next()`: the validation gate now treats `CardinalityUnknown` the same as `CardinalityOne`:

```go
if card == schema.CardinalityOne || card == schema.CardinalityUnknown {
    if !it.validateCandidate(datom.E, datom.A) {
        continue
    }
}
```

This is consistent with the rest of the engine, which already treats schemaless attributes as cardinality-one (the planner routes them as such; the cache stores them as `OneValue`). It reuses the existing `validateCandidate` (EATV point lookup, exact `ValuesEqual(winner.V, boundV)`) — no new comparison logic, no key-format change. The cost is one EATV point lookup per candidate, which schema-declared cardinality-one already paid; for an empty-value query that is proportional to the over-scan, but correctness over a rare query shape is the right trade.

### Tests (`datalog/storage/empty_string_value_test.go`)

- `TestEmptyStringValueIsLiteralNotWildcard` — the original reproduction (empty string, E-unbound): RED before the fix (3 matches), GREEN after (1).
- `TestNonEmptyValueIsExactNotPrefix` — `"Bob"` does not match `"Bobby"`; locks in exact-equality semantics.
- `TestEmptyStringValueBoundEntity_IsLiteralNotWildcard` — E bound via `:in`, both the non-matching and matching cases.
- `TestEmptyStringValueJoinBoundEntity_IsLiteralNotWildcard` — E bound by a prior join clause.

Full `go test -count=1 ./...` passes; nothing depended on the old schemaless emit-all prefix behavior.
