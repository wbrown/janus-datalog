# Vector values compare by their rendered string, not elementwise

**Status**: Open. Found by reading `datalog/compare.go` and every non-test caller of `CompareValues`; no reproducer written yet. Sibling of the resolved `BUG_VECTOR_VALUES_DEGENERATE_HASHING` — same defect class, the other half of the value domain's contract.

## Summary

`CompareValues` has no case for vectors. Two vectors are ordered by `fmt.Sprintf("%v", …)` of the whole slice, so ordering is lexicographic over the rendered text rather than elementwise over the elements.

```
CompareValues([]int64{10}, []int64{2})
  renders "[10]" and "[2]" → "[1" < "[2" → returns -1
  i.e. [10] sorts before [2]
```

## Mechanism

`typeRank` (`compare.go:20-45`) enumerates the value domain — nil 0, numerics 1, bool 2, `time.Time` 3, string 4, `[]byte` 5, `Keyword` 6, `Symbol` 7, `Identity` 8, `ElementID` 9 — and has **no slice case**. `[]interface{}`, `[]string`, `[]int64` and every other vector representation fall through to `default: return 10`.

`CompareValues` exhausts its typed arms without matching a slice and reaches `compareByRank` (line 182). Both operands rank 10, so the equal-rank arm runs `strings.Compare(stringValue(left), stringValue(right))` (line 56). `stringValue` has no slice case either, so both operands take its `default: fmt.Sprintf("%v", v)` (line 471).

Two silent defaults in sequence, each absorbing a type the domain declares valid.

## This is an incomplete fix, not an oversight

`typeRank` exists **because this exact failure already happened once**. Before it, `CompareValues` fell back to Sprintf for every cross-type pair, and the merge join's advance comparator consequently judged the string `"<L85 text>"` equal to the `Identity` whose L85 rendering it was — two different values, identical rendering, `cmp == 0`. `TestMergeJoinMixedEntityBindingsMatchOnlyIdentities` (`merge_join_comparator_test.go:20-24`) pins that case and names the cause: "which the old Sprintf fallback compared equal."

The rank table fixed the scalar half. It enumerated ten scalar classes and left the domain's one composite type in the unknown bucket, still reaching the same Sprintf fallback for the same reason. `[]interface{}{"a b"}` versus `[]interface{}{"a","b"}` is the identical defect — different values, both render `[a b]`, `cmp == 0` — one type-kind later.

## Failures

| Inputs | Renders | `CompareValues` | Correct |
|---|---|---|---|
| `[]int64{10}`, `[]int64{2}` | `[10]`, `[2]` | -1 | +1 |
| `[]int64{2}`, `[]int64{10}` | `[2]`, `[10]` | +1 | -1 |
| `[]interface{}{"a b"}`, `[]interface{}{"a","b"}` | `[a b]`, `[a b]` | 0 | non-zero — different lengths |

The third row is the more serious one: `CompareValues` reports 0 while `ValuesEqual` reports false for the same pair, so any consumer treating a zero comparison as equality disagrees with the equality function.

## Exposure, by consumer

Every non-test caller of `CompareValues`, read:

| Consumer | Location | Exposed |
|---|---|---|
| `:order-by` | `executor/executor_utils.go:96`, `compareTuplesByOrder` | **yes** |
| `min` / `max` aggregates | `executor/aggregation.go:585, 596` | **yes** |
| `<` `<=` `>` `>=` predicates | `query/predicate.go:113-119` (`Comparison`), `188-194` (`ChainedComparison`) | **yes** |
| `Relation.Sorted()` | `executor/relation.go:710` (materialized), `1294` (streaming) | **yes** — sorts every tuple position lexicographically |
| merge-join advance | `storage/hash_join_matcher.go:779, 819, 827` | **no** — see below |

`=` and `!=` are unaffected: they route through `ValuesEqual`, not the comparator, and `predicate.go:101-106` documents the split deliberately. Equality, hashing, joins and dedup are all correct — `ValuesEqual` compares slices elementwise via reflect (`compare.go:422-437`) across representations, and the hashing half was fixed under `BUG_VECTOR_VALUES_DEGENERATE_HASHING`.

## The merge join is guarded, and the guard is why this matters

`mergeJoinIterator` uses `CompareValues` not to order output but to **advance a join** — skipping binding groups below the probe key (line 819) and identifying the matching group (line 827). A comparator that disagrees with the probe stream's order there does not sort wrongly; it skips past bindings that should have matched and silently drops join tuples.

Vectors cannot reach it. `chooseJoinStrategy` (`hash_join_matcher.go:66-81`) restricts merge join to `position == 0`, and its comment states the requirement exactly: the merge is correct only when three orders agree — the binding sort (`CompareValues` via `Sorted()`), the advance comparator, and the storage scan order of the probe stream — which is provable for Identity keys because probe datoms arrive in hash-byte order, and which "deliberately does not hold for value-position keys, whose on-disk type-tag order differs from `CompareValues`' rank order." Value-position joins take the order-free hash join. `TestChooseJoinStrategySelectsMergeJoinOnlyForEntityPosition` pins the restriction.

So the blast radius is ordering, not join correctness — but only because a separate guard holds. The guard is stated in terms of `CompareValues` being a correct total order for the key type; it protects the value position today by routing around it entirely, not by the comparator being right there.

## Fix shape

Vectors need a rank of their own and an elementwise comparison that recurses through `CompareValues`. Three sub-decisions:

- **Where in the rank order.** Vectors are the domain's only composite; ranking them above every scalar leaves the scalar order untouched.
- **Unequal lengths.** Lexicographic — compare elementwise to the shorter length, then the shorter vector sorts first.
- **Nested vectors.** Recursion handles them once the element comparison is `CompareValues` itself.

A separate design question this exposes: `typeRank`'s `default: 10` and `stringValue`'s `default: %v` absorb *anything* outside the enumerated domain and return a plausible answer. `ValuesEqual` panics in the same situation, naming the type. Had the comparator adopted that convention when `typeRank` was introduced, the vector gap would have failed loudly on first use instead of surviving the fix that was written to eliminate it.

## Relationship to canonical set ordering

Rendering a cardinality-many set as a canonically ordered vector does **not** depend on this fix. That ordering sorts *members*, which are scalars — `Members` is `map[interface{}]interface{}` keyed by `setKey`, whose documentation enumerates only `[]byte` as needing key conversion, so a vector member would panic as an unhashable Go map key. Scalar member sorting is sound under the existing rank table, and set *equality* routes through `ValuesEqual` rather than the comparator.

The two are adjacent, not sequenced.
